/*
Copyright 2025 The Vitess Authors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
		http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vreplication

import (
	"context"
	"fmt"
	"io"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

const (
	countWorkers          = 6
	maxWorkerEvents       = 500
	maxCountWorkersEvents = countWorkers * maxWorkerEvents
	maxIdleWorkerDuration = 250 * time.Millisecond

	aggregatePosInterval = 250 * time.Millisecond

	noUncommittedSequence = math.MaxInt64
)

type parallelProducer struct {
	vp *vplayer

	workers  []*parallelWorker
	startPos replication.Position

	posReached   atomic.Bool
	workerErrors chan error

	sequenceToWorkersMap      map[int64]int // sequence number => worker index
	lowestUncommittedSequence atomic.Int64
	lowestSequenceListeners   map[int64]chan int64
	sequenceToWorkersMapMu    sync.RWMutex

	commitWorkerEventSequence atomic.Int64
	assignSequence            int64

	countAssignedToCurrentWorker int64

	newDBClient                 func() (*vdbClient, error)
	aggregateWorkersPosQuery    string
	lastAggregatedWorkersPosStr string

	numCommits         atomic.Int64 // temporary. TODO: remove
	currentConcurrency atomic.Int64 // temporary. TODO: remove
	maxConcurrency     atomic.Int64 // temporary. TODO: remove
}

func newParallelProducer(ctx context.Context, dbClientGen dbClientGenerator, vp *vplayer) (*parallelProducer, error) {
	p := &parallelProducer{
		vp:                       vp,
		workers:                  make([]*parallelWorker, countWorkers),
		workerErrors:             make(chan error, countWorkers+1),
		sequenceToWorkersMap:     make(map[int64]int),
		lowestSequenceListeners:  make(map[int64]chan int64),
		aggregateWorkersPosQuery: binlogplayer.ReadVReplicationCombinedWorkersGTIDs(vp.vr.id),
	}
	p.lowestUncommittedSequence.Store(noUncommittedSequence)

	p.newDBClient = func() (*vdbClient, error) {
		dbClient, err := dbClientGen()
		if err != nil {
			return nil, err
		}
		vdbClient := newVDBClient(dbClient, vp.vr.stats, 0)
		_, err = vp.vr.setSQLMode(ctx, vdbClient)
		if err != nil {
			return nil, err
		}
		// vdbClient.maxBatchSize = vp.vr.dbClient.maxBatchSize
		vdbClient.maxBatchSize = 0

		return vdbClient, nil
	}
	for i := range p.workers {
		w := newParallelWorker(i, p, maxWorkerEvents)
		var err error
		if w.dbClient, err = p.newDBClient(); err != nil {
			return nil, err
		}
		w.queryFunc = func(ctx context.Context, sql string) (*sqltypes.Result, error) {
			// REMOVE for batched commit
			// if !w.dbClient.InTransaction { // Should be sent down the wire immediately
			// 	return w.dbClient.Execute(sql)
			// }
			// return nil, w.dbClient.AddQueryToTrxBatch(sql) // Should become part of the trx batch
			return w.dbClient.Execute(sql)
		}
		w.commitFunc = func() error {
			// REMOVE for batched commit
			// return w.dbClient.CommitTrxQueryBatch() // Commit the current trx batch
			return w.dbClient.Commit()
		}
		// INSERT a row into _vt.vreplication_worker_pos with an empty position
		query := binlogplayer.GenerateInitWorkerPos(vp.vr.id, w.index)
		log.Errorf("========== QQQ initWorkersPos query: %v", query)
		if _, err := w.dbClient.ExecuteFetch(query, -1); err != nil {
			return nil, err
		}
		p.workers[i] = w
	}

	return p, nil
}

// commitAll commits all workers and waits for them to complete.
func (p *parallelProducer) commitAll(ctx context.Context, except *parallelWorker) error {
	var eg errgroup.Group
	for _, w := range p.workers {
		if except != nil && w.index == except.index {
			continue
		}
		eg.Go(func() error {
			return <-w.commitEvents(ctx)
		})
	}
	return eg.Wait()
}

// updatePos updates _vt.vreplication with the given position and timestamp.
// This producer updates said position based on the aggregation of all committed workers positions.
func (p *parallelProducer) updatePos(ctx context.Context, pos replication.Position, ts int64, dbClient *vdbClient) (posReached bool, err error) {
	update := binlogplayer.GenerateUpdatePos(p.vp.vr.id, pos, time.Now().Unix(), ts, p.vp.vr.stats.CopyRowCount.Get(), p.vp.vr.workflowConfig.StoreCompressedGTID)
	if _, err := dbClient.ExecuteWithRetry(ctx, update); err != nil {
		return false, fmt.Errorf("error %v updating position", err)
	}
	// p.vp.numAccumulatedHeartbeats = 0
	// p.vp.unsavedEvent = nil
	// p.vp.timeLastSaved = time.Now()
	// p.vp.vr.stats.SetLastPosition(p.vp.pos)
	return posReached, nil
}

// updateTimeThrottled updates the time_throttled field in the _vt.vreplication record
// with a rate limit so that it's only saved in the database at most once per
// throttleUpdatesRateLimiter.tickerTime.
// It also increments the throttled count in the stats to keep track of how many
// times a VReplication workflow, and the specific sub-component, is throttled by the
// tablet throttler over time. It also increments the global throttled count to keep
// track of how many times in total vreplication has been throttled across all workflows
// (both ones that currently exist and ones that no longer do).
func (p *parallelProducer) updateTimeThrottled(appThrottled throttlerapp.Name, reasonThrottled string, dbClient *vdbClient) error {
	appName := appThrottled.String()
	p.vp.vr.stats.ThrottledCounts.Add([]string{"tablet", appName}, 1)
	globalStats.ThrottledCount.Add(1)
	err := p.vp.vr.throttleUpdatesRateLimiter.Do(func() error {
		tm := time.Now().Unix()
		update, err := binlogplayer.GenerateUpdateTimeThrottled(p.vp.vr.id, tm, appName, reasonThrottled)
		if err != nil {
			return err
		}
		if _, err := dbClient.ExecuteFetch(update, maxRows); err != nil {
			return fmt.Errorf("error %v updating time throttled", err)
		}
		return nil
	})
	return err
}

// aggregateWorkersPos aggregates the committed GTID positions of all workers, along with their transaction timestamps.
// If any change since last call is detected, the combined position is written to _vt.vreplication.
func (p *parallelProducer) aggregateWorkersPos(ctx context.Context, dbClient *vdbClient, onlyFirstContiguous bool) (aggregatedWorkersPos replication.Position, combinedPos replication.Position, err error) {
	qr, err := dbClient.ExecuteFetch(p.aggregateWorkersPosQuery, -1)
	if err != nil {
		log.Errorf("Error fetching vreplication worker positions: %v. isclosed? %v", err, dbClient.IsClosed())
		return aggregatedWorkersPos, combinedPos, err
	}
	var aggregatedWorkersPosStr string
	var lastEventTimestamp int64
	for _, row := range qr.Rows { // there's actually exactly one row in this result set
		aggregatedWorkersPosStr = row[0].ToString()
		lastEventTimestamp, err = row[1].ToInt64()
		if err != nil {
			return aggregatedWorkersPos, combinedPos, err
		}
	}
	if aggregatedWorkersPosStr == p.lastAggregatedWorkersPosStr {
		// Nothing changed since last visit. Skip all the parsing and updates.
		return aggregatedWorkersPos, combinedPos, nil
	}
	aggregatedWorkersPos, err = binlogplayer.DecodeMySQL56Position(aggregatedWorkersPosStr)
	if err != nil {
		return aggregatedWorkersPos, combinedPos, err
	}

	if onlyFirstContiguous && aggregatedWorkersPos.GTIDSet != nil {
		// This is a performance optimization which, for the running duration, only
		// considers the first contiguous part of the aggregated GTID set.
		// The idea is that with out-of-order workers, a worker's GTID set
		// is punctured, and that means its representation is very long.
		// Consider something like:
		// 9ee2b9ca-e848-11ef-b80c-091a65af3e28:4697:4699:4702:4704:4706:4708:4710:4713:4717:4719:4723:4726:4728:4730:4732:4734:4737:4739:4742:4744:4746:4748:4751:4753:4756:4758:4760:4762:4765:4768-4769:4772-4774:...
		// Even when combined with multiple workers, it's enough that one worker is behind,
		// that the same amount of punctures exists in the aggregated GTID set.
		// What this leads to is:
		// - Longer and more complex parsing of GTID sets.
		// - More data to be sent over the wire.
		// - More data to be stored in the database. Easily surpassing VARCHAR(10000) limitation.
		// And the observation is that we don't really need all of this data _at this time_.
		// Consider: when do we stop the workflow?
		// - Either startPos is defined (and in all likelihood it is contiguous, as in 9ee2b9ca-e848-11ef-b80c-091a65af3e28:1-100000)
		// - Or sommething is running VReplicationWaitForPos (e.g. Online DDL), and again, it's contiguous.
		// So the conditions for which we wait don't care about the punctures. These are as good as "not applied"
		// when it comes to comparing to the expected terminal position.
		// However, when the workflow is stopped, we do need to know the full GTID set, so that we
		// have accurate information about what was applied and what wasn't (consider Online DDL reverts
		// that need to start at that precise position). And so when the workflow is stopped, we will
		// have onlyFirstContiguous==false.
		mysql56gtid := aggregatedWorkersPos.GTIDSet.(replication.Mysql56GTIDSet)
		for sid := range mysql56gtid {
			mysql56gtid[sid] = mysql56gtid[sid][:1]
		}
	}
	// The aggregatedWorkersPos only looks at the GTI entries actually processed in the binary logs.
	// The combinedPos also takes into account the startPos. combinedPos is what we end up storing
	// in _vt.vreplication, and it's what will be compared to stopPosition or used by VReplicationWaitForPos.
	combinedPos = replication.AppendGTIDSet(aggregatedWorkersPos, p.vp.startPos.GTIDSet)
	p.vp.pos = combinedPos // TODO(shlomi) potential for race condition

	if !onlyFirstContiguous {
		log.Errorf("========== QQQ aggregateWorkersPos aggregatedWorkersPos: %v", aggregatedWorkersPos)
		log.Errorf("========== QQQ aggregateWorkersPos combinedPos: %v", combinedPos)

	}
	// Update _vt.vreplication. This write reflects everything we could read from the workers table,
	// which means that data was committed by the workers, which means this is a de-factor "what's been
	// actually applied"."
	if _, err := p.updatePos(ctx, combinedPos, lastEventTimestamp, dbClient); err != nil {
		log.Errorf("========== QQQ aggregateWorkersPos error: %v", err)
		return aggregatedWorkersPos, combinedPos, err
	}
	p.lastAggregatedWorkersPosStr = aggregatedWorkersPosStr
	return aggregatedWorkersPos, combinedPos, nil
}

// watchPos runs in a goroutine and is in charge of peridocially aggregating workers
// positions and writing the aggregated value to _vt.vreplication, as well as
// sending the aggregated value to the workers themselves.
func (p *parallelProducer) watchPos(ctx context.Context) error {
	dbClient, err := p.newDBClient()
	if err != nil {
		return err
	}
	defer dbClient.Close()

	aggregatePosTicker := time.NewTicker(aggregatePosInterval)
	defer aggregatePosTicker.Stop()
	// noProgressRateLimiter := timer.NewRateLimiter(time.Second)
	// defer noProgressRateLimiter.Stop()
	// var lastCombinedPos replication.Position
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-aggregatePosTicker.C:
			aggregatedWorkersPos, combinedPos, err := p.aggregateWorkersPos(ctx, dbClient, true)
			if err != nil {
				log.Errorf("Error aggregating vreplication worker positions: %v. isclosed? %v", err, dbClient.IsClosed())
				continue
			}
			if aggregatedWorkersPos.IsZero() {
				// This happens when there's been no change since last polling. It's a performance
				// optimization to save the cost of updating the position.
				log.Errorf("========== QQQ watchPos aggregatedWorkersPos is IDLE: %v", p.lastAggregatedWorkersPosStr)
				continue
			}
			// log.Errorf("========== QQQ watchPos aggregatedWorkersPos: %v, combinedPos: %v, stop: %v", aggregatedWorkersPos, combinedPos, p.vp.stopPos)

			// Write back this combined pos to all workers, so that we condense their otherwise sparse GTID sets.
			for _, w := range p.workers {
				go func() { w.aggregatedPosChan <- aggregatedWorkersPos.String() }()
			}
			// log.Errorf("========== QQQ watchPos pushed combined pos")
			// if combinedPos.GTIDSet.Equal(lastCombinedPos.GTIDSet) {
			// 	// no progress has been made
			// 	err := noProgressRateLimiter.Do(func() error {
			// 		log.Errorf("========== QQQ watchPos no progress!! committing all")
			// 		return p.commitAll(ctx, nil)
			// 	})
			// 	if err != nil {
			// 		return err
			// 	}
			// } else {
			// 	// progress has been made
			// 	lastCombinedPos.GTIDSet = combinedPos.GTIDSet
			// }
			if !p.vp.stopPos.IsZero() && combinedPos.AtLeast(p.vp.stopPos) {
				if err := p.commitAll(ctx, nil); err != nil {
					return err
				}
				p.posReached.Store(true)
				return io.EOF
			}
		}
	}
}

func (p *parallelProducer) assignTransactionToWorker(sequenceNumber int64, lastCommitted int64, currentWorkerIndex int, preferCurrentWorker bool) (workerIndex int) {
	p.sequenceToWorkersMapMu.RLock()
	defer p.sequenceToWorkersMapMu.RUnlock()

	if sequenceNumber != 0 {
		if lowest := p.lowestUncommittedSequence.Load(); sequenceNumber < lowest {
			// log.Errorf("========== QQQ process lowest has CHANGED DOWN to %v", sequenceNumber)
			p.lowestUncommittedSequence.Store(sequenceNumber)
		}
	}

	p.assignSequence++
	if workerIndex, ok := p.sequenceToWorkersMap[sequenceNumber]; ok {
		// All events of same sequence should be executed by same worker
		return workerIndex
	}
	if workerIndex, ok := p.sequenceToWorkersMap[lastCommitted]; ok {
		// Transaction depends on another transaction, that is still being owned by some worker.
		// Use that same worker, so that this transaction is non-blocking.
		p.sequenceToWorkersMap[sequenceNumber] = workerIndex
		return workerIndex
	}
	for i := p.lowestUncommittedSequence.Load(); i < lastCommitted; i++ {
		if workerIndex, ok := p.sequenceToWorkersMap[i]; ok {
			// Transaction depends on another transaction, that is still being owned by some worker.
			// Use that same worker, so that this transaction is non-blocking.
			p.sequenceToWorkersMap[sequenceNumber] = workerIndex
			return workerIndex
		}
	}
	// No specific transaction dependency constraints (any parent transactions were long since
	// committed, and no worker longer indicates it owns any such transactions)
	if preferCurrentWorker && p.countAssignedToCurrentWorker < maxWorkerEvents {
		// Prefer the current worker, if it has capacity. On one hand, we want
		// to batch queries as possible. On the other hand, we want to spread the
		// the load across workers.
		workerIndex = currentWorkerIndex
	} else {
		// Even if not specifically requested to assign current worker, we still
		// want to do some batching. We batch `maxWorkerEvents` events per worker.
		workerIndex = int(p.assignSequence/maxWorkerEvents) % len(p.workers)
	}
	p.sequenceToWorkersMap[sequenceNumber] = workerIndex
	return workerIndex
}

func (p *parallelProducer) registerLowestSequenceListener(notifyOnLowestAbove int64) (lowest int64, ch chan int64) {
	p.sequenceToWorkersMapMu.Lock()
	defer p.sequenceToWorkersMapMu.Unlock()

	lowest = p.lowestUncommittedSequence.Load()
	if lowest > notifyOnLowestAbove {
		// lowest uncommitted sequence is already above the requested value, so there is no  need to
		// register a listener, We return an empty channel as indication.
		// log.Errorf("========== QQQ registerLowestSequenceListener free pass for notifyOnLowestAbove=%v because lowest=%v", notifyOnLowestAbove, lowest)
		return lowest, nil
	}
	// log.Errorf("========== QQQ registerLowestSequenceListener worker %v registered for notifyOnLowestAbove=%v assigned to worker %v", worker, notifyOnLowestAbove, p.sequenceToWorkersMap[notifyOnLowestAbove])
	if ch, ok := p.lowestSequenceListeners[notifyOnLowestAbove]; ok {
		// listener already exists
		return lowest, ch
	}
	ch = make(chan int64, countWorkers)
	p.lowestSequenceListeners[notifyOnLowestAbove] = ch
	return lowest, ch
}

func (p *parallelProducer) evaluateLowestUncommittedSequence(completedSequence int64) (lowest int64, changed bool) {
	// assumed to be protected by sequenceToWorkersMapMu lock
	lowest = p.lowestUncommittedSequence.Load()
	if completedSequence != lowest {
		// means p.lowestUncommittedSequence is still undeleted
		return lowest, false
	}
	if len(p.sequenceToWorkersMap) == 0 {
		return noUncommittedSequence, true
	}
	// Find the lowest sequence number that is not yet committed.
	for {
		lowest++
		if _, ok := p.sequenceToWorkersMap[lowest]; ok {
			return lowest, true
		}
	}
}

// completeSequenceNumbers is called by a worker once it committed or applies transactions.
// The function removes the sequence numbers from the map and updates the lowest uncommitted sequence number.
// It also notifies any listeners that may have been waiting for the lowest uncommitted sequence number to
// cross a certain value.
func (p *parallelProducer) completeSequenceNumbers(sequenceNumbers []int64) {
	p.sequenceToWorkersMapMu.Lock()
	defer p.sequenceToWorkersMapMu.Unlock()

	for _, sequenceNumber := range sequenceNumbers {
		delete(p.sequenceToWorkersMap, sequenceNumber)
		if lowest, changed := p.evaluateLowestUncommittedSequence(sequenceNumber); changed {
			// log.Errorf("========== QQQ process lowest has CHANGED to %v", lowest)
			p.lowestUncommittedSequence.Store(lowest)
			for notifyOnLowestAbove, ch := range p.lowestSequenceListeners {
				if lowest > notifyOnLowestAbove {
					// log.Errorf("========== QQQ process notifying listener for %v on new lowest %v", notifyOnLowestAbove, lowest)
					for range countWorkers {
						ch <- lowest // nonblocking. The channel has enough capacity.
					}
					delete(p.lowestSequenceListeners, notifyOnLowestAbove)
				}
			}
		}
	}
}

// process is a goroutine that reads events from the input channel and assigns them to workers.
func (p *parallelProducer) process(ctx context.Context, events chan *binlogdatapb.VEvent) error {
	currentWorker := p.workers[0]
	hasFieldEvent := false
	workerWithFieldEvent := -1
	for {
		if p.posReached.Load() {
			return io.EOF
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event := <-events:
			canApplyInParallel := false
			switch event.Type {
			case binlogdatapb.VEventType_BEGIN,
				binlogdatapb.VEventType_ROW,
				binlogdatapb.VEventType_COMMIT,
				binlogdatapb.VEventType_GTID:
				// We can parallelize these events.
				canApplyInParallel = true
			case binlogdatapb.VEventType_FIELD:
				canApplyInParallel = true
				hasFieldEvent = true
				workerWithFieldEvent = currentWorker.index
			case binlogdatapb.VEventType_PREVIOUS_GTIDS:
				// This `case` is not required, but let's make this very explicit:
				// The transaction dependency graph is scoped to per-binary log.
				// When rotating into a new binary log, we must wait until all
				// existing workers have completed, as there is no information
				// about dependencies cross binlogs.
				canApplyInParallel = false
			}
			if !canApplyInParallel {
				// As an example, thus could be a DDL.
				// Wait for all existing workers to complete, including the one we are about to assign to.
				if err := p.commitAll(ctx, nil); err != nil {
					return err
				}
			}
			workerIndex := p.assignTransactionToWorker(event.SequenceNumber, event.CommitParent, currentWorker.index, event.PinWorker)
			if workerIndex == currentWorker.index {
				// Measure how many events we have assigned to the current worker. We will
				// cap at some value so as to distribute events to other workers and to avoid
				// one worker taking all the load.
				// See logic in assignTransactionToWorker()
				p.countAssignedToCurrentWorker++
			} else {
				currentWorker.events <- p.generateConsiderCommitWorkerEvent()
				p.countAssignedToCurrentWorker = 1
				if hasFieldEvent {
					log.Errorf("========== QQQ process FIELD issue: %v event seq=%v assigned to worker %v even though there was field event in worker %v",
						event.Type, event.SequenceNumber, workerIndex, workerWithFieldEvent)
				}
			}

			if hasFieldEvent && event.Type == binlogdatapb.VEventType_COMMIT {
				event.Skippable = false
			}
			currentWorker = p.workers[workerIndex]
			select {
			case <-ctx.Done():
				return ctx.Err()
			case currentWorker.events <- event:
			}
			if hasFieldEvent && event.Type == binlogdatapb.VEventType_COMMIT {
				// We wait until the field event is applied.
				if err := <-currentWorker.commitEvents(ctx); err != nil {
					return err
				}
				hasFieldEvent = false
			}
			if !canApplyInParallel {
				// Say this was a DDL. Then we need to wait until it is absolutely complete, before we allow the next event to be processed.
				if err := <-currentWorker.commitEvents(ctx); err != nil {
					return err
				}
			}
		}
	}
}

// applyEvents is a parallel variation on VPlayer's applyEvents() function. It spawns the necessary
// goroutines, starts the workers, processes the vents, and looks for errors.
func (p *parallelProducer) applyEvents(ctx context.Context, relay *relayLog) error {
	defer log.Errorf("========== QQQ applyEvents defer")

	dbClient, err := p.newDBClient()
	if err != nil {
		return err
	}
	defer dbClient.Close()

	go func() {
		if err := p.watchPos(ctx); err != nil {
			p.workerErrors <- err
		}
	}()

	var wg sync.WaitGroup
	defer wg.Wait()
	workersCtx, workersCancel := context.WithCancel(ctx)
	defer workersCancel()

	for _, w := range p.workers {
		wg.Add(1)
		w := w
		go func() {
			defer wg.Done()
			p.workerErrors <- w.applyQueuedEvents(workersCtx)
		}()
	}

	estimateLag := func() {
		behind := time.Now().UnixNano() - p.vp.lastTimestampNs - p.vp.timeOffsetNs
		p.vp.vr.stats.ReplicationLagSeconds.Store(behind / 1e9)
		p.vp.vr.stats.VReplicationLags.Add(strconv.Itoa(int(p.vp.vr.id)), time.Duration(behind/1e9)*time.Second)
	}

	eventQueue := make(chan *binlogdatapb.VEvent, 2*maxCountWorkersEvents)
	go p.process(ctx, eventQueue)

	// If we're not running, set ReplicationLagSeconds to be very high.
	// TODO(sougou): if we also stored the time of the last event, we
	// can estimate this value more accurately.
	defer p.vp.vr.stats.ReplicationLagSeconds.Store(math.MaxInt64)
	defer p.vp.vr.stats.VReplicationLags.Add(strconv.Itoa(int(p.vp.vr.id)), math.MaxInt64)
	var lagSecs int64
	for {
		if p.posReached.Load() {
			return io.EOF
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Check throttler.
		if checkResult, ok := p.vp.vr.vre.throttlerClient.ThrottleCheckOKOrWaitAppName(ctx, throttlerapp.Name(p.vp.throttlerAppName)); !ok {
			go func() {
				_ = p.updateTimeThrottled(throttlerapp.VPlayerName, checkResult.Summary(), dbClient)
				estimateLag()
			}()
			continue
		}

		items, err := relay.Fetch()
		if err != nil {
			return err
		}
		var pinWorker bool
		countPinnedWorkerEvents := 0
		lagSecs = -1
		for i, events := range items {
			for j, event := range events {
				if !p.vp.stopPos.IsZero() {
					// event.GTID is a GTIDSet of combined parsed events
					_, streamedGTID, err := replication.DecodePositionMySQL56(event.Gtid)
					if err != nil {
						return err
					}
					if !p.vp.stopPos.GTIDSet.Contains(streamedGTID) {
						// This event goes beyond the stop position. We skip it.
						continue
					}
				}
				if event.EventGtid != "" && !p.startPos.IsZero() {
					// eventGTID is a singular GTID entry
					eventGTID, err := replication.ParseMysql56GTID(event.EventGtid)
					if err != nil {
						return err
					}
					if p.startPos.GTIDSet.ContainsGTID(eventGTID) {
						// This event was already processed.
						log.Errorf("========== QQQ applyEvents skipping GTID entry %v", eventGTID)
						continue
					}
				}

				if event.Timestamp != 0 {
					// If the event is a heartbeat sent while throttled then do not update
					// the lag based on it.
					// If the batch consists only of throttled heartbeat events then we cannot
					// determine the actual lag, as the vstreamer is fully throttled, and we
					// will estimate it after processing the batch.
					if !(event.Type == binlogdatapb.VEventType_HEARTBEAT && event.Throttled) {
						p.vp.lastTimestampNs = event.Timestamp * 1e9
						p.vp.timeOffsetNs = time.Now().UnixNano() - event.CurrentTime
						lagSecs = event.CurrentTime/1e9 - event.Timestamp
					}
				}
				switch event.Type {
				case binlogdatapb.VEventType_COMMIT:
					// If we've reached the stop position, we must save the current commit
					// even if it's empty. So, the next applyEvent is invoked with the
					// mustSave flag.
					if !p.vp.stopPos.IsZero() && p.vp.pos.AtLeast(p.vp.stopPos) {
						// We break early, so we never set `event.Skippable = true`.
						// This is the equivalent of `mustSave` in sequential VPlayer
						break
					}
					// In order to group multiple commits into a single one, we look ahead for
					// the next commit. If there is one, we skip the current commit, which ends up
					// applying the next set of events as part of the current transaction. This approach
					// also handles the case where the last transaction is partial. In that case,
					// we only group the transactions with commits we've seen so far.
					// if countPinnedWorkerEvents < 2*maxCountWorkersEvents && false {
					if countPinnedWorkerEvents < maxCountWorkersEvents && hasAnotherCommit(items, i, j+1) {
						pinWorker = true
						event.Skippable = true
					} else {
						pinWorker = false
					}
				case binlogdatapb.VEventType_DDL, binlogdatapb.VEventType_OTHER, binlogdatapb.VEventType_JOURNAL:
					pinWorker = false
				}
				if pinWorker {
					countPinnedWorkerEvents++
				} else {
					countPinnedWorkerEvents = 0
				}
				event.PinWorker = pinWorker
				select {
				case eventQueue <- event: // to be consumed by p.Process()
				case <-ctx.Done():
					return ctx.Err()
				case err := <-p.workerErrors:
					if err != io.EOF {
						p.vp.vr.stats.ErrorCounts.Add([]string{"Apply"}, 1)
						var table, tableLogMsg, gtidLogMsg string
						switch {
						case event.GetFieldEvent() != nil:
							table = event.GetFieldEvent().TableName
						case event.GetRowEvent() != nil:
							table = event.GetRowEvent().TableName
						}
						if table != "" {
							tableLogMsg = fmt.Sprintf(" for table %s", table)
						}
						pos := getNextPosition(items, i, j+1)
						if pos != "" {
							gtidLogMsg = fmt.Sprintf(" while processing position %s", pos)
						}
						log.Errorf("Error applying event%s%s: %s", tableLogMsg, gtidLogMsg, err.Error())
						err = vterrors.Wrapf(err, "error applying event%s%s", tableLogMsg, gtidLogMsg)
					}
					return err
				}
			}
		}

		if lagSecs >= 0 {
			p.vp.vr.stats.ReplicationLagSeconds.Store(lagSecs)
			p.vp.vr.stats.VReplicationLags.Add(strconv.Itoa(int(p.vp.vr.id)), time.Duration(lagSecs)*time.Second)
		} else { // We couldn't determine the lag, so we need to estimate it
			estimateLag()
		}
	}
}
