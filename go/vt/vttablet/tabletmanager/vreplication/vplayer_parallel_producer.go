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
	countWorkers = 4
)

type parallelProducer struct {
	vp *vplayer

	workers []*parallelWorker

	dbClient *vdbClient

	posReached                atomic.Bool
	workerErrors              chan error
	sequenceToWorkersMap      map[int64]int // sequence number => worker index
	completedSequenceNumbers  chan map[int64]bool
	commitWorkerEventSequence atomic.Int64
	assignSequence            int64

	numCommits         atomic.Int64 // temporary. TODO: remove
	currentConcurrency atomic.Int64 // temporary. TODO: remove
	maxConcurrency     atomic.Int64 // temporary. TODO: remove
}

func newParallelProducer(ctx context.Context, dbClientGen dbClientGenerator, vp *vplayer) (*parallelProducer, error) {
	p := &parallelProducer{
		vp:                       vp,
		dbClient:                 vp.vr.dbClient,
		workers:                  make([]*parallelWorker, countWorkers),
		workerErrors:             make(chan error, countWorkers),
		sequenceToWorkersMap:     make(map[int64]int),
		completedSequenceNumbers: make(chan map[int64]bool, countWorkers),
	}
	{
		// TODO(shlomi): just use the dbClient from vp.vr.
		dbClient, err := dbClientGen()
		if err != nil {
			return nil, err
		}
		p.dbClient = newVDBClient(dbClient, vp.vr.stats, 0)
	}
	for i := range p.workers {
		w := newParallelWorker(i, p, vp.vr.workflowConfig.RelayLogMaxItems)
		dbClient, err := dbClientGen()
		if err != nil {
			return nil, err
		}
		w.dbClient = newVDBClient(dbClient, vp.vr.stats, 0)
		_, err = vp.vr.setSQLMode(ctx, w.dbClient)
		if err != nil {
			return nil, err
		}
		w.queryFunc = func(ctx context.Context, sql string) (*sqltypes.Result, error) {
			if !w.dbClient.InTransaction { // Should be sent down the wire immediately
				return w.dbClient.Execute(sql)
			}
			return nil, w.dbClient.AddQueryToTrxBatch(sql) // Should become part of the trx batch
		}
		w.dbClient.maxBatchSize = vp.vr.dbClient.maxBatchSize
		// INSERT a row into _vt.vreplication_worker_pos with an empty position
		if _, err := w.dbClient.ExecuteFetch(binlogplayer.GenerateInitWorkerPos(vp.vr.id, w.index), -1); err != nil {
			return nil, err
		}

		p.workers[i] = w
	}

	return p, nil
}

func (p *parallelProducer) commitWorkerEvent() *binlogdatapb.VEvent {
	return &binlogdatapb.VEvent{
		Type:           binlogdatapb.VEventType_UNKNOWN,
		SequenceNumber: p.commitWorkerEventSequence.Add(-1),
	}
}

func (p *parallelProducer) assignTransactionToWorker(sequenceNumber int64, lastCommitted int64) (workerIndex int) {
	if workerIndex, ok := p.sequenceToWorkersMap[sequenceNumber]; ok {
		// Pin for the duration of the transaction
		// log.Errorf("========== QQQ assignTransactionToWorker same trx sequenceNumber=%v, lastCommitted=%v, workerIndex=%v", sequenceNumber, lastCommitted, workerIndex)
		return workerIndex
	}
	if workerIndex, ok := p.sequenceToWorkersMap[lastCommitted]; ok {
		// Assign transaction to the same worker who owns the last committed transaction
		// log.Errorf("========== QQQ assignTransactionToWorker dependent trx sequenceNumber=%v, lastCommitted=%v, workerIndex=%v", sequenceNumber, lastCommitted, workerIndex)
		p.sequenceToWorkersMap[sequenceNumber] = workerIndex
		return workerIndex
	}
	// workerIndex = int((p.assignSequence / 10) % countWorkers)
	workerIndex = int(p.assignSequence % countWorkers)
	// log.Errorf("========== QQQ assignTransactionToWorker free trx p.sequence=%v, sequenceNumber=%v, lastCommitted=%v, workerIndex=%v", p.assignSequence, sequenceNumber, lastCommitted, workerIndex)
	p.assignSequence++
	p.sequenceToWorkersMap[sequenceNumber] = workerIndex
	return workerIndex
}

func (p *parallelProducer) commitAll(ctx context.Context, except *parallelWorker) error {
	// TODO(shlomi) remove
	{
		exceptString := ""
		if except != nil {
			exceptString = fmt.Sprintf(" except %v", except.index)
		}
		log.Errorf("========== QQQ commitAll%v", exceptString)
	}
	var eg errgroup.Group
	for _, w := range p.workers {
		w := w
		if except != nil && w.index == except.index {
			continue
		}
		eg.Go(func() error {
			return <-w.commitEvents()
		})
	}
	return eg.Wait()
}
func (p *parallelProducer) aggregateWorkersPos(ctx context.Context) (aggregatedWorkersPos replication.Position, combinedPos replication.Position, err error) {
	// TODO(shlomi): this query can be computed once in the lifetime of the producer
	query := binlogplayer.ReadVReplicationWorkersGTIDs(p.vp.vr.id)
	qr, err := p.dbClient.ExecuteFetch(query, -1)
	if err != nil {
		log.Errorf("Error fetching vreplication worker positions: %v. isclosed? %v", err, p.dbClient.IsClosed())
		return aggregatedWorkersPos, combinedPos, err
	}
	var lastEventTimestamp int64
	for _, row := range qr.Rows {
		current, err := binlogplayer.DecodeMySQL56Position(row[0].ToString())
		if err != nil {
			return aggregatedWorkersPos, combinedPos, err
		}
		eventTimestamp, err := row[1].ToInt64()
		if err != nil {
			return aggregatedWorkersPos, combinedPos, err
		}
		lastEventTimestamp = max(lastEventTimestamp, eventTimestamp)
		aggregatedWorkersPos = replication.AppendGTIDSet(aggregatedWorkersPos, current.GTIDSet)
	}
	combinedPos = replication.AppendGTIDSet(aggregatedWorkersPos, p.vp.startPos.GTIDSet)
	p.vp.pos = combinedPos // TODO(shlomi) potential for race condition

	log.Errorf("========== QQQ aggregateWorkersPos updatePos ts=%v, pos=%v", lastEventTimestamp, combinedPos)
	if _, err := p.vp.updatePos(ctx, lastEventTimestamp); err != nil {
		return aggregatedWorkersPos, combinedPos, err
	}
	if err := p.vp.commit(); err != nil {
		return aggregatedWorkersPos, combinedPos, err
	}
	return aggregatedWorkersPos, combinedPos, nil
}

func (p *parallelProducer) watchPos(ctx context.Context) error {
	// if p.vp.stopPos.IsZero() {
	// 	return nil
	// }
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastCombinedPos replication.Position
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			log.Errorf("========== QQQ watchPos ticker")
			aggregatedWorkersPos, combinedPos, err := p.aggregateWorkersPos(ctx)
			if err != nil {
				log.Errorf("Error aggregating vreplication worker positions: %v. isclosed? %v", err, p.dbClient.IsClosed())
				continue
			}
			log.Errorf("========== QQQ watchPos aggregatedWorkersPos: %v, combinedPos: %v, stop: %v", aggregatedWorkersPos, combinedPos, p.vp.stopPos)

			// Write back this combined pos to all workers, so that we condense their otherwise sparse GTID sets.
			log.Errorf("========== QQQ watchPos pushing combined pos %v", combinedPos)
			for _, w := range p.workers {
				log.Errorf("========== QQQ watchPos pushing combined pos worker %v", w.index)
				w.aggregatedPosChan <- aggregatedWorkersPos
			}
			log.Errorf("========== QQQ watchPos pushed combined pos")
			if combinedPos.GTIDSet.Equal(lastCombinedPos.GTIDSet) {
				// no progress has been made
				log.Errorf("========== QQQ watchPos no progress!! committing all")
				if err := p.commitAll(ctx, nil); err != nil {
					return err
				}
				log.Errorf("========== QQQ watchPos no progress!! committed all")
			} else {
				// progress has been made
				lastCombinedPos = combinedPos
			}
			if !p.vp.stopPos.IsZero() && combinedPos.AtLeast(p.vp.stopPos) {
				if err := p.commitAll(ctx, nil); err != nil {
					return err
				}
				p.posReached.Store(true)
				return io.EOF
			}
			log.Errorf("========== QQQ watchPos end loop cycle")
		}
	}
}

func (p *parallelProducer) process(ctx context.Context, events chan *binlogdatapb.VEvent) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// ticker := time.NewTicker(50 * time.Millisecond)
	// defer ticker.Stop()
	// var lastGoodTime time.Time
	//
	// 	processEvent := func(ctx context.Context, event *binlogdatapb.VEvent) err {
	// 		workerIndex := p.assignTransactionToWorker(event.SequenceNumber, event.CommitParent)
	// 		worker := p.workers[workerIndex]
	// 		for {
	// 			select {
	// 			case worker.events <- event:
	// 				// We managed to assign the event onto the worker.
	// 				return nil
	// 			case t := <-ticker.C:
	// 				if t.After(lastGoodTime.Add(50 * time.Millisecond)) {
	// 					// We're falling behind. Commit all transactions.
	// 					p.commitAll(ctx)
	// 					lastGoodTime = t
	// 				}
	// 			case <-ctx.Done():
	// 				return ctx.Err()
	// 			}
	// 		}
	// 	}

	for {
		if p.posReached.Load() {
			return io.EOF
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sequenceNumbers := <-p.completedSequenceNumbers:
			// log.Errorf("========== QQQ process completedSequenceNumbers=%v", sequenceNumbers)
			for sequenceNumber := range sequenceNumbers {
				delete(p.sequenceToWorkersMap, sequenceNumber)
			}
		// case t := <-ticker.C:
		// 	lastGoodTime = t
		case event := <-events:
			// processEvent(ctx, event)
			workerIndex := p.assignTransactionToWorker(event.SequenceNumber, event.CommitParent)
			worker := p.workers[workerIndex]
			// We know the worker has enough capacity and thus the following will not block.
			// log.Errorf("========== QQQ process: assigning event.Type %v seq=%v, parent=%v, to worker.Index %v at index %v", event.Type, event.SequenceNumber, event.CommitParent, worker.index, workerIndex)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case worker.events <- event:
			}
		}
	}
}

func (p *parallelProducer) applyEvents(ctx context.Context, relay *relayLog) error {
	// TODO(shlomi): do not cancel context, because if we do, that can terminate async queries still running.
	// ctx, cancel := context.WithCancel(ctx)
	// defer cancel()

	log.Errorf("========== QQQ applyEvents defer")

	go func() {
		if err := p.watchPos(ctx); err != nil {
			p.workerErrors <- err
		}
	}()
	for _, w := range p.workers {
		w := w
		go func() {
			p.workerErrors <- w.applyQueuedEvents(ctx)
		}()
	}

	estimateLag := func() {
		behind := time.Now().UnixNano() - p.vp.lastTimestampNs - p.vp.timeOffsetNs
		p.vp.vr.stats.ReplicationLagSeconds.Store(behind / 1e9)
		p.vp.vr.stats.VReplicationLags.Add(strconv.Itoa(int(p.vp.vr.id)), time.Duration(behind/1e9)*time.Second)
	}

	eventQueue := make(chan *binlogdatapb.VEvent, 1000)
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
				_ = p.vp.vr.updateTimeThrottled(throttlerapp.VPlayerName, checkResult.Summary())
				estimateLag()
			}()
			continue
		}

		items, err := relay.Fetch()
		if err != nil {
			return err
		}

		lagSecs = -1
		for i, events := range items {
			for j, event := range events {
				// event's GTID is singular, but we parse it as a GTIDSet
				_, eventGTID, err := replication.DecodePositionMySQL56(event.Gtid)
				if err != nil {
					return err
				}
				if !p.vp.stopPos.IsZero() && !p.vp.stopPos.GTIDSet.Contains(eventGTID) {
					// This event goes beyond the stop position. We skip it.
					continue
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
