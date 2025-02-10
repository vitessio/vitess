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
	countWorkers = 8
)

type parallelProducer struct {
	vp *vplayer

	workers    []*parallelWorker
	lastWorker int

	dbClient *vdbClient

	posReached                atomic.Bool
	workerErrors              chan error
	sequenceToWorkersMap      map[int64]int // sequence number => worker index
	completedSequenceNumbers  chan []int64
	commitWorkerEventSequence atomic.Int64

	numCommits         atomic.Int64 // temporary. TODO: remove
	currentConcurrency atomic.Int64 // temporary. TODO: remove
	maxConcurrency     atomic.Int64 // temporary. TODO: remove
}

func newParallelProducer(ctx context.Context, dbClientGen dbClientGenerator, vp *vplayer) (*parallelProducer, error) {
	p := &parallelProducer{
		vp:                       vp,
		workers:                  make([]*parallelWorker, countWorkers),
		workerErrors:             make(chan error, countWorkers),
		dbClient:                 vp.vr.dbClient,
		completedSequenceNumbers: make(chan []int64, countWorkers),
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
	if workerIndex, ok := p.sequenceToWorkersMap[lastCommitted]; ok {
		// Assign transaction to the same worker who owns the last committed transaction
		p.sequenceToWorkersMap[sequenceNumber] = workerIndex
		return workerIndex
	}
	p.lastWorker = (p.lastWorker + 1) % countWorkers
	return p.lastWorker
}

func (p *parallelProducer) commitAll(ctx context.Context, except *parallelWorker) error {
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

func (p *parallelProducer) watchPos(ctx context.Context) error {
	if p.vp.stopPos.IsZero() {
		return nil
	}
	query := binlogplayer.ReadVReplicationWorkersGTIDs(p.vp.vr.id)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			qr, err := p.dbClient.ExecuteFetch(query, -1)
			if err != nil {
				log.Errorf("Error fetching vreplication worker positions: %v", err)
			}
			combinedPos := p.vp.startPos
			for _, row := range qr.Rows {
				current, err := binlogplayer.DecodeMySQL56Position(row[0].ToString())
				if err != nil {
					return err
				}
				combinedPos = replication.AppendGTIDSet(combinedPos, current.GTIDSet)
			}

			if combinedPos.AtLeast(p.vp.stopPos) {
				if err := p.commitAll(ctx, nil); err != nil {
					return err
				}
				p.posReached.Store(true)
				return io.EOF
			}
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
			for _, sequenceNumber := range sequenceNumbers {
				delete(p.sequenceToWorkersMap, sequenceNumber)
			}
		// case t := <-ticker.C:
		// 	lastGoodTime = t
		case event := <-events:
			// processEvent(ctx, event)
			workerIndex := p.assignTransactionToWorker(event.SequenceNumber, event.CommitParent)
			worker := p.workers[workerIndex]
			// We know the worker has enough capacity and thus the following will not block.
			worker.events <- event
		}
	}
}

func (p *parallelProducer) applyEvents(ctx context.Context, relay *relayLog) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

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
