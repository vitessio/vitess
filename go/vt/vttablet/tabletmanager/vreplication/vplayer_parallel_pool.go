/*
Copyright 2024 The Vitess Authors.

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
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/vterrors"
)

const (
	defaultParallelWorkersPoolSize = 8
	maxBatchedCommitsPerWorker     = 10
)

type parallelWorkersPool struct {
	workers      []*parallelWorker
	head         int // position of head worker
	pool         chan *parallelWorker
	mu           sync.Mutex
	workerErrors chan error
	posReached   atomic.Bool

	currentConcurrency         atomic.Int64
	maxConcurrency             atomic.Int64
	maxBatchedCommitsPerWorker int

	numCommits atomic.Int64 // temporary. TODO: remove

	wakeup sync.Cond
}

func newParallelWorkersPool(size int, dbClientGen dbClientGenerator, vp *vplayer) (p *parallelWorkersPool, err error) {
	p = &parallelWorkersPool{
		workers:      make([]*parallelWorker, size),
		pool:         make(chan *parallelWorker, size),
		workerErrors: make(chan error, size*2),
	}
	p.wakeup.L = &p.mu
	for i := range size {
		w := &parallelWorker{
			index: i,
			pool:  p,
			vp:    vp,
		}
		dbClient, err := dbClientGen()
		if err != nil {
			return nil, err
		}
		if vp.vr.source.Filter != nil {
			if err := setDBClientSettings(dbClient, vp.vr.workflowConfig); err != nil {
				return nil, err
			}
		}

		w.dbClient = newVDBClient(dbClient, vp.vr.stats, 0)
		if vp.batchMode {
			log.Errorf("======= QQQ batchMode")
			w.queryFunc = func(ctx context.Context, sql string) (*sqltypes.Result, error) {
				if !w.dbClient.InTransaction { // Should be sent down the wire immediately
					return w.dbClient.Execute(sql)
				}
				return nil, w.dbClient.AddQueryToTrxBatch(sql) // Should become part of the trx batch
			}
			w.dbClient.maxBatchSize = vp.vr.dbClient.maxBatchSize
		} else {
			w.queryFunc = func(ctx context.Context, sql string) (*sqltypes.Result, error) {
				return w.dbClient.ExecuteWithRetry(ctx, sql)
			}
		}
		p.workers[i] = w
		p.pool <- w
	}
	if size > 1 {
		p.maxBatchedCommitsPerWorker = maxBatchedCommitsPerWorker
	} else {
		p.maxBatchedCommitsPerWorker = vp.vr.workflowConfig.RelayLogMaxItems
	}
	return p, nil
}

func (p *parallelWorkersPool) drain(ctx context.Context) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	terminateWorkers := func() error {
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, w := range p.workers {
			if err := w.applyEvent(ctx, terminateWorkerEvent); err != nil {
				return err
			}
		}
		return nil
	}
	if err := terminateWorkers(); err != nil {
		return err
	}
	// Get all workers (ensures they're all idle):
	for range len(p.workers) {
		_, err := p.availableWorker(ctx, -1, -1, false)
		if err != nil {
			return vterrors.Wrapf(err, "drain aborted")
		}
	}
	return p.workersError()
}

func (p *parallelWorkersPool) recycleWorker(w *parallelWorker) {
	p.pool <- w
}

func (p *parallelWorkersPool) availableWorker(ctx context.Context, lastCommitted int64, sequenceNumber int64, firstInBinlog bool) (w *parallelWorker, err error) {
	select {
	case w = <-p.pool:
	case err := <-p.workerErrors:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	events := make(chan *binlogdatapb.VEvent, p.maxBatchedCommitsPerWorker*5)

	p.mu.Lock()
	defer p.mu.Unlock()

	w.events = events
	w.lastCommitted = lastCommitted
	w.sequenceNumber = sequenceNumber
	w.isFirstInBinlog = firstInBinlog
	// log.Errorf("========== QQQ availableWorker w=%v initialized with seq=%v, lastComm=%v, firstInBinlog=%v", w.index, w.sequenceNumber, w.lastCommitted, firstInBinlog)

	if lastCommitted < 0 {
		// Only happens when called by drain(), in which case there is no need for this worker
		// to start applying events, nor will this worker be recycled. This is the end of the line.
		return w, nil
	}

	go func() {
		if err := w.applyQueuedEvents(ctx); err != nil {
			if errors.Is(vterrors.UnwrapAll(err), io.EOF) {
				w.pool.posReached.Store(true)
			}
			p.workerErrors <- err
			// log.Errorf("========== QQQ applyQueuedEvents worker %v is done with error=%v", w.index, err)
		}
		// log.Errorf("========== QQQ applyQueuedEvents worker %v is done!", w.index)
		p.mu.Lock()
		defer p.mu.Unlock()

		if w.index == p.head {
			p.handoverHead(w.index)
			// log.Errorf("========== QQQ applyQueuedEvents new head=%v with %d queued, first in binlog =%v", p.head, len(p.workers[p.head].events), p.workers[p.head].isFirstInBinlog)
		}
		w.lastCommitted = 0
		w.sequenceNumber = 0
		w.isFirstInBinlog = false
		p.recycleWorker(w)
	}()
	return w, nil
}

func (p *parallelWorkersPool) workersError() error {
	select {
	case err := <-p.workerErrors:
		return err
	default:
		if p.posReached.Load() {
			return io.EOF
		}
		return nil
	}
}

func (p *parallelWorkersPool) handoverHead(fromIndex int) {
	p.head = (fromIndex + 1) % len(p.workers)
	for _, w := range p.workers {
		if w.index == fromIndex {
			continue
		}
		p.wakeup.Broadcast()
	}
}

func (p *parallelWorkersPool) isApplicable(w *parallelWorker, event *binlogdatapb.VEvent) bool {
	if w.index == p.head {
		// head worker is always applicable
		return true
	}

	// Not head.
	if w.isFirstInBinlog {
		// First in the binary log. Only applicable when this worker is at head.
		return false
	}
	if event.SequenceNumber == 0 {
		// No info. We therefore execute sequentially.
		return false
	}
	switch event.Type {
	case binlogdatapb.VEventType_GTID,
		binlogdatapb.VEventType_BEGIN,
		binlogdatapb.VEventType_FIELD,
		binlogdatapb.VEventType_ROW:
		// logic to follow
	default:
		// Only parallelize row events.
		return false
	}

	for i := range len(p.workers) {
		otherWorker := p.workers[(p.head+i)%len(p.workers)] // head based
		if otherWorker.index == w.index {
			// reached this worker. It is applicable.
			return true
		}
		if otherWorker.sequenceNumber < 0 {
			// Happens on draining. Skip this worker.
			continue
		}
		if otherWorker.sequenceNumber == 0 {
			// unknown event.
			log.Errorf("========== QQQ isApplicable WHOA0 otherWorker %v sequenceNumber=%v", otherWorker.index, otherWorker.sequenceNumber)
			return false
		}
		if otherWorker.isFirstInBinlog && i > 0 {
			// log.Errorf("========== QQQ isApplicable: false, because worker %v sees otherWorker %v which is first in binlog at i=%v. head=%v", w.index, otherWorker.index, i, p.head)
			// This means we've rotated a binary log. We therefore
			// Wait until all previous binlog events are consumed.
			return false
		}
		if otherWorker.sequenceNumber <= event.LastCommitted {
			// worker w depends on a previous event that has not committed yet.
			return false
		}
	}
	// Never going to reach this code, because our loop will always eventually hit `otherWorker.index == w.index`.
	return true
}
