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
	"math"
	"sync"
	"sync/atomic"

	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/vterrors"
)

const (
	defaultParallelWorkersPoolSize = 8
	maxBatchedCommitsPerWorker     = 5
	maxWorkerEventsQueueSize       = 10
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

	numCommits atomic.Int64
}

func newParallelWorkersPool(size int, dbClientGen dbClientGenerator, vp *vplayer) (p *parallelWorkersPool, err error) {
	p = &parallelWorkersPool{
		workers:      make([]*parallelWorker, size),
		pool:         make(chan *parallelWorker, size),
		workerErrors: make(chan error, size),
	}
	for i := range size {
		w := &parallelWorker{
			index:  i,
			pool:   p,
			wakeup: make(chan int, 1),
			vp:     vp,
		}
		dbClient, err := dbClientGen()
		if err != nil {
			return nil, err
		}
		w.dbClient = newVDBClient(dbClient, vp.vr.stats, 0)
		w.queryFunc = func(ctx context.Context, sql string) (*sqltypes.Result, error) {
			return w.dbClient.ExecuteWithRetry(ctx, sql)
		}
		p.workers[i] = w
		p.pool <- w
	}
	if size > 1 {
		p.maxBatchedCommitsPerWorker = maxBatchedCommitsPerWorker
	} else {
		p.maxBatchedCommitsPerWorker = math.MaxInt
	}
	return p, nil
}

func (p *parallelWorkersPool) drain(ctx context.Context) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var rollbackErr error

	func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		for i := range len(p.workers) {
			p.workers[i].applyEvent(ctx, terminateWorkerEvent, true)
		}
	}()
	for range len(p.workers) {
		w, err := p.availableWorker(ctx, 0, 0, true) // blocks until all workers are idle
		if err != nil {
			return vterrors.Wrapf(err, "drain aborted")
		}
		// Release at end of function, after all have been acquired.
		w.applyEvent(ctx, terminateWorkerEvent, true)
	}
	// context cancellation will recycle all workers.
	return rollbackErr
}

func (p *parallelWorkersPool) availableWorker(ctx context.Context, lastCommitted int64, sequenceNumber int64, forDrain bool) (w *parallelWorker, err error) {
	select {
	case w = <-p.pool:
	case err := <-p.workerErrors:
		return nil, vterrors.Wrapf(err, "parallel worker rejected due to error from previous worker")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if forDrain {
		return w, nil
	}

	w.events = make(chan *binlogdatapb.VEvent, maxWorkerEventsQueueSize)
	w.lastCommitted = lastCommitted
	w.sequenceNumber = sequenceNumber

	go func() {
		if err := w.applyQueuedEvents(ctx); err != nil {
			p.workerErrors <- err
		}
		p.mu.Lock()
		defer p.mu.Unlock()

		if w.index == p.head {
			p.handoverHead(w.index)
		}
		w.events = nil // GC
		w.recycle()
	}()
	return w, nil
}

func (p *parallelWorkersPool) workersError() error {
	select {
	case err := <-p.workerErrors:
		p.workerErrors <- err
		return err
	default:
		return nil
	}
}

func (p *parallelWorkersPool) incrementHead() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.head = (p.head + 1) % len(p.workers)
}

func (p *parallelWorkersPool) handoverHead(fromIndex int) {
	p.head = (fromIndex + 1) % len(p.workers)
	go func() { p.workers[p.head].wakeup <- fromIndex }()
}

func (p *parallelWorkersPool) headIndex() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.head
}

func (p *parallelWorkersPool) isApplicable(w *parallelWorker, eventType binlogdatapb.VEventType) bool {
	switch eventType {
	case binlogdatapb.VEventType_GTID:
		return true
	case binlogdatapb.VEventType_BEGIN:
		return true
	case binlogdatapb.VEventType_FIELD:
		return true
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if w.index == p.head {
		// head worker is always applicable
		return true
	}
	if eventType != binlogdatapb.VEventType_ROW {
		return false
	}

	if w.sequenceNumber == 0 {
		// No info. We therefore execute sequentially.
		return false
	}
	if w.sequenceNumber == 1 {
		// First in the binary log. We therefore execute sequentially.
		return false
	}

	for i := range len(p.workers) {
		otherWorker := p.workers[(p.head+i)%len(p.workers)] // head based
		if otherWorker.index == w.index {
			// reached this worker. It is applicable.
			return true
		}
		if otherWorker.sequenceNumber == 0 {
			// unknown event. Used for draining. Sequentialize.
			return false
		}
		if otherWorker.sequenceNumber <= w.lastCommitted {
			// worker w depends on a previous event that has not committed yet.
			// Was this event applied by the same worker who is asking? If so, that's fine
			return false
		}
	}
	// Technically we'll never get here. The loop will always exit with "otherWorker.index == w.index"
	return true
}
