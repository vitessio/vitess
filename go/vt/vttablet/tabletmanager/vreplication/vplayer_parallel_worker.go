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

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

const (
	workerCapacity = 1000
)

type parallelWorker struct {
	index          int
	lastCommitted  int64
	sequenceNumber int64
	dbClient       *vdbClient
	queryFunc      func(ctx context.Context, sql string) (*sqltypes.Result, error)
	vp             *vplayer
	lastPos        replication.Position

	producer *parallelProducer

	events          chan *binlogdatapb.VEvent
	stats           *VrLogStats
	sequenceNumbers []int64
}

func newParallelWorker() *parallelWorker {
	return &parallelWorker{
		events:          make(chan *binlogdatapb.VEvent, workerCapacity),
		sequenceNumbers: make([]int64, 0, workerCapacity),
	}

}

func (w *parallelWorker) commit() error {
	if w.producer.posReached.Load() {
		return nil
	}
	if w.vp.batchMode {
		return w.dbClient.CommitTrxQueryBatch() // Commit the current trx batch
	} else {
		return w.dbClient.Commit()
	}
}

func (w *parallelWorker) applyEvent(ctx context.Context, event *binlogdatapb.VEvent) error {
	select {
	case w.events <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
