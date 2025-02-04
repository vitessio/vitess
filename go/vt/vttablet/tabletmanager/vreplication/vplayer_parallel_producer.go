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
	"sync/atomic"

	"vitess.io/vitess/go/sqltypes"
)

const (
	countWorkers = 8
)

var (
	sequenceToWorkersMap = make(map[int64]int) // sequence number => worker index
)

type parallelProducer struct {
	workers    []*parallelWorker
	lastWorker int

	posReached atomic.Bool
}

func newParallelProducer(ctx context.Context, dbClientGen dbClientGenerator, vp *vplayer) (*parallelProducer, error) {
	p := &parallelProducer{
		workers: make([]*parallelWorker, countWorkers),
	}
	for i := range p.workers {
		w := newParallelWorker()
		w.producer = p
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

		p.workers[i] = w
	}
	return p, nil
}

func (p *parallelProducer) applyEvents(ctx context.Context, relay *relayLog) error {
	return nil
}

func (p *parallelProducer) assignTransactionToWorker(sequenceNumber int64, lastCommitted int64) (workerIndex int) {
	if workerIndex, ok := sequenceToWorkersMap[lastCommitted]; ok {
		// Assign transaction to the same worker who owns the last committed transaction
		sequenceToWorkersMap[sequenceNumber] = workerIndex
		return workerIndex
	}
	p.lastWorker = (p.lastWorker + 1) % countWorkers
	return p.lastWorker
}
