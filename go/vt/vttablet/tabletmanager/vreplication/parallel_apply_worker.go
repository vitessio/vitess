/*
Copyright 2026 The Vitess Authors.

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
	"log/slog"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"
	vttablet "vitess.io/vitess/go/vt/vttablet/common"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

type applyWorker struct {
	ctx context.Context
	vr  *vreplicator
	// conns holds a pair of MySQL connections for double-buffering. While
	// one connection is being committed by the commitLoop, the worker can
	// immediately start applying the next transaction on the other. This
	// decouples the worker's apply phase from the serial commitLoop,
	// allowing true pipeline parallelism.
	conns  [2]*vdbClient
	active int
	// client points to conns[active] for convenience. Updated by rotate().
	client *vdbClient
	// batchMode indicates whether this worker buffers SQL statements and
	// flushes them as a single multi-statement request. When true, the
	// apply phase buffers INSERTs via AddQueryToTrxBatch (near-zero cost),
	// then flushWorkerBatch sends them all to MySQL in one ExecuteFetchMulti
	// call. This happens during the parallel apply phase, so all workers
	// execute their multi-statement batches concurrently. The commitLoop
	// then just does a quick COMMIT + position update.
	batchMode bool
	// query executes a SQL statement on this worker's active connection.
	// Rebound by rotate() to use the new active connection.
	query func(ctx context.Context, sql string) (*sqltypes.Result, error)
	// commit commits the current transaction on this worker's active connection.
	// Rebound by rotate() alongside query.
	commit func() error
}

// createWorkerConn creates a single configured vdbClient for a worker.
func createWorkerConn(ctx context.Context, vr *vreplicator) (*vdbClient, error) {
	dbClient := vr.vre.dbClientFactoryFiltered()
	if err := dbClient.Connect(); err != nil {
		return nil, err
	}
	if err := setDBClientSettings(dbClient, vr.workflowConfig); err != nil {
		dbClient.Close()
		return nil, err
	}
	vdbc := newVDBClientWithID(dbClient, vr.stats, vr.workflowConfig.RelayLogMaxItems, vr.id)
	if _, err := vr.setSQLMode(ctx, vdbc); err != nil {
		dbClient.Close()
		return nil, err
	}
	if err := vr.resetFKCheckAfterCopy(vdbc); err != nil {
		dbClient.Close()
		return nil, err
	}
	if err := vr.resetFKRestrictAfterCopy(vdbc); err != nil {
		dbClient.Close()
		return nil, err
	}
	return vdbc, nil
}

func newApplyWorker(ctx context.Context, vr *vreplicator) (*applyWorker, error) {
	batchMode := vr.workflowConfig.ExperimentalFlags&vttablet.VReplicationExperimentalFlagVPlayerBatching != 0

	var conns [2]*vdbClient
	for i := range 2 {
		vdbc, err := createWorkerConn(ctx, vr)
		if err != nil {
			// Close any previously created connections.
			for j := range i {
				conns[j].Close()
			}
			return nil, err
		}
		conns[i] = vdbc
	}

	if batchMode {
		maxBatchSize := int64(vr.workflowConfig.RelayLogMaxSize)
		res, err := conns[0].ExecuteFetch(SqlMaxAllowedPacket, 1)
		if err != nil {
			log.Error("Worker: error getting max_allowed_packet, will use relay-log-max-size value", slog.Int64("bytes", int64(vr.workflowConfig.RelayLogMaxSize)), slog.Any("error", err))
		} else {
			if pkt, err := res.Rows[0][0].ToInt64(); err != nil {
				log.Error("Worker: error getting max_allowed_packet, will use relay-log-max-size value", slog.Int64("bytes", int64(vr.workflowConfig.RelayLogMaxSize)), slog.Any("error", err))
			} else {
				maxBatchSize = pkt
			}
		}
		maxBatchSize -= 64
		for _, c := range conns {
			c.maxBatchSize = maxBatchSize
		}
	}

	worker := &applyWorker{
		ctx:       ctx,
		vr:        vr,
		conns:     conns,
		active:    0,
		client:    conns[0],
		batchMode: batchMode,
	}
	worker.bindFunctions()
	return worker, nil
}

// bindFunctions sets the query and commit closures to use the active connection.
func (w *applyWorker) bindFunctions() {
	vdbc := w.client
	if w.batchMode {
		w.query = func(ctx context.Context, sql string) (*sqltypes.Result, error) {
			if !vdbc.InTransaction {
				return vdbc.Execute(sql)
			}
			return nil, vdbc.AddQueryToTrxBatch(sql)
		}
		w.commit = func() error {
			return vdbc.Commit()
		}
	} else {
		w.query = func(ctx context.Context, sql string) (*sqltypes.Result, error) {
			return vdbc.ExecuteWithRetry(ctx, sql)
		}
		w.commit = func() error {
			return vdbc.Commit()
		}
	}
}

// rotate switches the worker to its spare connection for the next transaction.
// The commitLoop will continue committing the previous transaction on the old
// connection. This double-buffering allows the worker's apply phase to overlap
// with the commitLoop's commit phase, enabling true pipeline parallelism.
func (w *applyWorker) rotate() {
	w.active = 1 - w.active
	w.client = w.conns[w.active]
	w.bindFunctions()
}

// flushWorkerBatch sends all buffered SQL statements to MySQL in one
// multi-statement call via ExecuteTrxQueryBatch. This is called after
// the worker has finished applying all events for a transaction, moving
// the MySQL work into the parallel apply phase (before the serial
// commitLoop). If batch mode is disabled, this is a no-op.
func (w *applyWorker) flushWorkerBatch() error {
	if !w.batchMode || w.client == nil {
		return nil
	}
	_, err := w.client.ExecuteTrxQueryBatch()
	return err
}

func (w *applyWorker) close() {
	for _, c := range w.conns {
		if c != nil {
			if c.InTransaction {
				_ = c.Rollback()
			}
			c.Close()
		}
	}
}

func (w *applyWorker) rollback() {
	if w.client != nil {
		_ = w.client.Rollback()
	}
}

func (w *applyWorker) applyEvent(ctx context.Context, event *binlogdatapb.VEvent, mustSave bool, vp *vplayer) error {
	if w.client == nil {
		return errors.New("apply worker has no active client")
	}
	prevLocal := vp.dbClient
	prevQuery := vp.query
	prevCommit := vp.commit
	vp.query = w.query
	vp.commit = w.commit
	vp.dbClient = w.client
	defer func() {
		vp.dbClient = prevLocal
		vp.query = prevQuery
		vp.commit = prevCommit
	}()
	return vp.applyEvent(ctx, event, mustSave)
}

func (w *applyWorker) stats() *binlogplayer.Stats {
	return w.vr.stats
}
