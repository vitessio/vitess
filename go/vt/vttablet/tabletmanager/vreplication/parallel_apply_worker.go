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

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

type applyWorker struct {
	ctx context.Context
	vr  *vreplicator
	// client is this worker's private MySQL connection, created at startup.
	// Each worker has its own connection so transactions can be applied in
	// parallel without contention on a shared connection.
	client *vdbClient
	// query executes a SQL statement on this worker's connection with retry.
	// Swapped onto the vplayer copy during applyEvent so the existing
	// serial applier code path uses this worker's connection transparently.
	query func(ctx context.Context, sql string) (*sqltypes.Result, error)
	// commit commits the current transaction on this worker's connection.
	// Swapped onto the vplayer copy alongside query.
	commit func() error
}

func newApplyWorker(ctx context.Context, vr *vreplicator) (*applyWorker, error) {
	dbClient := vr.vre.dbClientFactoryFiltered()
	if err := dbClient.Connect(); err != nil {
		return nil, err
	}
	// Close the connection if any subsequent setup step fails, to avoid
	// leaking the MySQL connection opened by Connect() above.
	if err := setDBClientSettings(dbClient, vr.workflowConfig); err != nil {
		dbClient.Close()
		return nil, err
	}
	vdbc := newVDBClientWithID(dbClient, vr.stats, vr.workflowConfig.RelayLogMaxItems, vr.id)
	if err := vr.clearFKCheck(vdbc); err != nil {
		dbClient.Close()
		return nil, err
	}
	if err := vr.clearFKRestrict(vdbc); err != nil {
		dbClient.Close()
		return nil, err
	}

	worker := &applyWorker{
		ctx:    ctx,
		vr:     vr,
		client: vdbc,
	}
	worker.query = func(ctx context.Context, sql string) (*sqltypes.Result, error) {
		return vdbc.ExecuteWithRetry(ctx, sql)
	}
	worker.commit = func() error {
		return vdbc.Commit()
	}
	return worker, nil
}

func (w *applyWorker) close() {
	if w.client != nil {
		if w.client.InTransaction {
			// parallelDebugLog(fmt.Sprintf("WORKER CLOSE with pending txn: stream=%d", w.vr.id))
			_ = w.client.Rollback()
		}
		w.client.Close()
	}
}

func (w *applyWorker) rollback() {
	if w.client != nil {
		_ = w.client.Rollback()
	}
}

func (w *applyWorker) applyEvent(ctx context.Context, event *binlogdatapb.VEvent, mustSave bool, vp *vplayer) error {
	if w.client == nil {
		return nil
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
