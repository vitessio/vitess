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

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	vttablet "vitess.io/vitess/go/vt/vttablet/common"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

// workerSession is one MySQL connection that apply workers use to apply a
// transaction. Sessions are not owned by a worker: a worker borrows a free
// session from the shared workerSessionPool for each transaction it applies,
// hands the session (with its still-open MySQL transaction) to the commitLoop,
// and immediately borrows another session for its next transaction. The
// commitLoop returns the session to the pool once the transaction commits.
//
// This mirrors the "Session Service" of MySQL's Change Stream Applier (CSA,
// WL#10500): the apply phase and the commit phase are decoupled, so a worker
// whose transaction cannot commit yet (because an earlier-ordered transaction
// is still being applied elsewhere) parks the transaction on its session and
// picks up another dependency-ready transaction instead of sitting idle.
type workerSession struct {
	pool   *workerSessionPool
	client *vdbClient
	// query and commit are bound once per session (rather than per
	// transaction) so that handing a session from one worker to another
	// does not allocate.
	query  func(ctx context.Context, sql string) (*sqltypes.Result, error)
	commit func() error
}

// workerSessionPool holds the sessions shared by all apply workers of one
// parallel applier. The scheduler accounts for how many sessions are in use
// (see applyScheduler.sessionLimit) and only dispatches a worker transaction
// when a session is free, so get never has to wait for a session in practice.
type workerSessionPool struct {
	sessions []*workerSession
	free     chan *workerSession
}

// newWorkerSessionPool wraps the given clients into sessions. In batch mode a
// session's query buffers statements in the open transaction so the worker can
// flush them as a single multi-statement request.
func newWorkerSessionPool(clients []*vdbClient, batchMode bool) *workerSessionPool {
	pool := &workerSessionPool{
		sessions: make([]*workerSession, 0, len(clients)),
		free:     make(chan *workerSession, len(clients)),
	}
	for _, vdbc := range clients {
		sess := &workerSession{pool: pool, client: vdbc}
		if batchMode {
			sess.query = func(ctx context.Context, sql string) (*sqltypes.Result, error) {
				if !vdbc.InTransaction {
					return vdbc.Execute(sql)
				}
				return nil, vdbc.AddQueryToTrxBatch(sql)
			}
		} else {
			sess.query = func(ctx context.Context, sql string) (*sqltypes.Result, error) {
				return vdbc.ExecuteWithRetry(ctx, sql)
			}
		}
		sess.commit = vdbc.Commit
		pool.sessions = append(pool.sessions, sess)
		pool.free <- sess
	}
	return pool
}

// size returns the total number of sessions in the pool.
func (p *workerSessionPool) size() int {
	return len(p.sessions)
}

// get borrows a free session, blocking until one is returned or ctx is done.
func (p *workerSessionPool) get(ctx context.Context) (*workerSession, error) {
	select {
	case sess := <-p.free:
		return sess, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// put returns a session to the pool. The session must not have an open
// transaction.
func (p *workerSessionPool) put(sess *workerSession) {
	p.free <- sess
}

// close releases every session's MySQL connection, rolling back first if a
// session is mid-transaction (e.g. a transaction that was parked waiting for
// its commit turn when the applier stopped) so no half-applied state leaks.
// It must only be called once no worker or commitLoop is using a session.
func (p *workerSessionPool) close() {
	for _, sess := range p.sessions {
		if sess.client.InTransaction {
			_ = sess.client.Rollback()
		}
		sess.client.Close()
	}
}

type applyWorker struct {
	ctx context.Context
	vr  *vreplicator
	// sessions is the pool shared by all workers. Each transaction is
	// applied on a session borrowed from it; see workerSession.
	sessions *workerSessionPool
	// session is the session the worker is currently applying on, or nil
	// between transactions.
	session *workerSession
	// client points to session.client for convenience.
	client *vdbClient
	// batchMode indicates whether this worker buffers SQL statements and
	// flushes them as a single multi-statement request. When true, the
	// apply phase buffers INSERTs via AddQueryToTrxBatch (near-zero cost),
	// then flushWorkerBatch sends them all to MySQL in one ExecuteFetchMulti
	// call. This happens during the parallel apply phase, so all workers
	// execute their multi-statement batches concurrently. The commitLoop
	// then just does a quick COMMIT + position update.
	batchMode bool
	// query executes a SQL statement on this worker's current session.
	query func(ctx context.Context, sql string) (*sqltypes.Result, error)
	// commit commits the current transaction on this worker's current session.
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
	// Workers apply transactions concurrently. The writeset scheduler models
	// PK/unique/FK conflicts, but it cannot model InnoDB gap/next-key locks,
	// which REPEATABLE READ takes even for point operations on absent rows
	// (e.g. DELETE of a row that does not exist, or delete-marking in a
	// non-unique secondary index). A later-ordered transaction's gap lock can
	// block an earlier-ordered transaction's INSERT while the commitLoop's
	// strict ordering keeps that gap lock held until the earlier transaction
	// commits — a deadlock InnoDB's detector cannot see because half the
	// cycle lives in the commitLoop (MySQL's MTA has its Commit_order_manager
	// for exactly this). READ COMMITTED takes no gap locks for row-image
	// application and is MySQL's own recommendation for row-based parallel
	// appliers. Statement-based events force-serialize, so RC cannot change
	// their outcome either.
	//
	// Use the SQL-standard statement form rather than setting the
	// transaction_isolation sysvar: this connection goes directly to the
	// target mysqld (no vtgate sysvar compatibility layer), and the sysvar
	// spelling is flavor-specific (MariaDB used tx_isolation until 11.1;
	// MySQL only added transaction_isolation in 5.7.20). Keep it lowercase
	// to match the other session-setup statements (set names, set @@session.*).
	if _, err := dbClient.ExecuteFetch("set session transaction isolation level read committed", 1); err != nil {
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

// newWorkerSessions creates count configured worker sessions. In batch mode it
// also reads MySQL's max_allowed_packet to size the multi-statement flush so a
// worker's batched INSERTs cannot exceed the wire limit.
func newWorkerSessions(ctx context.Context, vr *vreplicator, count int) (*workerSessionPool, error) {
	batchMode := vr.workflowConfig.ExperimentalFlags&vttablet.VReplicationExperimentalFlagVPlayerBatching != 0

	clients := make([]*vdbClient, 0, count)
	for range count {
		vdbc, err := createWorkerConn(ctx, vr)
		if err != nil {
			// Close any previously created connections.
			for _, c := range clients {
				c.Close()
			}
			return nil, err
		}
		clients = append(clients, vdbc)
	}

	if batchMode {
		maxBatchSize := vr.maxQuerySize(clients[0])
		for _, c := range clients {
			c.maxBatchSize = maxBatchSize
		}
	}
	return newWorkerSessionPool(clients, batchMode), nil
}

// newApplyWorker constructs a worker that applies transactions on sessions
// borrowed from the shared pool.
func newApplyWorker(ctx context.Context, vr *vreplicator, sessions *workerSessionPool) *applyWorker {
	return &applyWorker{
		ctx:       ctx,
		vr:        vr,
		sessions:  sessions,
		batchMode: vr.workflowConfig.ExperimentalFlags&vttablet.VReplicationExperimentalFlagVPlayerBatching != 0,
	}
}

// acquireSession borrows a session from the pool for the next transaction.
func (w *applyWorker) acquireSession(ctx context.Context) error {
	sess, err := w.sessions.get(ctx)
	if err != nil {
		return err
	}
	w.useSession(sess)
	return nil
}

// useSession binds the worker's client/query/commit to sess.
func (w *applyWorker) useSession(sess *workerSession) {
	w.session = sess
	w.client = sess.client
	w.query = sess.query
	w.commit = sess.commit
}

// detachSession unbinds and returns the worker's current session. The caller
// takes ownership of it: either the commitLoop (which returns it to the pool
// after commit) or the error path.
func (w *applyWorker) detachSession() *workerSession {
	sess := w.session
	w.session = nil
	w.client = nil
	w.query = nil
	w.commit = nil
	return sess
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

// rollback discards in-progress work on the worker's current session after an
// apply error. The session is not returned to the pool: the applier is being
// torn down and workerSessionPool.close releases every connection.
func (w *applyWorker) rollback() {
	if w.client != nil {
		_ = w.client.Rollback()
	}
}

// applyEvent dispatches through the shared vplayer.applyEvent code path while
// temporarily rebinding vp.dbClient/query/commit to this worker's current
// session. Bindings are restored on return so the orchestrator's vplayer
// (shared by the scheduler and commitLoop) never ends up pointing at
// worker-owned state.
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

// stats exposes the underlying vreplication stats so helpers that only hold
// an *applyWorker can record counters without reaching through w.vr.
func (w *applyWorker) stats() *binlogplayer.Stats {
	return w.vr.stats
}
