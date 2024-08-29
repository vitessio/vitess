/*
Copyright 2019 The Vitess Authors.

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

package tabletserver

import (
	"context"
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/pools/smartconnpool"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/dtids"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tx"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/txlimiter"
)

type txEngineState int

// The TxEngine can be in any of these states
const (
	NotServing txEngineState = iota
	Transitioning
	AcceptingReadAndWrite
	AcceptingReadOnly
)

func (state txEngineState) String() string {
	names := [...]string{
		"NotServing",
		"Transitioning",
		"AcceptReadWrite",
		"AcceptingReadOnly"}

	if state < NotServing || state > AcceptingReadOnly {
		return fmt.Sprintf("Unknown - %d", int(state))
	}

	return names[state]
}

// TxEngine is responsible for handling the tx-pool and keeping read-write, read-only or not-serving
// states. It will start and shut down the underlying tx-pool as required.
type TxEngine struct {
	env tabletenv.Env

	// stateLock is to protect state and beginRequests changes.
	stateLock sync.Mutex
	state     txEngineState

	// beginRequests is used to make sure that we do not make a state
	// transition while creating new transactions
	beginRequests sync.WaitGroup

	// twopcEnabled is the flag value of whether the user has enabled twopc or not.
	twopcEnabled bool
	// twopcAllowed is wether it is safe to allow two pc transactions or not.
	// If the primary tablet doesn't run with semi-sync we set this to false, and disallow any prepared calls.
	twopcAllowed        bool
	shutdownGracePeriod time.Duration
	coordinatorAddress  string
	abandonAge          time.Duration
	ticks               *timer.Timer

	// reservedConnStats keeps statistics about reserved connections
	reservedConnStats *servenv.TimingsWrapper

	txPool       *TxPool
	preparedPool *TxPreparedPool
	twoPC        *TwoPC
	dxNotify     func()
}

// NewTxEngine creates a new TxEngine.
func NewTxEngine(env tabletenv.Env, dxNotifier func()) *TxEngine {
	config := env.Config()
	te := &TxEngine{
		env:                 env,
		shutdownGracePeriod: config.GracePeriods.Shutdown,
		reservedConnStats:   env.Exporter().NewTimings("ReservedConnections", "Reserved connections stats", "operation"),
	}
	limiter := txlimiter.New(env)
	te.txPool = NewTxPool(env, limiter)
	// We initially allow twoPC (handles vttablet restarts).
	// We will disallow them, when a new tablet is promoted if semi-sync is turned off.
	te.twopcAllowed = true
	te.twopcEnabled = config.TwoPCEnable
	if te.twopcEnabled {
		if config.TwoPCAbandonAge <= 0 {
			log.Error("2PC abandon age not specified: Disabling 2PC")
			te.twopcEnabled = false
		}
	}
	te.abandonAge = config.TwoPCAbandonAge.Get()
	te.ticks = timer.NewTimer(te.abandonAge / 2)

	// Set the prepared pool capacity to something lower than
	// tx pool capacity. Those spare connections are needed to
	// perform metadata state change operations. Without this,
	// the system can deadlock if all connections get moved to
	// the TxPreparedPool.
	te.preparedPool = NewTxPreparedPool(config.TxPool.Size-2, te.twopcEnabled)
	readPool := connpool.NewPool(env, "TxReadPool", tabletenv.ConnPoolConfig{
		Size:        3,
		IdleTimeout: env.Config().TxPool.IdleTimeout,
	})
	te.twoPC = NewTwoPC(readPool)
	te.dxNotify = dxNotifier
	te.state = NotServing
	return te
}

// AcceptReadWrite will start accepting all transactions.
func (te *TxEngine) AcceptReadWrite() {
	te.transition(AcceptingReadAndWrite)
}

// AcceptReadOnly transitions to read-only mode. If current state
// is read-write, then we wait for shutdown and then transition.
func (te *TxEngine) AcceptReadOnly() {
	te.transition(AcceptingReadOnly)
}

func (te *TxEngine) transition(state txEngineState) {
	te.stateLock.Lock()
	defer te.stateLock.Unlock()
	if te.state == state {
		return
	}

	log.Infof("TxEngine transition: %v", state)

	// When we are transitioning from read write state, we should close all transactions.
	if te.state == AcceptingReadAndWrite {
		te.shutdownLocked()
	}

	te.state = state
	if te.twopcEnabled && te.state == AcceptingReadAndWrite {
		// If the prepared pool is not open, then we need to redo the prepared transactions
		// before we open the transaction engine to accept new writes.
		// This check is required because during a Promotion, we would have already setup the prepared pool
		// and redid the prepared transactions when we turn super_read_only off. So we don't need to do it again.
		if !te.preparedPool.IsOpen() {
			// We need to redo prepared transactions here to handle vttablet restarts.
			// If MySQL continues to work fine, then we won't end up redoing the prepared transactions as part of any RPC call
			// since VTOrc won't call `UndoDemotePrimary`. We need to do them as part of this transition.
			te.redoPreparedTransactionsLocked()
		}
		te.startTransactionWatcher()
	}
	te.txPool.Open(te.env.Config().DB.AppWithDB(), te.env.Config().DB.DbaWithDB(), te.env.Config().DB.AppDebugWithDB())
}

// RedoPreparedTransactions acquires the state lock and calls redoPreparedTransactionsLocked.
func (te *TxEngine) RedoPreparedTransactions() {
	if te.twopcEnabled {
		te.stateLock.Lock()
		defer te.stateLock.Unlock()
		te.redoPreparedTransactionsLocked()
	}
}

// redoPreparedTransactionsLocked redoes the prepared transactions.
// If there are errors, we choose to raise an alert and
// continue anyway. Serving traffic is considered more important
// than blocking everything for the sake of a few transactions.
// We do this async; so we do not end up blocking writes on
// failover for our setup tasks if using semi-sync replication.
func (te *TxEngine) redoPreparedTransactionsLocked() {
	oldState := te.state
	// We shutdown to ensure no other writes are in progress.
	te.shutdownLocked()
	defer func() {
		te.state = oldState
	}()

	if err := te.twoPC.Open(te.env.Config().DB); err != nil {
		te.env.Stats().InternalErrors.Add("TwopcOpen", 1)
		log.Errorf("Could not open TwoPC engine: %v", err)
		return
	}

	// We should only open the prepared pool and the transaction pool if the opening of twoPC pool is successful.
	// We use the prepared pool being open to know if we need to redo the prepared transactions.
	// So if we open the prepared pool and then opening of twoPC fails, we will never end up opening the twoPC pool at all!
	// This is why opening prepared pool after the twoPC pool is crucial for correctness.
	te.preparedPool.Open()
	// We have to defer opening the transaction pool because we call shutdown in the beginning that closes it.
	// We want to open the transaction pool after the prepareFromRedo has run. Also, we want this to run even if that fails.
	defer te.txPool.Open(te.env.Config().DB.AppWithDB(), te.env.Config().DB.DbaWithDB(), te.env.Config().DB.AppDebugWithDB())

	if err := te.prepareFromRedo(); err != nil {
		te.env.Stats().InternalErrors.Add("TwopcResurrection", 1)
		log.Errorf("Could not prepare transactions: %v", err)
	}
}

// Close will disregard common rules for when to kill transactions
// and wait forever for transactions to wrap up
func (te *TxEngine) Close() {
	log.Infof("TxEngine - started Close. Acquiring stateLock lock")
	te.stateLock.Lock()
	log.Infof("TxEngine - acquired stateLock")
	defer func() {
		te.state = NotServing
		te.stateLock.Unlock()
	}()
	if te.state == NotServing {
		log.Infof("TxEngine - state is not serving already")
		return
	}

	log.Infof("TxEngine - starting shutdown")
	te.shutdownLocked()
	log.Info("TxEngine: closed")
}

func (te *TxEngine) isTxPoolAvailable(addToWaitGroup func(int)) error {
	te.stateLock.Lock()
	defer te.stateLock.Unlock()

	canOpenTransactions := te.state == AcceptingReadOnly || te.state == AcceptingReadAndWrite
	if !canOpenTransactions {
		return vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, vterrors.TxEngineClosed, te.state)
	}
	addToWaitGroup(1)
	return nil
}

// Begin begins a transaction, and returns the associated transaction id and the
// statement(s) used to execute the begin (if any).
//
// Subsequent statements can access the connection through the transaction id.
func (te *TxEngine) Begin(ctx context.Context, savepointQueries []string, reservedID int64, setting *smartconnpool.Setting, options *querypb.ExecuteOptions) (int64, string, string, error) {
	span, ctx := trace.NewSpan(ctx, "TxEngine.Begin")
	defer span.Finish()

	// if the connection is already reserved then, we should not apply the settings.
	if reservedID != 0 && setting != nil {
		return 0, "", "", vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] should not mix reserved connection and connection with setting")
	}

	err := te.isTxPoolAvailable(te.beginRequests.Add)
	if err != nil {
		return 0, "", "", err
	}

	defer te.beginRequests.Done()
	conn, beginSQL, sessionStateChanges, err := te.txPool.Begin(ctx, options, te.state == AcceptingReadOnly, reservedID, savepointQueries, setting)
	if err != nil {
		return 0, "", "", err
	}
	defer conn.UnlockUpdateTime()
	return conn.ReservedID(), beginSQL, sessionStateChanges, err
}

// Commit commits the specified transaction and renews connection id if one exists.
func (te *TxEngine) Commit(ctx context.Context, transactionID int64) (int64, string, error) {
	span, ctx := trace.NewSpan(ctx, "TxEngine.Commit")
	defer span.Finish()
	var query string
	var err error
	connID, err := te.txFinish(transactionID, tx.TxCommit, func(conn *StatefulConnection) error {
		query, err = te.txPool.Commit(ctx, conn)
		return err
	})

	return connID, query, err
}

// Rollback rolls back the specified transaction.
func (te *TxEngine) Rollback(ctx context.Context, transactionID int64) (int64, error) {
	span, ctx := trace.NewSpan(ctx, "TxEngine.Rollback")
	defer span.Finish()

	return te.txFinish(transactionID, tx.TxRollback, func(conn *StatefulConnection) error {
		return te.txPool.Rollback(ctx, conn)
	})
}

func (te *TxEngine) txFinish(transactionID int64, reason tx.ReleaseReason, f func(*StatefulConnection) error) (int64, error) {
	conn, err := te.txPool.GetAndLock(transactionID, reason.String())
	if err != nil {
		return 0, err
	}
	err = f(conn)
	if err != nil || !conn.IsTainted() {
		conn.Release(reason)
		return 0, err
	}
	err = conn.Renew()
	if err != nil {
		conn.Release(tx.ConnRenewFail)
		return 0, err
	}
	return conn.ConnID, nil
}

// shutdownLocked closes the TxEngine. If the immediate flag is on,
// then all current transactions are immediately rolled back.
// Otherwise, the function waits for all current transactions
// to conclude. If a shutdown grace period was specified,
// the transactions are rolled back if they're not resolved
// by that time.
func (te *TxEngine) shutdownLocked() {
	log.Infof("TxEngine - called shutdownLocked")
	immediate := true
	if te.state == AcceptingReadAndWrite {
		immediate = false
	}

	// Unlock, wait for all begin requests to complete, and relock.
	te.state = Transitioning
	te.stateLock.Unlock()
	log.Infof("TxEngine - waiting for begin requests")
	te.beginRequests.Wait()
	log.Infof("TxEngine - acquiring state lock again")
	te.stateLock.Lock()
	log.Infof("TxEngine - state lock acquired again")

	poolEmpty := make(chan bool)
	rollbackDone := make(chan bool)
	// This goroutine decides if transactions have to be
	// forced to rollback, and if so, when. Once done,
	// the function closes rollbackDone, which can be
	// verified to make sure it won't kick in later.
	go func() {
		defer func() {
			te.env.LogError()
			close(rollbackDone)
		}()
		if immediate {
			// Immediately rollback everything and return.
			log.Info("Immediate shutdown: rolling back now.")
			te.txPool.scp.ShutdownNonTx()
			te.shutdownTransactions()
			return
		}
		// If not immediate, we start with shutting down non-tx (reserved)
		// connections.
		te.txPool.scp.ShutdownNonTx()
		if te.shutdownGracePeriod <= 0 {
			log.Info("No grace period specified: performing normal wait.")
			return
		}
		tmr := time.NewTimer(te.shutdownGracePeriod)
		defer tmr.Stop()
		select {
		case <-tmr.C:
			log.Info("Grace period exceeded: rolling back now.")
			te.shutdownTransactions()
		case <-poolEmpty:
			// The pool cleared before the timer kicked in. Just return.
			log.Info("Transactions completed before grace period: shutting down.")
		}
	}()
	// It is important to note, that we aren't rolling back prepared transactions here.
	// That is happneing in the same place where we are killing queries. This will block
	// until either all prepared transactions get resolved or rollbacked.
	log.Infof("TxEngine - waiting for empty txPool")
	te.txPool.WaitForEmpty()
	// If the goroutine is still running, signal that it can exit.
	close(poolEmpty)
	// Make sure the goroutine has returned.
	log.Infof("TxEngine - making sure the goroutine has returned")
	<-rollbackDone

	// We stop the transaction watcher so late, because if the user isn't running
	// with any shutdown grace period, we still want the watcher to run while we are waiting
	// for resolving transactions.
	log.Infof("TxEngine - stop transaction watcher")
	te.stopTransactionWatcher()

	// Mark the prepared pool closed.
	log.Infof("TxEngine - closing the txPool")
	te.txPool.Close()
	log.Infof("TxEngine - closing twoPC")
	te.twoPC.Close()
	log.Infof("TxEngine - closing the prepared pool")
	te.preparedPool.Close()
	log.Infof("TxEngine - finished shutdownLocked")
}

// prepareFromRedo replays and prepares the transactions
// from the redo log, loads previously failed transactions
// into the reserved list, and adjusts the txPool LastID
// to ensure there are no future collisions.
func (te *TxEngine) prepareFromRedo() error {
	ctx := tabletenv.LocalContext()
	var allErr concurrency.AllErrorRecorder
	prepared, failed, err := te.twoPC.ReadAllRedo(ctx)
	if err != nil {
		return err
	}

	maxid := int64(0)
outer:
	for _, preparedTx := range prepared {
		txid, err := dtids.TransactionID(preparedTx.Dtid)
		if err != nil {
			log.Errorf("Error extracting transaction ID from dtid: %v", err)
		}
		if txid > maxid {
			maxid = txid
		}
		// We need to redo the prepared transactions using a dba user because MySQL might still be in read only mode.
		conn, err := te.beginNewDbaConnection(ctx)
		if err != nil {
			allErr.RecordError(vterrors.Wrapf(err, "dtid - %v", preparedTx.Dtid))
			continue
		}
		for _, stmt := range preparedTx.Queries {
			conn.TxProperties().RecordQuery(stmt, te.env.Environment().Parser())
			_, err := conn.Exec(ctx, stmt, 1, false)
			if err != nil {
				allErr.RecordError(vterrors.Wrapf(err, "dtid - %v", preparedTx.Dtid))
				te.txPool.RollbackAndRelease(ctx, conn)
				continue outer
			}
		}
		// We should not use the external Prepare because
		// we don't want to write again to the redo log.
		err = te.preparedPool.Put(conn, preparedTx.Dtid)
		if err != nil {
			allErr.RecordError(vterrors.Wrapf(err, "dtid - %v", preparedTx.Dtid))
			continue
		}
	}
	for _, preparedTx := range failed {
		txid, err := dtids.TransactionID(preparedTx.Dtid)
		if err != nil {
			log.Errorf("Error extracting transaction ID from dtid: %v", err)
		}
		if txid > maxid {
			maxid = txid
		}
		te.preparedPool.SetFailed(preparedTx.Dtid)
	}
	te.txPool.AdjustLastID(maxid)
	log.Infof("TwoPC: Prepared %d transactions, and registered %d failures.", len(prepared), len(failed))
	return allErr.Error()
}

// shutdownTransactions rolls back all open transactions that are idol.
// These are transactions that are open but no write is executing on them right now.
// By definition, prepared transactions aren't part of them since these are transactions on which
// the user has issued a commit command. These transactions are rollbacked elsewhere when we kill all writes.
// This is used for transitioning from a primary to a non-primary serving type.
func (te *TxEngine) shutdownTransactions() {
	ctx := tabletenv.LocalContext()
	te.txPool.Shutdown(ctx)
}

// RollbackPrepared rollbacks all the prepared transactions.
// This should only be called after we are certain no other writes are in progress.
// If there were some other conflicting write in progress that hadn't been killed, then it could potentially go through
// and cause data corruption since we won't be able to prepare the transaction again.
func (te *TxEngine) RollbackPrepared() {
	ctx := tabletenv.LocalContext()
	for _, conn := range te.preparedPool.FetchAllForRollback() {
		te.txPool.Rollback(ctx, conn)
		conn.Release(tx.TxRollback)
	}
}

// startTransactionWatcher starts the watchdog goroutine, which looks for abandoned
// transactions and calls the notifier on them.
func (te *TxEngine) startTransactionWatcher() {
	te.ticks.Start(func() {
		ctx, cancel := context.WithTimeout(tabletenv.LocalContext(), te.abandonAge/4)
		defer cancel()

		// Raise alerts on prepares that have been unresolved for too long.
		// Use 5x abandonAge to give opportunity for transaction coordinator to resolve these redo logs.
		count, err := te.twoPC.CountUnresolvedRedo(ctx, time.Now().Add(-te.abandonAge*5))
		if err != nil {
			te.env.Stats().InternalErrors.Add("RedoWatcherFail", 1)
			log.Errorf("Error reading prepared transactions: %v", err)
		}
		te.env.Stats().Unresolved.Set("Prepares", count)

		// Notify lingering distributed transactions.
		count, err = te.twoPC.CountUnresolvedTransaction(ctx, time.Now().Add(-te.abandonAge))
		if err != nil {
			te.env.Stats().InternalErrors.Add("TransactionWatcherFail", 1)
			log.Errorf("Error reading unresolved transactions: %v", err)
			return
		}
		if count > 0 {
			te.dxNotify()
		}
	})
}

// stopTransactionWatcher stops the watchdog goroutine.
func (te *TxEngine) stopTransactionWatcher() {
	te.ticks.Stop()
}

// ReserveBegin creates a reserved connection, and in it opens a transaction
func (te *TxEngine) ReserveBegin(ctx context.Context, options *querypb.ExecuteOptions, preQueries []string, savepointQueries []string) (int64, string, error) {
	span, ctx := trace.NewSpan(ctx, "TxEngine.ReserveBegin")
	defer span.Finish()
	err := te.isTxPoolAvailable(te.beginRequests.Add)
	if err != nil {
		return 0, "", err
	}
	defer te.beginRequests.Done()

	conn, err := te.reserve(ctx, options, preQueries)
	if err != nil {
		return 0, "", err
	}
	defer conn.UnlockUpdateTime()
	_, sessionStateChanges, err := te.txPool.begin(ctx, options, te.state == AcceptingReadOnly, conn, savepointQueries)
	if err != nil {
		conn.Close()
		conn.Release(tx.ConnInitFail)
		return 0, "", err
	}
	return conn.ReservedID(), sessionStateChanges, nil
}

var noop = func(int) {}

// Reserve creates a reserved connection and returns the id to it
func (te *TxEngine) Reserve(ctx context.Context, options *querypb.ExecuteOptions, txID int64, preQueries []string) (int64, error) {
	span, ctx := trace.NewSpan(ctx, "TxEngine.Reserve")
	defer span.Finish()
	if txID == 0 {
		err := te.isTxPoolAvailable(noop)
		if err != nil {
			return 0, err
		}
		conn, err := te.reserve(ctx, options, preQueries)
		if err != nil {
			return 0, err
		}
		defer conn.Unlock()
		return conn.ReservedID(), nil
	}

	conn, err := te.txPool.GetAndLock(txID, "to reserve")
	if err != nil {
		return 0, err
	}
	defer conn.Unlock()

	err = te.taintConn(ctx, conn, preQueries)
	if err != nil {
		return 0, err
	}
	return conn.ReservedID(), nil
}

// Reserve creates a reserved connection and returns the id to it
func (te *TxEngine) reserve(ctx context.Context, options *querypb.ExecuteOptions, preQueries []string) (*StatefulConnection, error) {
	conn, err := te.txPool.scp.NewConn(ctx, options, nil)
	if err != nil {
		return nil, err
	}

	err = te.taintConn(ctx, conn, preQueries)
	if err != nil {
		return nil, err
	}

	return conn, err
}

func (te *TxEngine) taintConn(ctx context.Context, conn *StatefulConnection, preQueries []string) error {
	err := conn.Taint(ctx, te.reservedConnStats)
	if err != nil {
		return err
	}
	for _, query := range preQueries {
		_, err := conn.Exec(ctx, query, 0 /*maxrows*/, false /*wantFields*/)
		if err != nil {
			conn.Releasef("error during connection setup: %s\n%v", query, err)
			return err
		}
	}
	return nil
}

// Release closes the underlying connection.
func (te *TxEngine) Release(connID int64) error {
	conn, err := te.txPool.GetAndLock(connID, "for release")
	if err != nil {
		return err
	}

	conn.Release(tx.ConnRelease)

	return nil
}

// beginNewDbaConnection gets a new dba connection and starts a transaction in it.
// This should only be used to redo prepared transactions. All the other writes should use the normal pool.
func (te *TxEngine) beginNewDbaConnection(ctx context.Context) (*StatefulConnection, error) {
	dbConn, err := connpool.NewConn(ctx, te.env.Config().DB.DbaWithDB(), nil, nil, te.env)
	if err != nil {
		return nil, err
	}

	sc := &StatefulConnection{
		dbConn: &connpool.PooledConn{
			Conn: dbConn,
		},
		env: te.env,
	}

	_, _, err = te.txPool.begin(ctx, nil, false, sc, nil)
	return sc, err
}
