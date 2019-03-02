/*
Copyright 2017 Google Inc.

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
	"fmt"
	"sync"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/dtids"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/txlimiter"

	querypb "vitess.io/vitess/go/vt/proto/query"
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
// It does this in a concurrently safe way.
type TxEngine struct {
	// the following four fields are interconnected. `state` and `nextState` should be protected by the
	// `stateLock`
	//
	// `nextState` is used when state is Transitioning. This means that in order to change the state of
	// the transaction engine, we had to close transactions. `nextState` is the state we'll end up in
	// once the transactions are closed
	// while transitioning, `transitionSignal` will contain an open channel. Once the transition is
	// over, the channel is closed to signal to any waiting goroutines that the state change is done.
	stateLock        sync.Mutex
	state            txEngineState
	nextState        txEngineState
	transitionSignal chan struct{}

	// beginRequests is used to make sure that we do not make a state
	// transition while creating new transactions
	beginRequests sync.WaitGroup

	dbconfigs *dbconfigs.DBConfigs

	twopcEnabled        bool
	shutdownGracePeriod time.Duration
	coordinatorAddress  string
	abandonAge          time.Duration
	ticks               *timer.Timer

	txPool       *TxPool
	preparedPool *TxPreparedPool
	twoPC        *TwoPC
}

// NewTxEngine creates a new TxEngine.
func NewTxEngine(checker connpool.MySQLChecker, config tabletenv.TabletConfig) *TxEngine {
	te := &TxEngine{
		shutdownGracePeriod: time.Duration(config.TxShutDownGracePeriod * 1e9),
	}
	limiter := txlimiter.New(
		config.TransactionCap,
		config.TransactionLimitPerUser,
		config.EnableTransactionLimit,
		config.EnableTransactionLimitDryRun,
		config.TransactionLimitByUsername,
		config.TransactionLimitByPrincipal,
		config.TransactionLimitByComponent,
		config.TransactionLimitBySubcomponent,
	)
	te.txPool = NewTxPool(
		config.PoolNamePrefix,
		config.PoolImpl(),
		config.TransactionCap,
		config.FoundRowsPoolSize,
		time.Duration(config.TransactionTimeout*1e9),
		time.Duration(config.IdleTimeout*1e9),
		config.TxPoolMinActive,
		config.TxPoolWaiterCap,
		checker,
		limiter,
	)
	te.twopcEnabled = config.TwoPCEnable
	if te.twopcEnabled {
		if config.TwoPCCoordinatorAddress == "" {
			log.Error("Coordinator address not specified: Disabling 2PC")
			te.twopcEnabled = false
		}
		if config.TwoPCAbandonAge <= 0 {
			log.Error("2PC abandon age not specified: Disabling 2PC")
			te.twopcEnabled = false
		}
	}
	te.coordinatorAddress = config.TwoPCCoordinatorAddress
	te.abandonAge = time.Duration(config.TwoPCAbandonAge * 1e9)
	te.ticks = timer.NewTimer(te.abandonAge / 2)

	// Set the prepared pool capacity to something lower than
	// tx pool capacity. Those spare connections are needed to
	// perform metadata state change operations. Without this,
	// the system can deadlock if all connections get moved to
	// the TxPreparedPool.
	te.preparedPool = NewTxPreparedPool(config.TransactionCap - 2)
	readPool := connpool.New(
		config.PoolNamePrefix+"TxReadPool",
		config.PoolImpl(),
		3,
		time.Duration(config.IdleTimeout*1e9),
		0,
		checker,
	)
	te.twoPC = NewTwoPC(readPool)
	te.transitionSignal = make(chan struct{})
	// By immediately closing this channel, all state changes can simply be made blocking by issuing the
	// state change desired, and then selecting on this channel. It will contain an open channel while
	// transitioning.
	close(te.transitionSignal)
	te.nextState = -1
	te.state = NotServing
	return te
}

// Stop will stop accepting any new transactions. Transactions are immediately aborted.
func (te *TxEngine) Stop() error {
	te.beginRequests.Wait()
	te.stateLock.Lock()

	switch te.state {
	case NotServing:
		// Nothing to do. We are already stopped or stopping
		te.stateLock.Unlock()
		return nil

	case AcceptingReadAndWrite:
		return te.transitionTo(NotServing)

	case AcceptingReadOnly:
		// We are not master, so it's safe to kill all read-only transactions
		te.close(true)
		te.state = NotServing
		te.stateLock.Unlock()
		return nil

	case Transitioning:
		te.nextState = NotServing
		te.stateLock.Unlock()
		te.blockUntilEndOfTransition()
		return nil

	default:
		te.stateLock.Unlock()
		return te.unknownStateError()
	}
}

// AcceptReadWrite will start accepting all transactions.
// If transitioning from RO mode, transactions might need to be
// rolled back before new transactions can be accepts.
func (te *TxEngine) AcceptReadWrite() error {
	te.beginRequests.Wait()
	te.stateLock.Lock()

	switch te.state {
	case AcceptingReadAndWrite:
		// Nothing to do
		te.stateLock.Unlock()
		return nil

	case NotServing:
		te.state = AcceptingReadAndWrite
		te.open()
		te.stateLock.Unlock()
		return nil

	case Transitioning:
		te.nextState = AcceptingReadAndWrite
		te.stateLock.Unlock()
		te.blockUntilEndOfTransition()
		return nil

	case AcceptingReadOnly:
		// We need to restart the tx-pool to make sure we handle 2PC correctly
		te.close(true)
		te.state = AcceptingReadAndWrite
		te.open()
		te.stateLock.Unlock()
		return nil

	default:
		return te.unknownStateError()
	}
}

// AcceptReadOnly will start accepting read-only transactions, but not full read and write transactions.
// If the engine is currently accepting full read and write transactions, they need to
// be rolled back.
func (te *TxEngine) AcceptReadOnly() error {
	te.beginRequests.Wait()
	te.stateLock.Lock()
	switch te.state {
	case AcceptingReadOnly:
		// Nothing to do
		te.stateLock.Unlock()
		return nil

	case NotServing:
		te.state = AcceptingReadOnly
		te.open()
		te.stateLock.Unlock()
		return nil

	case AcceptingReadAndWrite:
		return te.transitionTo(AcceptingReadOnly)

	case Transitioning:
		te.nextState = AcceptingReadOnly
		te.stateLock.Unlock()
		te.blockUntilEndOfTransition()
		return nil

	default:
		te.stateLock.Unlock()
		return te.unknownStateError()
	}
}

// Begin begins a transaction, and returns the associated transaction id.
// Subsequent statements can access the connection through the transaction id.
func (te *TxEngine) Begin(ctx context.Context, options *querypb.ExecuteOptions) (int64, error) {
	te.stateLock.Lock()

	canOpenTransactions := te.state == AcceptingReadOnly || te.state == AcceptingReadAndWrite
	if !canOpenTransactions {
		// We are not in a state where we can start new transactions. Abort.
		te.stateLock.Unlock()
		return 0, vterrors.Errorf(vtrpc.Code_UNAVAILABLE, "tx engine can't accept new transactions in state %v", te.state)
	}

	isWriteTransaction := options == nil || options.TransactionIsolation != querypb.ExecuteOptions_CONSISTENT_SNAPSHOT_READ_ONLY
	if te.state == AcceptingReadOnly && isWriteTransaction {
		te.stateLock.Unlock()
		return 0, vterrors.Errorf(vtrpc.Code_UNAVAILABLE, "tx engine can only accept read-only transactions in current state")
	}

	// By Add() to beginRequests, we block others from initiating state
	// changes until we have finished adding this transaction
	te.beginRequests.Add(1)
	te.stateLock.Unlock()

	defer te.beginRequests.Done()
	return te.txPool.Begin(ctx, options)
}

// Commit commits the specified transaction.
func (te *TxEngine) Commit(ctx context.Context, transactionID int64, mc messageCommitter) error {
	return te.txPool.Commit(ctx, transactionID, mc)
}

// Rollback rolls back the specified transaction.
func (te *TxEngine) Rollback(ctx context.Context, transactionID int64) error {
	return te.txPool.Rollback(ctx, transactionID)
}

func (te *TxEngine) unknownStateError() error {
	return vterrors.Errorf(vtrpc.Code_INTERNAL, "unknown state %v", te.state)
}

func (te *TxEngine) blockUntilEndOfTransition() error {
	<-te.transitionSignal
	return nil
}

func (te *TxEngine) transitionTo(nextState txEngineState) error {
	te.state = Transitioning
	te.nextState = nextState
	te.transitionSignal = make(chan struct{})
	te.stateLock.Unlock()

	// We do this outside the lock so others can see our state while we close up waiting transactions
	te.close(true)

	te.stateLock.Lock()
	defer func() {
		// we use a lambda to make it clear in which order things need to happen
		te.stateLock.Unlock()
		close(te.transitionSignal)
	}()

	if te.state != Transitioning {
		return vterrors.Errorf(vtrpc.Code_INTERNAL, "this should never happen. the goroutine starting the transition should also finish it")
	}

	// Once we reach this point, it's as if our state is NotServing,
	// and we need to decide what the next step is
	switch te.nextState {
	case AcceptingReadAndWrite, AcceptingReadOnly:
		te.state = te.nextState
		te.open()
	case NotServing:
		te.state = NotServing
	case Transitioning:
		return vterrors.Errorf(vtrpc.Code_INTERNAL, "this should never happen. nextState cannot be transitioning")
	}

	te.nextState = -1

	return nil
}

// InitDBConfig must be called before Init.
func (te *TxEngine) InitDBConfig(dbcfgs *dbconfigs.DBConfigs) {
	te.dbconfigs = dbcfgs
}

// Init must be called once when vttablet starts for setting
// up the metadata tables.
func (te *TxEngine) Init() error {
	if te.twopcEnabled {
		return te.twoPC.Init(te.dbconfigs.SidecarDBName.Get(), te.dbconfigs.DbaWithDB())
	}
	return nil
}

// open opens the TxEngine. If 2pc is enabled, it restores
// all previously prepared transactions from the redo log.
// this should only be called when the state is already locked
func (te *TxEngine) open() {
	te.txPool.Open(te.dbconfigs.AppWithDB(), te.dbconfigs.DbaWithDB(), te.dbconfigs.AppDebugWithDB())

	if te.twopcEnabled && te.state == AcceptingReadAndWrite {
		te.twoPC.Open(te.dbconfigs)
		if err := te.prepareFromRedo(); err != nil {
			// If this operation fails, we choose to raise an alert and
			// continue anyway. Serving traffic is considered more important
			// than blocking everything for the sake of a few transactions.
			tabletenv.InternalErrors.Add("TwopcResurrection", 1)
			log.Errorf("Could not prepare transactions: %v", err)
		}
		te.startWatchdog()
	}
}

// StopGently will disregard common rules for when to kill transactions
// and wait forever for transactions to wrap up
func (te *TxEngine) StopGently() {
	te.stateLock.Lock()
	defer te.stateLock.Unlock()
	te.close(false)
	te.state = NotServing
}

// Close closes the TxEngine. If the immediate flag is on,
// then all current transactions are immediately rolled back.
// Otherwise, the function waits for all current transactions
// to conclude. If a shutdown grace period was specified,
// the transactions are rolled back if they're not resolved
// by that time.
func (te *TxEngine) close(immediate bool) {
	// Shut down functions are idempotent.
	// No need to check if 2pc is enabled.
	te.stopWatchdog()

	poolEmpty := make(chan bool)
	rollbackDone := make(chan bool)
	// This goroutine decides if transactions have to be
	// forced to rollback, and if so, when. Once done,
	// the function closes rollbackDone, which can be
	// verified to make sure it won't kick in later.
	go func() {
		defer func() {
			tabletenv.LogError()
			close(rollbackDone)
		}()
		if immediate {
			// Immediately rollback everything and return.
			log.Info("Immediate shutdown: rolling back now.")
			te.rollbackTransactions()
			return
		}
		if te.shutdownGracePeriod <= 0 {
			// No grace period was specified. Never rollback.
			te.rollbackPrepared()
			log.Info("No grace period specified: performing normal wait.")
			return
		}
		tmr := time.NewTimer(te.shutdownGracePeriod)
		defer tmr.Stop()
		select {
		case <-tmr.C:
			log.Info("Grace period exceeded: rolling back now.")
			te.rollbackTransactions()
		case <-poolEmpty:
			// The pool cleared before the timer kicked in. Just return.
			log.Info("Transactions completed before grace period: shutting down.")
		}
	}()
	te.txPool.WaitForEmpty()
	// If the goroutine is still running, signal that it can exit.
	close(poolEmpty)
	// Make sure the goroutine has returned.
	<-rollbackDone

	te.txPool.Close()
	te.twoPC.Close()
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
	for _, tx := range prepared {
		txid, err := dtids.TransactionID(tx.Dtid)
		if err != nil {
			log.Errorf("Error extracting transaction ID from ditd: %v", err)
		}
		if txid > maxid {
			maxid = txid
		}
		conn, err := te.txPool.LocalBegin(ctx, &querypb.ExecuteOptions{})
		if err != nil {
			allErr.RecordError(err)
			continue
		}
		for _, stmt := range tx.Queries {
			conn.RecordQuery(stmt)
			_, err := conn.Exec(ctx, stmt, 1, false)
			if err != nil {
				allErr.RecordError(err)
				te.txPool.LocalConclude(ctx, conn)
				continue outer
			}
		}
		// We should not use the external Prepare because
		// we don't want to write again to the redo log.
		err = te.preparedPool.Put(conn, tx.Dtid)
		if err != nil {
			allErr.RecordError(err)
			continue
		}
	}
	for _, tx := range failed {
		txid, err := dtids.TransactionID(tx.Dtid)
		if err != nil {
			log.Errorf("Error extracting transaction ID from ditd: %v", err)
		}
		if txid > maxid {
			maxid = txid
		}
		te.preparedPool.SetFailed(tx.Dtid)
	}
	te.txPool.AdjustLastID(maxid)
	log.Infof("Prepared %d transactions, and registered %d failures.", len(prepared), len(failed))
	return allErr.Error()
}

// rollbackTransactions rolls back all open transactions
// including the prepared ones.
// This is used for transitioning from a master to a non-master
// serving type.
func (te *TxEngine) rollbackTransactions() {
	ctx := tabletenv.LocalContext()
	for _, c := range te.preparedPool.FetchAll() {
		te.txPool.LocalConclude(ctx, c)
	}
	// The order of rollbacks is currently not material because
	// we don't allow new statements or commits during
	// this function. In case of any such change, this will
	// have to be revisited.
	te.txPool.RollbackNonBusy(ctx)
}

func (te *TxEngine) rollbackPrepared() {
	ctx := tabletenv.LocalContext()
	for _, c := range te.preparedPool.FetchAll() {
		te.txPool.LocalConclude(ctx, c)
	}
}

// startWatchdog starts the watchdog goroutine, which looks for abandoned
// transactions and calls the notifier on them.
func (te *TxEngine) startWatchdog() {
	te.ticks.Start(func() {
		ctx, cancel := context.WithTimeout(tabletenv.LocalContext(), te.abandonAge/4)
		defer cancel()

		// Raise alerts on prepares that have been unresolved for too long.
		// Use 5x abandonAge to give opportunity for watchdog to resolve these.
		count, err := te.twoPC.CountUnresolvedRedo(ctx, time.Now().Add(-te.abandonAge*5))
		if err != nil {
			tabletenv.InternalErrors.Add("WatchdogFail", 1)
			log.Errorf("Error reading unresolved prepares: '%v': %v", te.coordinatorAddress, err)
		}
		tabletenv.Unresolved.Set("Prepares", count)

		// Resolve lingering distributed transactions.
		txs, err := te.twoPC.ReadAbandoned(ctx, time.Now().Add(-te.abandonAge))
		if err != nil {
			tabletenv.InternalErrors.Add("WatchdogFail", 1)
			log.Errorf("Error reading transactions for 2pc watchdog: %v", err)
			return
		}
		if len(txs) == 0 {
			return
		}

		coordConn, err := vtgateconn.Dial(ctx, te.coordinatorAddress)
		if err != nil {
			tabletenv.InternalErrors.Add("WatchdogFail", 1)
			log.Errorf("Error connecting to coordinator '%v': %v", te.coordinatorAddress, err)
			return
		}
		defer coordConn.Close()

		var wg sync.WaitGroup
		for tx := range txs {
			wg.Add(1)
			go func(dtid string) {
				defer wg.Done()
				if err := coordConn.ResolveTransaction(ctx, dtid); err != nil {
					tabletenv.InternalErrors.Add("WatchdogFail", 1)
					log.Errorf("Error notifying for dtid %s: %v", dtid, err)
				}
			}(tx)
		}
		wg.Wait()
	})
}

// stopWatchdog stops the watchdog goroutine.
func (te *TxEngine) stopWatchdog() {
	te.ticks.Stop()
}
