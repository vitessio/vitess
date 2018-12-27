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
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/dtids"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/txlimiter"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

type TxEngineState int

// The TxEngine can be in any of these states
const (
	NotServing TxEngineState = iota
	Transitioning
	AcceptingReadAndWrite
	AcceptingReadOnly
)

func (state TxEngineState) String() string {
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

// TxEngine handles transactions.
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
	state            TxEngineState
	nextState        TxEngineState
	transitionSignal chan struct{}

	dbconfigs *dbconfigs.DBConfigs

	isOpen, twopcEnabled bool
	shutdownGracePeriod  time.Duration
	coordinatorAddress   string
	abandonAge           time.Duration
	ticks                *timer.Timer

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
		config.TransactionCap,
		config.FoundRowsPoolSize,
		time.Duration(config.TransactionTimeout*1e9),
		time.Duration(config.IdleTimeout*1e9),
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
		3,
		time.Duration(config.IdleTimeout*1e9),
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

func (te *TxEngine) Stop() error {
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
		te.Close(true)
		te.state = NotServing
		te.stateLock.Unlock()
		return nil

	case Transitioning:
		te.nextState = NotServing
		te.stateLock.Unlock()
		return nil

	default:
		te.stateLock.Unlock()
		return vterrors.Errorf(vtrpc.Code_INTERNAL, "unknown state %v", te.state)
	}
}

func (te *TxEngine) AcceptReadWrite() error {
	te.stateLock.Lock()
	defer te.stateLock.Unlock()
	switch te.state {
	case AcceptingReadAndWrite:
		// Nothing to do
		return nil

	case NotServing:
		te.Open()
		te.state = AcceptingReadAndWrite
		return nil

	case Transitioning:
		te.nextState = AcceptingReadAndWrite
		return nil

	case AcceptingReadOnly:
		// We need to restart the tx-pool to make sure we handle 2PC correctly
		te.Close(true)
		te.Open()
		te.state = AcceptingReadAndWrite
		return nil

	default:
		return vterrors.Errorf(vtrpc.Code_INTERNAL, "unknown state %v", te.state)
	}
}

func (te *TxEngine) AcceptReadOnly() error {
	te.stateLock.Lock()
	switch te.state {
	case AcceptingReadOnly:
		// Nothing to do
		te.stateLock.Unlock()
		return nil

	case NotServing:
		te.Open()
		te.state = AcceptingReadOnly
		te.stateLock.Unlock()
		return nil

	case AcceptingReadAndWrite:
		return te.transitionTo(AcceptingReadOnly)

	default:
		te.stateLock.Unlock()
		return vterrors.NewWithoutCode("not implemented yet")
	}
}

// BlockUntilEndOfTransition blocks the current goroutine until it has finished transitioning into a new state.
// If no transition is in progress, it returns immediately.
func (te *TxEngine) BlockUntilEndOfTransition(ctx context.Context) error {
	select {
	case <-te.transitionSignal:
		return nil
	case <-ctx.Done():
		return vterrors.Errorf(vtrpc.Code_DEADLINE_EXCEEDED, "transition did not finish in a timely fashion")
	}
}

func (te *TxEngine) transitionTo(nextState TxEngineState) error {
	te.state = Transitioning
	te.nextState = nextState
	te.transitionSignal = make(chan struct{})
	te.stateLock.Unlock()

	// We do this outside the lock so others can see our state while we close up waiting transactions
	te.Close(false)

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
		te.Open()
	case NotServing:
		// nothing to do
	case Transitioning:
		return vterrors.Errorf(vtrpc.Code_INTERNAL, "this should never happen. nextState cannot be transitioning")
	}

	te.state = te.nextState
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

// Open opens the TxEngine. If 2pc is enabled, it restores
// all previously prepared transactions from the redo log.
func (te *TxEngine) Open() {
	if te.isOpen {
		return
	}
	te.txPool.Open(te.dbconfigs.AppWithDB(), te.dbconfigs.DbaWithDB(), te.dbconfigs.AppDebugWithDB())
	if !te.twopcEnabled {
		te.isOpen = true
		return
	}

	te.twoPC.Open(te.dbconfigs)
	if err := te.prepareFromRedo(); err != nil {
		// If this operation fails, we choose to raise an alert and
		// continue anyway. Serving traffic is considered more important
		// than blocking everything for the sake of a few transactions.
		tabletenv.InternalErrors.Add("TwopcResurrection", 1)
		log.Errorf("Could not prepare transactions: %v", err)
	}
	te.startWatchdog()
	te.isOpen = true
}

// Close closes the TxEngine. If the immediate flag is on,
// then all current transactions are immediately rolled back.
// Otherwise, the function waits for all current transactions
// to conclude. If a shutdown grace period was specified,
// the transactions are rolled back if they're not resolved
// by that time.
func (te *TxEngine) Close(immediate bool) {
	if !te.isOpen {
		return
	}
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
			log.Info("No grace period specified: performing normal wait.")
			return
		}
		tmr := time.NewTimer(te.shutdownGracePeriod)
		defer tmr.Stop()
		select {
		case <-tmr.C:
			// The grace period has passed. Rollback, but don't touch the 2pc transactions.
			log.Info("Grace period exceeded: rolling back non-2pc transactions now.")
			te.txPool.RollbackNonBusy(tabletenv.LocalContext())
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
	te.isOpen = false
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
	// The order of rollbacks is currently not material because
	// we don't allow new statements or commits during
	// this function. In case of any such change, this will
	// have to be revisited.
	te.txPool.RollbackNonBusy(ctx)
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
