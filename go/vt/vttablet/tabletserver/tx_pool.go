// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysqlconn"
	"github.com/youtube/vitess/go/pools"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/timer"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/connpool"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/messager"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"github.com/youtube/vitess/go/vt/vterrors"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// These consts identify how a transaction was resolved.
const (
	TxClose    = "close"
	TxCommit   = "commit"
	TxRollback = "rollback"
	TxPrepare  = "prepare"
	TxKill     = "kill"
)

const txLogInterval = time.Duration(1 * time.Minute)

var (
	txOnce  sync.Once
	txStats = stats.NewTimings("Transactions")
)

// TxPool is the transaction pool for the query service.
type TxPool struct {
	conns      *connpool.Pool
	activePool *pools.Numbered
	lastID     sync2.AtomicInt64
	timeout    sync2.AtomicDuration
	ticks      *timer.Timer
	checker    MySQLChecker
	// Tracking culprits that cause tx pool full errors.
	logMu   sync.Mutex
	lastLog time.Time
}

// NewTxPool creates a new TxPool. It's not operational until it's Open'd.
func NewTxPool(
	name string,
	capacity int,
	timeout time.Duration,
	idleTimeout time.Duration,
	checker MySQLChecker) *TxPool {
	axp := &TxPool{
		conns:      connpool.New(name, capacity, idleTimeout, checker),
		activePool: pools.NewNumbered(),
		lastID:     sync2.NewAtomicInt64(time.Now().UnixNano()),
		timeout:    sync2.NewAtomicDuration(timeout),
		ticks:      timer.NewTimer(timeout / 10),
		checker:    checker,
	}
	txOnce.Do(func() {
		// Careful: conns also exports name+"xxx" vars,
		// but we know it doesn't export Timeout.
		stats.Publish(name+"Timeout", stats.DurationFunc(axp.timeout.Get))
	})
	return axp
}

// Open makes the TxPool operational. This also starts the transaction killer
// that will kill long-running transactions.
func (axp *TxPool) Open(appParams, dbaParams *sqldb.ConnParams) {
	log.Infof("Starting transaction id: %d", axp.lastID)
	axp.conns.Open(appParams, dbaParams)
	axp.ticks.Start(func() { axp.transactionKiller() })
}

// Close closes the TxPool. A closed pool can be reopened.
func (axp *TxPool) Close() {
	axp.ticks.Stop()
	for _, v := range axp.activePool.GetOutdated(time.Duration(0), "for closing") {
		conn := v.(*TxConnection)
		log.Warningf("killing transaction for shutdown: %s", conn.Format(nil))
		tabletenv.InternalErrors.Add("StrayTransactions", 1)
		conn.Close()
		conn.conclude(TxClose)
	}
	axp.conns.Close()
}

// AdjustLastID adjusts the last transaction id to be at least
// as large as the input value. This will ensure that there are
// no dtid collisions with future transactions.
func (axp *TxPool) AdjustLastID(id int64) {
	if current := axp.lastID.Get(); current < id {
		log.Infof("Adjusting transaction id to: %d", id)
		axp.lastID.Set(id)
	}
}

// RollbackNonBusy rolls back all transactions that are not in use.
// Transactions can be in use for situations like executing statements
// or in prepared state.
func (axp *TxPool) RollbackNonBusy(ctx context.Context) {
	for _, v := range axp.activePool.GetOutdated(time.Duration(0), "for transition") {
		axp.LocalConclude(ctx, v.(*TxConnection))
	}
}

func (axp *TxPool) transactionKiller() {
	defer tabletenv.LogError()
	for _, v := range axp.activePool.GetOutdated(time.Duration(axp.Timeout()), "for rollback") {
		conn := v.(*TxConnection)
		log.Warningf("killing transaction (exceeded timeout: %v): %s", axp.Timeout(), conn.Format(nil))
		tabletenv.KillStats.Add("Transactions", 1)
		conn.Close()
		conn.conclude(TxKill)
	}
}

// WaitForEmpty waits until all active transactions are completed.
func (axp *TxPool) WaitForEmpty() {
	axp.activePool.WaitForEmpty()
}

// Begin begins a transaction, and returns the associated transaction id.
// Subsequent statements can access the connection through the transaction id.
func (axp *TxPool) Begin(ctx context.Context) (int64, error) {
	conn, err := axp.conns.Get(ctx)
	if err != nil {
		switch err {
		case connpool.ErrConnPoolClosed:
			return 0, err
		case pools.ErrTimeout:
			axp.LogActive()
			return 0, vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, "transaction pool connection limit exceeded")
		}
		return 0, err
	}
	if _, err := conn.Exec(ctx, "begin", 1, false); err != nil {
		conn.Recycle()
		return 0, err
	}
	transactionID := axp.lastID.Add(1)
	axp.activePool.Register(
		transactionID,
		newTxConnection(
			conn,
			transactionID,
			axp,
			callerid.ImmediateCallerIDFromContext(ctx),
			callerid.EffectiveCallerIDFromContext(ctx),
		),
	)
	return transactionID, nil
}

// Commit commits the specified transaction.
func (axp *TxPool) Commit(ctx context.Context, transactionID int64, messager *messager.Engine) error {
	conn, err := axp.Get(transactionID, "for commit")
	if err != nil {
		return err
	}
	return axp.LocalCommit(ctx, conn, messager)
}

// Rollback rolls back the specified transaction.
func (axp *TxPool) Rollback(ctx context.Context, transactionID int64) error {
	conn, err := axp.Get(transactionID, "for rollback")
	if err != nil {
		return err
	}
	return axp.localRollback(ctx, conn)
}

// Get fetches the connection associated to the transactionID.
// You must call Recycle on TxConnection once done.
func (axp *TxPool) Get(transactionID int64, reason string) (*TxConnection, error) {
	v, err := axp.activePool.Get(transactionID, reason)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_ABORTED, "transaction %d: %v", transactionID, err)
	}
	return v.(*TxConnection), nil
}

// LocalBegin is equivalent to Begin->Get.
// It's used for executing transactions within a request. It's safe
// to always call LocalConclude at the end.
func (axp *TxPool) LocalBegin(ctx context.Context) (*TxConnection, error) {
	transactionID, err := axp.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return axp.Get(transactionID, "for local query")
}

// LocalCommit is the commit function for LocalBegin.
func (axp *TxPool) LocalCommit(ctx context.Context, conn *TxConnection, messager *messager.Engine) error {
	defer conn.conclude(TxCommit)
	defer messager.LockDB(conn.NewMessages, conn.ChangedMessages)()
	txStats.Add("Completed", time.Now().Sub(conn.StartTime))
	if _, err := conn.Exec(ctx, "commit", 1, false); err != nil {
		conn.Close()
		return err
	}
	messager.UpdateCaches(conn.NewMessages, conn.ChangedMessages)
	return nil
}

// LocalConclude concludes a transaction started by LocalBegin.
// If the transaction was not previously concluded, it's rolled back.
func (axp *TxPool) LocalConclude(ctx context.Context, conn *TxConnection) {
	if conn.DBConn != nil {
		_ = axp.localRollback(ctx, conn)
	}
}

func (axp *TxPool) localRollback(ctx context.Context, conn *TxConnection) error {
	defer conn.conclude(TxRollback)
	txStats.Add("Aborted", time.Now().Sub(conn.StartTime))
	if _, err := conn.Exec(ctx, "rollback", 1, false); err != nil {
		conn.Close()
		return err
	}
	return nil
}

// LogActive causes all existing transactions to be logged when they complete.
// The logging is throttled to no more than once every txLogInterval.
func (axp *TxPool) LogActive() {
	axp.logMu.Lock()
	defer axp.logMu.Unlock()
	if time.Now().Sub(axp.lastLog) < txLogInterval {
		return
	}
	axp.lastLog = time.Now()
	conns := axp.activePool.GetAll()
	for _, c := range conns {
		c.(*TxConnection).LogToFile.Set(1)
	}
}

// Timeout returns the transaction timeout.
func (axp *TxPool) Timeout() time.Duration {
	return axp.timeout.Get()
}

// SetTimeout sets the transaction timeout.
func (axp *TxPool) SetTimeout(timeout time.Duration) {
	axp.timeout.Set(timeout)
	axp.ticks.SetInterval(timeout / 10)
}

// TxConnection is meant for executing transactions. It can return itself to
// the tx pool correctly. It also does not retry statements if there
// are failures.
type TxConnection struct {
	*connpool.DBConn
	TransactionID     int64
	pool              *TxPool
	StartTime         time.Time
	EndTime           time.Time
	Queries           []string
	NewMessages       map[string][]*messager.MessageRow
	ChangedMessages   map[string][]string
	Conclusion        string
	LogToFile         sync2.AtomicInt32
	ImmediateCallerID *querypb.VTGateCallerID
	EffectiveCallerID *vtrpcpb.CallerID
}

func newTxConnection(conn *connpool.DBConn, transactionID int64, pool *TxPool, immediate *querypb.VTGateCallerID, effective *vtrpcpb.CallerID) *TxConnection {
	return &TxConnection{
		DBConn:            conn,
		TransactionID:     transactionID,
		pool:              pool,
		StartTime:         time.Now(),
		NewMessages:       make(map[string][]*messager.MessageRow),
		ChangedMessages:   make(map[string][]string),
		ImmediateCallerID: immediate,
		EffectiveCallerID: effective,
	}
}

// Exec executes the statement for the current transaction.
func (txc *TxConnection) Exec(ctx context.Context, query string, maxrows int, wantfields bool) (*sqltypes.Result, error) {
	r, err := txc.DBConn.ExecOnce(ctx, query, maxrows, wantfields)
	if err != nil {
		if mysqlconn.IsConnErr(err) {
			txc.pool.checker.CheckMySQL()
		}
		return nil, err
	}
	return r, nil
}

// BeginAgain commits the existing transaction and begins a new one
func (txc *TxConnection) BeginAgain(ctx context.Context) error {
	if _, err := txc.DBConn.Exec(ctx, "commit", 1, false); err != nil {
		return err
	}
	if _, err := txc.DBConn.Exec(ctx, "begin", 1, false); err != nil {
		return err
	}
	return nil
}

// Recycle returns the connection to the pool. The transaction remains
// active.
func (txc *TxConnection) Recycle() {
	if txc.IsClosed() {
		txc.conclude(TxClose)
	} else {
		txc.pool.activePool.Put(txc.TransactionID)
	}
}

// RecordQuery records the query against this transaction.
func (txc *TxConnection) RecordQuery(query string) {
	txc.Queries = append(txc.Queries, query)
}

func (txc *TxConnection) conclude(conclusion string) {
	txc.pool.activePool.Unregister(txc.TransactionID)
	txc.DBConn.Recycle()
	txc.DBConn = nil
	txc.log(conclusion)
}

func (txc *TxConnection) log(conclusion string) {
	txc.Conclusion = conclusion
	txc.EndTime = time.Now()

	username := callerid.GetPrincipal(txc.EffectiveCallerID)
	if username == "" {
		username = callerid.GetUsername(txc.ImmediateCallerID)
	}
	duration := txc.EndTime.Sub(txc.StartTime)
	tabletenv.UserTransactionCount.Add([]string{username, conclusion}, 1)
	tabletenv.UserTransactionTimesNs.Add([]string{username, conclusion}, int64(duration))
	if txc.LogToFile.Get() != 0 {
		log.Infof("Logged transaction: %s", txc.Format(nil))
	}
	tabletenv.TxLogger.Send(txc)
}

// EventTime returns the time the event was created.
func (txc *TxConnection) EventTime() time.Time {
	return txc.EndTime
}

// Format returns a printable version of the connection info.
func (txc *TxConnection) Format(params url.Values) string {
	return fmt.Sprintf(
		"%v\t'%v'\t'%v'\t%v\t%v\t%.6f\t%v\t%v\t\n",
		txc.TransactionID,
		callerid.GetPrincipal(txc.EffectiveCallerID),
		callerid.GetUsername(txc.ImmediateCallerID),
		txc.StartTime.Format(time.StampMicro),
		txc.EndTime.Format(time.StampMicro),
		txc.EndTime.Sub(txc.StartTime).Seconds(),
		txc.Conclusion,
		strings.Join(txc.Queries, ";"),
	)
}
