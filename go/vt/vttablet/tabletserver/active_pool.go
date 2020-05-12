/*
Copyright 2020 The Vitess Authors.

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
	"time"

	"vitess.io/vitess/go/vt/servenv"

	"vitess.io/vitess/go/pools"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/tx"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

//ActivePool keeps track of currently and future active connections
//it's used whenever the session has some state that requires a dedicated connection
type ActivePool struct {
	// conns is the 'regular' pool. By default, connections
	// are pulled from here for starting transactions.
	conns *connpool.Pool

	// foundRowsPool is the alternate pool that creates
	// connections with CLIENT_FOUND_ROWS flag set. A separate
	// pool is needed because this option can only be set at
	// connection time.
	foundRowsPool *connpool.Pool
	active        *pools.Numbered
	lastID        sync2.AtomicInt64
	env           tabletenv.Env

	// Tracking culprits that cause tx pool full errors.
	txStats *servenv.TimingsWrapper
}

//NewActivePool creates an ActivePool
func NewActivePool(env tabletenv.Env) *ActivePool {
	config := env.Config()

	return &ActivePool{
		env:           env,
		conns:         connpool.NewPool(env, "TransactionPool", config.TxPool),
		foundRowsPool: connpool.NewPool(env, "FoundRowsPool", config.TxPool),
		active:        pools.NewNumbered(),
		lastID:        sync2.NewAtomicInt64(time.Now().UnixNano()),
		txStats:       env.Exporter().NewTimings("Transactions", "Transaction stats", "operation"),
	}
}

// Open makes the TxPool operational. This also starts the transaction killer
// that will kill long-running transactions.
func (ap *ActivePool) Open(appParams, dbaParams, appDebugParams dbconfigs.Connector) {
	log.Infof("Starting transaction id: %d", ap.lastID)
	ap.conns.Open(appParams, dbaParams, appDebugParams)
	foundRowsParam, _ := appParams.MysqlParams()
	foundRowsParam.EnableClientFoundRows()
	appParams = dbconfigs.New(foundRowsParam)
	ap.foundRowsPool.Open(appParams, dbaParams, appDebugParams)
}

// Close closes the TxPool. A closed pool can be reopened.
func (ap *ActivePool) Close() {
	for _, v := range ap.active.GetOutdated(time.Duration(0), "for closing") {
		conn := v.(*TxConnection)
		log.Warningf("killing transaction for shutdown: %s", conn.String())
		ap.env.Stats().InternalErrors.Add("StrayTransactions", 1)
		conn.Close()
		conn.conclude(TxClose, "pool closed")
	}
	ap.conns.Close()
	ap.foundRowsPool.Close()
}

// AdjustLastID adjusts the last transaction id to be at least
// as large as the input value. This will ensure that there are
// no dtid collisions with future transactions.
func (ap *ActivePool) AdjustLastID(id int64) {
	if current := ap.lastID.Get(); current < id {
		log.Infof("Adjusting transaction id to: %d", id)
		ap.lastID.Set(id)
	}
}

// GetOutdated returns a list of connections that are older than age.
// It does not return any connections that are in use.
func (ap *ActivePool) GetOutdated(age time.Duration, purpose string) []*TxConnection {
	return mapToTxConn(ap.active.GetOutdated(age, purpose))
}

func mapToTxConn(outdated []interface{}) []*TxConnection {
	result := make([]*TxConnection, len(outdated))
	for i, el := range outdated {
		result[i] = el.(*TxConnection)
	}
	return result
}

// WaitForEmpty returns as soon as the pool becomes empty
func (ap *ActivePool) WaitForEmpty() {
	ap.active.WaitForEmpty()
}

// Get locks the connection for use. It accepts a purpose as a string.
// If it cannot be found, it returns a "not found" error. If in use,
// it returns a "in use: purpose" error.
func (ap *ActivePool) Get(id int64, reason string) (*TxConnection, error) {
	conn, err := ap.active.Get(id, reason)
	if err != nil {
		return nil, err
	}
	return conn.(*TxConnection), nil
}

//NewConn creates a new TxConn. It will be created from either the normal pool or
//the found_rows pool, depending on the options provided
func (ap *ActivePool) NewConn(ctx context.Context, options *querypb.ExecuteOptions, effectiveCaller, immediateCaller string, f func(*TxConnection) error) (*TxConnection, error) {
	var conn *connpool.DBConn
	var err error

	if options.GetClientFoundRows() {
		conn, err = ap.foundRowsPool.Get(ctx)
	} else {
		conn, err = ap.conns.Get(ctx)
	}
	if err != nil {
		return nil, err
	}

	transactionID := ap.lastID.Add(1)

	txConn := &TxConnection{
		dbConn:          conn,
		TransactionID:   transactionID,
		pool:            ap,
		StartTime:       time.Now(),
		EffectiveCaller: effectiveCaller,
		ImmediateCaller: immediateCaller,
		env:             ap.env,
		txStats:         ap.txStats,
	}

	err = f(txConn)
	if err != nil {
		txConn.Release(ConnInitFail)
		return nil, err
	}
	err = ap.active.Register(
		txConn.TransactionID,
		txConn,
		options.GetWorkload() != querypb.ExecuteOptions_DBA,
	)
	if err != nil {
		txConn.Release(ConnInitFail)
		return nil, err
	}
	return txConn, nil
}

//ForAllTxProperties executes a function an every connection that has a not-nil TxProperties
func (ap *ActivePool) ForAllTxProperties(f func(*TxProperties)) {
	for _, connection := range mapToTxConn(ap.active.GetAll()) {
		props := connection.TxProps
		if props != nil {
			f(props)
		}
	}
}

// Unregister forgets the specified connection.  If the connection is not present, it's ignored.
func (ap *ActivePool) Unregister(id tx.ConnID, reason string) {
	ap.active.Unregister(id, reason)
}

//MarkAsInactive marks the connection as not in use at the moment
func (ap *ActivePool) MarkAsInactive(id tx.ConnID) {
	ap.active.Put(id)
}

// Capacity returns the pool capacity.
func (ap *ActivePool) Capacity() int {
	return int(ap.conns.Capacity())
}
