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
	"fmt"
	"time"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/vt/callerid"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/servenv"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/tx"

	"context"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

// StatefulConnection is used in the situations where we need a dedicated connection for a vtgate session.
// This is used for transactions and reserved connections.
// NOTE: After use, if must be returned either by doing a Unlock() or a Release().
type StatefulConnection struct {
	pool           *StatefulConnectionPool
	dbConn         *connpool.DBConn
	ConnID         tx.ConnID
	env            tabletenv.Env
	txProps        *tx.Properties
	reservedProps  *Properties
	tainted        bool
	enforceTimeout bool
}

// Properties contains meta information about the connection
type Properties struct {
	EffectiveCaller *vtrpcpb.CallerID
	ImmediateCaller *querypb.VTGateCallerID
	StartTime       time.Time
	Stats           *servenv.TimingsWrapper
}

// Close closes the underlying connection. When the connection is Unblocked, it will be Released
func (sc *StatefulConnection) Close() {
	if sc.dbConn != nil {
		sc.dbConn.Close()
	}
}

// IsClosed returns true when the connection is still operational
func (sc *StatefulConnection) IsClosed() bool {
	return sc.dbConn == nil || sc.dbConn.IsClosed()
}

// IsInTransaction returns true when the connection has tx state
func (sc *StatefulConnection) IsInTransaction() bool {
	return sc.txProps != nil
}

// Exec executes the statement in the dedicated connection
func (sc *StatefulConnection) Exec(ctx context.Context, query string, maxrows int, wantfields bool) (*sqltypes.Result, error) {
	if sc.IsClosed() {
		if sc.IsInTransaction() {
			return nil, vterrors.Errorf(vtrpcpb.Code_ABORTED, "transaction was aborted: %v", sc.txProps.Conclusion)
		}
		return nil, vterrors.New(vtrpcpb.Code_ABORTED, "connection was aborted")
	}
	r, err := sc.dbConn.ExecOnce(ctx, query, maxrows, wantfields)
	if err != nil {
		if mysql.IsConnErr(err) {
			select {
			case <-ctx.Done():
				// If the context is done, the query was killed.
				// So, don't trigger a mysql check.
			default:
				sc.env.CheckMySQL()
			}
			return nil, err
		}
		return nil, err
	}
	return r, nil
}

func (sc *StatefulConnection) execWithRetry(ctx context.Context, query string, maxrows int, wantfields bool) error {
	if sc.IsClosed() {
		return vterrors.New(vtrpcpb.Code_CANCELED, "connection is closed")
	}
	if _, err := sc.dbConn.Exec(ctx, query, maxrows, wantfields); err != nil {
		return err
	}
	return nil
}

// FetchNext returns the next result set.
func (sc *StatefulConnection) FetchNext(ctx context.Context, maxrows int, wantfields bool) (*sqltypes.Result, error) {
	if sc.IsClosed() {
		return nil, vterrors.New(vtrpcpb.Code_CANCELED, "connection is closed")
	}
	return sc.dbConn.FetchNext(ctx, maxrows, wantfields)
}

// Unlock returns the connection to the pool. The connection remains active.
// This method is idempotent and can be called multiple times
func (sc *StatefulConnection) Unlock() {
	// when in a transaction, we count from the time created, so each use of the connection does not update the time
	updateTime := !sc.IsInTransaction()
	sc.unlock(updateTime)
}

// UnlockUpdateTime returns the connection to the pool. The connection remains active.
// This method is idempotent and can be called multiple times
func (sc *StatefulConnection) UnlockUpdateTime() {
	sc.unlock(true)
}

func (sc *StatefulConnection) unlock(updateTime bool) {
	if sc.dbConn == nil {
		return
	}
	if sc.dbConn.IsClosed() {
		sc.Releasef("unlocked closed connection")
	} else {
		sc.pool.markAsNotInUse(sc, updateTime)
	}
}

// Release is used when the connection will not be used ever again.
// The underlying dbConn is removed so that this connection cannot be used by mistake.
func (sc *StatefulConnection) Release(reason tx.ReleaseReason) {
	sc.Releasef(reason.String())
}

// Releasef is used when the connection will not be used ever again.
// The underlying dbConn is removed so that this connection cannot be used by mistake.
func (sc *StatefulConnection) Releasef(reasonFormat string, a ...interface{}) {
	if sc.dbConn == nil {
		return
	}
	sc.pool.unregister(sc.ConnID, fmt.Sprintf(reasonFormat, a...))
	sc.dbConn.Recycle()
	sc.dbConn = nil
	sc.logReservedConn()
}

// Renew the existing connection with new connection id.
func (sc *StatefulConnection) Renew() error {
	err := sc.pool.renewConn(sc)
	if err != nil {
		sc.Close()
		return vterrors.Wrap(err, "connection renew failed")
	}
	return nil
}

// String returns a printable version of the connection info.
func (sc *StatefulConnection) String() string {
	return fmt.Sprintf(
		"%v\t%s",
		sc.ConnID,
		sc.txProps.String(),
	)
}

// Current returns the currently executing query
func (sc *StatefulConnection) Current() string {
	return sc.dbConn.Current()
}

// ID returns the mysql connection ID
func (sc *StatefulConnection) ID() int64 {
	return sc.dbConn.ID()
}

// Kill kills the currently executing query and connection
func (sc *StatefulConnection) Kill(reason string, elapsed time.Duration) error {
	return sc.dbConn.Kill(reason, elapsed)
}

// TxProperties returns the transactional properties of the connection
func (sc *StatefulConnection) TxProperties() *tx.Properties {
	return sc.txProps
}

// ReservedID returns the identifier for this connection
func (sc *StatefulConnection) ReservedID() tx.ConnID {
	return sc.ConnID
}

// UnderlyingDBConn returns the underlying database connection
func (sc *StatefulConnection) UnderlyingDBConn() *connpool.DBConn {
	return sc.dbConn
}

// CleanTxState cleans out the current transaction state
func (sc *StatefulConnection) CleanTxState() {
	sc.txProps = nil
}

// Stats implements the tx.IStatefulConnection interface
func (sc *StatefulConnection) Stats() *tabletenv.Stats {
	return sc.env.Stats()
}

// Taint taints the existing connection.
func (sc *StatefulConnection) Taint(ctx context.Context, stats *servenv.TimingsWrapper) error {
	if sc.dbConn == nil {
		return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "connection is closed")
	}
	if sc.tainted {
		return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "connection is already reserved")
	}
	immediateCaller := callerid.ImmediateCallerIDFromContext(ctx)
	effectiveCaller := callerid.EffectiveCallerIDFromContext(ctx)

	sc.tainted = true
	sc.reservedProps = &Properties{
		EffectiveCaller: effectiveCaller,
		ImmediateCaller: immediateCaller,
		StartTime:       time.Now(),
		Stats:           stats,
	}
	sc.dbConn.Taint()
	sc.Stats().UserActiveReservedCount.Add(sc.getUsername(), 1)
	return nil
}

// IsTainted tells us whether this connection is tainted
func (sc *StatefulConnection) IsTainted() bool {
	return sc.tainted
}

// LogTransaction logs transaction related stats
func (sc *StatefulConnection) LogTransaction(reason tx.ReleaseReason) {
	if sc.txProps == nil {
		return //Nothing to log as no transaction exists on this connection.
	}
	sc.txProps.Conclusion = reason.Name()
	sc.txProps.EndTime = time.Now()

	username := callerid.GetPrincipal(sc.txProps.EffectiveCaller)
	if username == "" {
		username = callerid.GetUsername(sc.txProps.ImmediateCaller)
	}
	duration := sc.txProps.EndTime.Sub(sc.txProps.StartTime)
	sc.Stats().UserTransactionCount.Add([]string{username, reason.Name()}, 1)
	sc.Stats().UserTransactionTimesNs.Add([]string{username, reason.Name()}, int64(duration))
	sc.txProps.Stats.Add(reason.Name(), duration)
	if sc.txProps.LogToFile {
		log.Infof("Logged transaction: %s", sc.String())
	}
	tabletenv.TxLogger.Send(sc)
}

// logReservedConn logs reserved connection related stats.
func (sc *StatefulConnection) logReservedConn() {
	if sc.reservedProps == nil {
		return //Nothing to log as this connection is not reserved.
	}
	duration := time.Since(sc.reservedProps.StartTime)
	username := sc.getUsername()
	sc.Stats().UserActiveReservedCount.Add(username, -1)
	sc.Stats().UserReservedCount.Add(username, 1)
	sc.Stats().UserReservedTimesNs.Add(username, int64(duration))
}

func (sc *StatefulConnection) getUsername() string {
	username := callerid.GetPrincipal(sc.reservedProps.EffectiveCaller)
	if username != "" {
		return username
	}
	return callerid.GetUsername(sc.reservedProps.ImmediateCaller)
}
