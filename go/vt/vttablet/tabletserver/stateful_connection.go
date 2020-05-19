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
	"strings"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/tx"

	"golang.org/x/net/context"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

// StatefulConnection is used in the situations where we need a dedicated connection for a vtgate session.
// This is used for transactions and reserved connections.
// NOTE: After use, if must be returned either by doing a Unlock() or a Release().
type StatefulConnection struct {
	pool   *StatefulConnectionPool
	dbConn *connpool.DBConn
	ConnID tx.ConnID
	env    tabletenv.Env

	txProps *tx.Properties
}

// Close closes the underlying connection. When the connection is Unblocked, it will be Released
func (sc *StatefulConnection) Close() {
	if sc.dbConn != nil {
		sc.dbConn.Close()
	}
}

//IsOpen returns true when the connection is still operational
func (sc *StatefulConnection) IsOpen() bool {
	return sc.dbConn != nil
}

//IsInTransaction returns true when the connection has tx state
func (sc *StatefulConnection) IsInTransaction() bool {
	return sc.txProps != nil
}

// Exec executes the statement in the dedicated connection
func (sc *StatefulConnection) Exec(ctx context.Context, query string, maxrows int, wantfields bool) (*sqltypes.Result, error) {
	if sc.dbConn == nil {
		if sc.txProps != nil {
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
		}
		return nil, err
	}
	return r, nil
}

func (sc *StatefulConnection) execWithRetry(ctx context.Context, query string, maxrows int, wantfields bool) error {
	if sc.dbConn == nil {
		return nil
	}
	if _, err := sc.dbConn.Exec(ctx, query, maxrows, wantfields); err != nil {
		return err
	}
	return nil
}

// Unlock returns the connection to the pool. The connection remains active.
// This method is idempotent and can be called multiple times
func (sc *StatefulConnection) Unlock() {
	if sc.dbConn == nil {
		return
	}
	if sc.dbConn.IsClosed() {
		sc.conclude("unlocked closed connection")
	} else {
		sc.pool.markAsNotInUse(sc.ConnID)
	}
}

//Release is used when the connection will not be used ever again.
//The underlying dbConn is removed so that this connection cannot be used by mistake.
func (sc *StatefulConnection) Release(reason tx.ReleaseReason) {
	sc.conclude(reason.String())
}

func (sc *StatefulConnection) conclude(reason string) {
	if sc.dbConn == nil {
		return
	}
	sc.pool.unregister(sc.ConnID, reason)
	sc.dbConn.Recycle()
	sc.dbConn = nil
}

// String returns a printable version of the connection info.
func (sc *StatefulConnection) String() string {
	return fmt.Sprintf(
		"%v\t'%v'\t'%v'\t%v\t%v\t%.6f\t%v\t%v\t\n",
		sc.ConnID,
		sc.txProps.EffectiveCaller,
		sc.txProps.ImmediateCaller,
		sc.txProps.StartTime.Format(time.StampMicro),
		sc.txProps.EndTime.Format(time.StampMicro),
		sc.txProps.EndTime.Sub(sc.txProps.StartTime).Seconds(),
		sc.txProps.Conclusion,
		strings.Join(sc.txProps.Queries, ";"),
	)
}

//TxProperties returns the transactional properties of the connection
func (sc *StatefulConnection) TxProperties() *tx.Properties {
	return sc.txProps
}

//ID returns the identifier for this connection
func (sc *StatefulConnection) ID() tx.ConnID {
	return sc.ConnID
}

//UnderlyingdDBConn returns the underlying database connection
func (sc *StatefulConnection) UnderlyingdDBConn() *connpool.DBConn {
	return sc.dbConn
}

//CleanTxState cleans out the current transaction state
func (sc *StatefulConnection) CleanTxState() {
	sc.txProps = nil
}

//Stats implements the tx.TrustedConnection interface
func (sc *StatefulConnection) Stats() *tabletenv.Stats {
	return sc.env.Stats()
}

var _ tx.TrustedConnection = (*StatefulConnection)(nil)
