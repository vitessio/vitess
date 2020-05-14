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

	tainted bool
	TxProps *TxProperties
}

// Close closes the underlying connection. When the connection is Unblocked, it will be Released
func (sc *StatefulConnection) Close() {
	if sc.dbConn != nil {
		sc.dbConn.Close()
	}
}

// Exec executes the statement in the dedicated connection
func (sc *StatefulConnection) Exec(ctx context.Context, query string, maxrows int, wantfields bool) (*sqltypes.Result, error) {
	if sc.dbConn == nil {
		if sc.TxProps != nil {
			return nil, vterrors.Errorf(vtrpcpb.Code_ABORTED, "transaction was aborted: %v", sc.TxProps.Conclusion)
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
		sc.Release(tx.TxClose)
	} else {
		sc.pool.markAsNotInUse(sc.ConnID)
	}
}

//Release implements the tx.TrustedConnection interface
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
		sc.TxProps.EffectiveCaller,
		sc.TxProps.ImmediateCaller,
		sc.TxProps.StartTime.Format(time.StampMicro),
		sc.TxProps.EndTime.Format(time.StampMicro),
		sc.TxProps.EndTime.Sub(sc.TxProps.StartTime).Seconds(),
		sc.TxProps.Conclusion,
		strings.Join(sc.TxProps.Queries, ";"),
	)
}

func (sc *StatefulConnection) renewConnection() {
	panic("cannot renew txconnection, reserve conn not implemented")
}

func (sc *StatefulConnection) txClean() {
	sc.TxProps = nil
}

var _ tx.TrustedConnection = (*StatefulConnection)(nil)
