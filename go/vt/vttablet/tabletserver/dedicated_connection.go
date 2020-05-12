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

// DedicatedConnection is used in the situations where we need a dedicated connection for a vtgate session.
// This is used for transactions and reserved connections.
// NOTE: After use, if must be returned either by doing a Recycle() or a Release().
type DedicatedConnection struct {
	pool   *ActivePool
	dbConn *connpool.DBConn
	ConnID tx.ConnID
	env    tabletenv.Env

	reserved bool
	TxProps  *TxProperties
}

// Close closes the underlying connection.
func (dc *DedicatedConnection) Close() {
	if dc.dbConn != nil {
		dc.dbConn.Close()
	}
}

// Exec executes the statement in the dedicated connection
func (dc *DedicatedConnection) Exec(ctx context.Context, query string, maxrows int, wantfields bool) (*sqltypes.Result, error) {
	if dc.dbConn == nil {
		if dc.TxProps != nil {
			return nil, vterrors.Errorf(vtrpcpb.Code_ABORTED, "transaction was aborted: %v", dc.TxProps.Conclusion)
		}
		return nil, vterrors.New(vtrpcpb.Code_ABORTED, "connection was aborted")
	}
	r, err := dc.dbConn.ExecOnce(ctx, query, maxrows, wantfields)
	if err != nil {
		if mysql.IsConnErr(err) {
			select {
			case <-ctx.Done():
				// If the context is done, the query was killed.
				// So, don't trigger a mysql check.
			default:
				dc.env.CheckMySQL()
			}
		}
		return nil, err
	}
	return r, nil
}

// BeginAgain commits the existing transaction and begins a new one
// TODO: move from this file
func (dc *DedicatedConnection) BeginAgain(ctx context.Context) error {
	if dc.dbConn == nil || dc.TxProps.Autocommit {
		return nil
	}
	if _, err := dc.dbConn.Exec(ctx, "commit", 1, false); err != nil {
		return err
	}
	if _, err := dc.dbConn.Exec(ctx, "begin", 1, false); err != nil {
		return err
	}
	return nil
}

func (dc *DedicatedConnection) execWithRetry(ctx context.Context, query string, maxrows int, wantfields bool) error {
	if dc.dbConn == nil {
		return nil
	}
	if _, err := dc.dbConn.Exec(ctx, query, maxrows, wantfields); err != nil {
		return err
	}
	return nil
}

// Recycle returns the connection to the pool. The connection remains active.
func (dc *DedicatedConnection) Recycle() {
	if dc.dbConn == nil {
		return
	}
	if dc.dbConn.IsClosed() {
		dc.Release(tx.TxClose)
	} else {
		dc.pool.markAsNotInUse(dc.ConnID)
	}
}

//Release implements the tx.TrustedConnection interface
func (dc *DedicatedConnection) Release(reason tx.ReleaseReason) {
	dc.conclude(reason.String())
}

func (dc *DedicatedConnection) conclude(reason string) {
	if dc.dbConn == nil {
		return
	}
	dc.pool.unregister(dc.ConnID, reason)
	dc.dbConn.Recycle()
	dc.dbConn = nil
}

// String returns a printable version of the connection info.
func (dc *DedicatedConnection) String() string {
	return fmt.Sprintf(
		"%v\t'%v'\t'%v'\t%v\t%v\t%.6f\t%v\t%v\t\n",
		dc.ConnID,
		dc.TxProps.EffectiveCaller,
		dc.TxProps.ImmediateCaller,
		dc.TxProps.StartTime.Format(time.StampMicro),
		dc.TxProps.EndTime.Format(time.StampMicro),
		dc.TxProps.EndTime.Sub(dc.TxProps.StartTime).Seconds(),
		dc.TxProps.Conclusion,
		strings.Join(dc.TxProps.Queries, ";"),
	)
}

func (dc *DedicatedConnection) renewConnection() {
	panic("cannot renew txconnection, reserve conn not implemented")
}

func (dc *DedicatedConnection) txClean() {
	dc.TxProps = nil
}

var _ tx.TrustedConnection = (*DedicatedConnection)(nil)
