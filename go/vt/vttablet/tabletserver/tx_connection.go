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

// TxConnection is meant for executing transactions. It can return itself to
// the tx pool correctly. It also does not retry statements if there
// are failures.
type TxConnection struct {
	pool          *ActivePool
	dbConn        *connpool.DBConn
	TransactionID int64
	env           tabletenv.Env

	reserved bool
	TxProps  *TxProperties
}

// Close closes the connection.
func (txc *TxConnection) Close() {
	if txc.dbConn != nil {
		txc.dbConn.Close()
	}
}

// Exec executes the statement for the current transaction.
func (txc *TxConnection) Exec(ctx context.Context, query string, maxrows int, wantfields bool) (*sqltypes.Result, error) {
	if txc.dbConn == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_ABORTED, "transaction was aborted: %v", txc.TxProps.Conclusion)
	}
	r, err := txc.dbConn.ExecOnce(ctx, query, maxrows, wantfields)
	if err != nil {
		if mysql.IsConnErr(err) {
			select {
			case <-ctx.Done():
				// If the context is done, the query was killed.
				// So, don't trigger a mysql check.
			default:
				txc.env.CheckMySQL()
			}
		}
		return nil, err
	}
	return r, nil
}

// BeginAgain commits the existing transaction and begins a new one
func (txc *TxConnection) BeginAgain(ctx context.Context) error {
	if txc.dbConn == nil || txc.TxProps.Autocommit {
		return nil
	}
	if _, err := txc.dbConn.Exec(ctx, "commit", 1, false); err != nil {
		return err
	}
	if _, err := txc.dbConn.Exec(ctx, "begin", 1, false); err != nil {
		return err
	}
	return nil
}

func (txc *TxConnection) execWithRetry(ctx context.Context, query string, maxrows int, wantfields bool) error {
	if txc.dbConn == nil {
		return nil
	}
	if _, err := txc.dbConn.Exec(ctx, query, maxrows, wantfields); err != nil {
		return err
	}
	return nil
}

// Recycle returns the connection to the pool. The transaction remains
// active.
func (txc *TxConnection) Recycle() {
	if txc.dbConn == nil {
		return
	}
	if txc.dbConn.IsClosed() {
		txc.Release(TxClose)
	} else {
		txc.pool.MarkAsInactive(txc.TransactionID)
	}
}

// RecordQuery records the query against this transaction.
func (txc *TxConnection) RecordQuery(query string) {
	txc.TxProps.Queries = append(txc.TxProps.Queries, query)
}

//Release implements the tx.TrustedConnection interface
func (txc *TxConnection) Release(reason string) {
	txc.conclude(txResolution[reason])
}

func (txc *TxConnection) conclude(reason string) {
	if txc.dbConn == nil {
		return
	}
	txc.pool.unregister(txc.TransactionID, reason)
	txc.dbConn.Recycle()
	txc.dbConn = nil
}

// String returns a printable version of the connection info.
func (txc *TxConnection) String() string {
	return fmt.Sprintf(
		"%v\t'%v'\t'%v'\t%v\t%v\t%.6f\t%v\t%v\t\n",
		txc.TransactionID,
		txc.TxProps.EffectiveCaller,
		txc.TxProps.ImmediateCaller,
		txc.TxProps.StartTime.Format(time.StampMicro),
		txc.TxProps.EndTime.Format(time.StampMicro),
		txc.TxProps.EndTime.Sub(txc.TxProps.StartTime).Seconds(),
		txc.TxProps.Conclusion,
		strings.Join(txc.TxProps.Queries, ";"),
	)
}

func (txc *TxConnection) renewTxConnection() {
	panic("cannot renew txconnection, reserve conn not implemented")
}

func (txc *TxConnection) txClean() {
	txc.TxProps = nil
}

var _ tx.TrustedConnection = (*TxConnection)(nil)
