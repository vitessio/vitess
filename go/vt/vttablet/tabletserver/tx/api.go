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

package tx

import (
	"context"
	"fmt"
	"strings"
	"time"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

type (
	ConnID            = int64
	DTID              = string
	TransactionalConn interface {
		// Executes a query inside the scope of the transaction
		Exec(ctx context.Context, query string, maxrows int, wantfields bool) (*sqltypes.Result, error)

		// Should not be needed
		BeginAgain(ctx context.Context) error

		// String returns a printable version of the connection info.
		String() string
	}
	FuncWithConnection func(TransactionalConn) error
	TransactionEngine  interface {
		// Local transactions
		Begin(ctx context.Context, options *querypb.ExecuteOptions, exec FuncWithConnection) (ConnID, string, error)
		ReserveBegin(ctx context.Context, options *querypb.ExecuteOptions, exec FuncWithConnection, connection ConnID) (ConnID, string, error)
		Reserve(ctx context.Context, options *querypb.ExecuteOptions, setStatements []string, exec FuncWithConnection, connection ConnID) (ConnID, error)

		Exec(ctx context.Context, connection ConnID, exec FuncWithConnection) error
		Commit(ctx context.Context, transactionID ConnID) (string, ConnID, error)
		Rollback(ctx context.Context, transactionID ConnID) (ConnID, error)

		// 2PC Transactions
		Prepare(transactionID ConnID, dtid DTID) error
		CommitPrepared(dtid DTID) error
		RollbackPrepared(dtid DTID, originalID ConnID) error
		CreateTransaction(dtid DTID, participants []*querypb.Target) error
		StartCommit(transactionID ConnID, dtid DTID) error
		SetRollback(dtid DTID, transactionID ConnID) error
		ConcludeTransaction(dtid DTID) error
		ReadTransaction(dtid DTID) (*querypb.TransactionMetadata, error)
		ReadTwopcInflight() (distributed []*DistributedTx, prepared, failed []*PreparedTx, err error)
	}
	//TxEngineStateMachine is used to control the state the transactional engine -
	//whether new connections and/or transactions are allowed or not.
	TxEngineStateMachine interface {
		Init() error
		AcceptReadWrite() error
		AcceptReadOnly() error
		StopGently()
	}

	//TrustedConnection is a connection where the user is trusted to clean things up after using the connection
	TrustedConnection interface {
		// Executes a query on the connection
		Exec(ctx context.Context, query string, maxrows int, wantfields bool) (*sqltypes.Result, error)

		// Release is used after we are done with the connection and will not use it again
		Release(reason ReleaseReason)

		// Unlock marks the connection as not in use. The connection remains active.
		Unlock()

		IsInTransaction() bool

		Close()

		IsOpen() bool

		String() string

		TxProperties() *Properties

		ID() ConnID

		UnderlyingdDBConn() *connpool.DBConn

		CleanTxState()

		Stats() *tabletenv.Stats
	}
	ReleaseReason int

	//Properties contains all information that is related to the currently running
	//transaction on the connection
	Properties struct {
		EffectiveCaller *vtrpcpb.CallerID
		ImmediateCaller *querypb.VTGateCallerID
		StartTime       time.Time
		EndTime         time.Time
		Queries         []string
		Autocommit      bool
		Conclusion      string
		LogToFile       bool

		Stats *servenv.TimingsWrapper
	}
)

const (
	TxClose ReleaseReason = iota
	TxCommit
	TxRollback
	TxKill
	ConnInitFail
)

func (r ReleaseReason) String() string {
	return txResolutions[r]
}

func (r ReleaseReason) Name() string {
	return txNames[r]
}

var txResolutions = map[ReleaseReason]string{
	TxClose:      "closed",
	TxCommit:     "transaction committed",
	TxRollback:   "transaction rolled back",
	TxKill:       "kill",
	ConnInitFail: "initFail",
}

var txNames = map[ReleaseReason]string{
	TxClose:      "close",
	TxCommit:     "commit",
	TxRollback:   "rollback",
	TxKill:       "kill",
	ConnInitFail: "initFail",
}

// RecordQuery records the query against this transaction.
func (p *Properties) RecordQuery(query string) {
	if p == nil {
		return
	}
	p.Queries = append(p.Queries, query)
}

// InTransaction returns true as soon as this struct is not nil
func (p *Properties) InTransaction() bool { return p != nil }

// String returns a printable version of the transaction
func (p *Properties) String() string {
	if p == nil {
		return ""
	}

	return fmt.Sprintf(
		"'%v'\t'%v'\t%v\t%v\t%.6f\t%v\t%v\t\n",
		p.EffectiveCaller,
		p.ImmediateCaller,
		p.StartTime.Format(time.StampMicro),
		p.EndTime.Format(time.StampMicro),
		p.EndTime.Sub(p.StartTime).Seconds(),
		p.Conclusion,
		strings.Join(p.Queries, ";"),
	)
}
