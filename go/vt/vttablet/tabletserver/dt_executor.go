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
	"context"
	"time"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tx"
)

// DTExecutor is used for executing a distributed transactional request.
type DTExecutor struct {
	ctx      context.Context
	logStats *tabletenv.LogStats
	te       *TxEngine
	qe       *QueryEngine
}

// NewDTExecutor creates a new distributed transaction executor.
func NewDTExecutor(ctx context.Context, te *TxEngine, qe *QueryEngine, logStats *tabletenv.LogStats) *DTExecutor {
	return &DTExecutor{
		ctx:      ctx,
		te:       te,
		qe:       qe,
		logStats: logStats,
	}
}

// Prepare performs a prepare on a connection including the redo log work.
// If there is any failure, an error is returned. No cleanup is performed.
// A subsequent call to RollbackPrepared, which is required by the 2PC
// protocol, will perform all the cleanup.
func (dte *DTExecutor) Prepare(transactionID int64, dtid string) error {
	if !dte.te.twopcEnabled {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "2pc is not enabled")
	}
	if !dte.te.twopcAllowed {
		return vterrors.VT10002("two-pc is enabled, but semi-sync is not")
	}
	defer dte.te.env.Stats().QueryTimings.Record("PREPARE", time.Now())
	dte.logStats.TransactionID = transactionID

	conn, err := dte.te.txPool.GetAndLock(transactionID, "for prepare")
	if err != nil {
		return err
	}

	// If no queries were executed, we just rollback.
	if len(conn.TxProperties().Queries) == 0 {
		dte.te.txPool.RollbackAndRelease(dte.ctx, conn)
		return nil
	}

	// We can only prepare on a Unix socket connection.
	// Unix socket are reliable and we can be sure that the connection is not lost with the server after prepare.
	if !conn.IsUnixSocket() {
		dte.te.txPool.RollbackAndRelease(dte.ctx, conn)
		return vterrors.VT10002("cannot prepare the transaction on a network connection")
	}

	// If the connection is tainted, we cannot prepare it. As there could be temporary tables involved.
	if conn.IsTainted() {
		dte.te.txPool.RollbackAndRelease(dte.ctx, conn)
		return vterrors.VT10002("cannot prepare the transaction on a reserved connection")
	}

	// Fail Prepare if any query rule disallows it.
	// This could be due to ongoing cutover happening in vreplication workflow
	// regarding OnlineDDL or MoveTables.
	for _, query := range conn.TxProperties().Queries {
		qr := dte.qe.queryRuleSources.FilterByPlan(query.Sql, 0, query.Tables...)
		if qr != nil {
			act, _, _, _ := qr.GetAction("", "", nil, sqlparser.MarginComments{})
			if act != rules.QRContinue {
				dte.te.txPool.RollbackAndRelease(dte.ctx, conn)
				return vterrors.VT10002("cannot prepare the transaction due to query rule")
			}
		}
	}

	err = dte.te.preparedPool.Put(conn, dtid)
	if err != nil {
		dte.te.txPool.RollbackAndRelease(dte.ctx, conn)
		return vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, "prepare failed for transaction %d: %v", transactionID, err)
	}

	// Recheck the rules. As some prepare transaction could have passed the first check.
	// If they are put in the prepared pool, then vreplication workflow waits.
	// This check helps reject the prepare that came later.
	for _, query := range conn.TxProperties().Queries {
		qr := dte.qe.queryRuleSources.FilterByPlan(query.Sql, 0, query.Tables...)
		if qr != nil {
			act, _, _, _ := qr.GetAction("", "", nil, sqlparser.MarginComments{})
			if act != rules.QRContinue {
				dte.te.txPool.RollbackAndRelease(dte.ctx, conn)
				dte.te.preparedPool.FetchForRollback(dtid)
				return vterrors.VT10002("cannot prepare the transaction due to query rule")
			}
		}
	}

	// If OnlineDDL killed the connection. We should avoid the prepare for it.
	if conn.IsClosed() {
		dte.te.txPool.RollbackAndRelease(dte.ctx, conn)
		dte.te.preparedPool.FetchForRollback(dtid)
		return vterrors.VT10002("cannot prepare the transaction on a closed connection")
	}

	return dte.inTransaction(func(localConn *StatefulConnection) error {
		return dte.te.twoPC.SaveRedo(dte.ctx, localConn, dtid, conn.TxProperties().Queries)
	})

}

// CommitPrepared commits a prepared transaction. If the operation
// fails, an error counter is incremented and the transaction is
// marked as failed in the redo log.
func (dte *DTExecutor) CommitPrepared(dtid string) (err error) {
	if !dte.te.twopcEnabled {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "2pc is not enabled")
	}
	defer dte.te.env.Stats().QueryTimings.Record("COMMIT_PREPARED", time.Now())
	var conn *StatefulConnection
	conn, err = dte.te.preparedPool.FetchForCommit(dtid)
	if err != nil {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "cannot commit dtid %s, err: %v", dtid, err)
	}
	// No connection means the transaction was already committed.
	if conn == nil {
		return nil
	}
	// We have to use a context that will never give up,
	// even if the original context expires.
	ctx := trace.CopySpan(context.Background(), dte.ctx)
	defer func() {
		if err != nil {
			dte.markFailed(ctx, dtid)
			log.Warningf("failed to commit the prepared transaction '%s' with error: %v", dtid, err)
		}
		dte.te.txPool.RollbackAndRelease(ctx, conn)
	}()
	if err = dte.te.twoPC.DeleteRedo(ctx, conn, dtid); err != nil {
		return err
	}
	if _, err = dte.te.txPool.Commit(ctx, conn); err != nil {
		return err
	}
	dte.te.preparedPool.Forget(dtid)
	return nil
}

// markFailed does the necessary work to mark a CommitPrepared
// as failed. It marks the dtid as failed in the prepared pool,
// increments the InternalErros counter, and also changes the
// state of the transaction in the redo log as failed. If the
// state change does not succeed, it just logs the event.
// The function uses the passed in context that has no timeout
// instead of DTExecutor's context.
func (dte *DTExecutor) markFailed(ctx context.Context, dtid string) {
	dte.te.env.Stats().InternalErrors.Add("TwopcCommit", 1)
	dte.te.preparedPool.SetFailed(dtid)
	conn, _, _, err := dte.te.txPool.Begin(ctx, &querypb.ExecuteOptions{}, false, 0, nil, nil)
	if err != nil {
		log.Errorf("markFailed: Begin failed for dtid %s: %v", dtid, err)
		return
	}
	defer dte.te.txPool.RollbackAndRelease(ctx, conn)

	if err = dte.te.twoPC.UpdateRedo(ctx, conn, dtid, RedoStateFailed); err != nil {
		log.Errorf("markFailed: UpdateRedo failed for dtid %s: %v", dtid, err)
		return
	}

	if _, err = dte.te.txPool.Commit(ctx, conn); err != nil {
		log.Errorf("markFailed: Commit failed for dtid %s: %v", dtid, err)
	}
}

// RollbackPrepared rolls back a prepared transaction. This function handles
// the case of an incomplete prepare.
//
// If the prepare completely failed, it will just rollback the original
// transaction identified by originalID.
//
// If the connection was moved to the prepared pool, but redo log
// creation failed, then it will rollback that transaction and
// return the conn to the txPool.
//
// If prepare was fully successful, it will also delete the redo log.
// If the redo log deletion fails, it returns an error indicating that
// a retry is needed.
//
// In recovery mode, the original transaction id will not be available.
// If so, it must be set to 0, and the function will not attempt that
// step. If the original transaction is still alive, the transaction
// killer will be the one to eventually roll it back.
func (dte *DTExecutor) RollbackPrepared(dtid string, originalID int64) error {
	if !dte.te.twopcEnabled {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "2pc is not enabled")
	}
	defer dte.te.env.Stats().QueryTimings.Record("ROLLBACK_PREPARED", time.Now())
	defer func() {
		if preparedConn := dte.te.preparedPool.FetchForRollback(dtid); preparedConn != nil {
			dte.te.txPool.RollbackAndRelease(dte.ctx, preparedConn)
		}
		if originalID != 0 {
			dte.te.Rollback(dte.ctx, originalID)
		}
	}()
	return dte.inTransaction(func(conn *StatefulConnection) error {
		return dte.te.twoPC.DeleteRedo(dte.ctx, conn, dtid)
	})
}

// CreateTransaction creates the metadata for a 2PC transaction.
func (dte *DTExecutor) CreateTransaction(dtid string, participants []*querypb.Target) error {
	if !dte.te.twopcEnabled {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "2pc is not enabled")
	}
	defer dte.te.env.Stats().QueryTimings.Record("CREATE_TRANSACTION", time.Now())
	return dte.inTransaction(func(conn *StatefulConnection) error {
		return dte.te.twoPC.CreateTransaction(dte.ctx, conn, dtid, participants)
	})
}

// StartCommit atomically commits the transaction along with the
// decision to commit the associated 2pc transaction.
func (dte *DTExecutor) StartCommit(transactionID int64, dtid string) error {
	if !dte.te.twopcEnabled {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "2pc is not enabled")
	}
	defer dte.te.env.Stats().QueryTimings.Record("START_COMMIT", time.Now())
	dte.logStats.TransactionID = transactionID

	conn, err := dte.te.txPool.GetAndLock(transactionID, "for 2pc commit")
	if err != nil {
		return err
	}
	defer dte.te.txPool.RollbackAndRelease(dte.ctx, conn)

	// If the connection is tainted, we cannot take a commit decision on it.
	if conn.IsTainted() {
		dte.inTransaction(func(conn *StatefulConnection) error {
			return dte.te.twoPC.Transition(dte.ctx, conn, dtid, DTStateRollback)
		})
		// return the error, defer call above will roll back the transaction.
		return vterrors.VT10002("cannot commit the transaction on a reserved connection")
	}

	err = dte.te.twoPC.Transition(dte.ctx, conn, dtid, DTStateCommit)
	if err != nil {
		return err
	}
	_, err = dte.te.txPool.Commit(dte.ctx, conn)
	return err
}

// SetRollback transitions the 2pc transaction to the Rollback state.
// If a transaction id is provided, that transaction is also rolled back.
func (dte *DTExecutor) SetRollback(dtid string, transactionID int64) error {
	if !dte.te.twopcEnabled {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "2pc is not enabled")
	}
	defer dte.te.env.Stats().QueryTimings.Record("SET_ROLLBACK", time.Now())
	dte.logStats.TransactionID = transactionID

	if transactionID != 0 {
		// If the transaction is still open, it will be rolled back.
		// Otherwise, it would have been rolled back by other means, like a timeout or vttablet/mysql restart.
		dte.te.Rollback(dte.ctx, transactionID)
	} else {
		// This is a warning because it should not happen in normal operation.
		log.Warningf("SetRollback called with no transactionID for dtid %s", dtid)
	}

	return dte.inTransaction(func(conn *StatefulConnection) error {
		return dte.te.twoPC.Transition(dte.ctx, conn, dtid, DTStateRollback)
	})
}

// ConcludeTransaction deletes the 2pc transaction metadata
// essentially resolving it.
func (dte *DTExecutor) ConcludeTransaction(dtid string) error {
	if !dte.te.twopcEnabled {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "2pc is not enabled")
	}
	defer dte.te.env.Stats().QueryTimings.Record("RESOLVE", time.Now())

	return dte.inTransaction(func(conn *StatefulConnection) error {
		return dte.te.twoPC.DeleteTransaction(dte.ctx, conn, dtid)
	})
}

// ReadTransaction returns the metadata for the specified dtid.
func (dte *DTExecutor) ReadTransaction(dtid string) (*querypb.TransactionMetadata, error) {
	if !dte.te.twopcEnabled {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "2pc is not enabled")
	}
	return dte.te.twoPC.ReadTransaction(dte.ctx, dtid)
}

// ReadTwopcInflight returns info about all in-flight 2pc transactions.
func (dte *DTExecutor) ReadTwopcInflight() (distributed []*tx.DistributedTx, prepared, failed []*tx.PreparedTx, err error) {
	if !dte.te.twopcEnabled {
		return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "2pc is not enabled")
	}
	prepared, failed, err = dte.te.twoPC.ReadAllRedo(dte.ctx)
	if err != nil {
		return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "Could not read redo: %v", err)
	}
	distributed, err = dte.te.twoPC.ReadAllTransactions(dte.ctx)
	if err != nil {
		return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "Could not read redo: %v", err)
	}
	return distributed, prepared, failed, nil
}

func (dte *DTExecutor) inTransaction(f func(*StatefulConnection) error) error {
	conn, _, _, err := dte.te.txPool.Begin(dte.ctx, &querypb.ExecuteOptions{}, false, 0, nil, nil)
	if err != nil {
		return err
	}
	defer dte.te.txPool.RollbackAndRelease(dte.ctx, conn)

	err = f(conn)
	if err != nil {
		return err
	}

	_, err = dte.te.txPool.Commit(dte.ctx, conn)
	if err != nil {
		return err
	}
	return nil
}

// UnresolvedTransactions returns the list of unresolved distributed transactions.
func (dte *DTExecutor) UnresolvedTransactions() ([]*querypb.TransactionMetadata, error) {
	return dte.te.twoPC.UnresolvedTransactions(dte.ctx, time.Now().Add(-dte.te.abandonAge))
}
