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
	"time"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/tx"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/trace"
	"vitess.io/vitess/go/vt/log"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

// TxExecutor is used for executing a transactional request.
// TODO: merge this with tx_engine
type TxExecutor struct {
	// TODO(sougou): Parameterize this.
	ctx      context.Context
	logStats *tabletenv.LogStats
	te       *TxEngine
}

// Prepare performs a prepare on a connection including the redo log work.
// If there is any failure, an error is returned. No cleanup is performed.
// A subsequent call to RollbackPrepared, which is required by the 2PC
// protocol, will perform all the cleanup.
func (txe *TxExecutor) Prepare(transactionID int64, dtid string) error {
	if !txe.te.twopcEnabled {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "2pc is not enabled")
	}
	defer txe.te.env.Stats().QueryTimings.Record("PREPARE", time.Now())
	txe.logStats.TransactionID = transactionID

	conn, err := txe.te.txPool.GetAndLock(transactionID, "for prepare")
	if err != nil {
		return err
	}

	// If no queries were executed, we just rollback.
	if len(conn.TxProperties().Queries) == 0 {
		conn.Release(tx.TxRollback)
		return nil
	}

	err = txe.te.preparedPool.Put(conn, dtid)
	if err != nil {
		txe.te.txPool.RollbackAndRelease(txe.ctx, conn)
		return vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED, "prepare failed for transaction %d: %v", transactionID, err)
	}

	return txe.inTransaction(func(localConn *StatefulConnection) error {
		return txe.te.twoPC.SaveRedo(txe.ctx, localConn, dtid, conn.TxProperties().Queries)
	})

}

// CommitPrepared commits a prepared transaction. If the operation
// fails, an error counter is incremented and the transaction is
// marked as failed in the redo log.
func (txe *TxExecutor) CommitPrepared(dtid string) error {
	if !txe.te.twopcEnabled {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "2pc is not enabled")
	}
	defer txe.te.env.Stats().QueryTimings.Record("COMMIT_PREPARED", time.Now())
	conn, err := txe.te.preparedPool.FetchForCommit(dtid)
	if err != nil {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "cannot commit dtid %s, state: %v", dtid, err)
	}
	if conn == nil {
		return nil
	}
	// We have to use a context that will never give up,
	// even if the original context expires.
	ctx := trace.CopySpan(context.Background(), txe.ctx)
	defer txe.te.txPool.RollbackAndRelease(ctx, conn)
	err = txe.te.twoPC.DeleteRedo(ctx, conn, dtid)
	if err != nil {
		txe.markFailed(ctx, dtid)
		return err
	}
	_, err = txe.te.txPool.Commit(ctx, conn)
	if err != nil {
		txe.markFailed(ctx, dtid)
		return err
	}
	txe.te.preparedPool.Forget(dtid)
	return nil
}

// markFailed does the necessary work to mark a CommitPrepared
// as failed. It marks the dtid as failed in the prepared pool,
// increments the InternalErros counter, and also changes the
// state of the transaction in the redo log as failed. If the
// state change does not succeed, it just logs the event.
// The function uses the passed in context that has no timeout
// instead of TxExecutor's context.
func (txe *TxExecutor) markFailed(ctx context.Context, dtid string) {
	txe.te.env.Stats().InternalErrors.Add("TwopcCommit", 1)
	txe.te.preparedPool.SetFailed(dtid)
	conn, _, err := txe.te.txPool.Begin(ctx, &querypb.ExecuteOptions{}, false, 0, nil)
	if err != nil {
		log.Errorf("markFailed: Begin failed for dtid %s: %v", dtid, err)
		return
	}
	defer txe.te.txPool.RollbackAndRelease(ctx, conn)

	if err = txe.te.twoPC.UpdateRedo(ctx, conn, dtid, RedoStateFailed); err != nil {
		log.Errorf("markFailed: UpdateRedo failed for dtid %s: %v", dtid, err)
		return
	}

	if _, err = txe.te.txPool.Commit(ctx, conn); err != nil {
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
func (txe *TxExecutor) RollbackPrepared(dtid string, originalID int64) error {
	if !txe.te.twopcEnabled {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "2pc is not enabled")
	}
	defer txe.te.env.Stats().QueryTimings.Record("ROLLBACK_PREPARED", time.Now())
	defer func() {
		if preparedConn := txe.te.preparedPool.FetchForRollback(dtid); preparedConn != nil {
			txe.te.txPool.RollbackAndRelease(txe.ctx, preparedConn)
		}
		if originalID != 0 {
			txe.te.Rollback(txe.ctx, originalID)
		}
	}()
	return txe.inTransaction(func(conn *StatefulConnection) error {
		return txe.te.twoPC.DeleteRedo(txe.ctx, conn, dtid)
	})
}

// CreateTransaction creates the metadata for a 2PC transaction.
func (txe *TxExecutor) CreateTransaction(dtid string, participants []*querypb.Target) error {
	if !txe.te.twopcEnabled {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "2pc is not enabled")
	}
	defer txe.te.env.Stats().QueryTimings.Record("CREATE_TRANSACTION", time.Now())
	return txe.inTransaction(func(conn *StatefulConnection) error {
		return txe.te.twoPC.CreateTransaction(txe.ctx, conn, dtid, participants)
	})
}

// StartCommit atomically commits the transaction along with the
// decision to commit the associated 2pc transaction.
func (txe *TxExecutor) StartCommit(transactionID int64, dtid string) error {
	if !txe.te.twopcEnabled {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "2pc is not enabled")
	}
	defer txe.te.env.Stats().QueryTimings.Record("START_COMMIT", time.Now())
	txe.logStats.TransactionID = transactionID

	conn, err := txe.te.txPool.GetAndLock(transactionID, "for 2pc commit")
	if err != nil {
		return err
	}
	defer txe.te.txPool.RollbackAndRelease(txe.ctx, conn)

	err = txe.te.twoPC.Transition(txe.ctx, conn, dtid, querypb.TransactionState_COMMIT)
	if err != nil {
		return err
	}
	_, err = txe.te.txPool.Commit(txe.ctx, conn)
	return err
}

// SetRollback transitions the 2pc transaction to the Rollback state.
// If a transaction id is provided, that transaction is also rolled back.
func (txe *TxExecutor) SetRollback(dtid string, transactionID int64) error {
	if !txe.te.twopcEnabled {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "2pc is not enabled")
	}
	defer txe.te.env.Stats().QueryTimings.Record("SET_ROLLBACK", time.Now())
	txe.logStats.TransactionID = transactionID

	if transactionID != 0 {
		txe.te.Rollback(txe.ctx, transactionID)
	}

	return txe.inTransaction(func(conn *StatefulConnection) error {
		return txe.te.twoPC.Transition(txe.ctx, conn, dtid, querypb.TransactionState_ROLLBACK)
	})
}

// ConcludeTransaction deletes the 2pc transaction metadata
// essentially resolving it.
func (txe *TxExecutor) ConcludeTransaction(dtid string) error {
	if !txe.te.twopcEnabled {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "2pc is not enabled")
	}
	defer txe.te.env.Stats().QueryTimings.Record("RESOLVE", time.Now())

	return txe.inTransaction(func(conn *StatefulConnection) error {
		return txe.te.twoPC.DeleteTransaction(txe.ctx, conn, dtid)
	})
}

// ReadTransaction returns the metadata for the sepcified dtid.
func (txe *TxExecutor) ReadTransaction(dtid string) (*querypb.TransactionMetadata, error) {
	if !txe.te.twopcEnabled {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "2pc is not enabled")
	}
	return txe.te.twoPC.ReadTransaction(txe.ctx, dtid)
}

// ReadTwopcInflight returns info about all in-flight 2pc transactions.
func (txe *TxExecutor) ReadTwopcInflight() (distributed []*tx.DistributedTx, prepared, failed []*tx.PreparedTx, err error) {
	if !txe.te.twopcEnabled {
		return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "2pc is not enabled")
	}
	prepared, failed, err = txe.te.twoPC.ReadAllRedo(txe.ctx)
	if err != nil {
		return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "Could not read redo: %v", err)
	}
	distributed, err = txe.te.twoPC.ReadAllTransactions(txe.ctx)
	if err != nil {
		return nil, nil, nil, vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "Could not read redo: %v", err)
	}
	return distributed, prepared, failed, nil
}

func (txe *TxExecutor) inTransaction(f func(*StatefulConnection) error) error {
	conn, _, err := txe.te.txPool.Begin(txe.ctx, &querypb.ExecuteOptions{}, false, 0, nil)
	if err != nil {
		return err
	}
	defer txe.te.txPool.RollbackAndRelease(txe.ctx, conn)

	err = f(conn)
	if err != nil {
		return err
	}

	_, err = txe.te.txPool.Commit(txe.ctx, conn)
	if err != nil {
		return err
	}
	return nil
}
