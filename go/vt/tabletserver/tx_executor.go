// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"time"

	"golang.org/x/net/context"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/trace"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// TxExecutor is used for executing a transactional request.
type TxExecutor struct {
	// TODO(sougou): Parameterize this.
	ctx      context.Context
	logStats *LogStats
	qe       *QueryEngine
}

// Prepare performs a prepare on a connection including the redo log work.
// If there is any failure, an error is returned. No cleanup is performed.
// A subsequent call to RollbackPrepared, which is required by the 2PC
// protocol, will perform all the cleanup.
func (txe *TxExecutor) Prepare(transactionID int64, dtid string) error {
	defer txe.qe.queryServiceStats.QueryStats.Record("PREPARE", time.Now())
	txe.logStats.TransactionID = transactionID

	conn, err := txe.qe.txPool.Get(transactionID, "for prepare")
	if err != nil {
		return err
	}

	// If no queries were executed, we just rollback.
	if len(conn.Queries) == 0 {
		txe.qe.txPool.LocalConclude(txe.ctx, conn)
		return nil
	}

	err = txe.qe.preparedPool.Put(conn, dtid)
	if err != nil {
		txe.qe.txPool.localRollback(txe.ctx, conn)
		return NewTabletError(vtrpcpb.ErrorCode_RESOURCE_EXHAUSTED, "prepare failed for transaction %d: %v", transactionID, err)
	}

	localConn, err := txe.qe.txPool.LocalBegin(txe.ctx)
	if err != nil {
		return err
	}
	defer txe.qe.txPool.LocalConclude(txe.ctx, localConn)

	err = txe.qe.twoPC.SaveRedo(txe.ctx, localConn, dtid, conn.Queries)
	if err != nil {
		return err
	}

	err = txe.qe.txPool.LocalCommit(txe.ctx, localConn)
	if err != nil {
		return err
	}

	return nil
}

// CommitPrepared commits a prepared transaction. If the operation
// fails, an error counter is incremented and the transaction is
// marked as failed in the redo log.
func (txe *TxExecutor) CommitPrepared(dtid string) error {
	defer txe.qe.queryServiceStats.QueryStats.Record("COMMIT_PREPARED", time.Now())
	conn, err := txe.qe.preparedPool.FetchForCommit(dtid)
	if err != nil {
		return NewTabletError(vtrpcpb.ErrorCode_BAD_INPUT, "cannot commit dtid %s, state: %v", dtid, err)
	}
	if conn == nil {
		return nil
	}
	// We have to use a context that will never give up,
	// even if the original context expires.
	ctx := trace.CopySpan(context.Background(), txe.ctx)
	defer txe.qe.txPool.LocalConclude(ctx, conn)
	err = txe.qe.twoPC.DeleteRedo(ctx, conn, dtid)
	if err != nil {
		txe.markFailed(ctx, dtid)
		return err
	}
	err = txe.qe.txPool.LocalCommit(ctx, conn)
	if err != nil {
		txe.markFailed(ctx, dtid)
		return err
	}
	txe.qe.preparedPool.Forget(dtid)
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
	txe.qe.queryServiceStats.InternalErrors.Add("TwopcCommit", 1)
	txe.qe.preparedPool.SetFailed(dtid)
	conn, err := txe.qe.txPool.LocalBegin(ctx)
	if err != nil {
		log.Errorf("markFailed: Begin failed for dtid %s: %v", dtid, err)
		return
	}
	defer txe.qe.txPool.LocalConclude(ctx, conn)

	if err = txe.qe.twoPC.UpdateRedo(ctx, conn, dtid, "Failed"); err != nil {
		log.Errorf("markFailed: UpdateRedo failed for dtid %s: %v", dtid, err)
		return
	}

	if err = txe.qe.txPool.LocalCommit(ctx, conn); err != nil {
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
	defer txe.qe.queryServiceStats.QueryStats.Record("ROLLBACK_PREPARED", time.Now())
	conn, err := txe.qe.txPool.LocalBegin(txe.ctx)
	if err != nil {
		goto returnConn
	}
	defer txe.qe.txPool.LocalConclude(txe.ctx, conn)

	err = txe.qe.twoPC.DeleteRedo(txe.ctx, conn, dtid)
	if err != nil {
		goto returnConn
	}

	err = txe.qe.txPool.LocalCommit(txe.ctx, conn)

returnConn:
	if preparedConn := txe.qe.preparedPool.FetchForRollback(dtid); preparedConn != nil {
		txe.qe.txPool.LocalConclude(txe.ctx, preparedConn)
	}
	if originalID != 0 {
		txe.qe.txPool.Rollback(txe.ctx, originalID)
	}

	return err
}

// CreateTransaction creates the metadata for a 2PC transaction.
func (txe *TxExecutor) CreateTransaction(dtid string, participants []*querypb.Target) error {
	defer txe.qe.queryServiceStats.QueryStats.Record("CREATE_TRANSACTION", time.Now())
	conn, err := txe.qe.txPool.LocalBegin(txe.ctx)
	if err != nil {
		return err
	}
	defer txe.qe.txPool.LocalConclude(txe.ctx, conn)

	err = txe.qe.twoPC.CreateTransaction(txe.ctx, conn, dtid, participants)
	if err != nil {
		return err
	}
	return txe.qe.txPool.LocalCommit(txe.ctx, conn)
}

// StartCommit atomically commits the transaction along with the
// decision to commit the associated 2pc transaction.
func (txe *TxExecutor) StartCommit(transactionID int64, dtid string) error {
	defer txe.qe.queryServiceStats.QueryStats.Record("START_COMMIT", time.Now())
	txe.logStats.TransactionID = transactionID

	conn, err := txe.qe.txPool.Get(transactionID, "for 2pc commit")
	if err != nil {
		return err
	}
	defer txe.qe.txPool.LocalConclude(txe.ctx, conn)

	err = txe.qe.twoPC.Transition(txe.ctx, conn, dtid, "Commit")
	if err != nil {
		return err
	}
	return txe.qe.txPool.LocalCommit(txe.ctx, conn)
}

// SetRollback transitions the 2pc transaction to the Rollback state.
// If a transaction id is provided, that transaction is also rolled back.
func (txe *TxExecutor) SetRollback(dtid string, transactionID int64) error {
	defer txe.qe.queryServiceStats.QueryStats.Record("SET_ROLLBACK", time.Now())
	txe.logStats.TransactionID = transactionID

	if transactionID != 0 {
		txe.qe.txPool.Rollback(txe.ctx, transactionID)
	}

	conn, err := txe.qe.txPool.LocalBegin(txe.ctx)
	if err != nil {
		return err
	}
	defer txe.qe.txPool.LocalConclude(txe.ctx, conn)

	err = txe.qe.twoPC.Transition(txe.ctx, conn, dtid, "Rollback")
	if err != nil {
		return err
	}

	err = txe.qe.txPool.LocalCommit(txe.ctx, conn)
	if err != nil {
		return err
	}

	return nil
}

// ResolveTransaction deletes the 2pc transaction metadata
// essentially resolving it.
func (txe *TxExecutor) ResolveTransaction(dtid string) error {
	defer txe.qe.queryServiceStats.QueryStats.Record("RESOLVE", time.Now())

	conn, err := txe.qe.txPool.LocalBegin(txe.ctx)
	if err != nil {
		return err
	}
	defer txe.qe.txPool.LocalConclude(txe.ctx, conn)

	err = txe.qe.twoPC.DeleteTransaction(txe.ctx, conn, dtid)
	if err != nil {
		return err
	}
	return txe.qe.txPool.LocalCommit(txe.ctx, conn)
}

// ReadTransaction returns the metadata for the sepcified dtid.
func (txe *TxExecutor) ReadTransaction(dtid string) (*querypb.TransactionMetadata, error) {
	conn, err := txe.qe.connPool.Get(txe.ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()
	return txe.qe.twoPC.ReadTransaction(txe.ctx, conn, dtid)
}
