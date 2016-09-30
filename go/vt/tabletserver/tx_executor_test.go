// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	"golang.org/x/net/context"
)

func TestTxExecutorPrepare(t *testing.T) {
	db := setUpQueryExecutorTest()
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	defer tsv.StopService()
	txe := newTestTxExecutor(ctx, tsv)
	txid := newTransaction(tsv)
	db.AddQueryPattern("insert into `_vt`\\.redo_log_transaction.*", &sqltypes.Result{})
	db.AddQueryPattern("delete from `_vt`\\.redo_log_transaction where dtid =.*", &sqltypes.Result{})
	db.AddQueryPattern("delete from `_vt`\\.redo_log_statement where dtid =.*", &sqltypes.Result{})
	err := txe.Prepare(txid, "aa")
	if err != nil {
		t.Error(err)
	}
	err = txe.RollbackPrepared("aa", 1)
	if err != nil {
		t.Error(err)
	}
}

func TestTxExecutorCommit(t *testing.T) {
	db := setUpQueryExecutorTest()
	ctx := context.Background()
	tsv := newTestTabletServer(ctx, noFlags, db)
	defer tsv.StopService()
	txe := newTestTxExecutor(ctx, tsv)
	txid := newTransaction(tsv)
	db.AddQueryPattern("insert into `_vt`\\.redo_log_transaction.*", &sqltypes.Result{})
	db.AddQueryPattern("delete from `_vt`\\.redo_log_transaction where dtid =.*", &sqltypes.Result{})
	db.AddQueryPattern("delete from `_vt`\\.redo_log_statement where dtid =.*", &sqltypes.Result{})
	err := txe.Prepare(txid, "aa")
	if err != nil {
		t.Error(err)
	}
	err = txe.CommitPrepared("aa")
	if err != nil {
		t.Error(err)
	}
}

func newTestTxExecutor(ctx context.Context, tsv *TabletServer) *TxExecutor {
	logStats := newLogStats("TestTxExecutor", ctx)
	return &TxExecutor{
		ctx:      ctx,
		logStats: logStats,
		qe:       tsv.qe,
	}
}
