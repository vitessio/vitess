/*
Copyright 2026 The Vitess Authors.

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

package vreplication

import (
	"context"
	"log/slog"
	"slices"
	"time"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/vt/log"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

// workerLockWaitTimeout is the innodb_lock_wait_timeout and the
// lock_wait_timeout of the parallel applier's worker connections, in seconds.
// A worker waiting on a row lock or a metadata lock held by a later-ordered
// transaction (directly, or through a DDL queued behind it), which waits for
// this one to commit, is in a cycle neither InnoDB nor the metadata lock
// deadlock detector can see (see applyScheduler.abortGen); the timeouts are
// how such a wait ends, so they are kept short.
const workerLockWaitTimeout = 1

// isLockWaitError reports whether err is a lock wait timeout or a deadlock,
// the errors a commit-order deadlock surfaces as.
func isLockWaitError(err error) bool {
	sqlErr, ok := sqlerror.NewSQLErrorFromError(err).(*sqlerror.SQLError)
	if !ok {
		return false
	}
	return sqlErr.Num == sqlerror.ERLockWaitTimeout || sqlErr.Num == sqlerror.ERLockDeadlock
}

// resolveCommitOrderLockWait is called after txn hit a lock wait timeout or
// deadlock and was rolled back, before it is applied again. When every
// earlier transaction has committed, a later one holds the lock (or something
// outside the stream does): abort every later uncommitted transaction, and if
// this is not the first attempt, wait as the serial applier does before
// retrying, in case the lock is not ours. Otherwise the lock may be held by
// an earlier transaction, which commits first: wait for this one's turn and
// apply it then, alone.
func (vp *vplayer) resolveCommitOrderLockWait(ctx context.Context, scheduler *applyScheduler, txn *applyTxn, attempt int, cause error) error {
	if !scheduler.isNext(txn.order) {
		return scheduler.waitForTurn(ctx, txn.order)
	}
	scheduler.requestAbortAfter(txn.order)
	vp.vr.stats.ErrorCounts.Add([]string{"CommitOrderDeadlock"}, 1)
	// Log the error's code rather than its text, which holds the statement
	// with its row values, and the tables the transaction writes: the error
	// does not say which of its statements waited.
	attrs := vp.vr.logAttrs(
		slog.Int64("order", txn.order),
		slog.Int("attempt", attempt),
		slog.Any("tables", rowEventTables(txn.payload.events)),
	)
	if sqlErr, ok := sqlerror.NewSQLErrorFromError(cause).(*sqlerror.SQLError); ok {
		attrs = append(attrs, slog.Int("errno", int(sqlErr.Num)), slog.String("sqlstate", sqlErr.State))
	}
	log.Info("Parallel apply lock wait: aborting later uncommitted transactions", attrs...)
	if attempt == 0 {
		return nil
	}
	select {
	case <-time.After(dbLockRetryDelay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// rowEventTables returns the distinct tables of the row events in events.
func rowEventTables(events []*binlogdatapb.VEvent) []string {
	var tables []string
	for _, event := range events {
		if event.Type != binlogdatapb.VEventType_ROW || event.RowEvent == nil {
			continue
		}
		if !slices.Contains(tables, event.RowEvent.TableName) {
			tables = append(tables, event.RowEvent.TableName)
		}
	}
	return tables
}
