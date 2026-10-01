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
	"time"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/vt/log"
)

// workerLockWaitTimeout is the innodb_lock_wait_timeout of the parallel
// applier's worker connections, in seconds. A worker waiting on a lock held
// by a later-ordered transaction, which waits for this one to commit, is in a
// cycle InnoDB cannot see (see applyScheduler.abortGen); the timeout is how
// such a wait ends, so it is kept short.
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

// resolveCommitOrderLockWait is called after the transaction with the given
// order hit a lock wait timeout or deadlock and was rolled back, before it is
// applied again. When every earlier transaction has committed, a later one
// holds the lock (or something outside the stream does): abort every later
// uncommitted transaction, and if this is not the first attempt, wait as the
// serial applier does before retrying, in case the lock is not ours. Otherwise
// the lock may be held by an earlier transaction, which commits first: wait
// for this one's turn and apply it then, alone.
func (vp *vplayer) resolveCommitOrderLockWait(ctx context.Context, scheduler *applyScheduler, order int64, attempt int, cause error) error {
	if !scheduler.isNext(order) {
		return scheduler.waitForTurn(ctx, order)
	}
	scheduler.requestAbortAfter(order)
	vp.vr.stats.ErrorCounts.Add([]string{"CommitOrderDeadlock"}, 1)
	log.Info("Parallel apply lock wait: aborting later uncommitted transactions",
		slog.String("workflow", vp.vr.WorkflowName),
		slog.Int64("order", order),
		slog.Int("attempt", attempt),
		slog.Any("error", cause),
	)
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
