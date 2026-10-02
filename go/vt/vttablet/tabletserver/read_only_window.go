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

package tabletserver

import (
	"context"
	"errors"
	"sync"
	"time"

	"vitess.io/vitess/go/mysql/sqlerror"
)

var (
	// readOnlyWindowGrace is how long after a read-only window ended a write that MySQL refused as
	// read-only is still retried: the refusal may reach the query executor after the window ended.
	readOnlyWindowGrace = 2 * time.Second
	// readOnlyWindowMaxWait bounds how long a write waits for a read-only window to end.
	readOnlyWindowMaxWait = 10 * time.Second
)

// readOnlyWindow is a known, short window during which MySQL refuses writes although the tablet
// serves as the primary: the tablet manager makes the MySQL of the serving primary bootstrap a
// Group Replication group, which is how MigrateReplicationMode converts a shard. MySQL turns
// super_read_only on while Group Replication starts, and off once the member is the group's
// primary, a few milliseconds later. A write in between fails with errno 1290
// (ER_OPTION_PREVENTS_STATEMENT), which the tablet reports as CLUSTER_EVENT. vtgate takes that for
// a failover: it buffers the write and waits for a new primary, which never comes since the same
// primary keeps serving, and the write fails after the buffering window (30s in the end-to-end
// tests); the next failover is then refused buffering as "too recent". A commit that was already
// under way when Group Replication started is refused by its before_commit hook instead, with
// errno 3100 (ER_RUN_HOOK_ERROR), and MySQL rolls the transaction back; vtgate fails it right
// away. A write that the tablet itself executes as a whole, outside of a client's transaction, and
// that MySQL refuses either way is instead retried once the window ended (see
// QueryExecutor.retryAfterReadOnlyWindow).
type readOnlyWindow struct {
	mu sync.Mutex
	// done is open while a window is in progress, and closed when it ends.
	done chan struct{}
	// endedAt is when the last window ended.
	endedAt time.Time
}

// set starts or ends a window.
func (w *readOnlyWindow) set(inProgress bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	switch {
	case inProgress && w.done == nil:
		w.done = make(chan struct{})
	case !inProgress && w.done != nil:
		close(w.done)
		w.done = nil
		w.endedAt = time.Now()
	}
}

// waitToRetry returns whether a write that failed with err may be retried: MySQL refused it as
// read-only (errno 1290), or Group Replication's before_commit hook refused it (errno 3100), while a
// window is in progress, or ended less than readOnlyWindowGrace ago. It waits until the window in
// progress ends, at most readOnlyWindowMaxWait, and returns false if ctx ends first.
func (w *readOnlyWindow) waitToRetry(ctx context.Context, err error) bool {
	if !isBootstrapRefusal(err) {
		return false
	}
	w.mu.Lock()
	done, endedAt := w.done, w.endedAt
	w.mu.Unlock()
	if done == nil {
		return !endedAt.IsZero() && time.Since(endedAt) < readOnlyWindowGrace
	}
	timer := time.NewTimer(readOnlyWindowMaxWait)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	case <-timer.C:
		return false
	}
}

// isBootstrapRefusal returns whether MySQL refused a statement in a way that a Group Replication
// bootstrap causes: it is read-only, or a replication hook (Group Replication's before_commit)
// refused the commit, which MySQL then rolled back.
func isBootstrapRefusal(err error) bool {
	var sqlErr *sqlerror.SQLError
	if !errors.As(err, &sqlErr) {
		return false
	}
	switch sqlErr.Number() {
	case sqlerror.EROptionPreventsStatement, sqlerror.ERRunHookError:
		return true
	}
	return false
}
