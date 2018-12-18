/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package txserializer provides the vttablet hot row protection.
// See the TxSerializer struct for details.
package txserializer

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	// waits stores how many times a transaction was queued because another
	// transaction was already in flight for the same row (range).
	// The key of the map is the table name of the query.
	waits = stats.NewCountersWithSingleLabel(
		"TxSerializerWaits",
		"Number of times a transaction was queued because another transaction was already in flight for the same row range",
		"table_name")
	// waitsDryRun is similar as "waits": In dry-run mode it records how many
	// transactions would have been queued.
	// The key of the map is the table name of the query.
	waitsDryRun = stats.NewCountersWithSingleLabel(
		"TxSerializerWaitsDryRun",
		"Dry run number of transactions that would've been queued",
		"table_name")

	// queueExceeded counts per table how many transactions were rejected because
	// the max queue size per row (range) was exceeded.
	queueExceeded = stats.NewCountersWithSingleLabel(
		"TxSerializerQueueExceeded",
		"Number of transactions that were rejected because the max queue size per row range was exceeded",
		"table_name")
	// queueExceededDryRun counts in dry-run mode how many transactions would have
	// been rejected due to exceeding the max queue size per row (range).
	queueExceededDryRun = stats.NewCountersWithSingleLabel(
		"TxSerializerQueueExceededDryRun",
		"Dry-run Number of transactions that were rejcted because the max queue size was exceeded",
		"table_name")

	// globalQueueExceeded is the same as queueExceeded but for the global queue.
	globalQueueExceeded = stats.NewCounter(
		"TxSerializerGlobalQueueExceeded",
		"Number of transactions that were rejected on the global queue because of exceeding the max queue size per row range")
	globalQueueExceededDryRun = stats.NewCounter(
		"TxSerializerGlobalQueueExceededDryRun",
		"Dry-run stats for TxSerializerGlobalQueueExceeded")
)

// TxSerializer serializes incoming transactions which target the same row range
// i.e. table name and WHERE clause are identical.
// Additional transactions are queued and woken up in arrival order.
//
// This implementation has some parallels to the sync2.Consolidator class.
// However, there are many substantial differences:
// - Results are not shared between queued transactions.
// - Only one waiting transaction and not all are notified when the current one
//   has finished.
// - Waiting transactions are woken up in FIFO order.
// - Waiting transactions are unblocked if their context is done.
// - Both the local queue (per row range) and global queue (whole process) are
//   limited to avoid that queued transactions can consume the full capacity
//   of vttablet. This is important if the capaciy is finite. For example, the
//   number of RPCs in flight could be limited by the RPC subsystem.
type TxSerializer struct {
	*sync2.ConsolidatorCache

	// Immutable fields.
	dryRun                 bool
	maxQueueSize           int
	maxGlobalQueueSize     int
	concurrentTransactions int

	log                          *logutil.ThrottledLogger
	logDryRun                    *logutil.ThrottledLogger
	logWaitsDryRun               *logutil.ThrottledLogger
	logQueueExceededDryRun       *logutil.ThrottledLogger
	logGlobalQueueExceededDryRun *logutil.ThrottledLogger

	mu         sync.Mutex
	queues     map[string]*queue
	globalSize int
}

// New returns a TxSerializer object.
func New(dryRun bool, maxQueueSize, maxGlobalQueueSize, concurrentTransactions int) *TxSerializer {
	return &TxSerializer{
		ConsolidatorCache:            sync2.NewConsolidatorCache(1000),
		dryRun:                       dryRun,
		maxQueueSize:                 maxQueueSize,
		maxGlobalQueueSize:           maxGlobalQueueSize,
		concurrentTransactions:       concurrentTransactions,
		log:                          logutil.NewThrottledLogger("HotRowProtection", 5*time.Second),
		logDryRun:                    logutil.NewThrottledLogger("HotRowProtection DryRun", 5*time.Second),
		logWaitsDryRun:               logutil.NewThrottledLogger("HotRowProtection Waits DryRun", 5*time.Second),
		logQueueExceededDryRun:       logutil.NewThrottledLogger("HotRowProtection QueueExceeded DryRun", 5*time.Second),
		logGlobalQueueExceededDryRun: logutil.NewThrottledLogger("HotRowProtection GlobalQueueExceeded DryRun", 5*time.Second),
		queues:                       make(map[string]*queue),
	}
}

// DoneFunc is returned by Wait() and must be called by the caller.
type DoneFunc func()

// Wait blocks if another transaction for the same range is already in flight.
// It returns when this transaction has its turn.
// "done" is != nil if err == nil and must be called once the transaction is
// done and the next waiting transaction can be unblocked.
// "waited" is true if Wait() had to wait for other transactions.
// "err" is not nil if a) the context is done or b) a queue limit was reached.
func (t *TxSerializer) Wait(ctx context.Context, key, table string) (done DoneFunc, waited bool, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	waited, err = t.lockLocked(ctx, key, table)
	if err != nil {
		if waited {
			// Waiting failed early e.g. due a canceled context and we did NOT get the
			// slot. Call "done" now because we don't return it to the caller.
			t.unlockLocked(key, false /* returnSlot */)
		}
		return nil, waited, err
	}
	return func() { t.unlock(key) }, waited, nil
}

// lockLocked queues this transaction. It will unblock immediately if this
// transaction is the first in the queue or when it acquired a slot.
// The method has the suffix "Locked" to clarify that "t.mu" must be locked.
func (t *TxSerializer) lockLocked(ctx context.Context, key, table string) (bool, error) {
	q, ok := t.queues[key]
	if !ok {
		// First transaction in the queue i.e. we don't wait and return immediately.
		t.queues[key] = newQueueForFirstTransaction(t.concurrentTransactions)
		t.globalSize++
		return false, nil
	}

	if t.globalSize >= t.maxGlobalQueueSize {
		if t.dryRun {
			globalQueueExceededDryRun.Add(1)
			t.logGlobalQueueExceededDryRun.Warningf("Would have rejected BeginExecute RPC because there are too many queued transactions (%d >= %d)", t.globalSize, t.maxGlobalQueueSize)
		} else {
			globalQueueExceeded.Add(1)
			return false, vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED,
				"hot row protection: too many queued transactions (%d >= %d)", t.globalSize, t.maxGlobalQueueSize)
		}
	}

	if q.size >= t.maxQueueSize {
		if t.dryRun {
			queueExceededDryRun.Add(table, 1)
			t.logQueueExceededDryRun.Warningf("Would have rejected BeginExecute RPC because there are too many queued transactions (%d >= %d) for the same row (table + WHERE clause: '%v')", q.size, t.maxQueueSize, key)
		} else {
			queueExceeded.Add(table, 1)
			return false, vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED,
				"hot row protection: too many queued transactions (%d >= %d) for the same row (table + WHERE clause: '%v')", q.size, t.maxQueueSize, key)
		}
	}

	if q.availableSlots == nil {
		// Hot row detected: A second, concurrent transaction is seen for the
		// first time.

		// As an optimization, we deferred the creation of the channel until now.
		q.availableSlots = make(chan struct{}, t.concurrentTransactions)
		q.availableSlots <- struct{}{}

		// Include first transaction in the count at /debug/hotrows. (It was not
		// recorded on purpose because it did not wait.)
		t.Record(key)
	}

	t.globalSize++
	q.size++
	q.count++
	if q.size > q.max {
		q.max = q.size
	}
	// Publish the number of waits at /debug/hotrows.
	t.Record(key)

	if t.dryRun {
		waitsDryRun.Add(table, 1)
		t.logWaitsDryRun.Warningf("Would have queued BeginExecute RPC for row (range): '%v' because another transaction to the same range is already in progress.", key)
		return false, nil
	}

	// Unlock before the wait and relock before returning because our caller
	// Wait() holds the lock and assumes it still has it.
	t.mu.Unlock()
	defer t.mu.Lock()

	// Non-blocking write attempt to get a slot.
	select {
	case q.availableSlots <- struct{}{}:
		// Return waited=false because a slot was immediately available.
		return false, nil
	default:
	}

	// Blocking wait for the next available slot.
	waits.Add(table, 1)
	select {
	case q.availableSlots <- struct{}{}:
		return true, nil
	case <-ctx.Done():
		return true, ctx.Err()
	}
}

func (t *TxSerializer) unlock(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.unlockLocked(key, true)
}

func (t *TxSerializer) unlockLocked(key string, returnSlot bool) {
	q := t.queues[key]
	q.size--
	t.globalSize--

	if q.size == 0 {
		// This is the last transaction in flight.
		delete(t.queues, key)

		if q.max > 1 {
			if t.dryRun {
				t.logDryRun.Infof("%v simultaneous transactions (%v in total) for the same row range (%v) would have been queued.", q.max, q.count, key)
			} else {
				t.log.Infof("%v simultaneous transactions (%v in total) for the same row range (%v) were queued.", q.max, q.count, key)
			}
		}

		// Return early because the queue "q" for this "key" will not be used any
		// more.
		// We intentionally skip returning the last slot and closing the
		// "availableSlots" channel because it is not required by Go.
		return
	}

	// Give up slot by removing ourselves from the channel.
	// Wakes up the next queued transaction.

	if t.dryRun {
		// Dry-run did not acquire a slot in the first place.
		return
	}

	if !returnSlot {
		// We did not acquire a slot in the first place e.g. due to a canceled context.
		return
	}

	// This should never block.
	<-q.availableSlots
}

// Pending returns the number of queued transactions (including the ones which
// are currently in flight.)
func (t *TxSerializer) Pending(key string) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	q, ok := t.queues[key]
	if !ok {
		return 0
	}
	return q.size
}

// ServeHTTP lists the most recent, cached queries and their count.
func (t *TxSerializer) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if *streamlog.RedactDebugUIQueries {
		response.Write([]byte(`
	<!DOCTYPE html>
	<html>
	<body>
	<h1>Redacted</h1>
	<p>/debug/hotrows has been redacted for your protection</p>
	</body>
	</html>
		`))
		return
	}

	if err := acl.CheckAccessHTTP(request, acl.DEBUGGING); err != nil {
		acl.SendError(response, err)
		return
	}
	items := t.Items()
	response.Header().Set("Content-Type", "text/plain")
	if items == nil {
		response.Write([]byte("empty\n"))
		return
	}
	response.Write([]byte(fmt.Sprintf("Length: %d\n", len(items))))
	for _, v := range items {
		response.Write([]byte(fmt.Sprintf("%v: %s\n", v.Count, v.Query)))
	}
}

// queue represents the local queue for a particular row (range).
//
// Note that we don't use a dedicated queue structure for all waiting
// transactions. Instead, we leverage that Go routines waiting for a channel
// are woken up in the order they are queued up. The "availableSlots" field is
// said channel which has n free slots (for the number of concurrent
// transactions which can access the tx pool). All queued transactions are
// competing for these slots and try to add themselves to the channel.
type queue struct {
	// NOTE: The following fields are guarded by TxSerializer.mu.
	// size counts how many transactions are currently queued/in flight (includes
	// the transactions which are not waiting.)
	size int
	// count is the same as "size", but never gets decremented.
	count int
	// max is the max of "size", i.e. the maximum number of transactions which
	// were simultaneously queued for the same row range.
	max int

	// availableSlots limits the number of concurrent transactions *per*
	// hot row (range). It holds one element for each allowed pending
	// transaction i.e. consumed tx pool slot. Consequently, if the channel
	// is full, subsequent transactions have to wait until they can place
	// their entry here.
	// NOTE: As an optimization, we defer the creation of the channel until
	// a second transaction for the same hot row is running.
	availableSlots chan struct{}
}

func newQueueForFirstTransaction(concurrentTransactions int) *queue {
	return &queue{
		size:  1,
		count: 1,
		max:   1,
	}
}
