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

// Package txserializer provides the vttablet hot row protection.
// See the TxSerializer struct for details.
package txserializer

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"context"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
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
	env tabletenv.Env
	*sync2.ConsolidatorCache

	// Immutable fields.
	dryRun                 bool
	maxQueueSize           int
	maxGlobalQueueSize     int
	concurrentTransactions int

	// waits stores how many times a transaction was queued because another
	// transaction was already in flight for the same row (range).
	// The key of the map is the table name of the query.
	//
	// waitsDryRun is similar as "waits": In dry-run mode it records how many
	// transactions would have been queued.
	// The key of the map is the table name of the query.
	//
	// queueExceeded counts per table how many transactions were rejected because
	// the max queue size per row (range) was exceeded.
	//
	// queueExceededDryRun counts in dry-run mode how many transactions would have
	// been rejected due to exceeding the max queue size per row (range).
	//
	// globalQueueExceeded is the same as queueExceeded but for the global queue.
	waits, waitsDryRun, queueExceeded, queueExceededDryRun *stats.CountersWithSingleLabel
	globalQueueExceeded, globalQueueExceededDryRun         *stats.Counter

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
func New(env tabletenv.Env) *TxSerializer {
	config := env.Config()
	return &TxSerializer{
		env:                    env,
		ConsolidatorCache:      sync2.NewConsolidatorCache(1000),
		dryRun:                 config.HotRowProtection.Mode == tabletenv.Dryrun,
		maxQueueSize:           config.HotRowProtection.MaxQueueSize,
		maxGlobalQueueSize:     config.HotRowProtection.MaxGlobalQueueSize,
		concurrentTransactions: config.HotRowProtection.MaxConcurrency,
		waits: env.Exporter().NewCountersWithSingleLabel(
			"TxSerializerWaits",
			"Number of times a transaction was queued because another transaction was already in flight for the same row range",
			"table_name"),
		waitsDryRun: env.Exporter().NewCountersWithSingleLabel(
			"TxSerializerWaitsDryRun",
			"Dry run number of transactions that would've been queued",
			"table_name"),
		queueExceeded: env.Exporter().NewCountersWithSingleLabel(
			"TxSerializerQueueExceeded",
			"Number of transactions that were rejected because the max queue size per row range was exceeded",
			"table_name"),
		queueExceededDryRun: env.Exporter().NewCountersWithSingleLabel(
			"TxSerializerQueueExceededDryRun",
			"Dry-run Number of transactions that were rejected because the max queue size was exceeded",
			"table_name"),
		globalQueueExceeded: env.Exporter().NewCounter(
			"TxSerializerGlobalQueueExceeded",
			"Number of transactions that were rejected on the global queue because of exceeding the max queue size per row range"),
		globalQueueExceededDryRun: env.Exporter().NewCounter(
			"TxSerializerGlobalQueueExceededDryRun",
			"Dry-run stats for TxSerializerGlobalQueueExceeded"),
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
func (txs *TxSerializer) Wait(ctx context.Context, key, table string) (done DoneFunc, waited bool, err error) {
	txs.mu.Lock()
	defer txs.mu.Unlock()

	waited, err = txs.lockLocked(ctx, key, table)
	if err != nil {
		if waited {
			// Waiting failed early e.g. due a canceled context and we did NOT get the
			// slot. Call "done" now because we don'txs return it to the caller.
			txs.unlockLocked(key, false /* returnSlot */)
		}
		return nil, waited, err
	}
	return func() { txs.unlock(key) }, waited, nil
}

// lockLocked queues this transaction. It will unblock immediately if this
// transaction is the first in the queue or when it acquired a slot.
// The method has the suffix "Locked" to clarify that "txs.mu" must be locked.
func (txs *TxSerializer) lockLocked(ctx context.Context, key, table string) (bool, error) {
	q, ok := txs.queues[key]
	if !ok {
		// First transaction in the queue i.e. we don't wait and return immediately.
		txs.queues[key] = newQueueForFirstTransaction(txs.concurrentTransactions)
		txs.globalSize++
		return false, nil
	}

	if txs.globalSize >= txs.maxGlobalQueueSize {
		if txs.dryRun {
			txs.globalQueueExceededDryRun.Add(1)
			txs.logGlobalQueueExceededDryRun.Warningf("Would have rejected BeginExecute RPC because there are too many queued transactions (%d >= %d)", txs.globalSize, txs.maxGlobalQueueSize)
		} else {
			txs.globalQueueExceeded.Add(1)
			return false, vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED,
				"hot row protection: too many queued transactions (%d >= %d)", txs.globalSize, txs.maxGlobalQueueSize)
		}
	}

	if q.size >= txs.maxQueueSize {
		if txs.dryRun {
			txs.queueExceededDryRun.Add(table, 1)
			txs.logQueueExceededDryRun.Warningf("Would have rejected BeginExecute RPC because there are too many queued transactions (%d >= %d) for the same row (table + WHERE clause: '%v')", q.size, txs.maxQueueSize, key)
		} else {
			txs.queueExceeded.Add(table, 1)
			return false, vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED,
				"hot row protection: too many queued transactions (%d >= %d) for the same row (table + WHERE clause: '%v')", q.size, txs.maxQueueSize, key)
		}
	}

	if q.availableSlots == nil {
		// Hot row detected: A second, concurrent transaction is seen for the
		// first time.

		// As an optimization, we deferred the creation of the channel until now.
		q.availableSlots = make(chan struct{}, txs.concurrentTransactions)
		q.availableSlots <- struct{}{}

		// Include first transaction in the count at /debug/hotrows. (It was not
		// recorded on purpose because it did not wait.)
		txs.Record(key)
	}

	txs.globalSize++
	q.size++
	q.count++
	if q.size > q.max {
		q.max = q.size
	}
	// Publish the number of waits at /debug/hotrows.
	txs.Record(key)

	if txs.dryRun {
		txs.waitsDryRun.Add(table, 1)
		txs.logWaitsDryRun.Warningf("Would have queued BeginExecute RPC for row (range): '%v' because another transaction to the same range is already in progress.", key)
		return false, nil
	}

	// Unlock before the wait and relock before returning because our caller
	// Wait() holds the lock and assumes it still has it.
	txs.mu.Unlock()
	defer txs.mu.Lock()

	// Non-blocking write attempt to get a slot.
	select {
	case q.availableSlots <- struct{}{}:
		// Return waited=false because a slot was immediately available.
		return false, nil
	default:
	}

	// Blocking wait for the next available slot.
	txs.waits.Add(table, 1)
	select {
	case q.availableSlots <- struct{}{}:
		return true, nil
	case <-ctx.Done():
		return true, ctx.Err()
	}
}

func (txs *TxSerializer) unlock(key string) {
	txs.mu.Lock()
	defer txs.mu.Unlock()

	txs.unlockLocked(key, true)
}

func (txs *TxSerializer) unlockLocked(key string, returnSlot bool) {
	q := txs.queues[key]
	q.size--
	txs.globalSize--

	if q.size == 0 {
		// This is the last transaction in flight.
		delete(txs.queues, key)

		if q.max > 1 {
			if txs.dryRun {
				txs.logDryRun.Infof("%v simultaneous transactions (%v in total) for the same row range (%v) would have been queued.", q.max, q.count, key)
			} else {
				txs.log.Infof("%v simultaneous transactions (%v in total) for the same row range (%v) were queued.", q.max, q.count, key)
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

	if txs.dryRun {
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
func (txs *TxSerializer) Pending(key string) int {
	txs.mu.Lock()
	defer txs.mu.Unlock()

	q, ok := txs.queues[key]
	if !ok {
		return 0
	}
	return q.size
}

// ServeHTTP lists the most recent, cached queries and their count.
func (txs *TxSerializer) ServeHTTP(response http.ResponseWriter, request *http.Request) {
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
	items := txs.Items()
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
