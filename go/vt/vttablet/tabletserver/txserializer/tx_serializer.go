// Package txserializer provides the vttablet hot row protection.
// See the TxSerializer struct for details.
package txserializer

import (
	"context"
	"sync"
	"time"

	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/vterrors"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

var (
	// waits stores how many times a transaction was queued because another
	// transaction was already in flight for the same row (range).
	// The key of the map is the table name of the query.
	waits = stats.NewCounters("TxSerializerWaits")
	// waitsDryRun is similar as "waits": In dry-run mode it records how many
	// transactions would have been queued.
	// The key of the map is the table name of the query.
	waitsDryRun = stats.NewCounters("TxSerializerWaitsDryRun")

	// queueExceeded counts per table how many transactions were rejected because
	// the max queue size per row (range) was exceeded.
	queueExceeded = stats.NewCounters("TxSerializerQueueExceeded")
	// queueExceededDryRun counts in dry-run mode how many transactions would have
	// been rejected due to exceeding the max queue size per row (range).
	queueExceededDryRun = stats.NewCounters("TxSerializerQueueExceededDryRun")
	// globalQueueExceeded is the same as queueExceeded but for the global queue.
	globalQueueExceeded       = stats.NewInt("TxSerializerGlobalQueueExceeded")
	globalQueueExceededDryRun = stats.NewInt("TxSerializerGlobalQueueExceededDryRun")
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
	dryRun             bool
	maxQueueSize       int
	maxGlobalQueueSize int

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
func New(dryRun bool, maxQueueSize, maxGlobalQueueSize int) *TxSerializer {
	return &TxSerializer{
		ConsolidatorCache:            sync2.NewConsolidatorCache(1000),
		dryRun:                       dryRun,
		maxQueueSize:                 maxQueueSize,
		maxGlobalQueueSize:           maxGlobalQueueSize,
		log:                          logutil.NewThrottledLogger("HotRowProtection", 5*time.Second),
		logDryRun:                    logutil.NewThrottledLogger("HotRowProtection DryRun", 5*time.Second),
		logWaitsDryRun:               logutil.NewThrottledLogger("HotRowProtection Waits DryRun", 5*time.Second),
		logQueueExceededDryRun:       logutil.NewThrottledLogger("HotRowProtection QueueExceeded DryRun", 5*time.Second),
		logGlobalQueueExceededDryRun: logutil.NewThrottledLogger("HotRowProtection GlobalQueueExceeded DryRun", 5*time.Second),
		queues: make(map[string]*queue),
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
			// token. Call "done" now because we don't return it to the caller.
			t.unlockLocked(key, false /* returnToken */)
		}
		return nil, waited, err
	}
	return func() { t.unlock(key) }, waited, nil
}

// lockLocked queues this transaction. It will unblock immediately if this
// transaction is the first in the queue or when it got the token (queue.lock).
// The method has the suffix "Locked" to clarify that "t.mu" must be locked.
func (t *TxSerializer) lockLocked(ctx context.Context, key, table string) (bool, error) {
	q, ok := t.queues[key]
	if !ok {
		// First transaction in the queue i.e. we don't wait and return immediately.
		t.queues[key] = newQueue(t.maxQueueSize)
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
	t.globalSize++

	if q.size >= t.maxQueueSize {
		if t.dryRun {
			queueExceededDryRun.Add(table, 1)
			t.logQueueExceededDryRun.Warningf("Would have rejected BeginExecute RPC because there are too many queued transactions (%d >= %d) for the same row (table + WHERE clause: '%v')", q.size, t.maxQueueSize, key)
		} else {
			// Decrement global queue size again because we return early.
			t.globalSize--
			queueExceeded.Add(table, 1)
			return false, vterrors.Errorf(vtrpcpb.Code_RESOURCE_EXHAUSTED,
				"hot row protection: too many queued transactions (%d >= %d) for the same row (table + WHERE clause: '%v')", q.size, t.maxQueueSize, key)
		}
	}
	q.size++
	q.count++
	if q.size > q.max {
		q.max = q.size
	}
	// Publish the number of waits at /debug/hotrows.
	t.Record(key)
	if q.size == 2 {
		// Include first transaction in the count. (It was not recorded on purpose
		// because it did not wait.)
		t.Record(key)
	}

	if t.dryRun {
		waitsDryRun.Add(table, 1)
		t.logWaitsDryRun.Warningf("Would have queued BeginExecute RPC for row (range): '%v' because another transaction to the same range is already in progress.", key)
		return false, nil
	}

	// Unlock before the wait and relock before returning because our caller
	// Wait() hold the lock and assumes it still has it.
	t.mu.Unlock()
	defer t.mu.Lock()

	waits.Add(table, 1)
	select {
	case <-q.lock:
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

func (t *TxSerializer) unlockLocked(key string, returnToken bool) {
	q := t.queues[key]
	q.size--
	t.globalSize--
	if q.size == 0 {
		delete(t.queues, key)

		if q.max > 1 {
			if t.dryRun {
				t.logDryRun.Infof("%v simultaneous transactions (%v in total) for the same row range (%v) would have been queued.", q.max, q.count, key)
			} else {
				t.log.Infof("%v simultaneous transactions (%v in total) for the same row range (%v) were queued.", q.max, q.count, key)
			}
		}
	}

	// Return token to queue. Wakes up the next queued transaction.
	if !t.dryRun && returnToken {
		q.lock <- struct{}{}
	}
}

// Pending returns the number of queued transactions (including the one which
// is currently in flight.)
func (t *TxSerializer) Pending(key string) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	q, ok := t.queues[key]
	if !ok {
		return 0
	}
	return q.size
}

// queue reprents the local queue for a particular row (range).
//
// Note that we don't use a dedicated queue structure for all waiting
// transactions. Instead, we leverage that Go routines waiting for a channel
// are woken up in the order they are queued up. The "lock" field is said
// channel which has exactly one element, a token. All queued transactions are
// competing for this token.
type queue struct {
	// NOTE: The following fields are guarded by TxSerializer.mu.
	// size counts how many transactions are queued (includes the one
	// transaction which is not waiting.)
	size int
	// count is the same as "size", but never gets decremented.
	count int
	// max is the max of "size", i.e. the maximum number of transactions which
	// were simultaneously queued for the same row range.
	max int

	lock chan struct{}
}

func newQueue(max int) *queue {
	return &queue{
		size:  1,
		count: 1,
		max:   1,
		lock:  make(chan struct{}, 1),
	}
}
