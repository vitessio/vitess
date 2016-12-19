package buffer

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/vterrors"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// shardBuffer buffers requests for an ongoing failover for a particular shard.
type shardBuffer struct {
	keyspace       string
	shard          string
	start          time.Time
	bufferSizeSema *sync2.Semaphore

	// statsKey is used to update the stats variables.
	statsKey []string

	// mu guards the fields below.
	mu sync.Mutex
	// queue is the list of buffered requests (ordered by arrival).
	queue []*entry
}

// entry is created per buffered request.
type entry struct {
	// done will be closed by shardBuffer when the failover is over and the
	// request can be retried.
	done chan struct{}

	// bufferCtx wraps the request ctx and is used to track the retry of a
	// request during the drain phase. Once the retry is done, the caller
	// must cancel this context (by calling bufferCancel).
	bufferCtx    context.Context
	bufferCancel func()
}

func newShardBuffer(keyspace, shard string, bufferSizeSema *sync2.Semaphore) *shardBuffer {
	// Reset monitoring data from previous failover.
	statsKey := []string{keyspace, shard}
	requestsInFlightMax.Set(statsKey, 0)
	failoverDurationMs.Set(statsKey, 0)

	return &shardBuffer{
		keyspace:       keyspace,
		shard:          shard,
		start:          time.Now(),
		bufferSizeSema: bufferSizeSema,
		statsKey:       statsKey,
		queue:          make([]*entry, 0),
	}
}

// waitForFailoverEnd creates a new entry in the queue for a request which
// should be buffered.
// It returns *entry which can be used as input for shardBuffer.cancel(). This
// is useful for canceled RPCs (e.g. due to deadline exceeded) which want to
// give up their spot in the buffer. It also holds the "bufferCancel" function.
// If buffering fails e.g. due to a full buffer, an error is returned.
func (sb *shardBuffer) waitForFailoverEnd(ctx context.Context) (*entry, error) {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	if !sb.bufferSizeSema.TryAcquire() {
		return nil, vterrors.FromError(vtrpcpb.ErrorCode_TRANSIENT_ERROR, errors.New("master buffer is full"))
	}

	// TODO(mberlin): Kill the oldest entry if full.

	e := &entry{
		done: make(chan struct{}),
	}
	e.bufferCtx, e.bufferCancel = context.WithCancel(ctx)
	sb.queue = append(sb.queue, e)
	requestsInFlightMax.Add(sb.statsKey, 1)
	return e, nil
}

func (sb *shardBuffer) drain() {
	failoverDurationMs.Set(sb.statsKey, int64(time.Since(sb.start)/time.Millisecond))

	sb.mu.Lock()
	defer sb.mu.Unlock()

	// TODO(mberlin): Parallelize the drain by pumping the data through a channel.
	for _, e := range sb.queue {
		close(e.done)
		<-e.bufferCtx.Done()
		sb.bufferSizeSema.Release()
	}
}
