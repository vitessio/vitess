package buffer

import (
	"context"
	"fmt"
	"sync"
	"time"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vterrors"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// Buffer is used to track ongoing MASTER tablet failovers and buffer
// requests while the MASTER tablet is unavailable.
// Once the new MASTER starts accepting requests, buffering stops and requests
// queued so far will be automatically retried.
//
// There should be exactly one instance of this buffer. For each failover, an
// instance of "ShardBuffer" will be created.
type Buffer struct {
	// Immutable configuration fields (parsed from command line flags).
	// shards is a set of keyspace/shard entries to which buffering is limited.
	// If empty (and *enabled==true), buffering is enabled for all shards.
	shards map[string]bool
	// shardsDryRun is the set of keyspace/shard entries for which dry-run mode
	// should be enabled (failover tracking), even if buffering is disabled.
	shardsDryRun map[string]bool

	// bufferSizeSema limits how many requests can be buffered
	// ("-vtgate_buffer_size") and is shared by all shardBuffer instances.
	bufferSizeSema *sync2.Semaphore

	// mu guards all fields in this group.
	// In particular, it is used to serialize the following Go routines:
	// - 1. Requests which may buffer (RLock, can be run in parallel)
	// - 2. Request which starts buffering (based on the seen error)
	// - 3. HealthCheck listener ("StatsUpdate") which stops buffering
	// - 4. Timer which may stop buffering after -vtgate_buffer_max_failover_duration
	mu sync.RWMutex
	// failovers is the list of ongoing failovers.
	// Key Format: "<keyspace>/<shard>"
	failovers map[string]*shardBuffer
	// externallyReparented tracks the last time each shard was reparented.
	// The value is the seen maximum value of
	// "StreamHealthResponse.TabletexternallyReparentedTimestamp".
	// Key Format: "<keyspace>/<shard>"
	externallyReparented map[string]int64
	// lastFailoverEnd tracks the last time we detected a failover end (i.e. the
	// time we stopped buffering and started a drain).
	// It is used to avoid triggering another buffer event after a recent failover
	// end ("-vtgate_buffer_min_time_between_failovers").
	lastFailoverEnd map[string]time.Time

	logTooRecent *logutil.ThrottledLogger
}

// New creates a new Buffer object.
func New() *Buffer {
	if err := verifyFlags(); err != nil {
		log.Fatalf("Invalid buffer configuration: %v", err)
	}

	if *enabled {
		log.Infof("vtgate buffer enabled. MASTER requests will be buffered during detected failovers.")
	} else {
		log.Infof("vtgate buffer not enabled.")
	}

	return &Buffer{
		shards:       listToSet(*shards),
		shardsDryRun: listToSet(*shards),

		bufferSizeSema: sync2.NewSemaphore(*size, 0),

		failovers:            make(map[string]*shardBuffer),
		externallyReparented: make(map[string]int64),
		lastFailoverEnd:      make(map[string]time.Time),

		logTooRecent: logutil.NewThrottledLogger("FailoverTooRecent", 5*time.Second),
	}
}

func (b *Buffer) bufferingEnabled(keyspaceShard string) bool {
	if *enabled == false {
		return false
	}

	if len(b.shards) == 0 {
		// No explicit whitelist given i.e. all shards should be buffered.
		return true
	}

	return b.shards[keyspaceShard]
}

func (b *Buffer) dryRunEnabled(keyspaceShard string) bool {
	return b.shardsDryRun[keyspaceShard]
}

// RetryDoneFunc will be returned for each buffered request and must be called
// after the buffered request was retried.
// Without this signal, the buffer would not know how many buffered requests are
// currently retried.
type RetryDoneFunc context.CancelFunc

// WaitForFailoverEnd blocks until a pending buffering due to a failover for
// keyspace/shard is over.
// If there is no ongoing failover, "err" is checked. If it's caused by a
// failover, buffering may be started.
// It returns an error if buffering failed (e.g. buffer full).
// If it does not return an error, it may return a RetryDoneFunc which must be
// called after the request was retried.
func (b *Buffer) WaitForFailoverEnd(ctx context.Context, keyspace, shard string, err error) (RetryDoneFunc, error) {
	key := topoproto.KeyspaceShardString(keyspace, shard)

	if !b.bufferingEnabled(key) && !b.dryRunEnabled(key) {
		// Return immediately if buffering and the dry-run mode are disabled.
		return nil, nil
	}

	// If an err is given, it must be related to a failover.
	// We never buffer requests with other errors.
	if err != nil && !causedByFailover(err) {
		return nil, nil
	}

	// Check if a failover is already in progress.
	b.mu.RLock()
	if sb, ok := b.failovers[key]; ok {
		// Failover already in progress.
		// Queue ourselves while we hold the lock. (This is necessary to serialize
		// this thread and the possible stop of the buffering.)
		entry, err := sb.bufferRequest(ctx)
		b.mu.RUnlock()
		if err != nil {
			return nil, err
		}
		return entry.bufferCancel, wait(ctx, entry)
	}
	b.mu.RUnlock()

	// No failover in progress. Try to detect a new one.
	if err == nil {
		// No error given i.e. we cannot detect a failover.
		return nil, nil
	}

	// New failover detected. Start buffering and wait for the failover end.
	return b.startBufferingAndWait(ctx, keyspace, shard, key, err)
}

func (b *Buffer) startBufferingAndWait(ctx context.Context, keyspace, shard, key string, err error) (RetryDoneFunc, error) {
	// Check if buffering is allowed.
	if !b.bufferingEnabled(key) {
		// TODO(mberlin): Still track the failover duration for dry-run shards and log the first seen error.
		return nil, nil
	}

	b.mu.Lock()
	// Check if there was no other recent failover.
	if d := time.Now().Sub(b.lastFailoverEnd[key]); d < *minTimeBetweenFailovers {
		b.mu.Unlock()
		b.logTooRecent.Infof("NOT starting buffering for shard: %s because the last failover is too recent (%v < %v). (A failover was detected by this seen error: %v.)", key, d, *minTimeBetweenFailovers, err)
		return nil, nil
	}

	sb, ok := b.failovers[key]
	if !ok {
		// Start buffering.
		sb = b.startBufferingLocked(keyspace, shard, key)
		log.Infof("Starting buffering for shard: %s (window: %v, size: %v, max failover duration: %v) (A failover was detected by this seen error: %v.)", key, *window, *size, *maxFailoverDuration, err)
	}

	entry, err := sb.bufferRequest(ctx)
	b.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return entry.bufferCancel, wait(ctx, entry)
}

// wait blocks while the request is buffered during the failover.
func wait(ctx context.Context, e *entry) error {
	select {
	case <-ctx.Done():
		// TODO(mberlin): Cancel buffering.
		return vterrors.FromError(vtrpcpb.ErrorCode_TRANSIENT_ERROR, fmt.Errorf("context was canceled before failover finished (%v)", ctx.Err()))
	case <-e.done:
		return nil
	}
}

// StatsUpdate keeps track of the "tablet_externally_reparented_timestamp" of
// each master. This way we can detect the end of a failover.
// It is part of the discovery.HealthCheckStatsListener interface.
func (b *Buffer) StatsUpdate(ts *discovery.TabletStats) {
	if ts.Target.TabletType != topodatapb.TabletType_MASTER {
		panic(fmt.Sprintf("BUG: non MASTER TabletStats object must not be forwarded: %#v", ts))
	}

	key := topoproto.KeyspaceShardString(ts.Target.Keyspace, ts.Target.Shard)
	timestamp := ts.TabletExternallyReparentedTimestamp
	if timestamp == 0 {
		// Masters where TabletExternallyReparented was never called will return 0.
		// Ignore them.
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if timestamp <= b.externallyReparented[key] {
		// Do nothing. Equal values are reported if the MASTER has not changed.
		// Smaller values can be reported during the failover by the old master
		// after the new master already took over.
		return
	}

	b.externallyReparented[key] = timestamp

	b.stopBufferingLocked(key, "failover end detected")
}

func (b *Buffer) startBufferingLocked(keyspace, shard, key string) *shardBuffer {
	// TODO(mberlin): Start timer to enforce max failover duration.
	sb := newShardBuffer(keyspace, shard, b.bufferSizeSema)
	b.failovers[key] = sb
	return sb
}

func (b *Buffer) stopBufferingLocked(key, reason string) {
	// Buffering may already be stopped e.g. by the timer which stops buffering
	// after a maximum failover duration.
	if sb, ok := b.failovers[key]; ok {
		// Stop buffering.
		delete(b.failovers, key)
		b.lastFailoverEnd[key] = time.Now()

		log.Infof("Stopping buffering for shard: %s due to: %v. Draining TODO buffered requests now.", key, reason)

		// Start the drain. (Use a new Go routine to release the lock.)
		go sb.drain()
	}
}
