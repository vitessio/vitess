package buffer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

// HealthCheckBuffer is used to track ongoing PRIMARY tablet failovers and buffer
// requests while the PRIMARY tablet is unavailable.
// Once the new PRIMARY starts accepting requests, buffering stops and requests
// queued so far will be automatically retried.
//
// There should be exactly one instance of this buffer. For each failover, an
// instance of "ShardBuffer" will be created.
type HealthCheckBuffer struct {
	// Immutable configuration fields.
	// Except for "now", they are parsed from command line flags.
	// keyspaces has the same purpose as "shards" but applies to a whole keyspace.
	keyspaces map[string]bool
	// shards is a set of keyspace/shard entries to which buffering is limited.
	// If empty (and *enabled==true), buffering is enabled for all shards.
	shards map[string]bool
	// now returns the current time. Overridden in tests.
	now func() time.Time

	// bufferSizeSema limits how many requests can be buffered
	// ("-buffer_size") and is shared by all shardBuffer instances.
	bufferSizeSema *sync2.Semaphore

	// mu guards all fields in this group.
	// In particular, it is used to serialize the following Go routines:
	// - 1. Requests which may buffer (RLock, can be run in parallel)
	// - 2. Request which starts buffering (based on the seen error)
	// - 3. LegacyHealthCheck listener ("StatsUpdate") which stops buffering
	// - 4. Timer which may stop buffering after -buffer_max_failover_duration
	mu sync.RWMutex
	// buffers holds a shardBuffer object per shard, even if no failover is in
	// progress.
	// Key Format: "<keyspace>/<shard>"
	buffers map[string]*shardBufferHC
	// stopped is true after Shutdown() was run.
	stopped bool
}

// New creates a new Buffer object.
func NewHealthCheckBuffer() *HealthCheckBuffer {
	return newWithNow(time.Now)
}

func newWithNow(now func() time.Time) *HealthCheckBuffer {
	if err := verifyFlags(); err != nil {
		log.Fatalf("Invalid buffer configuration: %v", err)
	}
	bufferSize.Set(int64(*size))
	keyspaces, shards := keyspaceShardsToSets(*shards)

	if *enabledDryRun {
		log.Infof("vtgate buffer in dry-run mode enabled for all requests. Dry-run bufferings will log failovers but not buffer requests.")
	}

	if *enabled {
		log.Infof("vtgate buffer enabled. PRIMARY requests will be buffered during detected failovers.")

		// Log a second line if it's only enabled for some keyspaces or shards.
		header := "Buffering limited to configured "
		limited := ""
		if len(keyspaces) > 0 {
			limited += "keyspaces: " + setToString(keyspaces)
		}
		if len(shards) > 0 {
			if limited == "" {
				limited += " and "
			}
			limited += "shards: " + setToString(shards)
		}
		if limited != "" {
			limited = header + limited
			dryRunOverride := ""
			if *enabledDryRun {
				dryRunOverride = " Dry-run mode is overridden for these entries and actual buffering will take place."
			}
			log.Infof("%v.%v", limited, dryRunOverride)
		}
	}

	if !*enabledDryRun && !*enabled {
		log.Infof("vtgate buffer not enabled.")
	}

	return &HealthCheckBuffer{
		keyspaces:      keyspaces,
		shards:         shards,
		now:            now,
		bufferSizeSema: sync2.NewSemaphore(*size, 0),
		buffers:        make(map[string]*shardBufferHC),
	}
}

// mode determines for the given keyspace and shard if buffering, dry-run
// buffering or no buffering at all should be enabled.
func (b *HealthCheckBuffer) mode(keyspace, shard string) bufferMode {
	// Actual buffering is enabled if
	// a) no keyspaces and shards were listed in particular,
	if *enabled && len(b.keyspaces) == 0 && len(b.shards) == 0 {
		// No explicit whitelist given i.e. all shards should be buffered.
		return bufferEnabled
	}
	// b) or this keyspace is listed,
	if b.keyspaces[keyspace] {
		return bufferEnabled
	}
	// c) or this shard is listed.
	keyspaceShard := topoproto.KeyspaceShardString(keyspace, shard)
	if b.shards[keyspaceShard] {
		return bufferEnabled
	}

	if *enabledDryRun {
		return bufferDryRun
	}

	return bufferDisabled
}

// WaitForFailoverEnd blocks until a pending buffering due to a failover for
// keyspace/shard is over.
// If there is no ongoing failover, "err" is checked. If it's caused by a
// failover, buffering may be started.
// It returns an error if buffering failed (e.g. buffer full).
// If it does not return an error, it may return a RetryDoneFunc which must be
// called after the request was retried.
func (b *HealthCheckBuffer) WaitForFailoverEnd(ctx context.Context, keyspace, shard string, err error) (RetryDoneFunc, error) {
	// If an err is given, it must be related to a failover.
	// We never buffer requests with other errors.
	if err != nil && !CausedByFailover(err) {
		return nil, nil
	}

	sb := b.getOrCreateBuffer(keyspace, shard)
	if sb == nil {
		// Buffer is shut down. Ignore all calls.
		requestsSkipped.Add([]string{keyspace, shard, skippedShutdown}, 1)
		return nil, nil
	}
	if sb.disabled() {
		requestsSkipped.Add([]string{keyspace, shard, skippedDisabled}, 1)
		return nil, nil
	}

	return sb.waitForFailoverEnd(ctx, keyspace, shard, err)
}

// ProcessPrimaryHealth notifies the buffer to record a new primary
// and end any failover buffering that may be in progress
func (b *HealthCheckBuffer) ProcessPrimaryHealth(th *discovery.TabletHealth) {
	if th.Target.TabletType != topodatapb.TabletType_PRIMARY {
		panic(fmt.Sprintf("BUG: non-PRIMARY TabletHealth object must not be forwarded: %#v", th))
	}
	timestamp := th.PrimaryTermStartTime
	if timestamp == 0 {
		// Primarys where TabletExternallyReparented was never called will return 0.
		// Ignore them.
		return
	}

	sb := b.getOrCreateBuffer(th.Target.Keyspace, th.Target.Shard)
	if sb == nil {
		// Buffer is shut down. Ignore all calls.
		return
	}
	sb.recordExternallyReparentedTimestamp(timestamp, th.Tablet.Alias)
}

// StatsUpdate keeps track of the "tablet_externally_reparented_timestamp" of
// each primary. This way we can detect the end of a failover.
// It is part of the discovery.LegacyHealthCheckStatsListener interface.
func (b *HealthCheckBuffer) StatsUpdate(ts *discovery.LegacyTabletStats) {
	if ts.Target.TabletType != topodatapb.TabletType_PRIMARY {
		panic(fmt.Sprintf("BUG: non-PRIMARY LegacyTabletStats object must not be forwarded: %#v", ts))
	}

	timestamp := ts.TabletExternallyReparentedTimestamp
	if timestamp == 0 {
		// Primarys where TabletExternallyReparented was never called will return 0.
		// Ignore them.
		return
	}

	sb := b.getOrCreateBuffer(ts.Target.Keyspace, ts.Target.Shard)
	if sb == nil {
		// Buffer is shut down. Ignore all calls.
		return
	}
	sb.recordExternallyReparentedTimestamp(timestamp, ts.Tablet.Alias)
}

// getOrCreateBuffer returns the ShardBuffer for the given keyspace and shard.
// It returns nil if Buffer is shut down and all calls should be ignored.
func (b *HealthCheckBuffer) getOrCreateBuffer(keyspace, shard string) *shardBufferHC {
	key := topoproto.KeyspaceShardString(keyspace, shard)
	b.mu.RLock()
	sb, ok := b.buffers[key]
	stopped := b.stopped
	b.mu.RUnlock()

	if stopped {
		return nil
	}
	if ok {
		return sb
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	// Look it up again because it could have been created in the meantime.
	sb, ok = b.buffers[key]
	if !ok {
		sb = newShardBufferHealthCheck(b.mode(keyspace, shard), keyspace, shard, b.now, b.bufferSizeSema)
		b.buffers[key] = sb
	}
	return sb
}

// Shutdown blocks until all pending ShardBuffer objects are shut down.
// In particular, it guarantees that all launched Go routines are stopped after
// it returns.
func (b *HealthCheckBuffer) Shutdown() {
	b.shutdown()
	b.waitForShutdown()
}

func (b *HealthCheckBuffer) shutdown() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, sb := range b.buffers {
		sb.shutdown()
	}
	b.stopped = true
}

func (b *HealthCheckBuffer) waitForShutdown() {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, sb := range b.buffers {
		sb.waitForShutdown()
	}
}
