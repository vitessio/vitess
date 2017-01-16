// Package buffer provides a buffer for MASTER traffic during failovers.
//
// Instead of returning an error to the application (when the vttablet master
// becomes unavailable), the buffer will automatically retry buffered requests
// after the end of the failover was detected.
//
// Buffering (stalling) requests will increase the number of requests in flight
// within vtgate and at upstream layers. Therefore, it is important to limit
// the size of the buffer and the buffering duration (window) per request.
// See the file flags.go for the available configuration and its defaults.
package buffer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vterrors"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

var (
	bufferFullError   = vterrors.FromError(vtrpcpb.ErrorCode_TRANSIENT_ERROR, errors.New("master buffer is full"))
	entryEvictedError = vterrors.FromError(vtrpcpb.ErrorCode_TRANSIENT_ERROR, errors.New("buffer full: request evicted for newer request"))
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
	// buffers holds a shardBuffer object per shard, even if no failover is in
	// progress.
	// Key Format: "<keyspace>/<shard>"
	buffers map[string]*shardBuffer
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
		shards:         listToSet(*shards),
		shardsDryRun:   listToSet(*shards),
		bufferSizeSema: sync2.NewSemaphore(*size, 0),
		buffers:        make(map[string]*shardBuffer),
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

	// TODO(mberlin): Go through some of the logic if the dry-run mode is enabled.
	if !b.bufferingEnabled(key) {
		return nil, nil
	}

	// If an err is given, it must be related to a failover.
	// We never buffer requests with other errors.
	if err != nil && !causedByFailover(err) {
		return nil, nil
	}

	sb := b.getOrCreateBuffer(keyspace, shard)
	return sb.waitForFailoverEnd(ctx, keyspace, shard, err)
}

// StatsUpdate keeps track of the "tablet_externally_reparented_timestamp" of
// each master. This way we can detect the end of a failover.
// It is part of the discovery.HealthCheckStatsListener interface.
func (b *Buffer) StatsUpdate(ts *discovery.TabletStats) {
	if ts.Target.TabletType != topodatapb.TabletType_MASTER {
		panic(fmt.Sprintf("BUG: non MASTER TabletStats object must not be forwarded: %#v", ts))
	}

	timestamp := ts.TabletExternallyReparentedTimestamp
	if timestamp == 0 {
		// Masters where TabletExternallyReparented was never called will return 0.
		// Ignore them.
		return
	}

	sb := b.getOrCreateBuffer(ts.Target.Keyspace, ts.Target.Shard)
	sb.recordExternallyReparentedTimestamp(timestamp)
}

// causedByFailover returns true if "err" was supposedly caused by a failover.
// To simplify things, we've merged the detection for different MySQL flavors
// in one function. Supported flavors: MariaDB, MySQL, Google internal.
func causedByFailover(err error) bool {
	log.V(2).Infof("Checking error (type: %T) if it is caused by a failover. err: %v", err, err)

	if vtErr, ok := err.(vterrors.VtError); ok {
		switch vtErr.VtErrorCode() {
		case vtrpcpb.ErrorCode_QUERY_NOT_SERVED:
			// All flavors.
			if strings.Contains(err.Error(), "retry: operation not allowed in state NOT_SERVING") ||
				strings.Contains(err.Error(), "retry: operation not allowed in state SHUTTING_DOWN") ||
				// Match 1290 if -queryserver-config-terse-errors explicitly hid the error message
				// (which it does to avoid logging the original query including any PII).
				strings.Contains(err.Error(), "retry: (errno 1290) (sqlstate HY000) during query:") {
				return true
			}
			// MariaDB flavor.
			if strings.Contains(err.Error(), "retry: The MariaDB server is running with the --read-only option so it cannot execute this statement (errno 1290) (sqlstate HY000)") {
				return true
			}
			// MySQL flavor.
			if strings.Contains(err.Error(), "retry: The MySQL server is running with the --read-only option so it cannot execute this statement (errno 1290) (sqlstate HY000)") {
				return true
			}
		case vtrpcpb.ErrorCode_INTERNAL_ERROR:
			// Google internal flavor.
			if strings.Contains(err.Error(), "fatal: failover in progress (errno 1227) (sqlstate 42000)") {
				return true
			}
		case vtrpcpb.ErrorCode_UNKNOWN_ERROR:
			// Google internal flavor.
			if strings.Contains(err.Error(), "fatal: MySQL server has gone away (errno 2006) (sqlstate HY000)") {
				return true
			}
		}
	}
	return false
}

func (b *Buffer) getOrCreateBuffer(keyspace, shard string) *shardBuffer {
	key := topoproto.KeyspaceShardString(keyspace, shard)
	b.mu.RLock()
	sb, ok := b.buffers[key]
	b.mu.RUnlock()

	if ok {
		return sb
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	// Look it up again because it could have been created in the meantime.
	sb, ok = b.buffers[key]
	if !ok {
		sb = newShardBuffer(keyspace, shard, b.bufferSizeSema)
		b.buffers[key] = sb
	}
	return sb
}
