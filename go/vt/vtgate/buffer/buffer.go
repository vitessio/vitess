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
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	bufferFullError      = vterrors.New(vtrpcpb.Code_UNAVAILABLE, "master buffer is full")
	entryEvictedError    = vterrors.New(vtrpcpb.Code_UNAVAILABLE, "buffer full: request evicted for newer request")
	contextCanceledError = vterrors.New(vtrpcpb.Code_UNAVAILABLE, "context was canceled before failover finished")
)

// bufferMode specifies how the buffer is configured for a given shard.
type bufferMode int

const (
	// bufferDisabled will let all requests pass through and do nothing.
	bufferDisabled bufferMode = iota
	// bufferEnabled means all requests should be buffered.
	bufferEnabled
	// bufferDryRun will track the failover, but not actually buffer requests.
	bufferDryRun
)

// Buffer is used to track ongoing MASTER tablet failovers and buffer
// requests while the MASTER tablet is unavailable.
// Once the new MASTER starts accepting requests, buffering stops and requests
// queued so far will be automatically retried.
//
// There should be exactly one instance of this buffer. For each failover, an
// instance of "ShardBuffer" will be created.
type Buffer struct {
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
	// - 3. HealthCheck listener ("StatsUpdate") which stops buffering
	// - 4. Timer which may stop buffering after -buffer_max_failover_duration
	mu sync.RWMutex
	// buffers holds a shardBuffer object per shard, even if no failover is in
	// progress.
	// Key Format: "<keyspace>/<shard>"
	buffers map[string]*shardBuffer
	// stopped is true after Shutdown() was run.
	stopped bool
}

// New creates a new Buffer object.
func New() *Buffer {
	return newWithNow(time.Now)
}

func newWithNow(now func() time.Time) *Buffer {
	if err := verifyFlags(); err != nil {
		log.Fatalf("Invalid buffer configuration: %v", err)
	}
	bufferSize.Set(int64(*size))
	keyspaces, shards := keyspaceShardsToSets(*shards)

	if *enabledDryRun {
		log.Infof("vtgate buffer in dry-run mode enabled for all requests. Dry-run bufferings will log failovers but not buffer requests.")
	}

	if *enabled {
		log.Infof("vtgate buffer enabled. MASTER requests will be buffered during detected failovers.")

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

	return &Buffer{
		keyspaces:      keyspaces,
		shards:         shards,
		now:            now,
		bufferSizeSema: sync2.NewSemaphore(*size, 0),
		buffers:        make(map[string]*shardBuffer),
	}
}

// mode determines for the given keyspace and shard if buffering, dry-run
// buffering or no buffering at all should be enabled.
func (b *Buffer) mode(keyspace, shard string) bufferMode {
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
	// If an err is given, it must be related to a failover.
	// We never buffer requests with other errors.
	if err != nil && !causedByFailover(err) {
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
	if sb == nil {
		// Buffer is shut down. Ignore all calls.
		return
	}
	sb.recordExternallyReparentedTimestamp(timestamp, ts.Tablet.Alias)
}

// causedByFailover returns true if "err" was supposedly caused by a failover.
// To simplify things, we've merged the detection for different MySQL flavors
// in one function. Supported flavors: MariaDB, MySQL, Google internal.
func causedByFailover(err error) bool {
	log.V(2).Infof("Checking error (type: %T) if it is caused by a failover. err: %v", err, err)

	// TODO(sougou): Remove the INTERNAL check after rollout.
	if code := vterrors.Code(err); code != vtrpcpb.Code_FAILED_PRECONDITION && code != vtrpcpb.Code_INTERNAL {
		return false
	}
	switch {
	// All flavors.
	case strings.Contains(err.Error(), "operation not allowed in state NOT_SERVING") ||
		strings.Contains(err.Error(), "operation not allowed in state SHUTTING_DOWN") ||
		// Match 1290 if -queryserver-config-terse-errors explicitly hid the error message
		// (which it does to avoid logging the original query including any PII).
		strings.Contains(err.Error(), "(errno 1290) (sqlstate HY000) during query:"):
		return true
	// MariaDB flavor.
	case strings.Contains(err.Error(), "The MariaDB server is running with the --read-only option so it cannot execute this statement (errno 1290) (sqlstate HY000)"):
		return true
	// MySQL flavor.
	case strings.Contains(err.Error(), "The MySQL server is running with the --read-only option so it cannot execute this statement (errno 1290) (sqlstate HY000)"):
		return true
	// Google internal flavor.
	case strings.Contains(err.Error(), "failover in progress (errno 1227) (sqlstate 42000)"):
		return true
	}
	return false
}

// getOrCreateBuffer returns the ShardBuffer for the given keyspace and shard.
// It returns nil if Buffer is shut down and all calls should be ignored.
func (b *Buffer) getOrCreateBuffer(keyspace, shard string) *shardBuffer {
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
		sb = newShardBuffer(b.mode(keyspace, shard), keyspace, shard, b.now, b.bufferSizeSema)
		b.buffers[key] = sb
	}
	return sb
}

// Shutdown blocks until all pending ShardBuffer objects are shut down.
// In particular, it guarantees that all launched Go routines are stopped after
// it returns.
func (b *Buffer) Shutdown() {
	b.shutdown()
	b.waitForShutdown()
}

func (b *Buffer) shutdown() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, sb := range b.buffers {
		sb.shutdown()
	}
	b.stopped = true
}

func (b *Buffer) waitForShutdown() {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, sb := range b.buffers {
		sb.waitForShutdown()
	}
}
