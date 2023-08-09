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

// Package buffer provides a buffer for PRIMARY traffic during failovers.
//
// Instead of returning an error to the application (when the vttablet primary
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
	"strings"
	"sync"

	"golang.org/x/sync/semaphore"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	ShardMissingError    = vterrors.New(vtrpcpb.Code_UNAVAILABLE, "destination shard is missing after a resharding operation")
	bufferFullError      = vterrors.New(vtrpcpb.Code_UNAVAILABLE, "primary buffer is full")
	entryEvictedError    = vterrors.New(vtrpcpb.Code_UNAVAILABLE, "buffer full: request evicted for newer request")
	contextCanceledError = vterrors.New(vtrpcpb.Code_UNAVAILABLE, "context was canceled before failover finished")
)

// bufferMode specifies how the buffer is configured for a given shard.
type bufferMode int

const (
	// bufferModeDisabled will let all requests pass through and do nothing.
	bufferModeDisabled bufferMode = iota
	// bufferModeEnabled means all requests should be buffered.
	bufferModeEnabled
	// bufferModeDryRun will track the failover, but not actually buffer requests.
	bufferModeDryRun
)

// RetryDoneFunc will be returned for each buffered request and must be called
// after the buffered request was retried.
// Without this signal, the buffer would not know how many buffered requests are
// currently retried.
type RetryDoneFunc context.CancelFunc

const (
	ClusterEventReshardingInProgress = "current keyspace is being resharded"
	ClusterEventReparentInProgress   = "primary is not serving, there may be a reparent operation in progress"
	ClusterEventMoveTables           = "disallowed due to rule"
)

var ClusterEvents []string

func init() {
	ClusterEvents = []string{
		ClusterEventReshardingInProgress,
		ClusterEventReparentInProgress,
		ClusterEventMoveTables,
	}
}

// CausedByFailover returns true if "err" was supposedly caused by a failover.
// To simplify things, we've merged the detection for different MySQL flavors
// in one function. Supported flavors: MariaDB, MySQL
func CausedByFailover(err error) bool {
	log.V(2).Infof("Checking error (type: %T) if it is caused by a failover. err: %v", err, err)
	reason, isFailover := isFailoverError(err)
	if isFailover {
		log.Infof("CausedByFailover signalling failover for reason: %s", reason)
	}
	return isFailover
}

// for debugging purposes
func getReason(err error) string {
	for _, ce := range ClusterEvents {
		if strings.Contains(err.Error(), ce) {
			return ce
		}
	}
	return ""
}

// isFailoverError looks at the error returned by the sql query execution to check if there is a cluster event
// (caused by resharding or reparenting) or a denied tables error seen during switch writes in MoveTables
func isFailoverError(err error) (string, bool) {
	var reason string
	var isFailover bool
	switch vterrors.Code(err) {
	case vtrpcpb.Code_CLUSTER_EVENT:
		isFailover = true
	case vtrpcpb.Code_FAILED_PRECONDITION:
		if strings.Contains(err.Error(), ClusterEventMoveTables) {
			isFailover = true
		}
	}
	if isFailover {
		reason = getReason(err)
	}
	return reason, isFailover
}

// Buffer is used to track ongoing PRIMARY tablet failovers and buffer
// requests while the PRIMARY tablet is unavailable.
// Once the new PRIMARY starts accepting requests, buffering stops and requests
// queued so far will be automatically retried.
//
// There should be exactly one instance of this buffer. For each failover, an
// instance of "ShardBuffer" will be created.
type Buffer struct {
	// Immutable configuration fields.
	config *Config

	// bufferSizeSema limits how many requests can be buffered
	// ("-buffer_size") and is shared by all shardBuffer instances.
	bufferSizeSema *semaphore.Weighted
	bufferSize     int

	// mu guards all fields in this group.
	// In particular, it is used to serialize the following Go routines:
	// - 1. Requests which may buffer (RLock, can be run in parallel)
	// - 2. Request which starts buffering (based on the seen error)
	// - 3. HealthCheck subscriber ("StatsUpdate") which stops buffering
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
func New(cfg *Config) *Buffer {
	return &Buffer{
		config:         cfg,
		bufferSizeSema: semaphore.NewWeighted(int64(cfg.Size)),
		bufferSize:     cfg.Size,
		buffers:        make(map[string]*shardBuffer),
	}
}

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

func (b *Buffer) HandleKeyspaceEvent(ksevent *discovery.KeyspaceEvent) {
	for _, shard := range ksevent.Shards {
		sb := b.getOrCreateBuffer(shard.Target.Keyspace, shard.Target.Shard)
		if sb != nil {
			sb.recordKeyspaceEvent(shard.Tablet, shard.Serving, ksevent)
		}
	}
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
		sb = newShardBufferHealthCheck(b, b.config.bufferingMode(keyspace, shard), keyspace, shard)
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
