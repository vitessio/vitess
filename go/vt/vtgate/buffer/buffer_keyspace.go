/*
Copyright 2021 The Vitess Authors.

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
	"sync"

	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

type KeyspaceEventBuffer struct {
	// Immutable configuration fields.
	config *Config

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
	buffers map[string]*shardBufferKS
	// stopped is true after Shutdown() was run.
	stopped bool
}

// New creates a new Buffer object.
func NewKeyspaceEventBuffer(cfg *Config) *KeyspaceEventBuffer {
	return &KeyspaceEventBuffer{
		config:         cfg,
		bufferSizeSema: sync2.NewSemaphore(cfg.Size, 0),
		buffers:        make(map[string]*shardBufferKS),
	}
}

// WaitForFailoverEnd blocks until a pending buffering due to a failover for
// keyspace/shard is over.
// If there is no ongoing failover, "err" is checked. If it's caused by a
// failover, buffering may be started.
// It returns an error if buffering failed (e.g. buffer full).
// If it does not return an error, it may return a RetryDoneFunc which must be
// called after the request was retried.
func (b *KeyspaceEventBuffer) WaitForFailoverEnd(ctx context.Context, keyspace, shard string, err error) (RetryDoneFunc, error) {
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

func (b *KeyspaceEventBuffer) HandleKeyspaceEvent(ksevent *discovery.KeyspaceEvent) {
	for _, shard := range ksevent.Shards {
		sb := b.getOrCreateBuffer(shard.Target.Keyspace, shard.Target.Shard)
		if sb != nil {
			sb.recordKeyspaceEvent(shard.Tablet, shard.Serving, shard.LastReparenting)
		}
	}
}

// getOrCreateBuffer returns the ShardBuffer for the given keyspace and shard.
// It returns nil if Buffer is shut down and all calls should be ignored.
func (b *KeyspaceEventBuffer) getOrCreateBuffer(keyspace, shard string) *shardBufferKS {
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
		sb = newShardBufferKeyspace(b, b.config.bufferingMode(keyspace, shard), keyspace, shard)
		b.buffers[key] = sb
	}
	return sb
}

// Shutdown blocks until all pending ShardBuffer objects are shut down.
// In particular, it guarantees that all launched Go routines are stopped after
// it returns.
func (b *KeyspaceEventBuffer) Shutdown() {
	b.shutdown()
	b.waitForShutdown()
}

func (b *KeyspaceEventBuffer) shutdown() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, sb := range b.buffers {
		sb.shutdown()
	}
	b.stopped = true
}

func (b *KeyspaceEventBuffer) waitForShutdown() {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, sb := range b.buffers {
		sb.waitForShutdown()
	}
}
