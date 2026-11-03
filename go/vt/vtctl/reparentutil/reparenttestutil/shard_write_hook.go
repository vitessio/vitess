/*
Copyright 2026 The Vitess Authors.

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

package reparenttestutil

import (
	"context"
	"strings"
	"sync"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

// ShardWriteHook is a memorytopo factory whose global cell runs a hook, once it is armed and only
// once, right before the next write of a shard record: between the read and the versioned write of
// topo.Server.UpdateShardFields. It records the errors of the writes of shard records from then on.
// Tests use it to land a concurrent write inside another writer's compare-and-swap.
type ShardWriteHook struct {
	*memorytopo.Factory

	mu        sync.Mutex
	hook      func()
	fired     bool
	writeErrs []error
}

// NewShardWriteHook returns a ShardWriteHook over the factory. A topo.Server created with it, with
// topo.NewWithFactory, writes to the same topology as the factory's other servers.
func NewShardWriteHook(factory *memorytopo.Factory) *ShardWriteHook {
	return &ShardWriteHook{Factory: factory}
}

// Create is part of the topo.Factory interface.
func (h *ShardWriteHook) Create(cell, serverAddr, root string) (topo.Conn, error) {
	conn, err := h.Factory.Create(cell, serverAddr, root)
	if err != nil || cell != topo.GlobalCell {
		return conn, err
	}
	return &shardWriteHookConn{Conn: conn, h: h}, nil
}

// Arm makes the next write of a shard record run hook first.
func (h *ShardWriteHook) Arm(hook func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.hook = hook
}

// WriteErrors returns the errors of the writes of shard records since the hook ran, starting with
// the write that it preceded.
func (h *ShardWriteHook) WriteErrors() []error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.writeErrs
}

type shardWriteHookConn struct {
	topo.Conn
	h *ShardWriteHook
}

// Update is part of the topo.Conn interface.
func (c *shardWriteHookConn) Update(ctx context.Context, filePath string, contents []byte, version topo.Version) (topo.Version, error) {
	if !strings.HasSuffix(filePath, "/"+topo.ShardFile) {
		return c.Conn.Update(ctx, filePath, contents, version)
	}
	c.h.mu.Lock()
	hook := c.h.hook
	c.h.hook = nil
	if hook != nil {
		c.h.fired = true
	}
	record := c.h.fired
	c.h.mu.Unlock()
	if hook != nil {
		hook()
	}
	v, err := c.Conn.Update(ctx, filePath, contents, version)
	if record {
		c.h.mu.Lock()
		c.h.writeErrs = append(c.h.writeErrs, err)
		c.h.mu.Unlock()
	}
	return v, err
}
