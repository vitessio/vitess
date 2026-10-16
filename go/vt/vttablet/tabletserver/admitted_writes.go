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

package tabletserver

import (
	"context"
	"sync"
	"sync/atomic"

	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// admittedWriteShards is the number of shards of admittedWrites. Each write
// takes the lock of one shard twice, so sharding keeps concurrent writes from
// contending on a single lock.
const admittedWriteShards = 16

// admittedWrites tracks the writes that are executing, so that loading a rule
// that denies tables can wait for the writes to them that were admitted under
// the previous rules.
//
// MoveTables SwitchTraffic stops writes to the moved tables on the source
// primary by loading such a rule, and then records the source position the
// target must catch up to. A write that passed the permission check before the
// rule was loaded can still be on its way to MySQL, for instance waiting for a
// transaction pool connection. Unless the switch waits for it, it commits after
// that position and never reaches the target.
type admittedWrites struct {
	next   atomic.Uint32
	shards [admittedWriteShards]admittedWriteShard
}

type admittedWriteShard struct {
	mu     sync.Mutex
	writes map[*admittedWrite]struct{}
}

// admittedWrite is a write registered with admittedWrites.
type admittedWrite struct {
	tables []string
	shard  *admittedWriteShard
	// done is closed when the write has finished.
	done chan struct{}
}

// admit registers a write to tables until finish is called on the result.
func (aw *admittedWrites) admit(tables []string) *admittedWrite {
	shard := &aw.shards[aw.next.Add(1)%admittedWriteShards]
	w := &admittedWrite{tables: tables, shard: shard, done: make(chan struct{})}
	shard.mu.Lock()
	if shard.writes == nil {
		shard.writes = make(map[*admittedWrite]struct{})
	}
	shard.writes[w] = struct{}{}
	shard.mu.Unlock()
	return w
}

// finish unregisters the write.
func (w *admittedWrite) finish() {
	w.shard.mu.Lock()
	delete(w.shard.writes, w)
	w.shard.mu.Unlock()
	close(w.done)
}

// writesTo reports whether the write names one of tables.
func (w *admittedWrite) writesTo(tables map[string]bool) bool {
	for _, table := range w.tables {
		if tables[table] {
			return true
		}
	}
	return false
}

// wait waits until every write to one of tables that was registered before it
// was called has finished, or until ctx is done.
func (aw *admittedWrites) wait(ctx context.Context, tables []string) error {
	if len(tables) == 0 {
		return nil
	}
	names := make(map[string]bool, len(tables))
	for _, table := range tables {
		names[table] = true
	}
	var pending []*admittedWrite
	for i := range aw.shards {
		shard := &aw.shards[i]
		shard.mu.Lock()
		for w := range shard.writes {
			if w.writesTo(names) {
				pending = append(pending, w)
			}
		}
		shard.mu.Unlock()
	}
	for i, w := range pending {
		select {
		case <-w.done:
		case <-ctx.Done():
			running := 0
			for _, w := range pending[i:] {
				select {
				case <-w.done:
				default:
					running++
				}
			}
			return vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED,
				"%d writes to %v admitted under the previous query rules are still running: %v", running, tables, ctx.Err())
		}
	}
	return nil
}
