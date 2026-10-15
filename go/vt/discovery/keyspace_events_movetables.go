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

package discovery

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync/atomic"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

// moveTablesBufferTokens hands out the tokens that identify one buffering
// cycle started by MarkShardMoveTablesBuffering.
var moveTablesBufferTokens atomic.Uint64

// moveTablesBuffering is the part of a keyspaceState that tracks the shards
// vtgate buffers for a MoveTables switch, i.e. whose primary denied a write to
// a table that the switch denies on it.
//
// Such buffering normally ends with the keyspace event for the SrvVSchema
// that switches the writes. Two cases never produce that event: a switch that
// is cancelled, which allows the tables again, and buffering that starts after
// the switch has already been seen. The watcher therefore evaluates the
// buffered shards when the buffering starts, on every SrvVSchema update, and on
// every health update from a buffered shard, and releases a shard with a
// keyspace event that carries its token once the keyspace's writes are
// switched or the shard no longer denies any table.
//
// All fields are protected by keyspaceState.mu.
type moveTablesBuffering struct {
	// lastSrvVSchema is the last SrvVSchema seen for the keyspace's cell.
	lastSrvVSchema *vschemapb.SrvVSchema
	// moveTablesBuffers maps each buffered shard to its buffering token.
	moveTablesBuffers map[string]uint64
	// mtEvalRunning is set while an evaluation runs, and mtEvalPending when
	// another one was requested in the meantime.
	mtEvalRunning bool
	mtEvalPending bool
}

// MarkShardMoveTablesBuffering records that vtgate starts buffering requests
// for the given shard because its primary denied a write to a table that a
// MoveTables switch denies on it, and starts evaluating whether the switch has
// already completed or was cancelled. It returns the token that the keyspace
// event ending this buffering will carry, and false if the watcher does not
// track the shard, in which case no keyspace event would end the buffering.
//
// Unlike MarkShardNotServing, this leaves the shard serving: its primary
// keeps serving, only the routing of its tables changes.
func (kew *KeyspaceEventWatcher) MarkShardMoveTablesBuffering(ctx context.Context, keyspace, shard string) (uint64, bool) {
	kss := kew.getKeyspaceStatus(ctx, keyspace)
	if kss == nil {
		// Only happens if the keyspace was deleted.
		return 0, false
	}
	kss.mu.Lock()
	defer kss.mu.Unlock()
	if kss.shards[shard] == nil {
		return 0, false
	}
	token := moveTablesBufferTokens.Add(1)
	if kss.moveTablesBuffers == nil {
		kss.moveTablesBuffers = make(map[string]uint64)
	}
	kss.moveTablesBuffers[shard] = token
	kss.requestMoveTablesEvalLocked()
	return token, true
}

// ClearShardMoveTablesBuffering forgets the buffering that
// MarkShardMoveTablesBuffering returned the token for, once it has ended for
// any reason. It does nothing if the shard has been marked again since.
func (kew *KeyspaceEventWatcher) ClearShardMoveTablesBuffering(keyspace, shard string, token uint64) {
	kew.mu.Lock()
	kss := kew.keyspaces[keyspace]
	kew.mu.Unlock()
	if kss == nil {
		return
	}
	kss.mu.Lock()
	defer kss.mu.Unlock()
	if kss.moveTablesBuffers[shard] == token {
		delete(kss.moveTablesBuffers, shard)
	}
}

// requestMoveTablesEvalLocked evaluates the buffered shards in the background,
// with at most one evaluation in flight per keyspace.
// Note: you MUST be holding kss.mu when calling this function.
func (kss *keyspaceState) requestMoveTablesEvalLocked() {
	if len(kss.moveTablesBuffers) == 0 {
		return
	}
	if kss.mtEvalRunning {
		kss.mtEvalPending = true
		return
	}
	kss.mtEvalRunning = true
	go kss.evalMoveTablesBuffers()
}

// evalMoveTablesBuffers runs evaluations until no further one was requested.
// The shard records are read without holding kss.mu, so that a slow topo
// server does not hold up the health check stream.
func (kss *keyspaceState) evalMoveTablesBuffers() {
	for {
		kss.mu.Lock()
		kss.mtEvalPending = false
		if len(kss.moveTablesBuffers) == 0 || kss.deleted {
			kss.mtEvalRunning = false
			kss.mu.Unlock()
			return
		}
		vs := kss.lastSrvVSchema
		buffers := maps.Clone(kss.moveTablesBuffers)
		kss.mu.Unlock()

		if ev := kss.evalMoveTablesBuffersOnce(vs, buffers); ev != nil {
			kss.kew.broadcast(ev)
		}

		kss.mu.Lock()
		if !kss.mtEvalPending {
			kss.mtEvalRunning = false
			kss.mu.Unlock()
			return
		}
		kss.mu.Unlock()
	}
}

// evalMoveTablesBuffersOnce returns the keyspace event that releases the
// buffered shards whose buffering is over, if any, and forgets them.
func (kss *keyspaceState) evalMoveTablesBuffersOnce(vs *vschemapb.SrvVSchema, buffers map[string]uint64) *KeyspaceEvent {
	// Only the buffered shards are read: their denied tables are the ones
	// whose routing matters to the buffered requests.
	shardInfos, err := kss.kew.readShards(kss.keyspace, slices.Collect(maps.Keys(buffers)))
	if err != nil {
		// The next SrvVSchema or health update evaluates again.
		log.Warn(fmt.Sprintf("keyspace %s: cannot evaluate the shards buffered for MoveTables: %v", kss.keyspace, err))
		return nil
	}
	// The routing rules gate of getMoveTablesStatus is not applied: it only
	// avoids shard record reads, and these were needed anyway.
	state := moveTablesStatusFromShards(kss.keyspace, vs, shardInfos)
	switched := state.Typ != MoveTablesNone && state.State == MoveTablesSwitched
	denied := make(map[string]bool, len(shardInfos))
	for _, si := range shardInfos {
		denied[si.ShardName()] = hasDeniedTables(si)
	}

	kss.mu.Lock()
	defer kss.mu.Unlock()
	ev := &KeyspaceEvent{
		Cell:     kss.kew.localCell,
		Keyspace: kss.keyspace,
	}
	if switched {
		ev.MoveTablesState = *state
	}
	for shard, token := range buffers {
		if kss.moveTablesBuffers[shard] != token {
			// Released or marked again since the snapshot.
			continue
		}
		if !switched && denied[shard] {
			// The switch is still in progress.
			continue
		}
		delete(kss.moveTablesBuffers, shard)
		shardEvent := ShardEvent{
			Target:                &querypb.Target{Keyspace: kss.keyspace, Shard: shard, TabletType: topodatapb.TabletType_PRIMARY},
			Serving:               true,
			MoveTablesBufferToken: token,
		}
		if sstate := kss.shards[shard]; sstate != nil {
			shardEvent.Tablet = sstate.currentPrimary
			shardEvent.Target = sstate.target
			shardEvent.Serving = sstate.serving
		}
		ev.Shards = append(ev.Shards, shardEvent)
	}
	if len(ev.Shards) == 0 {
		return nil
	}
	log.Info(fmt.Sprintf("keyspace %s: ending MoveTables buffering for %d shard(s), writes switched: %t", kss.keyspace, len(ev.Shards), switched))
	return ev
}

// hasDeniedTables returns whether the shard record denies any table.
func hasDeniedTables(si *topo.ShardInfo) bool {
	for _, tc := range si.TabletControls {
		if len(tc.DeniedTables) > 0 {
			return true
		}
	}
	return false
}
