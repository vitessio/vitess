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

package tabletmanager

import (
	"context"
	"log/slog"
	"maps"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// groupReplicationTopoCache is what the tablet last read from the topology about its keyspace's
// durability policy, its shard's voters and its shard's tablet records. A join or a bootstrap
// (StartGroupReplication) falls back to it when the topology does not answer within
// groupReplicationTopoReadTimeout: these RPCs recover the shard's group, typically right after a
// partition, and must not wait for a topology server that does not answer yet.
type groupReplicationTopoCache struct {
	mu sync.Mutex
	// durability is the name of the shard's durability policy (shardDurability), if hasDurability
	// is set.
	durability    string
	hasDurability bool
	// voters are the shard's voters, if hasVoters is set.
	voters    []*topodatapb.TabletAlias
	hasVoters bool
	// tablets are the shard's tablet records, by alias, if any was read.
	tablets map[string]*topo.TabletInfo
	// record is the shard's group record that the tablet read last (readShardGroupRecord), if any.
	// Its tablet records are reused by the next read while they identify every voter.
	record *shardGroupRecord
	// generation counts the shard records that the shard watch delivered. A reader captures it
	// before it reads the shard record, and stores the shard fields it read only if no watched
	// record arrived meanwhile: that one is newer (see storeRecord, noteShard).
	generation uint64
}

// readGeneration returns the generation to capture before reading the shard record.
func (c *groupReplicationTopoCache) readGeneration() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.generation
}

// storeRecord records a group record that the caller read after it captured readGen. If the shard
// watch delivered a record since, the shard fields of that newer record are kept, in the stored
// record and in rec, which the caller then uses; only the tablet records of rec are taken.
func (c *groupReplicationTopoCache) storeRecord(rec *shardGroupRecord, readGen uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.generation != readGen && c.record != nil {
		rec.incarnation = c.record.incarnation
		rec.voters = c.record.voters
		rec.primaryAlias = c.record.primaryAlias
		rec.durabilityPolicy = c.record.durabilityPolicy
		rec.intent = c.record.intent
		rec.identities = c.record.identities
	} else {
		c.voters, c.hasVoters = rec.voters, true
	}
	c.record = rec
}

// noteShard records the group fields of a shard record: one that the shard watch delivered
// (fromWatch), which starts a new generation, or one that a reader read after it captured readGen,
// which is dropped if the watch delivered a record since. It returns whether it recorded them.
func (c *groupReplicationTopoCache) noteShard(si *topodatapb.Shard, fromWatch bool, readGen uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if fromWatch {
		c.generation++
	} else if c.generation != readGen {
		return false
	}
	c.voters, c.hasVoters = si.GetGroupReplicationVoters(), true
	if c.record != nil {
		rec := *c.record
		rec.incarnation = si.GetGroupReplicationIncarnation()
		rec.voters = si.GetGroupReplicationVoters()
		rec.primaryAlias = si.GetPrimaryAlias()
		rec.durabilityPolicy = si.GetDurabilityPolicy()
		rec.intent = reparentutil.CurrentGroupReplicationBootstrapIntent(si)
		rec.identities = si.GetGroupReplicationVoterIdentities()
		c.record = &rec
	}
	return true
}

func (c *groupReplicationTopoCache) lastRecord() *shardGroupRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.record
}

func (c *groupReplicationTopoCache) setDurability(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.durability, c.hasDurability = name, true
}

func (c *groupReplicationTopoCache) lastDurability() (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.durability, c.hasDurability
}

func (c *groupReplicationTopoCache) lastVoters() ([]*topodatapb.TabletAlias, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.voters, c.hasVoters
}

// setTablets records the tablet records read from the topology. A partial result, from the cells
// that answered, replaces the records of those tablets only.
func (c *groupReplicationTopoCache) setTablets(tablets map[string]*topo.TabletInfo, partial bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !partial || c.tablets == nil {
		c.tablets = maps.Clone(tablets)
		return
	}
	merged := maps.Clone(c.tablets)
	maps.Copy(merged, tablets)
	c.tablets = merged
}

func (c *groupReplicationTopoCache) lastTablets() map[string]*topo.TabletInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tablets
}

// withTopoReadDeadline returns ctx bounded by deadline when bounded is set: the caller has a value
// to fall back to if the topology does not answer by then.
func withTopoReadDeadline(ctx context.Context, deadline time.Time, bounded bool) (context.Context, context.CancelFunc) {
	if !bounded || deadline.IsZero() {
		return ctx, func() {}
	}
	return context.WithDeadline(ctx, deadline)
}

// durabilityForGroupChange returns the shard's durability policy for a join or a bootstrap. If
// the tablet read the policy before, it waits for the topology until deadline only, and otherwise
// uses the policy it read last. A zero deadline does not bound the read.
func (tm *TabletManager) durabilityForGroupChange(ctx context.Context, deadline time.Time) (policy.Durabler, error) {
	last, known := tm.groupReplicationTopo.lastDurability()
	readCtx, cancel := withTopoReadDeadline(ctx, deadline, known)
	defer cancel()
	durability, err := tm.shardDurability(readCtx)
	if err == nil || !known || ctx.Err() != nil {
		return durability, err
	}
	log.Warn("Group replication: the topology did not answer in time, using the durability policy read last",
		slog.String("durability_policy", last), slog.Any("error", err))
	durability, perr := policy.GetDurabilityPolicy(last)
	if perr != nil {
		return nil, vterrors.Wrapf(perr, "cannot get durability policy %v", last)
	}
	return durability, nil
}

// votersForGroupChange returns the voters of the shard's group for a join or a bootstrap, like
// durabilityForGroupChange does for the durability policy.
func (tm *TabletManager) votersForGroupChange(ctx context.Context, deadline time.Time) ([]*topodatapb.TabletAlias, error) {
	last, known := tm.groupReplicationTopo.lastVoters()
	readCtx, cancel := withTopoReadDeadline(ctx, deadline, known)
	defer cancel()
	voters, err := tm.groupReplicationVoters(readCtx)
	if err == nil || !known || ctx.Err() != nil {
		return voters, err
	}
	log.Warn("Group replication: the topology did not answer in time, using the voters read last",
		slog.Int("voters", len(last)), slog.Any("error", err))
	return last, nil
}

// tabletsForGroupChange returns the tablet records of the shard for a join or a bootstrap: the
// group seeds are derived from them. The tablets of the cells that answer in time are enough; when
// none answers by deadline, it uses the records it read last, if any. A zero deadline does not
// bound the read.
func (tm *TabletManager) tabletsForGroupChange(ctx context.Context, keyspace, shard string, deadline time.Time) (map[string]*topo.TabletInfo, error) {
	last := tm.groupReplicationTopo.lastTablets()
	readCtx, cancel := withTopoReadDeadline(ctx, deadline, last != nil)
	defer cancel()
	tablets, err := tm.TopoServer.GetTabletMapForShardWithCellTimeout(readCtx, keyspace, shard, groupReplicationCellTimeout)
	switch {
	case err == nil:
		tm.groupReplicationTopo.setTablets(tablets, false)
		return tablets, nil
	case topo.IsErrType(err, topo.PartialResult):
		tm.groupReplicationTopo.setTablets(tablets, true)
		return tablets, nil
	case last != nil && ctx.Err() == nil:
		log.Warn("Group replication: the topology did not answer in time, using the tablet records read last",
			slog.Int("tablets", len(last)), slog.Any("error", err))
		return last, nil
	default:
		return nil, vterrors.Wrapf(err, "cannot read the tablets of shard %v/%v", keyspace, shard)
	}
}
