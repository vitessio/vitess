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
	"sync"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	// groupReplicationPeerTimeout bounds the FullStatus RPCs that a tablet sends to the other
	// tablets of its shard to learn about its group.
	groupReplicationPeerTimeout = 2 * time.Second
	// groupReplicationBootstrapGrace is how long a tablet treats the incarnation of a group that it
	// bootstrapped itself as the shard's legitimate group, before the shard record lists it. The
	// component that asked for the bootstrap records the incarnation right after the bootstrap.
	groupReplicationBootstrapGrace = 1 * time.Minute
)

// groupReplicationPeers is what a tablet knows about the MySQL of the other tablets of its shard.
type groupReplicationPeers struct {
	mu sync.Mutex
	// serverUUIDs maps tablet aliases to the server_uuid of their MySQL, as their FullStatus last
	// reported it.
	serverUUIDs map[string]string
	// bootstrappedIncarnation is the incarnation of the last group that this tablet
	// bootstrapped, at bootstrappedAt.
	bootstrappedIncarnation string
	bootstrappedAt          time.Time
}

func (p *groupReplicationPeers) serverUUID(alias string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.serverUUIDs[alias]
}

func (p *groupReplicationPeers) setServerUUID(alias, uuid string) {
	if uuid == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.serverUUIDs == nil {
		p.serverUUIDs = make(map[string]string)
	}
	p.serverUUIDs[alias] = uuid
}

// noteBootstrap remembers that this tablet bootstrapped a group of the given incarnation.
func (p *groupReplicationPeers) noteBootstrap(incarnation string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.bootstrappedIncarnation = incarnation
	p.bootstrappedAt = time.Now()
}

// recentlyBootstrapped returns the incarnation of the group this tablet bootstrapped within the
// grace period, if any.
func (p *groupReplicationPeers) recentlyBootstrapped() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.bootstrappedIncarnation == "" || time.Since(p.bootstrappedAt) > groupReplicationBootstrapGrace {
		return ""
	}
	return p.bootstrappedIncarnation
}

// shardGroupRecord is what the topology says about the shard's legitimate replication group.
type shardGroupRecord struct {
	incarnation  string
	voters       []*topodatapb.TabletAlias
	primaryAlias *topodatapb.TabletAlias
	// tablets are the tablet records of the shard, by alias.
	tablets map[string]*topodatapb.Tablet
}

// readShardGroupRecord reads the shard's group incarnation and voters from the shard record, and
// the tablet records of the shard.
func (tm *TabletManager) readShardGroupRecord(ctx context.Context) (*shardGroupRecord, error) {
	tablet := tm.Tablet()
	si, err := tm.TopoServer.GetShard(ctx, tablet.Keyspace, tablet.Shard)
	if err != nil {
		return nil, vterrors.Wrapf(err, "cannot read shard %v/%v", tablet.Keyspace, tablet.Shard)
	}
	rec := &shardGroupRecord{
		incarnation:  si.GetGroupReplicationIncarnation(),
		voters:       si.GetGroupReplicationVoters(),
		primaryAlias: si.PrimaryAlias,
		tablets:      make(map[string]*topodatapb.Tablet),
	}
	tabletMap, err := tm.TopoServer.GetTabletMapForShard(ctx, tablet.Keyspace, tablet.Shard)
	if err != nil && !topo.IsErrType(err, topo.PartialResult) {
		return nil, vterrors.Wrapf(err, "cannot read the tablets of shard %v/%v", tablet.Keyspace, tablet.Shard)
	}
	for alias, ti := range tabletMap {
		if ti != nil && ti.Tablet != nil {
			rec.tablets[alias] = ti.Tablet
		}
	}
	return rec, nil
}

// legitimateGroup returns the shard's legitimate group as this tablet sees it. The incarnation of
// a group that this tablet bootstrapped within the grace period is legitimate too: the component
// that asked for the bootstrap records it right after. When fetchMissing is set, the tablet asks
// the voters whose server_uuid it does not know yet for their FullStatus.
func (tm *TabletManager) legitimateGroup(ctx context.Context, rec *shardGroupRecord, status *replicationdatapb.GroupReplicationStatus, fetchMissing bool) *policy.LegitimateGroup {
	self := topoproto.TabletAliasString(tm.tabletAlias)
	if uuid, err := tm.MysqlDaemon.GetServerUUID(ctx); err == nil {
		tm.groupReplicationPeers.setServerUUID(self, uuid)
	}
	if fetchMissing {
		var missing []*topodatapb.Tablet
		for _, voter := range rec.voters {
			alias := topoproto.TabletAliasString(voter)
			if tablet := rec.tablets[alias]; tablet != nil && alias != self && tm.groupReplicationPeers.serverUUID(alias) == "" {
				missing = append(missing, tablet)
			}
		}
		tm.peerFullStatuses(ctx, missing)
	}
	uuids := make(map[string]string, len(rec.tablets))
	for alias := range rec.tablets {
		uuids[alias] = tm.groupReplicationPeers.serverUUID(alias)
	}
	incarnation := rec.incarnation
	if bootstrapped := tm.groupReplicationPeers.recentlyBootstrapped(); bootstrapped != "" && bootstrapped == policy.GroupIncarnation(status.GetViewId()) {
		incarnation = bootstrapped
	}
	return policy.NewLegitimateGroup(incarnation, rec.voters, rec.tablets, uuids)
}

// peerFullStatuses reads the FullStatus of the given tablets concurrently, each bounded by
// groupReplicationPeerTimeout, and remembers the server_uuids they report. Unreachable tablets
// are left out of the result.
func (tm *TabletManager) peerFullStatuses(ctx context.Context, tablets []*topodatapb.Tablet) map[string]*replicationdatapb.FullStatus {
	result := make(map[string]*replicationdatapb.FullStatus, len(tablets))
	if len(tablets) == 0 || tm.tmc == nil {
		return result
	}
	var (
		mu sync.Mutex
		wg sync.WaitGroup
	)
	for _, tablet := range tablets {
		wg.Go(func() {
			peerCtx, cancel := context.WithTimeout(ctx, groupReplicationPeerTimeout)
			defer cancel()
			status, err := tm.tmc.FullStatus(peerCtx, tablet)
			if err != nil || status == nil {
				return
			}
			alias := topoproto.TabletAliasString(tablet.Alias)
			tm.groupReplicationPeers.setServerUUID(alias, status.ServerUuid)
			mu.Lock()
			defer mu.Unlock()
			result[alias] = status
		})
	}
	wg.Wait()
	return result
}

// leaveForeignGroupLocked makes MySQL leave a group that is not the shard's legitimate group, and
// suspends the tablet's own rejoins: MySQL formed or joined a group of another incarnation than
// the one the shard record lists, and that group does not hold the shard's acknowledged
// transactions. MySQL stays super_read_only. An explicit StartGroupReplication, which VTOrc sends
// once the legitimate group is active elsewhere, lifts the suspension. The caller holds the
// action lock.
func (tm *TabletManager) leaveForeignGroupLocked(ctx context.Context, status *replicationdatapb.GroupReplicationStatus, recorded string) {
	log.Error("Group replication: MySQL is a member of a group that is not the shard's replication group, leaving it",
		slog.String("group", status.GetGroupName()),
		slog.String("view_id", status.GetViewId()),
		slog.String("incarnation", policy.GroupIncarnation(status.GetViewId())),
		slog.String("recorded_incarnation", recorded),
		slog.String("state", status.GetMemberState()),
		slog.String("role", status.GetMemberRole()),
		slog.Int("online_members", mysql.OnlineGroupMembers(status)))
	tm.groupReplicationRejoinSuspended.Store(true)
	if tm.Tablet().Type == topodatapb.TabletType_PRIMARY {
		// Stop serving before MySQL leaves; the group this tablet followed is not the shard's.
		if err := tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_REPLICA, DBActionNone); err != nil {
			log.Error("Group replication: failed to demote the tablet to REPLICA", slog.Any("error", err))
		}
	}
	if err := tm.MysqlDaemon.StopGroupReplication(ctx); err != nil {
		log.Error("Group replication: failed to leave the foreign group", slog.Any("error", err))
	}
}
