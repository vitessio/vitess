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
	"slices"
	"sync"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	// groupReplicationPeerTimeout bounds the FullStatus RPCs that a tablet sends to the other
	// tablets of its shard to learn about its group.
	groupReplicationPeerTimeout = 2 * time.Second
	// groupReplicationBootstrapGrace is how long a tablet treats the incarnation of a group that it
	// bootstrapped itself as the shard's legitimate group, before the shard record lists it. The
	// component that asked for the bootstrap records the incarnation right after the bootstrap.
	groupReplicationBootstrapGrace = 1 * time.Minute
	// groupReplicationCellTimeout bounds the read of the shard's tablet records in each cell. The
	// topology server of a cell that is cut off does not answer until the caller gives up: read
	// under the caller's deadline, it took the whole step of the sync loop, every step, and the
	// reads of the other cells' tablet records failed with it. The elected member of a group
	// whose old primary's cell was partitioned was not promoted until the partition healed (S9i
	// chaos scenario).
	groupReplicationCellTimeout = 2 * time.Second
	// groupReplicationTabletsCacheTTL is how long the sync loop reuses the tablet records it read
	// with the shard record, as long as it can identify every listed voter from them or from its
	// server_uuid. The tablet records only give the voters' MySQL addresses, which do not change
	// while a tablet runs, and reading them costs groupReplicationCellTimeout while a cell is cut
	// off: a promotion does not wait for it.
	groupReplicationTabletsCacheTTL = 30 * time.Second
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
	// legitimateSeeds are the group replication addresses of the peers that were last seen as
	// active members of the shard's legitimate group, at legitimateSeedsAt.
	legitimateSeeds   []string
	legitimateSeedsAt time.Time
}

// groupReplicationLegitimateSeedsTTL is how long a join prefers the peers that were last seen as
// active members of the shard's legitimate group.
const groupReplicationLegitimateSeedsTTL = 30 * time.Second

func (p *groupReplicationPeers) setActiveSeeds(seeds []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.legitimateSeeds = seeds
	p.legitimateSeedsAt = time.Now()
}

// activeSeeds returns the peers that were seen as active members of the shard's legitimate group
// within groupReplicationLegitimateSeedsTTL.
func (p *groupReplicationPeers) activeSeeds() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if time.Since(p.legitimateSeedsAt) > groupReplicationLegitimateSeedsTTL {
		return nil
	}
	return p.legitimateSeeds
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
	// durabilityPolicy is the shard's own durability policy, "" if the shard record does not set
	// one (see TabletManager.shardDurability).
	durabilityPolicy string
	// intent is the shard's bootstrap intent, if it applies to the recorded incarnation (see
	// reparentutil.CurrentGroupReplicationBootstrapIntent).
	intent *topodatapb.GroupReplicationBootstrapIntent
	// identities are the voter identities that the shard record holds (see publishVoterIdentity).
	identities []*topodatapb.GroupReplicationVoterIdentity
	// tablets are the tablet records of the shard, by alias, read at tabletsRead.
	tablets     map[string]*topodatapb.Tablet
	tabletsRead time.Time
}

// readShardGroupRecord reads the shard's group incarnation and voters from the shard record, and
// the tablet records of the shard. The tablet records of prev, if any, are reused when they were
// read within groupReplicationTabletsCacheTTL and identify, with the known server_uuids, every
// voter of the shard record.
func (tm *TabletManager) readShardGroupRecord(ctx context.Context, prev *shardGroupRecord) (*shardGroupRecord, error) {
	tablet := tm.Tablet()
	// A record that the shard watch delivers while this read is on its way is newer (storeRecord).
	readGen := tm.groupReplicationTopo.readGeneration()
	si, err := tm.TopoServer.GetShard(ctx, tablet.Keyspace, tablet.Shard)
	if err != nil {
		return nil, vterrors.Wrapf(err, "cannot read shard %v/%v", tablet.Keyspace, tablet.Shard)
	}
	rec := &shardGroupRecord{
		incarnation:      si.GetGroupReplicationIncarnation(),
		voters:           si.GetGroupReplicationVoters(),
		primaryAlias:     si.PrimaryAlias,
		durabilityPolicy: si.GetDurabilityPolicy(),
		intent:           reparentutil.CurrentGroupReplicationBootstrapIntent(si.Shard),
		identities:       si.GetGroupReplicationVoterIdentities(),
		tablets:          make(map[string]*topodatapb.Tablet),
	}
	if prev != nil && time.Since(prev.tabletsRead) < groupReplicationTabletsCacheTTL && tm.identifiesVoters(rec.voters, prev.tablets) {
		rec.tablets, rec.tabletsRead = prev.tablets, prev.tabletsRead
		tm.groupReplicationTopo.storeRecord(rec, readGen)
		return rec, nil
	}
	rec.tabletsRead = time.Now()
	// The tablet records only complete what the tablet knows about the voters (their MySQL
	// address): the records of the cells that answer in time are enough, and a voter whose record
	// is missing is still found by its server_uuid (buildLegitimateGroup).
	tabletMap, err := tm.TopoServer.GetTabletMapForShardWithCellTimeout(ctx, tablet.Keyspace, tablet.Shard, groupReplicationCellTimeout)
	if err != nil && !topo.IsErrType(err, topo.PartialResult) {
		return nil, vterrors.Wrapf(err, "cannot read the tablets of shard %v/%v", tablet.Keyspace, tablet.Shard)
	}
	tm.groupReplicationTopo.setTablets(tabletMap, err != nil)
	for alias, ti := range tabletMap {
		if ti != nil && ti.Tablet != nil {
			rec.tablets[alias] = ti.Tablet
		}
	}
	tm.groupReplicationTopo.storeRecord(rec, readGen)
	return rec, nil
}

// identifiesVoters returns whether every voter has a tablet record in tablets, or a known
// server_uuid.
func (tm *TabletManager) identifiesVoters(voters []*topodatapb.TabletAlias, tablets map[string]*topodatapb.Tablet) bool {
	for _, voter := range voters {
		alias := topoproto.TabletAliasString(voter)
		if tablets[alias] == nil && tm.groupReplicationPeers.serverUUID(alias) == "" {
			return false
		}
	}
	return true
}

// legitimateGroup returns the shard's legitimate group as this tablet sees it. The incarnation of
// a group that this tablet bootstrapped within the grace period is legitimate too: the component
// that asked for the bootstrap records it right after. When fetchMissing is set and the member's
// view does not hold a majority of the voters that the tablet can identify, the tablet asks the
// voters whose server_uuid it does not know yet for their FullStatus, until the majority is
// established or every answer is in: a voter that does not answer, for example the failed
// primary, does not delay a promotion that does not depend on it.
func (tm *TabletManager) legitimateGroup(ctx context.Context, rec *shardGroupRecord, status *replicationdatapb.GroupReplicationStatus, fetchMissing bool) *policy.LegitimateGroup {
	self := topoproto.TabletAliasString(tm.tabletAlias)
	if uuid, err := tm.MysqlDaemon.GetServerUUID(ctx); err == nil {
		tm.groupReplicationPeers.setServerUUID(self, uuid)
	}
	legitimate := tm.buildLegitimateGroup(rec, status)
	if !fetchMissing || legitimate.HasVoterMajority(status) {
		return legitimate
	}
	missing := tm.votersWithoutServerUUID(rec)
	if len(missing) == 0 {
		// The sync loop's background fetch (warmVoterServerUUIDs) may have learned the missing
		// server_uuids since legitimate was built: the group built before lacks those voters.
		return tm.buildLegitimateGroup(rec, status)
	}
	tm.fetchPeerServerUUIDs(ctx, missing, func() bool {
		return tm.buildLegitimateGroup(rec, status).HasVoterMajority(status)
	})
	return tm.buildLegitimateGroup(rec, status)
}

// votersWithoutServerUUID returns the tablets of the listed voters, other than this tablet, whose
// server_uuid the tablet does not know.
func (tm *TabletManager) votersWithoutServerUUID(rec *shardGroupRecord) []*topodatapb.Tablet {
	self := topoproto.TabletAliasString(tm.tabletAlias)
	var missing []*topodatapb.Tablet
	for _, voter := range rec.voters {
		alias := topoproto.TabletAliasString(voter)
		if tablet := rec.tablets[alias]; tablet != nil && alias != self && tm.groupReplicationPeers.serverUUID(alias) == "" {
			missing = append(missing, tablet)
		}
	}
	return missing
}

// fetchPeerServerUUIDs asks the given tablets for their FullStatus concurrently, each bounded by
// groupReplicationPeerTimeout, and remembers the server_uuids they report. It returns as soon as
// done returns true, which it checks first: the server_uuids can be learned concurrently, by the
// sync loop's background fetch, between the caller's check and this call, and waiting for an
// answer then made a promotion wait for a voter that does not answer. It otherwise returns once
// every tablet answered or timed out.
func (tm *TabletManager) fetchPeerServerUUIDs(ctx context.Context, tablets []*topodatapb.Tablet, done func() bool) {
	if tm.tmc == nil || done() {
		return
	}
	answered := make(chan struct{}, len(tablets))
	for _, tablet := range tablets {
		go func() {
			defer func() { answered <- struct{}{} }()
			peerCtx, cancel := context.WithTimeout(ctx, groupReplicationPeerTimeout)
			defer cancel()
			if status, err := tm.tmc.FullStatus(peerCtx, tablet); err == nil && status != nil {
				tm.groupReplicationPeers.setServerUUID(topoproto.TabletAliasString(tablet.Alias), status.ServerUuid)
			}
		}()
	}
	for range tablets {
		<-answered
		if done() {
			return
		}
	}
}

// buildLegitimateGroup returns the shard's legitimate group from the record and the server_uuids
// the tablet knows.
func (tm *TabletManager) buildLegitimateGroup(rec *shardGroupRecord, status *replicationdatapb.GroupReplicationStatus) *policy.LegitimateGroup {
	incarnation := rec.incarnation
	if bootstrapped := tm.groupReplicationPeers.recentlyBootstrapped(); bootstrapped != "" && bootstrapped == policy.GroupIncarnation(status.GetViewId()) {
		incarnation = bootstrapped
	}
	return policy.NewLegitimateGroup(incarnation, rec.voters, rec.tablets, tm.knownServerUUIDs(rec))
}

// recordedLegitimateGroup returns the shard's legitimate group exactly as the shard record lists it:
// unlike buildLegitimateGroup, it does not trust the incarnation of a group that the tablet
// bootstrapped itself. It does not ask the voters for their server_uuids.
func (tm *TabletManager) recordedLegitimateGroup(ctx context.Context, rec *shardGroupRecord) *policy.LegitimateGroup {
	if uuid, err := tm.MysqlDaemon.GetServerUUID(ctx); err == nil {
		tm.groupReplicationPeers.setServerUUID(topoproto.TabletAliasString(tm.tabletAlias), uuid)
	}
	return policy.NewLegitimateGroup(rec.incarnation, rec.voters, rec.tablets, tm.knownServerUUIDs(rec))
}

// knownServerUUIDs returns the server_uuids the tablet knows for the tablets and voters of the record.
func (tm *TabletManager) knownServerUUIDs(rec *shardGroupRecord) map[string]string {
	uuids := make(map[string]string, len(rec.tablets)+len(rec.voters))
	for alias := range rec.tablets {
		uuids[alias] = tm.groupReplicationPeers.serverUUID(alias)
	}
	// A voter whose tablet record could not be read is still found by its server_uuid.
	for _, voter := range rec.voters {
		alias := topoproto.TabletAliasString(voter)
		uuids[alias] = tm.groupReplicationPeers.serverUUID(alias)
	}
	return uuids
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

// isActiveLegitimatePeer returns whether a peer's MySQL is an active member of the shard's
// legitimate group whose view has quorum: a group that a joining member can join.
func isActiveLegitimatePeer(legitimate *policy.LegitimateGroup, groupName string, status *replicationdatapb.FullStatus) bool {
	gs := status.GetGroupReplicationStatus()
	return gs.GetGroupName() == groupName && legitimate.IsLegitimateMember(gs) && gs.GetHasQuorum()
}

// legitimateGroupActiveElsewhere returns whether another tablet of the shard reports that its
// MySQL is an active member of the shard's legitimate group, with quorum in its view. A member
// that starts Group Replication while no such group exists cannot join anything: its START blocks
// until MySQL's join timeout, during which a bootstrap on it fails, and it has been seen to form a
// group of its own. The shard primary is asked first; the other tablets only if it does not
// qualify. Every RPC is bounded by groupReplicationPeerTimeout.
func (tm *TabletManager) legitimateGroupActiveElsewhere(ctx context.Context, rec *shardGroupRecord) bool {
	tablet := tm.Tablet()
	self := topoproto.TabletAliasString(tm.tabletAlias)
	groupName := policy.GroupName(tablet.Keyspace, tablet.Shard)
	// The peers' own view ids are compared with the recorded incarnation only.
	legitimate := policy.NewLegitimateGroup(rec.incarnation, nil, nil, nil)

	var primary *topodatapb.Tablet
	var others []*topodatapb.Tablet
	for alias, peer := range rec.tablets {
		switch {
		case alias == self:
		case rec.primaryAlias != nil && topoproto.TabletAliasEqual(peer.Alias, rec.primaryAlias):
			primary = peer
		case groupReplicationAddress(peer) != "":
			others = append(others, peer)
		}
	}
	if primary != nil {
		for _, status := range tm.peerFullStatuses(ctx, []*topodatapb.Tablet{primary}) {
			if isActiveLegitimatePeer(legitimate, groupName, status) {
				tm.groupReplicationPeers.setActiveSeeds([]string{groupReplicationAddress(primary)})
				return true
			}
		}
	}
	var seeds []string
	for alias, status := range tm.peerFullStatuses(ctx, others) {
		if isActiveLegitimatePeer(legitimate, groupName, status) {
			seeds = append(seeds, groupReplicationAddress(rec.tablets[alias]))
		}
	}
	if len(seeds) == 0 {
		return false
	}
	slices.Sort(seeds)
	tm.groupReplicationPeers.setActiveSeeds(seeds)
	return true
}

// checkLegitimateGroupToJoin returns an error unless the shard record lists the incarnation of the
// shard's group and another tablet of the shard reports an active member of that group. Joins that
// the tablet starts on its own, at startup and in the sync loop, call it first.
//
// While the shard record lists no incarnation, every group counts as the shard's group, also the
// group of a bootstrap whose incarnation is not recorded yet, and a group of one that a join formed
// when the member it joined left (the TLA+ model's init_orc_lost): a join on the tablet's own could
// make such a group a majority of the voters, whose primary serves, and whose writes the incarnation
// recorded later does not hold. The component that bootstraps the group records its incarnation and
// then makes the voters join (VTOrc, the migration), or the voters join on their own once it is
// recorded (PlannedReparentShard's initial promotion).
func (tm *TabletManager) checkLegitimateGroupToJoin(ctx context.Context) error {
	rec, err := tm.readShardGroupRecord(ctx, nil)
	if err != nil {
		return err
	}
	if rec.incarnation == "" {
		return errNoRecordedIncarnationToJoin
	}
	if !tm.legitimateGroupActiveElsewhere(ctx, rec) {
		return errNoLegitimateGroupToJoin
	}
	return nil
}

// errNoRecordedIncarnationToJoin is returned when the shard record lists no incarnation of the
// shard's group (see checkLegitimateGroupToJoin).
var errNoRecordedIncarnationToJoin = vterrors.New(vtrpcpb.Code_UNAVAILABLE, "the shard record lists no incarnation of the shard's replication group; not joining until the bootstrap of the group is recorded")

// errNoLegitimateGroupToJoin is returned when no other tablet of the shard reports an active
// member of the shard's legitimate group.
var errNoLegitimateGroupToJoin = vterrors.New(vtrpcpb.Code_UNAVAILABLE, "no other tablet of the shard reports an active member of the shard's replication group with quorum; not joining, the group must be bootstrapped first")

// leaveForeignGroupLocked makes MySQL leave a group that is not the shard's legitimate group:
// MySQL formed or joined a group of another incarnation than the one the shard record lists, and
// that group does not hold the shard's acknowledged transactions. MySQL is fenced with
// super_read_only first: MySQL makes the primary of such a group writable, and its STOP
// GROUP_REPLICATION took 4.7s in the chaos tests, during which clients that write to MySQL directly
// could commit transactions that the shard's group does not have (doc/failover-audit/
// GroupReplication.md, "A join can form a group of one"). MySQL stays super_read_only afterwards.
// The tablet's own rejoins are not suspended: like any rejoin, the next one only starts once
// another tablet reports an active member of the legitimate group (checkLegitimateGroupToJoin).
// Waiting for VTOrc instead kept such a member out of its group until the group had a primary
// tablet again, which it could not get while the member was missing from its majority. The
// caller holds the action lock.
func (tm *TabletManager) leaveForeignGroupLocked(ctx context.Context, status *replicationdatapb.GroupReplicationStatus, recorded string) {
	log.Error("Group replication: MySQL is a member of a group that is not the shard's replication group, leaving it",
		slog.String("group", status.GetGroupName()),
		slog.String("view_id", status.GetViewId()),
		slog.String("incarnation", policy.GroupIncarnation(status.GetViewId())),
		slog.String("recorded_incarnation", recorded),
		slog.String("state", status.GetMemberState()),
		slog.String("role", status.GetMemberRole()),
		slog.Int("online_members", mysql.OnlineGroupMembers(status)))
	if f := &tm.groupReplicationFence; mysql.IsGroupPrimary(status) {
		// Fence first: the demotion below may wait for the topology, and MySQL's leave takes
		// seconds, during which MySQL would take writes. Under the action lock, the decision stands.
		if _, ok := f.decide(f.epoch.Load(), groupReplicationUnrecordedIncarnation, func() bool { return false }); ok {
			if err := tm.setFenceSuperReadOnly(ctx); err != nil {
				log.Error("Group replication: failed to fence MySQL with super_read_only before it leaves the foreign group", slog.Any("error", err))
			}
			f.unlock()
		}
	}
	if tm.Tablet().Type == topodatapb.TabletType_PRIMARY {
		// Stop serving before MySQL leaves; the group this tablet followed is not the shard's. The
		// record is published in the background if the topology server does not answer in time.
		if err := tm.tmState.ChangeTabletTypeWithPublishTimeout(ctx, topodatapb.TabletType_REPLICA, DBActionNone, groupReplicationDemotionPublishTimeout); err != nil {
			log.Error("Group replication: failed to demote the tablet to REPLICA", slog.Any("error", err))
		}
	}
	if err := tm.MysqlDaemon.StopGroupReplication(ctx); err != nil {
		log.Error("Group replication: failed to leave the foreign group", slog.Any("error", err))
		return
	}
	// MySQL is out of any group, and Group Replication keeps it super_read_only.
	tm.groupReplicationFence.reset()
}
