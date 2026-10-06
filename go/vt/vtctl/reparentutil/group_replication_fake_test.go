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

package reparentutil

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	querypb "vitess.io/vitess/go/vt/proto/query"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// fakeGRTablet is the simulated MySQL and vttablet state of one tablet.
type fakeGRTablet struct {
	alias string
	cell  string
	uuid  string
	// grEnabled is reported in FullStatus: the tablet supports group replication.
	grEnabled bool
	// shardPolicy is reported in FullStatus: the tablet applies the shard's own durability
	// policy over the keyspace's.
	shardPolicy bool
	// primary marks the shard's topo primary. Its tablet loop enforces the effective
	// semi-sync rule.
	primary bool
	// unreachable makes every RPC to the tablet fail.
	unreachable bool
	// member is set while the MySQL is an active member of the group. state overrides
	// ONLINE for the member's own state.
	member bool
	state  string
	// source is the alias of the async replication source, if the tablet replicates.
	source          string
	semiSyncReplica bool
	// semiSyncPrimary is the primary's current rpl_semi_sync_source_enabled. The tablet
	// loop updates it to the effective value after each FullStatus, so a change is only
	// visible on the second poll.
	semiSyncPrimary bool
	superReadOnly   bool
	version         string
	gtidMode        string
	// incarnation overrides the incarnation of the group the member reports, to simulate a
	// member alone in a group it formed on its own.
	incarnation string
	// groupName overrides the group name the tablet reports, to simulate a member of another
	// shard's group.
	groupName string
	// startInProgress makes the tablet report a START GROUP_REPLICATION in progress.
	startInProgress bool
}

// fakeGRCluster is a TabletManagerClient that simulates a shard running asynchronous
// replication, MySQL Group Replication, or a mix during a migration, with the tablet
// behaviour described in doc/design-docs/GroupReplication.md. It records the calls that
// change state, and records a violation whenever a step would make the primary's commits
// block (the last semi-sync acker leaves while semi-sync is enabled) or a primary leave its
// group without semi-sync while the policy requires it.
type fakeGRCluster struct {
	tmclient.TabletManagerClient

	t        *testing.T
	ts       *topo.Server
	keyspace string

	mu           sync.Mutex
	tablets      map[string]*fakeGRTablet
	tabletRecs   map[string]*topodatapb.Tablet
	groupPrimary string
	// incarnation is the incarnation of the group's view ids. Every bootstrap creates a new one.
	incarnation   string
	bootstrapSeqs int
	// noQuorum makes every member report that its view has no quorum.
	noQuorum   bool
	calls      []string
	violations []string
	// failOnce fails the named call ("StartGroupReplication(zone3-0000000300)") once.
	failOnce   map[string]bool
	schemaRows []string
	queries    []string
	// votersAtInitPrimary are the voters the shard record listed when InitPrimary ran.
	votersAtInitPrimary string
	// keyspaceAtFirstBootstrap is the keyspace record when the first StartGroupReplication with a
	// bootstrap ran.
	keyspaceAtFirstBootstrap *topodatapb.Keyspace
	// onQuery runs, with the number of queries so far, when ExecuteFetchAsDba runs (the migration's
	// schema check), under c.mu: it may only change the topology.
	onQuery func(n int)
	// onCall runs once when the named call that changes state is made, under c.mu: it may only
	// change the topology.
	onCall map[string]func()
}

func (c *fakeGRCluster) record(call string) error {
	c.calls = append(c.calls, call)
	if hook := c.onCall[call]; hook != nil {
		delete(c.onCall, call)
		hook()
	}
	if c.failOnce[call] {
		delete(c.failOnce, call)
		return fmt.Errorf("injected failure of %s", call)
	}
	return nil
}

func (c *fakeGRCluster) get(tablet *topodatapb.Tablet) (*fakeGRTablet, error) {
	ft, ok := c.tablets[topoproto.TabletAliasString(tablet.Alias)]
	if !ok {
		return nil, fmt.Errorf("unknown tablet %v", topoproto.TabletAliasString(tablet.Alias))
	}
	if ft.unreachable {
		return nil, fmt.Errorf("tablet %v is unreachable", ft.alias)
	}
	return ft, nil
}

func (c *fakeGRCluster) members() []*fakeGRTablet {
	var members []*fakeGRTablet
	for _, alias := range slices.Sorted(maps.Keys(c.tablets)) {
		if ft := c.tablets[alias]; ft.member {
			members = append(members, ft)
		}
	}
	return members
}

func (c *fakeGRCluster) onlineMembers() int {
	n := 0
	for _, ft := range c.members() {
		if !ft.unreachable && (ft.state == "" || ft.state == mysql.GroupMemberStateOnline) {
			n++
		}
	}
	return n
}

// policyNeedsSemiSync returns whether the shard's durability policy, its own or else the
// keyspace's, requires the primary to use semi-sync. A tablet that does not know the shard's own
// policy uses the keyspace's.
func (c *fakeGRCluster) policyNeedsSemiSync(ft *fakeGRTablet) bool {
	name, err := c.ts.GetShardDurability(c.t.Context(), c.keyspace, "-")
	if !ft.shardPolicy {
		name, err = c.ts.GetKeyspaceDurability(c.t.Context(), c.keyspace)
	}
	require.NoError(c.t, err)
	d, err := policy.GetDurabilityPolicy(name)
	require.NoError(c.t, err)
	return policy.SemiSyncAckers(d, c.tabletRecs[ft.alias]) > 0
}

// effectiveSemiSync is the tablet loop's rule: semi-sync is required if the policy needs it
// and the tablet is not an active member of a group with at least two ONLINE members, unless the
// shard's own policy, which the tablet knows, is the asynchronous policy of a migration back: then
// the primary enables semi-sync as soon as an acker replicates from it, while the group runs. It
// enables semi-sync only while an acker replicates, and keeps it.
func (c *fakeGRCluster) effectiveSemiSync(ft *fakeGRTablet) bool {
	if !ft.primary || !c.policyNeedsSemiSync(ft) {
		return false
	}
	superseded := ft.member && c.onlineMembers() >= 2 && !c.leavingGroup(ft)
	if superseded {
		return false
	}
	return ft.semiSyncPrimary || c.connectedAckers("") > 0 || !ft.member || c.onlineMembers() < 2
}

// leavingGroup returns whether the tablet knows the shard's own policy, and that policy is not a
// group replication policy.
func (c *fakeGRCluster) leavingGroup(ft *fakeGRTablet) bool {
	if !ft.shardPolicy {
		return false
	}
	si, err := c.ts.GetShard(c.t.Context(), c.keyspace, "-")
	require.NoError(c.t, err)
	if si.DurabilityPolicy == "" {
		return false
	}
	d, err := policy.GetDurabilityPolicy(si.DurabilityPolicy)
	require.NoError(c.t, err)
	return !policy.IsGroupReplication(d)
}

// connectedAckers counts the async semi-sync replicas of the primary.
func (c *fakeGRCluster) connectedAckers(exclude string) int {
	n := 0
	for _, ft := range c.tablets {
		if ft.alias != exclude && !ft.member && !ft.unreachable && ft.semiSyncReplica && ft.source != "" && c.tablets[ft.source].primary {
			n++
		}
	}
	return n
}

func (c *fakeGRCluster) primaryTablet() *fakeGRTablet {
	for _, ft := range c.tablets {
		if ft.primary {
			return ft
		}
	}
	return nil
}

func (c *fakeGRCluster) groupStatus(ft *fakeGRTablet) *replicationdatapb.GroupReplicationStatus {
	gs := &replicationdatapb.GroupReplicationStatus{
		PluginActive: true,
		GroupName:    policy.GroupName(c.keyspace, "-"),
		MemberState:  mysql.GroupMemberStateOffline,
	}
	if ft.groupName != "" {
		gs.GroupName = ft.groupName
	}
	gs.StartInProgress = ft.startInProgress
	if !ft.member {
		return gs
	}
	online := 0
	for _, m := range c.members() {
		state := mysql.GroupMemberStateOnline
		switch {
		case m.unreachable:
			state = mysql.GroupMemberStateUnreachable
		case m.state != "":
			state = m.state
		}
		role := mysql.GroupMemberRoleSecondary
		if m.alias == c.groupPrimary {
			role = mysql.GroupMemberRolePrimary
		}
		gs.Members = append(gs.Members, &replicationdatapb.GroupReplicationMember{MemberUuid: m.uuid, State: state, Role: role})
		if state == mysql.GroupMemberStateOnline {
			online++
			if role == mysql.GroupMemberRolePrimary {
				gs.PrimaryUuid = m.uuid
			}
		}
		if m == ft {
			gs.MemberState = state
			if state == mysql.GroupMemberStateOnline {
				gs.MemberRole = role
			}
		}
	}
	gs.HasQuorum = !c.noQuorum && online > len(gs.Members)/2
	incarnation := c.incarnation
	if ft.incarnation != "" {
		incarnation = ft.incarnation
	}
	gs.ViewId = incarnation + ":" + strconv.Itoa(len(gs.Members))
	return gs
}

// FullStatus is part of the tmclient.TabletManagerClient interface.
func (c *fakeGRCluster) FullStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.FullStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ft, err := c.get(tablet)
	if err != nil {
		return nil, err
	}
	fs := &replicationdatapb.FullStatus{
		ServerUuid:                  ft.uuid,
		Version:                     ft.version,
		GtidMode:                    ft.gtidMode,
		BinlogFormat:                "ROW",
		SemiSyncPrimaryEnabled:      ft.semiSyncPrimary,
		SemiSyncReplicaEnabled:      ft.semiSyncReplica,
		SemiSyncWaitForReplicaCount: 1,
		SuperReadOnly:               ft.superReadOnly,
		ReadOnly:                    ft.superReadOnly,
		GroupReplicationStatus:      c.groupStatus(ft),
		GroupReplicationEnabled:     ft.grEnabled,
		// A tablet that knows the shard's own durability policy reports it.
		ShardDurabilityPolicySupported: ft.shardPolicy,
	}
	if ft.source != "" {
		src := c.tabletRecs[ft.source]
		fs.ReplicationStatus = &replicationdatapb.Status{
			SourceHost: src.MysqlHostname,
			SourcePort: src.MysqlPort,
			IoState:    int32(replication.ReplicationStateRunning),
			SqlState:   int32(replication.ReplicationStateRunning),
		}
	}
	// The tablet loop runs after the status was read.
	if ft.primary {
		ft.semiSyncPrimary = c.effectiveSemiSync(ft)
	}
	return fs, nil
}

// StartGroupReplication is part of the tmclient.TabletManagerClient interface.
func (c *fakeGRCluster) StartGroupReplication(ctx context.Context, tablet *topodatapb.Tablet, req *tabletmanagerdatapb.StartGroupReplicationRequest) (*replicationdatapb.GroupReplicationStatus, error) {
	bootstrap := req.GetBootstrap()
	c.mu.Lock()
	defer c.mu.Unlock()
	ft, err := c.get(tablet)
	if err != nil {
		return nil, err
	}
	name := fmt.Sprintf("StartGroupReplication(%s)", ft.alias)
	if bootstrap {
		name = fmt.Sprintf("StartGroupReplication(%s, bootstrap)", ft.alias)
		if c.keyspaceAtFirstBootstrap == nil {
			ki, err := c.ts.GetKeyspace(ctx, c.keyspace)
			if err != nil {
				return nil, err
			}
			c.keyspaceAtFirstBootstrap = ki.CloneVT()
		}
	}
	if err := c.record(name); err != nil {
		return nil, err
	}
	if ft.member {
		return c.groupStatus(ft), nil
	}
	if bootstrap {
		if len(c.members()) > 0 {
			c.violations = append(c.violations, "bootstrapped a second group on "+ft.alias)
		}
		c.groupPrimary = ft.alias
		c.bootstrapSeqs++
		c.incarnation = strconv.Itoa(1790000000 + c.bootstrapSeqs)
	} else if c.onlineMembers() == 0 {
		return nil, errors.New("no group to join")
	}
	if !bootstrap && ft.semiSyncReplica {
		if p := c.primaryTablet(); p != nil && p.semiSyncPrimary && c.connectedAckers(ft.alias) == 0 {
			c.violations = append(c.violations, ft.alias+" joined as the last semi-sync acker while the primary had semi-sync enabled")
		}
	}
	ft.member = true
	ft.source = ""
	ft.semiSyncReplica = false
	return c.groupStatus(ft), nil
}

// StopGroupReplication is part of the tmclient.TabletManagerClient interface.
func (c *fakeGRCluster) StopGroupReplication(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.GroupReplicationStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ft, err := c.get(tablet)
	if err != nil {
		return nil, err
	}
	if err := c.record(fmt.Sprintf("StopGroupReplication(%s)", ft.alias)); err != nil {
		return nil, err
	}
	if !ft.member {
		return c.groupStatus(ft), nil
	}
	p := c.primaryTablet()
	if ft.primary {
		if c.policyNeedsSemiSync(ft) && !ft.semiSyncPrimary {
			c.violations = append(c.violations, "the primary left its group without semi-sync")
		}
	} else if p != nil && c.policyNeedsSemiSync(p) && c.onlineMembers() <= 2 && c.connectedAckers("") == 0 {
		c.violations = append(c.violations, ft.alias+" left the group while no semi-sync acker replicates from the primary")
	} else if p != nil && p.member && c.policyNeedsSemiSync(p) && c.onlineMembers() <= 2 && !p.semiSyncPrimary {
		// The primary's group shrinks to the primary alone: its commits would need neither a group
		// majority nor a semi-sync acknowledgement until its tablet enables semi-sync.
		c.violations = append(c.violations, ft.alias+" left the group, leaving the primary alone in it without semi-sync")
	}
	ft.member = false
	if c.groupPrimary == ft.alias {
		c.groupPrimary = ""
	}
	// A PRIMARY tablet restores read-write; others stay super_read_only.
	ft.superReadOnly = !ft.primary
	return c.groupStatus(ft), nil
}

// SetReplicationSource is part of the tmclient.TabletManagerClient interface.
func (c *fakeGRCluster) SetReplicationSource(ctx context.Context, tablet *topodatapb.Tablet, parent *topodatapb.TabletAlias, timeCreatedNS int64, waitPosition string, forceStartReplication bool, semiSync bool, heartbeatInterval float64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	ft, err := c.get(tablet)
	if err != nil {
		return err
	}
	if err := c.record(fmt.Sprintf("SetReplicationSource(%s, %s, semiSync=%v)", ft.alias, topoproto.TabletAliasString(parent), semiSync)); err != nil {
		return err
	}
	if ft.member {
		// On an active member, only the tablet type follows.
		return nil
	}
	ft.source = topoproto.TabletAliasString(parent)
	ft.semiSyncReplica = semiSync
	return nil
}

// PromoteReplica is part of the tmclient.TabletManagerClient interface.
func (c *fakeGRCluster) PromoteReplica(ctx context.Context, tablet *topodatapb.Tablet, semiSync bool) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ft, err := c.get(tablet)
	if err != nil {
		return "", err
	}
	if err := c.record(fmt.Sprintf("PromoteReplica(%s)", ft.alias)); err != nil {
		return "", err
	}
	if !ft.member {
		c.violations = append(c.violations, "promoted non-member "+ft.alias)
	}
	c.groupPrimary = ft.alias
	return "MySQL56/" + policy.GroupName(c.keyspace, "-") + ":1-100", nil
}

// PopulateReparentJournal is part of the tmclient.TabletManagerClient interface.
func (c *fakeGRCluster) PopulateReparentJournal(ctx context.Context, tablet *topodatapb.Tablet, timeCreatedNS int64, actionName string, primaryAlias *topodatapb.TabletAlias, pos string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.record(fmt.Sprintf("PopulateReparentJournal(%s)", topoproto.TabletAliasString(tablet.Alias)))
}

// InitPrimary is part of the tmclient.TabletManagerClient interface. Under a group
// replication policy it bootstraps the group on the tablet; it records the voters that the
// shard record listed at that moment.
func (c *fakeGRCluster) InitPrimary(ctx context.Context, tablet *topodatapb.Tablet, semiSync bool) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ft, err := c.get(tablet)
	if err != nil {
		return "", err
	}
	if err := c.record(fmt.Sprintf("InitPrimary(%s)", ft.alias)); err != nil {
		return "", err
	}
	si, err := c.ts.GetShard(ctx, c.keyspace, "-")
	if err != nil {
		return "", err
	}
	c.votersAtInitPrimary = votersString(si.GroupReplicationVoters)
	if len(c.members()) > 0 {
		c.violations = append(c.violations, "bootstrapped a second group on "+ft.alias)
	}
	ft.primary = true
	ft.member = true
	ft.source = ""
	ft.superReadOnly = false
	c.groupPrimary = ft.alias
	c.bootstrapSeqs++
	c.incarnation = strconv.Itoa(1790000000 + c.bootstrapSeqs)
	return "", nil
}

// ReplicationStatus is part of the tmclient.TabletManagerClient interface. The tablets of a
// shard that never had a primary have no transactions.
func (c *fakeGRCluster) ReplicationStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.Status, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.get(tablet); err != nil {
		return nil, err
	}
	return &replicationdatapb.Status{}, nil
}

// RefreshState is part of the tmclient.TabletManagerClient interface.
func (c *fakeGRCluster) RefreshState(ctx context.Context, tablet *topodatapb.Tablet) error {
	return nil
}

// GetGlobalStatusVars is part of the tmclient.TabletManagerClient interface.
func (c *fakeGRCluster) GetGlobalStatusVars(ctx context.Context, tablet *topodatapb.Tablet, variables []string) (map[string]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.get(tablet); err != nil {
		return nil, err
	}
	return map[string]string{}, nil
}

// DemotePrimary is part of the tmclient.TabletManagerClient interface.
func (c *fakeGRCluster) DemotePrimary(ctx context.Context, tablet *topodatapb.Tablet, force bool) (*replicationdatapb.PrimaryStatus, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return &replicationdatapb.PrimaryStatus{}, c.record(fmt.Sprintf("DemotePrimary(%s)", topoproto.TabletAliasString(tablet.Alias)))
}

// UndoDemotePrimary is part of the tmclient.TabletManagerClient interface.
func (c *fakeGRCluster) UndoDemotePrimary(ctx context.Context, tablet *topodatapb.Tablet, semiSync bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.record(fmt.Sprintf("UndoDemotePrimary(%s)", topoproto.TabletAliasString(tablet.Alias)))
}

// PrimaryPosition is part of the tmclient.TabletManagerClient interface.
func (c *fakeGRCluster) PrimaryPosition(ctx context.Context, tablet *topodatapb.Tablet) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return "", c.record(fmt.Sprintf("PrimaryPosition(%s)", topoproto.TabletAliasString(tablet.Alias)))
}

// ExecuteFetchAsDba is part of the tmclient.TabletManagerClient interface. It answers the
// schema check with schemaRows ("schema|table|engine").
func (c *fakeGRCluster) ExecuteFetchAsDba(ctx context.Context, tablet *topodatapb.Tablet, usePool bool, req *tabletmanagerdatapb.ExecuteFetchAsDbaRequest) (*querypb.QueryResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.get(tablet); err != nil {
		return nil, err
	}
	c.queries = append(c.queries, string(req.Query))
	if c.onQuery != nil {
		c.onQuery(len(c.queries))
	}
	result := sqltypes.MakeTestResult(sqltypes.MakeTestFields("TABLE_SCHEMA|TABLE_NAME|ENGINE", "varchar|varchar|varchar"), c.schemaRows...)
	return sqltypes.ResultToProto3(result), nil
}

// asyncReparentPath records that a reparent took the asynchronous replication path, which the
// fake does not simulate, and fails the call.
func (c *fakeGRCluster) asyncReparentPath(call string, tablet *topodatapb.Tablet) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	name := fmt.Sprintf("%s(%s)", call, topoproto.TabletAliasString(tablet.Alias))
	c.calls = append(c.calls, name)
	c.violations = append(c.violations, "asynchronous reparent path: "+name)
	return fmt.Errorf("the fake does not simulate %s", name)
}

// StopReplicationAndGetStatus is part of the tmclient.TabletManagerClient interface. Only the
// asynchronous ERS path calls it.
func (c *fakeGRCluster) StopReplicationAndGetStatus(ctx context.Context, tablet *topodatapb.Tablet, mode replicationdatapb.StopReplicationMode) (*replicationdatapb.StopReplicationStatus, error) {
	return nil, c.asyncReparentPath("StopReplicationAndGetStatus", tablet)
}

// WaitForPosition is part of the tmclient.TabletManagerClient interface.
func (c *fakeGRCluster) WaitForPosition(ctx context.Context, tablet *topodatapb.Tablet, pos string) error {
	return c.asyncReparentPath("WaitForPosition", tablet)
}

// PrimaryStatus is part of the tmclient.TabletManagerClient interface.
func (c *fakeGRCluster) PrimaryStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error) {
	return nil, c.asyncReparentPath("PrimaryStatus", tablet)
}

func (c *fakeGRCluster) mutatingCalls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.calls)
}

func (c *fakeGRCluster) violationsSoFar() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.violations)
}

func (c *fakeGRCluster) tablet(alias string) *fakeGRTablet {
	c.mu.Lock()
	defer c.mu.Unlock()
	ft := *c.tablets[alias]
	return &ft
}

// fakeGRTabletSpec describes a tablet of a test shard.
type fakeGRTabletSpec struct {
	cell       string
	uid        uint32
	tabletType topodatapb.TabletType
	noGR       bool
	// noShardPolicy makes the tablet a vttablet that does not know the shard's own durability
	// policy.
	noShardPolicy bool
}

// addTabletOfOtherShard adds a REPLICA tablet of another shard of the keyspace, which answers
// FullStatus and is in no group.
func (c *fakeGRCluster) addTabletOfOtherShard(t *testing.T, cell string, uid uint32, shard string) *fakeGRTablet {
	tablet := &topodatapb.Tablet{
		Alias:         &topodatapb.TabletAlias{Cell: cell, Uid: uid},
		Keyspace:      c.keyspace,
		Shard:         shard,
		Type:          topodatapb.TabletType_REPLICA,
		Hostname:      fmt.Sprintf("host-%d", uid),
		MysqlHostname: fmt.Sprintf("mysql-%d", uid),
		MysqlPort:     3306,
		PortMap:       map[string]int32{"vt": 15000, "grpc": 16000},
	}
	require.NoError(t, c.ts.CreateTablet(t.Context(), tablet))
	alias := topoproto.TabletAliasString(tablet.Alias)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tabletRecs[alias] = tablet
	ft := &fakeGRTablet{
		alias: alias, cell: cell, uuid: fmt.Sprintf("00000000-0000-0000-0000-%012d", uid),
		version: "8.4.11", gtidMode: "ON", grEnabled: true, shardPolicy: true, superReadOnly: true,
	}
	c.tablets[alias] = ft
	return ft
}

// newFakeGRCluster creates the keyspace "ks" with shard "-" in a memory topo, with the given
// durability policy and tablets. The PRIMARY tablet is the shard primary; every other tablet
// replicates asynchronously from it, with semi-sync if the policy says so. No group is active.
// Without a PRIMARY tablet, the shard never had a primary and no tablet replicates.
func newFakeGRCluster(t *testing.T, durability string, specs ...fakeGRTabletSpec) (*fakeGRCluster, *topo.Server) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "zone1", "zone2", "zone3")
	t.Cleanup(ts.Close)
	require.NoError(t, ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{DurabilityPolicy: durability}))
	d, err := policy.GetDurabilityPolicy(durability)
	require.NoError(t, err)

	c := &fakeGRCluster{
		t:          t,
		ts:         ts,
		keyspace:   "ks",
		tablets:    make(map[string]*fakeGRTablet),
		tabletRecs: make(map[string]*topodatapb.Tablet),
		failOnce:   make(map[string]bool),
		// The incarnation of a group that the test makes active without a bootstrap.
		incarnation: "1790000000",
	}
	var primary *topodatapb.Tablet
	for _, spec := range specs {
		tablet := &topodatapb.Tablet{
			Alias:         &topodatapb.TabletAlias{Cell: spec.cell, Uid: spec.uid},
			Keyspace:      "ks",
			Shard:         "-",
			Type:          spec.tabletType,
			Hostname:      fmt.Sprintf("host-%d", spec.uid),
			MysqlHostname: fmt.Sprintf("mysql-%d", spec.uid),
			MysqlPort:     3306,
			PortMap:       map[string]int32{"vt": 15000, "grpc": 16000},
		}
		if spec.tabletType == topodatapb.TabletType_PRIMARY {
			tablet.PrimaryTermStartTime = protoutil.TimeToProto(time.Now())
			primary = tablet
		}
		testutil.AddTablet(ctx, t, ts, tablet, &testutil.AddTabletOptions{AlsoSetShardPrimary: true})
		alias := topoproto.TabletAliasString(tablet.Alias)
		c.tabletRecs[alias] = tablet
		c.tablets[alias] = &fakeGRTablet{
			alias:    alias,
			cell:     spec.cell,
			uuid:     fmt.Sprintf("00000000-0000-0000-0000-%012d", spec.uid),
			primary:  spec.tabletType == topodatapb.TabletType_PRIMARY,
			version:  "8.4.11",
			gtidMode: "ON",
			// The tablet runs with --enable-group-replication.
			grEnabled:   !spec.noGR,
			shardPolicy: !spec.noShardPolicy,
		}
	}
	if primary == nil {
		for _, ft := range c.tablets {
			ft.superReadOnly = true
		}
		return c, ts
	}
	primaryAlias := topoproto.TabletAliasString(primary.Alias)
	for alias, ft := range c.tablets {
		if ft.primary {
			ft.semiSyncPrimary = policy.SemiSyncAckers(d, primary) > 0
			continue
		}
		ft.source = primaryAlias
		ft.semiSyncReplica = policy.IsReplicaSemiSync(d, primary, c.tabletRecs[alias])
		ft.superReadOnly = true
	}
	return c, ts
}

// formGroup selects the voters of the group policy, stores them in the shard record, and
// makes them ONLINE members of a group whose primary is the shard primary, as after a
// migration. The other tablets stay asynchronous replicas without semi-sync.
func (c *fakeGRCluster) formGroup(t *testing.T, groupPolicy string) {
	d, err := policy.GetDurabilityPolicy(groupPolicy)
	require.NoError(t, err)
	grd, ok := policy.AsGroupReplication(d)
	require.True(t, ok)
	c.mu.Lock()
	defer c.mu.Unlock()
	var candidates []policy.VoterCandidate
	var primary *topodatapb.TabletAlias
	for alias, ft := range c.tablets {
		candidates = append(candidates, policy.VoterCandidate{Tablet: c.tabletRecs[alias]})
		if ft.primary {
			primary = c.tabletRecs[alias].Alias
		}
	}
	voters := policy.SelectVoters(grd, nil, primary, candidates)
	c.setVotersLocked(t, voters)
	for alias, ft := range c.tablets {
		if !policy.IsVoter(voters, c.tabletRecs[alias].Alias) {
			ft.semiSyncReplica = false
			continue
		}
		ft.member = true
		ft.source = ""
		ft.semiSyncReplica = false
		ft.semiSyncPrimary = false
		if ft.primary {
			c.groupPrimary = alias
		}
	}
}

// setVotersLocked stores the voters in the shard record.
func (c *fakeGRCluster) setVotersLocked(t *testing.T, voters []*topodatapb.TabletAlias) {
	_, err := c.ts.UpdateShardFields(t.Context(), c.keyspace, "-", func(si *topo.ShardInfo) error {
		si.GroupReplicationVoters = voters
		return nil
	})
	require.NoError(t, err)
}

// setShardPolicy stores the shard's own durability policy in the shard record, as
// MigrateReplicationMode does once it converted the shard.
func (c *fakeGRCluster) setShardPolicy(t *testing.T, durability string) {
	_, err := c.ts.UpdateShardFields(t.Context(), c.keyspace, "-", func(si *topo.ShardInfo) error {
		si.DurabilityPolicy = durability
		return nil
	})
	require.NoError(t, err)
}

// shardPolicy returns the shard's own durability policy stored in the shard record.
func (c *fakeGRCluster) shardPolicy(t *testing.T) string {
	si, err := c.ts.GetShard(t.Context(), c.keyspace, "-")
	require.NoError(t, err)
	return si.DurabilityPolicy
}

// setIncarnation stores the group incarnation in the shard record.
func (c *fakeGRCluster) setIncarnation(t *testing.T, incarnation string) {
	_, err := c.ts.UpdateShardFields(t.Context(), c.keyspace, "-", func(si *topo.ShardInfo) error {
		si.GroupReplicationIncarnation = incarnation
		return nil
	})
	require.NoError(t, err)
}

// recordedIncarnation returns the group incarnation stored in the shard record.
func (c *fakeGRCluster) recordedIncarnation(t *testing.T) string {
	si, err := c.ts.GetShard(t.Context(), c.keyspace, "-")
	require.NoError(t, err)
	return si.GroupReplicationIncarnation
}

// setVoters stores the voters, given as alias strings, in the shard record.
func (c *fakeGRCluster) setVoters(t *testing.T, aliases ...string) {
	var voters []*topodatapb.TabletAlias
	for _, alias := range aliases {
		voters = append(voters, mustAlias(t, alias))
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setVotersLocked(t, voters)
}

// voters returns the voters stored in the shard record, as sorted alias strings.
func (c *fakeGRCluster) voters(t *testing.T) []string {
	si, err := c.ts.GetShard(t.Context(), c.keyspace, "-")
	require.NoError(t, err)
	var aliases []string
	for _, v := range si.GroupReplicationVoters {
		aliases = append(aliases, topoproto.TabletAliasString(v))
	}
	slices.Sort(aliases)
	return aliases
}

func (c *fakeGRCluster) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = nil
	c.violations = nil
	c.queries = nil
}

func callsWithPrefix(calls []string, prefix string) []string {
	var out []string
	for _, call := range calls {
		if strings.HasPrefix(call, prefix) {
			out = append(out, call)
		}
	}
	return out
}

const (
	aliasP   = "zone1-0000000100"
	alias101 = "zone1-0000000101"
	alias102 = "zone1-0000000102"
	alias200 = "zone2-0000000200"
	alias201 = "zone2-0000000201"
	alias300 = "zone3-0000000300"
)

// migrationTestShard is a primary in zone1, one replica in each of zone1, zone2 and zone3,
// and an rdonly tablet in zone1.
func migrationTestShard() []fakeGRTabletSpec {
	return []fakeGRTabletSpec{
		{cell: "zone1", uid: 100, tabletType: topodatapb.TabletType_PRIMARY},
		{cell: "zone1", uid: 101, tabletType: topodatapb.TabletType_REPLICA},
		{cell: "zone2", uid: 200, tabletType: topodatapb.TabletType_REPLICA},
		{cell: "zone3", uid: 300, tabletType: topodatapb.TabletType_REPLICA},
		{cell: "zone1", uid: 102, tabletType: topodatapb.TabletType_RDONLY},
	}
}
