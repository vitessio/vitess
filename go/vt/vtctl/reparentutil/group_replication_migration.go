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
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"vitess.io/vitess/go/mysql/capabilities"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// Statuses of a replication mode migration step.
const (
	// MigrationStepPlanned is the status of a step of a dry run.
	MigrationStepPlanned = "planned"
	// MigrationStepDone is the status of a step that was carried out.
	MigrationStepDone = "done"
	// MigrationStepSkipped is the status of a step whose goal was already reached.
	MigrationStepSkipped = "skipped"
)

// Actions of a replication mode migration.
const (
	MigrationActionPreflight              = "preflight"
	MigrationActionBootstrapGroup         = "bootstrap_group"
	MigrationActionJoinGroup              = "join_group"
	MigrationActionWaitSemiSyncDisabled   = "wait_semi_sync_disabled"
	MigrationActionWaitSemiSyncAckers     = "wait_semi_sync_ackers"
	MigrationActionWaitSemiSyncEnabled    = "wait_semi_sync_enabled"
	MigrationActionSetReplicationSource   = "set_replication_source"
	MigrationActionLeaveGroup             = "leave_group"
	MigrationActionWaitWritable           = "wait_writable"
	MigrationActionSetDurabilityPolicy    = "set_durability_policy"
	MigrationActionKeepDurabilityPolicy   = "keep_durability_policy"
	MigrationActionSetVoters              = "set_voters"
	MigrationActionClearVoters            = "clear_voters"
	MigrationActionSetIncarnation         = "set_incarnation"
	MigrationActionClearIncarnation       = "clear_incarnation"
	minimumGroupReplicationMembers        = 3
	defaultReplicationModeMigrationWait   = 5 * time.Minute
	defaultReplicationModeMigrationPollAt = 500 * time.Millisecond
)

// groupReplicationSchemaCheckQuery lists the user tables that Group Replication cannot
// replicate: tables that are not InnoDB, or that have neither a primary key nor a unique key
// over NOT NULL columns. The %s is the quoted sidecar database name.
const groupReplicationSchemaCheckQuery = "SELECT t.TABLE_SCHEMA, t.TABLE_NAME, IFNULL(t.ENGINE, '') FROM information_schema.TABLES t " +
	"WHERE t.TABLE_TYPE = 'BASE TABLE' " +
	"AND t.TABLE_SCHEMA NOT IN ('mysql', 'sys', 'performance_schema', 'information_schema', %s) " +
	"AND (UPPER(IFNULL(t.ENGINE, '')) <> 'INNODB' OR NOT EXISTS (" +
	"SELECT 1 FROM information_schema.STATISTICS s " +
	"WHERE s.TABLE_SCHEMA = t.TABLE_SCHEMA AND s.TABLE_NAME = t.TABLE_NAME AND s.NON_UNIQUE = 0 " +
	"GROUP BY s.INDEX_NAME HAVING SUM(s.NULLABLE = 'YES') = 0)) " +
	"ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME LIMIT 20"

// ReplicationModeMigrator converts the shards of a keyspace between asynchronous (semi-sync)
// replication and MySQL Group Replication, online and one tablet at a time. It only uses the
// topology server and the tablet manager RPCs, so VTOrc can reuse it.
//
// Every step first inspects the observed state and is skipped when its goal is already
// reached, so a migration that failed half-way can be run again and continues where it
// stopped.
type ReplicationModeMigrator struct {
	ts     *topo.Server
	tmc    tmclient.TabletManagerClient
	logger logutil.Logger

	// pollInterval is how often the migrator polls FullStatus while it waits.
	pollInterval time.Duration
}

// MigrateReplicationModeOptions are the parameters of a replication mode migration.
type MigrateReplicationModeOptions struct {
	// Shards restricts the migration to these shards. Empty means every shard.
	Shards []string
	// DurabilityPolicy is the target durability policy.
	DurabilityPolicy string
	// DryRun returns the plan without changing anything.
	DryRun bool
	// WaitTimeout bounds every wait of the migration.
	WaitTimeout time.Duration
}

// NewReplicationModeMigrator returns a ReplicationModeMigrator. A nil logger is allowed.
func NewReplicationModeMigrator(ts *topo.Server, tmc tmclient.TabletManagerClient, logger logutil.Logger) *ReplicationModeMigrator {
	if logger == nil {
		logger = logutil.NewCallbackLogger(func(*logutilpb.Event) {})
	}
	return &ReplicationModeMigrator{ts: ts, tmc: tmc, logger: logger, pollInterval: defaultReplicationModeMigrationPollAt}
}

// migrationRun carries the state of one Migrate call.
type migrationRun struct {
	m        *ReplicationModeMigrator
	keyspace string
	opts     MigrateReplicationModeOptions
	current  policy.Durabler
	target   policy.Durabler
}

// migrationShard carries the state of the migration of one shard.
type migrationShard struct {
	run      *migrationRun
	shard    string
	primary  *topodatapb.Tablet
	tablets  []*topodatapb.Tablet
	statuses map[string]*fullStatusResult
	result   *vtctldatapb.ReplicationModeMigrationShardResult
	// recordedVoters are the voters stored in the shard record when the shard was read.
	recordedVoters []*topodatapb.TabletAlias
	// voters are the voting members of the group: the recorded voters, or, when converting
	// to Group Replication, the voters the preflight selected.
	voters []*topodatapb.TabletAlias
	// recordedIncarnation is the group incarnation stored in the shard record when the shard
	// was read.
	recordedIncarnation string
}

// Migrate converts the requested shards of the keyspace to the replication mode of the
// target durability policy, and updates the keyspace durability policy:
//
//   - To a group replication policy, the shards are converted first, one at a time under
//     their shard lock, and the keyspace policy is updated once every shard of the keyspace
//     runs Group Replication.
//   - To any other policy, the keyspace policy is updated first. Active groups supersede
//     semi-sync, so this changes nothing until the shards leave their groups, one at a time
//     under their shard lock.
//
// The response lists the steps of every shard, even when Migrate fails.
func (m *ReplicationModeMigrator) Migrate(ctx context.Context, keyspace string, opts MigrateReplicationModeOptions) (*vtctldatapb.MigrateReplicationModeResponse, error) {
	resp := &vtctldatapb.MigrateReplicationModeResponse{Keyspace: keyspace}
	if opts.WaitTimeout <= 0 {
		opts.WaitTimeout = defaultReplicationModeMigrationWait
	}
	if !policy.CheckDurabilityPolicyExists(opts.DurabilityPolicy) {
		return resp, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "durability policy <%v> is not a valid policy", opts.DurabilityPolicy)
	}
	target, err := policy.GetDurabilityPolicy(opts.DurabilityPolicy)
	if err != nil {
		return resp, err
	}
	currentName, err := m.ts.GetKeyspaceDurability(ctx, keyspace)
	if err != nil {
		return resp, err
	}
	resp.DurabilityPolicy = currentName
	current, err := policy.GetDurabilityPolicy(currentName)
	if err != nil {
		return resp, err
	}

	allShards, err := m.ts.GetShardNames(ctx, keyspace)
	if err != nil {
		return resp, err
	}
	slices.Sort(allShards)
	shards := opts.Shards
	if len(shards) == 0 {
		shards = allShards
	}
	for _, shard := range shards {
		if !slices.Contains(allShards, shard) {
			return resp, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "shard %s/%s does not exist", keyspace, shard)
		}
	}

	run := &migrationRun{m: m, keyspace: keyspace, opts: opts, current: current, target: target}
	if policy.IsGroupReplication(target) {
		err = run.toGroupReplication(ctx, resp, shards, allShards, currentName)
	} else {
		err = run.fromGroupReplication(ctx, resp, shards, currentName)
	}
	return resp, err
}

func (r *migrationRun) toGroupReplication(ctx context.Context, resp *vtctldatapb.MigrateReplicationModeResponse, shards, allShards []string, currentName string) error {
	converted := make(map[string]bool)
	for _, shard := range shards {
		result := &vtctldatapb.ReplicationModeMigrationShardResult{Shard: shard, ReplicationMode: policy.ReplicationModeAsync.String()}
		resp.Shards = append(resp.Shards, result)
		if err := r.migrateShard(ctx, shard, result, true); err != nil {
			return vterrors.Wrapf(err, "failed to convert shard %s/%s to group replication", r.keyspace, shard)
		}
		result.ReplicationMode = policy.ReplicationModeGroupReplication.String()
		converted[shard] = true
	}

	// Switch the keyspace only once every shard of the keyspace runs Group Replication.
	var notConverted []string
	for _, shard := range allShards {
		if converted[shard] {
			continue
		}
		ok, err := r.shardRunsGroupReplication(ctx, shard)
		if err != nil {
			r.m.logger.Warningf("cannot tell whether shard %s/%s runs group replication: %v", r.keyspace, shard, err)
		}
		if !ok {
			notConverted = append(notConverted, shard)
		}
	}
	if len(notConverted) > 0 {
		r.addKeyspaceStep(resp, MigrationActionKeepDurabilityPolicy, MigrationStepSkipped,
			fmt.Sprintf("keep durability policy %s: shards %s do not run group replication yet", currentName, strings.Join(notConverted, ", ")))
		return nil
	}
	return r.setKeyspacePolicy(ctx, resp, currentName)
}

func (r *migrationRun) fromGroupReplication(ctx context.Context, resp *vtctldatapb.MigrateReplicationModeResponse, shards []string, currentName string) error {
	// Active groups supersede semi-sync, so the policy can change first. The tablets then
	// re-enable semi-sync as their groups shrink.
	if err := r.setKeyspacePolicy(ctx, resp, currentName); err != nil {
		return err
	}
	for _, shard := range shards {
		result := &vtctldatapb.ReplicationModeMigrationShardResult{Shard: shard, ReplicationMode: policy.ReplicationModeGroupReplication.String()}
		resp.Shards = append(resp.Shards, result)
		if err := r.migrateShard(ctx, shard, result, false); err != nil {
			return vterrors.Wrapf(err, "failed to convert shard %s/%s to asynchronous replication", r.keyspace, shard)
		}
		result.ReplicationMode = policy.ReplicationModeAsync.String()
	}
	return nil
}

func (r *migrationRun) setKeyspacePolicy(ctx context.Context, resp *vtctldatapb.MigrateReplicationModeResponse, currentName string) error {
	desc := fmt.Sprintf("set the durability policy of keyspace %s from %s to %s", r.keyspace, currentName, r.opts.DurabilityPolicy)
	switch {
	case currentName == r.opts.DurabilityPolicy:
		r.addKeyspaceStep(resp, MigrationActionSetDurabilityPolicy, MigrationStepSkipped, fmt.Sprintf("keyspace %s already has durability policy %s", r.keyspace, currentName))
	case r.opts.DryRun:
		r.addKeyspaceStep(resp, MigrationActionSetDurabilityPolicy, MigrationStepPlanned, desc)
	default:
		if _, err := SetKeyspaceDurabilityPolicy(ctx, r.m.ts, r.keyspace, r.opts.DurabilityPolicy); err != nil {
			return vterrors.Wrapf(err, "failed to %s", desc)
		}
		r.addKeyspaceStep(resp, MigrationActionSetDurabilityPolicy, MigrationStepDone, desc)
		resp.DurabilityPolicy = r.opts.DurabilityPolicy
	}
	return nil
}

func (r *migrationRun) addKeyspaceStep(resp *vtctldatapb.MigrateReplicationModeResponse, action, status, desc string) {
	r.m.logger.Infof("%s: %s (%s)", r.keyspace, desc, status)
	resp.KeyspaceSteps = append(resp.KeyspaceSteps, &vtctldatapb.ReplicationModeMigrationStep{Action: action, Description: desc, Status: status})
}

// SetKeyspaceDurabilityPolicy validates the durability policy and stores it in the keyspace
// record, under the keyspace lock.
func SetKeyspaceDurabilityPolicy(ctx context.Context, ts *topo.Server, keyspace, durabilityPolicy string) (ki *topo.KeyspaceInfo, err error) {
	ctx, unlock, lockErr := ts.LockKeyspace(ctx, keyspace, "SetKeyspaceDurabilityPolicy")
	if lockErr != nil {
		return nil, lockErr
	}
	defer unlock(&err)

	ki, err = ts.GetKeyspace(ctx, keyspace)
	if err != nil {
		return nil, err
	}
	if !policy.CheckDurabilityPolicyExists(durabilityPolicy) {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "durability policy <%v> is not a valid policy. Please register it as a policy first", durabilityPolicy)
	}
	ki.DurabilityPolicy = durabilityPolicy
	if err = ts.UpdateKeyspace(ctx, ki); err != nil {
		return nil, err
	}
	return ki, nil
}

// migrateShard converts one shard, under its shard lock unless this is a dry run.
func (r *migrationRun) migrateShard(ctx context.Context, shard string, result *vtctldatapb.ReplicationModeMigrationShardResult, toGroup bool) (err error) {
	if !r.opts.DryRun {
		var unlock func(*error)
		ctx, unlock, err = r.m.ts.LockShard(ctx, r.keyspace, shard, fmt.Sprintf("MigrateReplicationMode(%s)", r.opts.DurabilityPolicy))
		if err != nil {
			return err
		}
		defer unlock(&err)
	}

	s, err := r.readShard(ctx, shard)
	if err != nil {
		return err
	}
	s.result = result
	if toGroup {
		return s.toGroupReplication(ctx)
	}
	return s.fromGroupReplication(ctx)
}

// readShard reads the shard's tablets and their FullStatus. Every tablet must be reachable,
// and no tablet may be in a backup or restore, whose tablet type hides the type it returns to.
func (r *migrationRun) readShard(ctx context.Context, shard string) (*migrationShard, error) {
	si, err := r.m.ts.GetShard(ctx, r.keyspace, shard)
	if err != nil {
		return nil, err
	}
	if si.PrimaryAlias == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "shard %s/%s has no primary", r.keyspace, shard)
	}
	tabletMap, err := r.m.ts.GetTabletMapForShard(ctx, r.keyspace, shard)
	if err != nil {
		return nil, vterrors.Wrapf(err, "failed to get tablet map for %s/%s", r.keyspace, shard)
	}
	s := &migrationShard{run: r, shard: shard, recordedVoters: si.GroupReplicationVoters, voters: si.GroupReplicationVoters, recordedIncarnation: si.GroupReplicationIncarnation}
	for _, alias := range slices.Sorted(maps.Keys(tabletMap)) {
		tablet := tabletMap[alias].Tablet
		switch tablet.Type {
		case topodatapb.TabletType_BACKUP, topodatapb.TabletType_RESTORE:
			return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "tablet %v is in %v; wait until it finishes", alias, tablet.Type)
		}
		s.tablets = append(s.tablets, tablet)
		if topoproto.TabletAliasEqual(tablet.Alias, si.PrimaryAlias) {
			s.primary = tablet
		}
	}
	if s.primary == nil || s.primary.Type != topodatapb.TabletType_PRIMARY {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the primary %v of shard %s/%s is not a PRIMARY tablet", topoproto.TabletAliasString(si.PrimaryAlias), r.keyspace, shard)
	}
	s.statuses = fetchFullStatuses(ctx, r.m.tmc, s.tablets, topo.RemoteOperationTimeout)
	for _, tablet := range s.tablets {
		if res := s.status(tablet); res.err != nil {
			return nil, vterrors.Wrapf(res.err, "every tablet of shard %s/%s must be reachable", r.keyspace, shard)
		}
	}
	return s, nil
}

// shardRunsGroupReplication returns whether the shard's voters were selected, its primary
// is the primary of its group and every voter is ONLINE in it.
func (r *migrationRun) shardRunsGroupReplication(ctx context.Context, shard string) (bool, error) {
	s, err := r.readShard(ctx, shard)
	if err != nil {
		return false, err
	}
	if len(s.voters) == 0 || !s.status(s.primary).isGroupPrimary() {
		return false, nil
	}
	for _, tablet := range s.voting() {
		if !s.status(tablet).isOnlineMember() {
			return false, nil
		}
	}
	return true, nil
}

func (s *migrationShard) status(tablet *topodatapb.Tablet) *fullStatusResult {
	return s.statuses[topoproto.TabletAliasString(tablet.Alias)]
}

func (s *migrationShard) isPrimary(tablet *topodatapb.Tablet) bool {
	return topoproto.TabletAliasEqual(tablet.Alias, s.primary.Alias)
}

// voting returns the tablets that are voters of the group, including the primary.
func (s *migrationShard) voting() []*topodatapb.Tablet {
	var voting []*topodatapb.Tablet
	for _, tablet := range s.tablets {
		if policy.IsVoter(s.voters, tablet.Alias) {
			voting = append(voting, tablet)
		}
	}
	return voting
}

// selectVoters selects the voters of the group for the target policy. Recorded voters are
// kept where possible, so that a re-run continues with the same group, and the primary is
// always a voter.
func (s *migrationShard) selectVoters(grd policy.GroupReplicationDurabler) []*topodatapb.TabletAlias {
	return policy.SelectVoters(grd, s.recordedVoters, s.primary.Alias, voterCandidates(s.tablets, s.statuses))
}

// votingCells returns the sorted cells of the voters.
func votingCells(voting []*topodatapb.Tablet) []string {
	var cells []string
	for _, tablet := range voting {
		if !slices.Contains(cells, tablet.Alias.Cell) {
			cells = append(cells, tablet.Alias.Cell)
		}
	}
	slices.Sort(cells)
	return cells
}

func (s *migrationShard) logf(format string, args ...any) {
	s.run.m.logger.Infof("%s/%s: %s", s.run.keyspace, s.shard, fmt.Sprintf(format, args...))
}

func (s *migrationShard) record(action string, tablet *topodatapb.Tablet, status, desc string) {
	var alias *topodatapb.TabletAlias
	if tablet != nil {
		alias = tablet.Alias
	}
	s.logf("%s (%s)", desc, status)
	s.result.Steps = append(s.result.Steps, &vtctldatapb.ReplicationModeMigrationStep{Action: action, Tablet: alias, Description: desc, Status: status})
}

// do carries out a step, or only records it in a dry run. It re-checks the shard lock before
// acting.
func (s *migrationShard) do(ctx context.Context, action string, tablet *topodatapb.Tablet, desc string, fn func(ctx context.Context) error) error {
	if s.run.opts.DryRun {
		s.record(action, tablet, MigrationStepPlanned, desc)
		return nil
	}
	if err := topo.CheckShardLocked(ctx, s.run.keyspace, s.shard); err != nil {
		return vterrors.Wrap(err, lostTopologyLockMsg)
	}
	s.logf("%s", desc)
	if err := fn(ctx); err != nil {
		return vterrors.Wrapf(err, "failed to %s", desc)
	}
	s.record(action, tablet, MigrationStepDone, desc)
	return nil
}

// waitFor polls FullStatus of the tablet until cond holds, bounded by the wait timeout. The
// latest status replaces the tablet's entry in s.statuses.
func (s *migrationShard) waitFor(ctx context.Context, tablet *topodatapb.Tablet, what string, cond func(*fullStatusResult) bool) error {
	waitCtx, cancel := context.WithTimeout(ctx, s.run.opts.WaitTimeout)
	defer cancel()
	ticker := time.NewTicker(s.run.m.pollInterval)
	defer ticker.Stop()
	alias := topoproto.TabletAliasString(tablet.Alias)
	for {
		res := fetchFullStatus(waitCtx, s.run.m.tmc, tablet, topo.RemoteOperationTimeout)
		if res.err == nil {
			s.statuses[alias] = res
			if cond(res) {
				return nil
			}
		}
		select {
		case <-waitCtx.Done():
			if res.err != nil {
				return vterrors.Wrapf(res.err, "timed out after %v waiting until %s", s.run.opts.WaitTimeout, what)
			}
			return vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "timed out after %v waiting until %s", s.run.opts.WaitTimeout, what)
		case <-ticker.C:
		}
	}
}

// wait is a step that waits for a condition, which is only recorded in a dry run.
func (s *migrationShard) wait(ctx context.Context, action string, tablet *topodatapb.Tablet, what string, cond func(*fullStatusResult) bool) error {
	return s.do(ctx, action, tablet, "wait until "+what, func(ctx context.Context) error {
		return s.waitFor(ctx, tablet, what, cond)
	})
}

// replicatesFrom returns whether the tablet's default replication channel points at the
// source tablet's MySQL.
func replicatesFrom(res *fullStatusResult, source *topodatapb.Tablet) bool {
	if res == nil || res.status == nil || res.status.ReplicationStatus == nil {
		return false
	}
	rs := res.status.ReplicationStatus
	return rs.SourceHost == source.MysqlHostname && rs.SourcePort == source.MysqlPort
}

// preflightToGroupReplication checks that the shard can run Group Replication.
func (s *migrationShard) preflightToGroupReplication(ctx context.Context) error {
	var problems []string
	for _, tablet := range s.tablets {
		alias := topoproto.TabletAliasString(tablet.Alias)
		fs := s.status(tablet).status
		if !strings.EqualFold(fs.GtidMode, "ON") {
			problems = append(problems, fmt.Sprintf("%v: gtid_mode is %q, not ON", alias, fs.GtidMode))
		}
		if !strings.EqualFold(fs.BinlogFormat, "ROW") {
			problems = append(problems, fmt.Sprintf("%v: binlog_format is %q, not ROW", alias, fs.BinlogFormat))
		}
		atLeast, err := capabilities.ServerVersionAtLeast(fs.Version, 8, 0, 27)
		if err != nil || !atLeast || strings.Contains(strings.ToLower(fs.Version+" "+fs.VersionComment), "mariadb") {
			problems = append(problems, fmt.Sprintf("%v: MySQL version %q does not support group replication; 8.0.27 or later is required", alias, fs.Version))
		}
	}

	grd, ok := policy.AsGroupReplication(s.run.target)
	if !ok {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "durability policy %s does not use group replication", s.run.opts.DurabilityPolicy)
	}
	s.voters = s.selectVoters(grd)
	voting := s.voting()
	if !policy.IsGroupMember(grd, s.primary) {
		problems = append(problems, fmt.Sprintf("the primary %v would not be a voting member of the group", topoproto.TabletAliasString(s.primary.Alias)))
	}
	if len(voting) < minimumGroupReplicationMembers {
		problem := fmt.Sprintf("the group would have %d voting members (%s) in cells %s; at least %d are required",
			len(voting), votersString(s.voters), strings.Join(votingCells(voting), ", "), minimumGroupReplicationMembers)
		if perCell := grd.MaxVotersPerCell(); perCell > 0 {
			allowed := fmt.Sprintf("%d voters", perCell)
			if perCell == 1 {
				allowed = "one voter"
			}
			problem += fmt.Sprintf("; %s allows %s per cell, so the shard needs eligible PRIMARY or REPLICA tablets in at least %d cells",
				s.run.opts.DurabilityPolicy, allowed, (minimumGroupReplicationMembers+perCell-1)/perCell)
		}
		problems = append(problems, problem)
	}
	if len(voting) > policy.MaxGroupReplicationMembers {
		problems = append(problems, fmt.Sprintf("the group would have %d voting members; at most %d are allowed",
			len(voting), policy.MaxGroupReplicationMembers))
	}
	for _, tablet := range voting {
		alias := topoproto.TabletAliasString(tablet.Alias)
		if !s.status(tablet).status.GetGroupReplicationEnabled() {
			problems = append(problems, fmt.Sprintf("%v: group replication is not enabled on the tablet; start vttablet with --enable-group-replication", alias))
		}
		if tablet.MysqlPort == 0 {
			problems = append(problems, fmt.Sprintf("%v: the tablet record has no MySQL port, through which the other members would reach it", alias))
		}
	}
	if grd.RequiresCrossCellMajority() {
		if cell, holds := policy.CellHoldsMajority(voting); holds {
			problems = append(problems, fmt.Sprintf("cell %s would hold a majority of the voting members, which %s does not allow", cell, s.run.opts.DurabilityPolicy))
		}
	}

	// An existing group must be the primary's group: a re-run continues it, but a group
	// that does not include the primary cannot be adopted.
	primaryGroup := s.status(s.primary).groupStatus()
	for _, tablet := range s.tablets {
		res := s.status(tablet)
		if !res.isActiveMember() || s.isPrimary(tablet) {
			continue
		}
		alias := topoproto.TabletAliasString(tablet.Alias)
		switch {
		case !s.status(s.primary).isGroupPrimary():
			problems = append(problems, fmt.Sprintf("%v is an active group member, but the primary %v is not the primary of a group",
				alias, topoproto.TabletAliasString(s.primary.Alias)))
		case res.groupStatus().GroupName != primaryGroup.GroupName:
			problems = append(problems, fmt.Sprintf("%v is a member of group %s, but the primary's group is %s", alias, res.groupStatus().GroupName, primaryGroup.GroupName))
		}
	}
	if s.status(s.primary).isActiveMember() && !s.status(s.primary).isGroupPrimary() {
		problems = append(problems, fmt.Sprintf("the primary %v is a member of a group but not its primary", topoproto.TabletAliasString(s.primary.Alias)))
	}

	schemaProblems, err := s.checkSchema(ctx)
	if err != nil {
		return err
	}
	problems = append(problems, schemaProblems...)

	if len(problems) > 0 {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "shard %s/%s cannot be converted to group replication: %s",
			s.run.keyspace, s.shard, strings.Join(problems, "; "))
	}
	s.record(MigrationActionPreflight, nil, MigrationStepDone, "preflight checks passed")
	return nil
}

// checkSchema lists the user tables that Group Replication cannot replicate.
func (s *migrationShard) checkSchema(ctx context.Context) ([]string, error) {
	sidecarName, err := s.run.m.ts.GetSidecarDBName(ctx, s.run.keyspace)
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf(groupReplicationSchemaCheckQuery, sqltypes.EncodeStringSQL(sidecarName))
	queryCtx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cancel()
	qr, err := s.run.m.tmc.ExecuteFetchAsDba(queryCtx, s.primary, false, &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
		Query:   []byte(query),
		MaxRows: 20,
	})
	if err != nil {
		return nil, vterrors.Wrapf(err, "failed to check the schema on primary %v", topoproto.TabletAliasString(s.primary.Alias))
	}
	var problems []string
	for _, row := range sqltypes.Proto3ToResult(qr).Rows {
		if len(row) < 3 {
			continue
		}
		table := fmt.Sprintf("%s.%s", row[0].ToString(), row[1].ToString())
		if engine := row[2].ToString(); !strings.EqualFold(engine, "InnoDB") {
			problems = append(problems, fmt.Sprintf("table %s uses engine %s; group replication requires InnoDB", table, engine))
		} else {
			problems = append(problems, fmt.Sprintf("table %s has no primary key or unique key over NOT NULL columns", table))
		}
	}
	return problems, nil
}

// toGroupReplication converts the shard from asynchronous replication to Group Replication:
//
//  1. Preflight, which selects the voters: the voting members of the group, with the
//     primary among them.
//  2. Store the voters in the shard record. Only the listed tablets join the group, and
//     only they rejoin it on their own.
//  3. Bootstrap the group on the primary. Writes continue, and semi-sync keeps working.
//  4. Join the other voters one at a time, cross-cell ones first. A joining replica
//     leaves its asynchronous channel, so before the last semi-sync acker joins, wait
//     until the primary has disabled semi-sync, which its tablet does once the group has
//     two ONLINE members. An acker that would be the last one is not joined while the
//     group has fewer than two ONLINE members.
//  5. Point the tablets that are not voters at the primary, without semi-sync.
func (s *migrationShard) toGroupReplication(ctx context.Context) error {
	if err := s.preflightToGroupReplication(ctx); err != nil {
		return err
	}
	primary := s.primary
	primaryAlias := topoproto.TabletAliasString(primary.Alias)

	// The voters are stored before the group exists, so that every tablet and VTOrc agree
	// on them from the first join.
	if votersEqual(s.recordedVoters, s.voters) {
		s.record(MigrationActionSetVoters, nil, MigrationStepSkipped, fmt.Sprintf("the shard record already lists the voters %s", votersString(s.voters)))
	} else {
		err := s.do(ctx, MigrationActionSetVoters, nil, fmt.Sprintf("store the voters %s in the shard record", votersString(s.voters)), func(ctx context.Context) error {
			return writeGroupReplicationVoters(ctx, s.run.m.ts, s.run.keyspace, s.shard, s.voters)
		})
		if err != nil {
			return err
		}
	}

	online := 0
	for _, tablet := range s.tablets {
		if s.status(tablet).isOnlineMember() {
			online++
		}
	}

	// Bootstrap. The preflight guarantees that no other tablet is an active member unless
	// the primary is the group's primary.
	if s.status(primary).isActiveMember() {
		s.record(MigrationActionBootstrapGroup, primary, MigrationStepSkipped, fmt.Sprintf("the group is already running on primary %v", primaryAlias))
	} else {
		err := s.do(ctx, MigrationActionBootstrapGroup, primary, fmt.Sprintf("bootstrap the group on primary %v", primaryAlias), func(ctx context.Context) error {
			startCtx, cancel := context.WithTimeout(ctx, s.run.opts.WaitTimeout)
			defer cancel()
			if _, err := s.run.m.tmc.StartGroupReplication(startCtx, primary, true); err != nil {
				return err
			}
			return s.waitFor(ctx, primary, fmt.Sprintf("%v is the primary of its group", primaryAlias), (*fullStatusResult).isGroupPrimary)
		})
		if err != nil {
			return err
		}
		online++
	}

	// The group Vitess bootstrapped is the shard's legitimate group: record its incarnation, so
	// that no component follows a group that a member forms on its own later.
	if incarnation := policy.GroupIncarnation(s.status(primary).groupStatus().GetViewId()); incarnation != "" && incarnation == s.recordedIncarnation {
		s.record(MigrationActionSetIncarnation, primary, MigrationStepSkipped, fmt.Sprintf("the shard record already lists the group incarnation %s", incarnation))
	} else {
		err := s.do(ctx, MigrationActionSetIncarnation, primary, fmt.Sprintf("record the incarnation of the group of primary %v in the shard record", primaryAlias), func(ctx context.Context) error {
			_, err := RecordGroupReplicationIncarnation(ctx, s.run.m.ts, s.run.m.tmc, s.run.keyspace, s.shard, primary)
			return err
		})
		if err != nil {
			return err
		}
	}

	// The semi-sync ackers are the tablets replicating asynchronously from the primary with
	// semi-sync. The primary needs semiSyncAcks of them while its semi-sync is enabled. A
	// voter stops acking when it joins the group. An acker that is not a voter keeps acking
	// until the last step, which only turns its semi-sync off after the primary disabled
	// semi-sync, so it counts as an acker for the whole join phase.
	primarySemiSync := s.status(primary).status.SemiSyncPrimaryEnabled
	semiSyncAcks := max(int(s.status(primary).status.SemiSyncWaitForReplicaCount), 1)
	ackers := make(map[string]bool)
	for _, tablet := range s.tablets {
		res := s.status(tablet)
		if !s.isPrimary(tablet) && !res.isActiveMember() && res.status.SemiSyncReplicaEnabled && replicatesFrom(res, primary) {
			ackers[topoproto.TabletAliasString(tablet.Alias)] = true
		}
	}
	// lastAcker returns whether the voter is an acker without which the primary would not
	// have enough ackers left while its semi-sync is enabled. Once the group has two ONLINE
	// members, the primary disables semi-sync, and no join can block its commits.
	lastAcker := func(alias string) bool {
		return primarySemiSync && ackers[alias] && len(ackers)-1 < semiSyncAcks
	}

	var pending []*topodatapb.Tablet
	for _, tablet := range s.voting() {
		if s.isPrimary(tablet) {
			continue
		}
		if s.status(tablet).isOnlineMember() {
			s.record(MigrationActionJoinGroup, tablet, MigrationStepSkipped, fmt.Sprintf("%v is already an ONLINE member", topoproto.TabletAliasString(tablet.Alias)))
			continue
		}
		pending = append(pending, tablet)
	}
	// Cross-cell members first: the group gets a cross-cell majority as early as possible.
	slices.SortStableFunc(pending, func(a, b *topodatapb.Tablet) int {
		return cmp.Compare(boolRank(a.Alias.Cell == primary.Alias.Cell), boolRank(b.Alias.Cell == primary.Alias.Cell))
	})

	for len(pending) > 0 {
		next := slices.IndexFunc(pending, func(t *topodatapb.Tablet) bool {
			return !lastAcker(topoproto.TabletAliasString(t.Alias)) || online >= 2
		})
		if next < 0 {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
				"cannot join %v: it is the last semi-sync acker of primary %v, and the group does not have two ONLINE members yet",
				topoproto.TabletAliasString(pending[0].Alias), primaryAlias)
		}
		tablet := pending[next]
		pending = slices.Delete(pending, next, next+1)
		alias := topoproto.TabletAliasString(tablet.Alias)

		if lastAcker(alias) {
			err := s.wait(ctx, MigrationActionWaitSemiSyncDisabled, primary,
				fmt.Sprintf("primary %v has disabled semi-sync, before its last semi-sync acker %v joins the group", primaryAlias, alias),
				func(res *fullStatusResult) bool { return !res.status.SemiSyncPrimaryEnabled })
			if err != nil {
				return err
			}
			primarySemiSync = false
		}

		joinDesc := fmt.Sprintf("join %v to the group", alias)
		if s.status(tablet).isActiveMember() {
			// A RECOVERING member already joined; it only needs to become ONLINE.
			joinDesc = fmt.Sprintf("wait until RECOVERING member %v is ONLINE", alias)
		}
		err := s.do(ctx, MigrationActionJoinGroup, tablet, joinDesc, func(ctx context.Context) error {
			if !s.status(tablet).isActiveMember() {
				startCtx, cancel := context.WithTimeout(ctx, s.run.opts.WaitTimeout)
				defer cancel()
				if _, err := s.run.m.tmc.StartGroupReplication(startCtx, tablet, false); err != nil {
					return err
				}
			}
			return s.waitFor(ctx, tablet, fmt.Sprintf("%v is an ONLINE member", alias), (*fullStatusResult).isOnlineMember)
		})
		if err != nil {
			return err
		}
		online++
		delete(ackers, alias)
	}

	// The group now provides durability. The primary's tablet disables semi-sync once the
	// group has two ONLINE members; wait for it before the remaining ackers stop acking.
	if primarySemiSync {
		err := s.wait(ctx, MigrationActionWaitSemiSyncDisabled, primary, fmt.Sprintf("primary %v has disabled semi-sync", primaryAlias),
			func(res *fullStatusResult) bool { return !res.status.SemiSyncPrimaryEnabled })
		if err != nil {
			return err
		}
	}

	// The tablets that are not voters replicate asynchronously from the primary, without
	// semi-sync. A tablet that is an active member but not a voter leaves the group first.
	for _, tablet := range s.tablets {
		if s.isPrimary(tablet) || policy.IsVoter(s.voters, tablet.Alias) {
			continue
		}
		if err := s.ensureAsyncReplica(ctx, tablet, false); err != nil {
			return err
		}
	}
	return nil
}

func boolRank(b bool) int {
	if b {
		return 1
	}
	return 0
}

// ensureAsyncReplica makes the tablet replicate asynchronously from the primary, leaving
// its group first if it is an active member.
func (s *migrationShard) ensureAsyncReplica(ctx context.Context, tablet *topodatapb.Tablet, semiSync bool) error {
	alias := topoproto.TabletAliasString(tablet.Alias)
	primaryAlias := topoproto.TabletAliasString(s.primary.Alias)
	res := s.status(tablet)
	if res.isActiveMember() {
		if err := s.leaveGroup(ctx, tablet); err != nil {
			return err
		}
	} else if replicatesFrom(res, s.primary) && res.status.SemiSyncReplicaEnabled == semiSync {
		s.record(MigrationActionSetReplicationSource, tablet, MigrationStepSkipped,
			fmt.Sprintf("%v already replicates from primary %v (semi-sync %v)", alias, primaryAlias, semiSync))
		return nil
	}
	return s.do(ctx, MigrationActionSetReplicationSource, tablet,
		fmt.Sprintf("replicate %v from primary %v (semi-sync %v)", alias, primaryAlias, semiSync),
		func(ctx context.Context) error {
			setCtx, cancel := context.WithTimeout(ctx, s.run.opts.WaitTimeout)
			defer cancel()
			if err := s.run.m.tmc.SetReplicationSource(setCtx, tablet, s.primary.Alias, 0, "", true, semiSync, 0); err != nil {
				return err
			}
			// Refresh the status: a later step counts this tablet as a semi-sync acker.
			s.statuses[alias] = fetchFullStatus(ctx, s.run.m.tmc, tablet, topo.RemoteOperationTimeout)
			return nil
		})
}

// leaveGroup makes the tablet leave its group and waits until it is no longer active.
func (s *migrationShard) leaveGroup(ctx context.Context, tablet *topodatapb.Tablet) error {
	alias := topoproto.TabletAliasString(tablet.Alias)
	return s.do(ctx, MigrationActionLeaveGroup, tablet, fmt.Sprintf("remove %v from the group", alias), func(ctx context.Context) error {
		stopCtx, cancel := context.WithTimeout(ctx, s.run.opts.WaitTimeout)
		defer cancel()
		if _, err := s.run.m.tmc.StopGroupReplication(stopCtx, tablet); err != nil {
			return err
		}
		return s.waitFor(ctx, tablet, fmt.Sprintf("%v is not an active group member", alias), func(res *fullStatusResult) bool {
			return !res.isActiveMember()
		})
	})
}

// preflightFromGroupReplication checks that the shard can leave Group Replication.
func (s *migrationShard) preflightFromGroupReplication() error {
	var problems []string
	primaryRes := s.status(s.primary)
	primaryAlias := topoproto.TabletAliasString(s.primary.Alias)
	if primaryRes.isActiveMember() && !primaryRes.isGroupPrimary() {
		problems = append(problems, fmt.Sprintf("the primary %v is a member of a group but not its primary with quorum", primaryAlias))
	}
	for _, tablet := range s.tablets {
		res := s.status(tablet)
		if s.isPrimary(tablet) || !res.isActiveMember() {
			continue
		}
		if !primaryRes.isActiveMember() {
			problems = append(problems, fmt.Sprintf("%v is an active group member, but the primary %v is not", topoproto.TabletAliasString(tablet.Alias), primaryAlias))
		} else if res.groupStatus().GroupName != primaryRes.groupStatus().GroupName {
			problems = append(problems, fmt.Sprintf("%v is a member of group %s, but the primary's group is %s",
				topoproto.TabletAliasString(tablet.Alias), res.groupStatus().GroupName, primaryRes.groupStatus().GroupName))
		}
	}
	if acks := policy.SemiSyncAckers(s.run.target, s.primary); acks > 0 {
		eligible := 0
		for _, tablet := range s.tablets {
			if !s.isPrimary(tablet) && policy.IsReplicaSemiSync(s.run.target, s.primary, tablet) {
				eligible++
			}
		}
		if eligible < acks {
			problems = append(problems, fmt.Sprintf("durability policy %s needs %d semi-sync ackers, but only %d tablets can acknowledge", s.run.opts.DurabilityPolicy, acks, eligible))
		}
	}
	if len(problems) > 0 {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "shard %s/%s cannot be converted to asynchronous replication: %s",
			s.run.keyspace, s.shard, strings.Join(problems, "; "))
	}
	s.record(MigrationActionPreflight, nil, MigrationStepDone, "preflight checks passed")
	return nil
}

// fromGroupReplication converts the shard from Group Replication back to asynchronous
// replication. The keyspace already has the target policy:
//
//  1. Point the tablets outside the group at the primary, with the target's semi-sync
//     setting, so they can acknowledge once the primary needs semi-sync again.
//  2. Remove the secondaries from the group one at a time, semi-sync ackers first, and
//     point each at the primary. Before the last one leaves, wait until enough ackers
//     replicate with semi-sync. After it left, the group has one member, and the primary's
//     tablet re-enables semi-sync; wait for that.
//  3. Remove the primary from the group. Its tablet restores read-write; wait for that.
func (s *migrationShard) fromGroupReplication(ctx context.Context) error {
	if err := s.preflightFromGroupReplication(); err != nil {
		return err
	}
	primary := s.primary
	primaryAlias := topoproto.TabletAliasString(primary.Alias)
	semiSyncFor := func(tablet *topodatapb.Tablet) bool {
		return policy.IsReplicaSemiSync(s.run.target, primary, tablet)
	}

	var secondaries []*topodatapb.Tablet
	for _, tablet := range s.tablets {
		if s.isPrimary(tablet) {
			continue
		}
		if s.status(tablet).isActiveMember() {
			secondaries = append(secondaries, tablet)
			continue
		}
		if err := s.ensureAsyncReplica(ctx, tablet, semiSyncFor(tablet)); err != nil {
			return err
		}
	}
	// Ackers leave first, so they acknowledge before the group shrinks to the primary.
	slices.SortStableFunc(secondaries, func(a, b *topodatapb.Tablet) int {
		return cmp.Compare(boolRank(!semiSyncFor(a)), boolRank(!semiSyncFor(b)))
	})

	acks := policy.SemiSyncAckers(s.run.target, primary)
	for i, tablet := range secondaries {
		if i == len(secondaries)-1 && acks > 0 && s.run.opts.DryRun {
			s.record(MigrationActionWaitSemiSyncAckers, primary, MigrationStepPlanned,
				fmt.Sprintf("wait until %d semi-sync ackers replicate from primary %v", acks, primaryAlias))
		} else if i == len(secondaries)-1 && acks > 0 {
			if err := s.waitForAckers(ctx, acks, tablet); err != nil {
				return err
			}
		}
		if err := s.ensureAsyncReplica(ctx, tablet, semiSyncFor(tablet)); err != nil {
			return err
		}
	}

	primaryRes := s.status(primary)
	if !primaryRes.isActiveMember() {
		s.record(MigrationActionLeaveGroup, primary, MigrationStepSkipped, fmt.Sprintf("primary %v is not a group member", primaryAlias))
		return s.clearVoters(ctx)
	}
	if acks > 0 {
		err := s.wait(ctx, MigrationActionWaitSemiSyncEnabled, primary, fmt.Sprintf("primary %v has enabled semi-sync", primaryAlias),
			func(res *fullStatusResult) bool { return res.status.SemiSyncPrimaryEnabled })
		if err != nil {
			return err
		}
	}
	if err := s.leaveGroup(ctx, primary); err != nil {
		return err
	}
	if err := s.clearVoters(ctx); err != nil {
		return err
	}
	return s.wait(ctx, MigrationActionWaitWritable, primary, fmt.Sprintf("primary %v is writable", primaryAlias),
		func(res *fullStatusResult) bool { return !res.status.SuperReadOnly && !res.status.ReadOnly })
}

// clearVoters removes the voters and the group incarnation from the shard record once the last
// member left the group. A shard converted to Group Replication again then selects its voters
// afresh, and records the incarnation of its new group.
func (s *migrationShard) clearVoters(ctx context.Context) error {
	if s.recordedIncarnation == "" {
		s.record(MigrationActionClearIncarnation, nil, MigrationStepSkipped, "the shard record lists no group incarnation")
	} else {
		err := s.do(ctx, MigrationActionClearIncarnation, nil, fmt.Sprintf("remove the group incarnation %s from the shard record", s.recordedIncarnation), func(ctx context.Context) error {
			return WriteGroupReplicationIncarnation(ctx, s.run.m.ts, s.run.keyspace, s.shard, "")
		})
		if err != nil {
			return err
		}
	}
	if len(s.recordedVoters) == 0 {
		s.record(MigrationActionClearVoters, nil, MigrationStepSkipped, "the shard record lists no voters")
		return nil
	}
	return s.do(ctx, MigrationActionClearVoters, nil, fmt.Sprintf("remove the voters %s from the shard record", votersString(s.recordedVoters)), func(ctx context.Context) error {
		return writeGroupReplicationVoters(ctx, s.run.m.ts, s.run.keyspace, s.shard, nil)
	})
}

// waitForAckers waits until enough tablets outside the group replicate from the primary
// with semi-sync, before the last secondary leaves the group. The last secondary itself
// does not count; if it is needed to reach the count, the wait is skipped because it
// becomes an acker right after it leaves.
func (s *migrationShard) waitForAckers(ctx context.Context, acks int, last *topodatapb.Tablet) error {
	var candidates []*topodatapb.Tablet
	for _, tablet := range s.tablets {
		if s.isPrimary(tablet) || topoproto.TabletAliasEqual(tablet.Alias, last.Alias) || !policy.IsReplicaSemiSync(s.run.target, s.primary, tablet) {
			continue
		}
		candidates = append(candidates, tablet)
	}
	primaryAlias := topoproto.TabletAliasString(s.primary.Alias)
	if len(candidates) < acks {
		s.record(MigrationActionWaitSemiSyncAckers, s.primary, MigrationStepSkipped,
			fmt.Sprintf("only %v can provide the semi-sync acknowledgements of primary %v once it leaves the group", topoproto.TabletAliasString(last.Alias), primaryAlias))
		return nil
	}
	isAcking := func(res *fullStatusResult) bool {
		return res.err == nil && !res.isActiveMember() && res.status.SemiSyncReplicaEnabled && replicatesFrom(res, s.primary) &&
			res.status.ReplicationStatus.IoState == int32(replication.ReplicationStateRunning)
	}
	return s.do(ctx, MigrationActionWaitSemiSyncAckers, s.primary,
		fmt.Sprintf("wait until %d semi-sync ackers replicate from primary %v", acks, primaryAlias),
		func(ctx context.Context) error {
			waitCtx, cancel := context.WithTimeout(ctx, s.run.opts.WaitTimeout)
			defer cancel()
			ticker := time.NewTicker(s.run.m.pollInterval)
			defer ticker.Stop()
			for {
				statuses := fetchFullStatuses(waitCtx, s.run.m.tmc, candidates, topo.RemoteOperationTimeout)
				n := 0
				for _, res := range statuses {
					if isAcking(res) {
						n++
					}
				}
				if n >= acks {
					return nil
				}
				select {
				case <-waitCtx.Done():
					return vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "timed out after %v: %d of %d semi-sync ackers replicate from primary %v",
						s.run.opts.WaitTimeout, n, acks, primaryAlias)
				case <-ticker.C:
				}
			}
		})
}
