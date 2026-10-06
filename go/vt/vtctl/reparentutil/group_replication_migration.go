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
	"vitess.io/vitess/go/vt/log"
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
	MigrationActionPreflight            = "preflight"
	MigrationActionBootstrapGroup       = "bootstrap_group"
	MigrationActionJoinGroup            = "join_group"
	MigrationActionWaitSemiSyncDisabled = "wait_semi_sync_disabled"
	MigrationActionWaitSemiSyncAckers   = "wait_semi_sync_ackers"
	MigrationActionWaitSemiSyncEnabled  = "wait_semi_sync_enabled"
	MigrationActionSetReplicationSource = "set_replication_source"
	MigrationActionLeaveGroup           = "leave_group"
	MigrationActionWaitWritable         = "wait_writable"
	MigrationActionSetDurabilityPolicy  = "set_durability_policy"
	MigrationActionKeepDurabilityPolicy = "keep_durability_policy"
	MigrationActionSetVoters            = "set_voters"
	MigrationActionClearVoters          = "clear_voters"
	MigrationActionSetIncarnation       = "set_incarnation"
	MigrationActionClearIncarnation     = "clear_incarnation"
	// MigrationActionSetShardDurabilityPolicy sets the shard's own durability policy to the target
	// policy (Shard.durability_policy): after its conversion to Group Replication, or before its
	// conversion back.
	MigrationActionSetShardDurabilityPolicy = "set_shard_durability_policy"
	// MigrationActionClearShardDurabilityPolicy removes a shard's own durability policy once the
	// keyspace has the same policy.
	MigrationActionClearShardDurabilityPolicy = "clear_shard_durability_policy"
	// MigrationActionClearMigrationSource removes the keyspace's migration source
	// (Keyspace.migration_source_durability_policy) once every shard is converted to Group
	// Replication.
	MigrationActionClearMigrationSource   = "clear_migration_source"
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
	target   policy.Durabler
	// keyspacePolicy is the keyspace's durability policy when the migration started.
	keyspacePolicy string
	// keyspaceRecord is the keyspace record as the migration read or wrote it last.
	keyspaceRecord *topodatapb.Keyspace
	// plannedSource is the migration source that a dry run planned to keep in the keyspace record.
	plannedSource string
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
	// selectedVoters is the preflight's selection when it differs from the recorded voters, which
	// the migration keeps on a shard whose group runs under a group replication policy.
	selectedVoters []*topodatapb.TabletAlias
	// recordedIncarnation is the group incarnation stored in the shard record when the shard
	// was read.
	recordedIncarnation string
	// recordedPolicy is the shard's own durability policy stored in the shard record when the
	// shard was read, "" if none.
	recordedPolicy string
}

// Migrate converts the requested shards of the keyspace to the replication mode of the
// target durability policy, and updates the keyspace durability policy:
//
//   - The shards are converted one at a time, under their shard lock. Each shard's durability
//     policy (topo.ShardDurabilityPolicy) changes only there, at a fixed step of its conversion:
//     the migration stores the target policy as the shard's own policy (Shard.durability_policy)
//     after the shard's conversion to Group Replication, or before its conversion back, before
//     its group shrinks. Every component then manages a converted shard by the target policy,
//     and the shards that are not converted yet by the policy they had.
//   - To Group Replication, the keyspace record names the target policy before any shard is
//     converted, in one write that keeps the policy it converts from as the keyspace's migration
//     source (Keyspace.migration_source_durability_policy): that is the policy of the shards that
//     are not converted yet. A component that knows neither field then reads the group replication
//     policy for every shard, and fails safe if it does not know it either, instead of managing a
//     shard that runs a group by the policy it converts from. Before that write, every tablet of
//     the keyspace that answers must report that it resolves both fields. The migration source is
//     cleared once every shard of the keyspace is converted.
//   - Back to asynchronous replication, the keyspace keeps its group replication policy until
//     every shard has left its group, and is then switched in one write that also clears a
//     migration source left by an interrupted migration to Group Replication.
//   - The shards' own policies, which are the keyspace's from then on, are removed afterwards.
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
	ki, err := m.ts.GetKeyspace(ctx, keyspace)
	if err != nil {
		return resp, err
	}
	currentName, err := m.ts.GetKeyspaceDurability(ctx, keyspace)
	if err != nil {
		return resp, err
	}
	resp.DurabilityPolicy = currentName
	if ki.GetDurabilityPolicy() == "" {
		// VTOrc does not manage a keyspace without a policy; step 0 would keep "none" as the
		// migration source, and VTOrc would start to recover the shards not converted yet.
		return resp, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"keyspace %s has no durability policy: set one with SetKeyspaceDurabilityPolicy first, then run MigrateReplicationMode", keyspace)
	}
	if warning := sourceNextToAsyncPolicyWarning(ki); warning != "" {
		m.logger.Warningf("%s", warning)
	}
	// A switch between two policies of the replication mode that the keyspace runs needs no
	// conversion: SetKeyspaceDurabilityPolicy makes it. Converted, every running group's voters
	// would be selected again for the target policy, outside VTOrc's rules, and a migration to Group
	// Replication would keep a group replication policy as the keyspace's migration source. A run
	// again to the keyspace's own policy continues or ends a migration.
	if currentMode, known := replicationModeOf(currentName); known && currentMode == policy.GetReplicationMode(target) && currentName != opts.DurabilityPolicy {
		return resp, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"keyspace %s already runs %s with durability policy %s: use SetKeyspaceDurabilityPolicy to change it to %s, which runs the same replication mode; MigrateReplicationMode converts a keyspace between asynchronous replication and group replication",
			keyspace, currentMode, currentName, opts.DurabilityPolicy)
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

	run := &migrationRun{m: m, keyspace: keyspace, opts: opts, target: target, keyspacePolicy: currentName, keyspaceRecord: ki.Keyspace}
	return resp, run.migrate(ctx, resp, shards, allShards, policy.IsGroupReplication(target))
}

// migrate converts the shards, and switches the keyspace policy once every shard of the keyspace
// is converted (see Migrate).
func (r *migrationRun) migrate(ctx context.Context, resp *vtctldatapb.MigrateReplicationModeResponse, shards, allShards []string, toGroup bool) error {
	from, to := policy.ReplicationModeGroupReplication.String(), policy.ReplicationModeAsync.String()
	desc := "asynchronous replication"
	if toGroup {
		from, to = to, from
		desc = "group replication"
		if err := r.preflightKeyspace(ctx, resp, allShards); err != nil {
			return err
		}
		if err := r.nameTargetPolicy(ctx, resp, shards); err != nil {
			return err
		}
	}
	converted := make(map[string]bool)
	for _, shard := range shards {
		result := &vtctldatapb.ReplicationModeMigrationShardResult{Shard: shard, ReplicationMode: from}
		resp.Shards = append(resp.Shards, result)
		if err := r.migrateShard(ctx, shard, result, toGroup); err != nil {
			return vterrors.Wrapf(err, "failed to convert shard %s/%s to %s", r.keyspace, shard, desc)
		}
		result.ReplicationMode = to
		converted[shard] = true
	}

	// The keyspace policy only changes once that changes no shard's policy: every shard of the
	// keyspace is converted, and has the target policy, its own or the keyspace's.
	var notConverted []string
	for _, shard := range allShards {
		if converted[shard] {
			continue
		}
		ok, err := r.shardConverted(ctx, shard, toGroup)
		if err != nil {
			r.m.logger.Warningf("cannot tell whether shard %s/%s is converted to %s: %v", r.keyspace, shard, desc, err)
		}
		if !ok {
			notConverted = append(notConverted, shard)
		}
	}
	if len(notConverted) > 0 {
		kept := "durability policy " + r.keyspacePolicy
		if source := r.keyspaceRecord.GetMigrationSourceDurabilityPolicy(); toGroup && source != "" {
			kept = "migration source " + source
		}
		r.addKeyspaceStep(resp, MigrationActionKeepDurabilityPolicy, MigrationStepSkipped,
			fmt.Sprintf("keep %s: shards %s are not converted to %s with durability policy %s yet",
				kept, strings.Join(notConverted, ", "), desc, r.opts.DurabilityPolicy))
		return nil
	}
	if toGroup && (r.keyspaceRecord.GetDurabilityPolicy() == r.opts.DurabilityPolicy || r.plannedSource != "") {
		if err := r.clearMigrationSource(ctx, resp); err != nil {
			return err
		}
	} else if err := r.setKeyspacePolicy(ctx, resp, r.keyspacePolicy); err != nil {
		return err
	}
	return r.clearShardPolicies(ctx, resp, allShards, converted)
}

// preflightKeyspace checks, before a migration to Group Replication names the target policy in the
// keyspace record, that every tablet of the keyspace that answers resolves a shard's policy from the
// shard's own policy and the keyspace's migration source (FullStatus.shard_durability_policy_supported):
// another vttablet would manage its shard by the keyspace's policy, the target policy, from then on.
// A tablet that does not answer is left out, with a warning: a vttablet that does not know the
// target policy exits when it starts, and one that knows it is the same version as the others.
func (r *migrationRun) preflightKeyspace(ctx context.Context, resp *vtctldatapb.MigrateReplicationModeResponse, allShards []string) error {
	var problems []string
	for _, shard := range allShards {
		tabletMap, err := r.m.ts.GetTabletMapForShard(ctx, r.keyspace, shard)
		if err != nil {
			return vterrors.Wrapf(err, "failed to get tablet map for %s/%s", r.keyspace, shard)
		}
		tablets := make([]*topodatapb.Tablet, 0, len(tabletMap))
		for _, alias := range slices.Sorted(maps.Keys(tabletMap)) {
			tablets = append(tablets, tabletMap[alias].Tablet)
		}
		statuses := fetchFullStatuses(ctx, r.m.tmc, tablets, topo.RemoteOperationTimeout)
		for _, tablet := range tablets {
			res := statuses[topoproto.TabletAliasString(tablet.Alias)]
			if res.err != nil {
				r.m.logger.Warningf("tablet %v of keyspace %s does not answer, the migration cannot check its vttablet: %v", topoproto.TabletAliasString(tablet.Alias), r.keyspace, res.err)
				continue
			}
			if problem := shardDurabilityPolicyProblem(tablet, res); problem != "" {
				problems = append(problems, problem)
			}
		}
	}
	if len(problems) > 0 {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "keyspace %s cannot be converted to group replication: %s", r.keyspace, strings.Join(problems, "; "))
	}
	r.addKeyspaceStep(resp, MigrationActionPreflight, MigrationStepDone, fmt.Sprintf("every tablet of keyspace %s that answers resolves the shards' own policies", r.keyspace))
	return nil
}

// nameTargetPolicy names the target policy in the keyspace record, and keeps the keyspace's policy
// as its migration source, in one write under the keyspace lock (see Migrate). A keyspace that
// names the target policy already keeps its record. Before the write, it checks the conversion of
// the shards with a dry run, so that a migration that its preflight refuses leaves the keyspace
// record as it was.
func (r *migrationRun) nameTargetPolicy(ctx context.Context, resp *vtctldatapb.MigrateReplicationModeResponse, shards []string) error {
	ks := r.keyspaceRecord
	if ks.GetDurabilityPolicy() == r.opts.DurabilityPolicy {
		r.addKeyspaceStep(resp, MigrationActionSetDurabilityPolicy, MigrationStepSkipped, fmt.Sprintf("keyspace %s already has durability policy %s", r.keyspace, r.opts.DurabilityPolicy))
		return nil
	}
	source := cmp.Or(ks.GetMigrationSourceDurabilityPolicy(), r.keyspacePolicy)
	desc := fmt.Sprintf("set the durability policy of keyspace %s from %s to %s, which applies to its shards as they are converted, and keep %s as its migration source until then",
		r.keyspace, r.keyspacePolicy, r.opts.DurabilityPolicy, source)
	if r.opts.DryRun {
		r.addKeyspaceStep(resp, MigrationActionSetDurabilityPolicy, MigrationStepPlanned, desc)
		r.plannedSource = source
		return nil
	}
	// The dry run's steps are not logged: the conversion logs its own. Its results are returned
	// only if it refuses the migration.
	quiet := *r.m
	quiet.logger = logutil.NewCallbackLogger(func(*logutilpb.Event) {})
	dry := *r
	dry.m = &quiet
	dry.opts.DryRun = true
	var results []*vtctldatapb.ReplicationModeMigrationShardResult
	for _, shard := range shards {
		result := &vtctldatapb.ReplicationModeMigrationShardResult{Shard: shard}
		results = append(results, result)
		if err := dry.migrateShard(ctx, shard, result, true); err != nil {
			resp.Shards = append(resp.Shards, results...)
			return vterrors.Wrapf(err, "the preflight of shard %s/%s refused the migration to group replication, and nothing was changed", r.keyspace, shard)
		}
	}
	ki, err := writeKeyspacePolicy(ctx, r.m.ts, r.keyspace, func(ks *topodatapb.Keyspace) error {
		if ks.MigrationSourceDurabilityPolicy == "" {
			ks.MigrationSourceDurabilityPolicy = cmp.Or(ks.DurabilityPolicy, policy.DurabilityNone)
		}
		ks.DurabilityPolicy = r.opts.DurabilityPolicy
		return nil
	})
	if err != nil {
		return vterrors.Wrapf(err, "failed to %s", desc)
	}
	r.keyspaceRecord = ki.Keyspace
	r.addKeyspaceStep(resp, MigrationActionSetDurabilityPolicy, MigrationStepDone, desc)
	resp.DurabilityPolicy = r.opts.DurabilityPolicy
	return nil
}

// clearMigrationSource removes the keyspace's migration source once every shard of the keyspace is
// converted to Group Replication. Under the keyspace lock, which a new shard's creation also takes,
// it checks again that every shard of the keyspace has the target policy as its own: the migration
// source is the policy of a shard that has none.
func (r *migrationRun) clearMigrationSource(ctx context.Context, resp *vtctldatapb.MigrateReplicationModeResponse) error {
	source := cmp.Or(r.keyspaceRecord.GetMigrationSourceDurabilityPolicy(), r.plannedSource)
	if source == "" {
		r.addKeyspaceStep(resp, MigrationActionClearMigrationSource, MigrationStepSkipped, fmt.Sprintf("keyspace %s has no migration source", r.keyspace))
		return nil
	}
	desc := fmt.Sprintf("remove the migration source %s of keyspace %s, whose shards are all converted", source, r.keyspace)
	if r.opts.DryRun {
		r.addKeyspaceStep(resp, MigrationActionClearMigrationSource, MigrationStepPlanned, desc)
		return nil
	}
	ki, err := writeKeyspacePolicy(ctx, r.m.ts, r.keyspace, func(ks *topodatapb.Keyspace) error {
		shards, err := r.m.ts.GetShardNames(ctx, r.keyspace)
		if err != nil {
			return err
		}
		for _, shard := range shards {
			si, err := r.m.ts.GetShard(ctx, r.keyspace, shard)
			if err != nil {
				return err
			}
			if own := si.GetDurabilityPolicy(); own != r.opts.DurabilityPolicy {
				return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "shard %s/%s does not have durability policy %s as its own; run the migration again to convert it", r.keyspace, shard, r.opts.DurabilityPolicy)
			}
		}
		ks.MigrationSourceDurabilityPolicy = ""
		return nil
	})
	if err != nil {
		return vterrors.Wrapf(err, "failed to %s", desc)
	}
	r.keyspaceRecord = ki.Keyspace
	r.addKeyspaceStep(resp, MigrationActionClearMigrationSource, MigrationStepDone, desc)
	return nil
}

// checkNoShardRunsGroup returns a FAILED_PRECONDITION error, before the keyspace record leaves a
// group replication policy for the asynchronous target policy, if a shard of the keyspace may run a
// group: unless its own policy is the target, a shard record that lists voters, an incarnation or a
// bootstrap intent, or a group replication policy of its own, may be a migration to Group Replication
// that runs at the same time. The caller holds the keyspace lock, which a new shard's creation takes
// too; the convertedness of the shards was checked earlier, outside of it.
func (r *migrationRun) checkNoShardRunsGroup(ctx context.Context) error {
	if policy.IsGroupReplication(r.target) {
		return nil
	}
	shards, err := r.m.ts.GetShardNames(ctx, r.keyspace)
	if err != nil {
		return err
	}
	slices.Sort(shards)
	for _, shard := range shards {
		si, err := r.m.ts.GetShard(ctx, r.keyspace, shard)
		if err != nil {
			return err
		}
		own := si.GetDurabilityPolicy()
		if own == r.opts.DurabilityPolicy {
			continue
		}
		ownDurability, err := policy.GetDurabilityPolicy(own)
		ownGroup := own != "" && (err != nil || policy.IsGroupReplication(ownDurability))
		if ownGroup || len(si.GroupReplicationVoters) > 0 || si.GroupReplicationIncarnation != "" || si.GroupReplicationBootstrapIntent != nil {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
				"shard %s/%s may run a group (its record lists voters, an incarnation, a bootstrap intent or a group replication policy of its own): keyspace %s keeps its group replication policy; run the migration again once no migration to group replication runs",
				r.keyspace, shard, r.keyspace)
		}
	}
	return nil
}

// checkKeyspaceNamesGroupPolicy returns a FAILED_PRECONDITION error unless the keyspace record names
// a group replication policy, as step 0 of a migration to Group Replication left it: a migration back
// that ran meanwhile may have switched it to an asynchronous policy, and a shard must not run a group
// in a keyspace whose policy an older component would read as asynchronous. The conversion checks it
// under the shard lock, and again right before it bootstraps the shard's group.
func (r *migrationRun) checkKeyspaceNamesGroupPolicy(ctx context.Context) error {
	ki, err := r.m.ts.GetKeyspace(ctx, r.keyspace)
	if err != nil {
		return vterrors.Wrapf(err, "failed to read keyspace %s", r.keyspace)
	}
	durability, err := policy.GetDurabilityPolicy(ki.GetDurabilityPolicy())
	if err != nil || !policy.IsGroupReplication(durability) {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"keyspace %s does not name a group replication policy (durability policy %q): a migration back to asynchronous replication may have run meanwhile; run MigrateReplicationMode again",
			r.keyspace, ki.GetDurabilityPolicy())
	}
	r.keyspaceRecord = ki.Keyspace
	return nil
}

// writeKeyspacePolicy changes the durability fields of the keyspace record with update, in one
// write under the keyspace lock.
func writeKeyspacePolicy(ctx context.Context, ts *topo.Server, keyspace string, update func(ks *topodatapb.Keyspace) error) (ki *topo.KeyspaceInfo, err error) {
	ctx, unlock, lockErr := ts.LockKeyspace(ctx, keyspace, "MigrateReplicationMode")
	if lockErr != nil {
		return nil, lockErr
	}
	defer unlock(&err)
	ki, err = ts.GetKeyspace(ctx, keyspace)
	if err != nil {
		return nil, err
	}
	if err = update(ki.Keyspace); err != nil {
		return nil, err
	}
	if !policy.CheckDurabilityPolicyExists(ki.DurabilityPolicy) {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "durability policy <%v> is not a valid policy", ki.DurabilityPolicy)
	}
	if err = ts.UpdateKeyspace(ctx, ki); err != nil {
		return nil, err
	}
	return ki, nil
}

// clearShardPolicies removes the shards' own durability policy, which equals the keyspace's now
// that the keyspace has the target policy. A shard that has another policy of its own keeps it.
//
// In a dry run, the shards converted by the run (converted) are planned to have the target policy
// as their own unless the keyspace has it already.
func (r *migrationRun) clearShardPolicies(ctx context.Context, resp *vtctldatapb.MigrateReplicationModeResponse, allShards []string, converted map[string]bool) error {
	for _, shard := range allShards {
		si, err := r.m.ts.GetShard(ctx, r.keyspace, shard)
		if err != nil {
			return err
		}
		own := si.GetDurabilityPolicy()
		if own == "" && r.opts.DryRun && converted[shard] && r.keyspacePolicy != r.opts.DurabilityPolicy {
			own = r.opts.DurabilityPolicy
		}
		if own == "" {
			continue
		}
		desc := fmt.Sprintf("remove the durability policy %s of shard %s/%s, which the keyspace has now", own, r.keyspace, shard)
		switch {
		case own != r.opts.DurabilityPolicy:
			r.addKeyspaceStep(resp, MigrationActionClearShardDurabilityPolicy, MigrationStepSkipped,
				fmt.Sprintf("shard %s/%s keeps its durability policy %s, which differs from the keyspace's %s", r.keyspace, shard, own, r.opts.DurabilityPolicy))
		case r.opts.DryRun:
			r.addKeyspaceStep(resp, MigrationActionClearShardDurabilityPolicy, MigrationStepPlanned, desc)
		default:
			if err := clearShardDurabilityPolicy(ctx, r.m.ts, r.keyspace, shard, own); err != nil {
				return vterrors.Wrapf(err, "failed to %s", desc)
			}
			r.addKeyspaceStep(resp, MigrationActionClearShardDurabilityPolicy, MigrationStepDone, desc)
		}
	}
	return nil
}

// clearShardDurabilityPolicy removes the shard's own durability policy, under the shard lock, if
// it is still the given one.
func clearShardDurabilityPolicy(ctx context.Context, ts *topo.Server, keyspace, shard, durabilityPolicy string) (err error) {
	ctx, unlock, err := ts.LockShard(ctx, keyspace, shard, "MigrateReplicationMode(clear shard durability policy)")
	if err != nil {
		return err
	}
	defer unlock(&err)
	_, err = ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
		if si.DurabilityPolicy != durabilityPolicy {
			return topo.NewError(topo.NoUpdateNeeded, keyspace+"/"+shard)
		}
		si.DurabilityPolicy = ""
		return nil
	})
	return err
}

// writeShardDurabilityPolicy stores the shard's own durability policy (Shard.durability_policy).
// The caller holds the shard lock.
func writeShardDurabilityPolicy(ctx context.Context, ts *topo.Server, keyspace, shard, durabilityPolicy string) error {
	if err := topo.CheckShardLocked(ctx, keyspace, shard); err != nil {
		return vterrors.Wrap(err, lostTopologyLockMsg)
	}
	if !policy.CheckDurabilityPolicyExists(durabilityPolicy) {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "durability policy <%v> is not a valid policy", durabilityPolicy)
	}
	_, err := ts.UpdateShardFields(ctx, keyspace, shard, func(si *topo.ShardInfo) error {
		if si.DurabilityPolicy == durabilityPolicy {
			return topo.NewError(topo.NoUpdateNeeded, keyspace+"/"+shard)
		}
		si.DurabilityPolicy = durabilityPolicy
		return nil
	})
	if err != nil {
		return vterrors.Wrapf(err, "failed to store the durability policy of shard %s/%s", keyspace, shard)
	}
	return nil
}

func (r *migrationRun) setKeyspacePolicy(ctx context.Context, resp *vtctldatapb.MigrateReplicationModeResponse, currentName string) error {
	desc := fmt.Sprintf("set the durability policy of keyspace %s from %s to %s", r.keyspace, currentName, r.opts.DurabilityPolicy)
	switch {
	case currentName == r.opts.DurabilityPolicy && r.keyspaceRecord.GetMigrationSourceDurabilityPolicy() == "":
		r.addKeyspaceStep(resp, MigrationActionSetDurabilityPolicy, MigrationStepSkipped, fmt.Sprintf("keyspace %s already has durability policy %s", r.keyspace, currentName))
	case r.opts.DryRun:
		r.addKeyspaceStep(resp, MigrationActionSetDurabilityPolicy, MigrationStepPlanned, desc)
	default:
		ki, err := writeKeyspacePolicy(ctx, r.m.ts, r.keyspace, func(ks *topodatapb.Keyspace) error {
			if err := r.checkNoShardRunsGroup(ctx); err != nil {
				return err
			}
			ks.DurabilityPolicy = r.opts.DurabilityPolicy
			ks.MigrationSourceDurabilityPolicy = ""
			return nil
		})
		if err != nil {
			return vterrors.Wrapf(err, "failed to %s", desc)
		}
		r.keyspaceRecord = ki.Keyspace
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
// record, under the keyspace lock. It refuses, with FAILED_PRECONDITION, to change the replication
// mode (asynchronous replication or MySQL Group Replication) of a keyspace that has an initialized
// shard, which only MigrateReplicationMode does safely, and any change while MigrateReplicationMode
// converts the keyspace (checkDurabilityPolicyChange).
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
	if err = checkDurabilityPolicyChange(ctx, ts, ki, durabilityPolicy); err != nil {
		return nil, err
	}
	ki.DurabilityPolicy = durabilityPolicy
	if err = ts.UpdateKeyspace(ctx, ki); err != nil {
		return nil, err
	}
	return ki, nil
}

// sourceNextToAsyncPolicyWarning describes a keyspace record whose migration source is set while its
// policy is not a group replication policy, "" otherwise. A migration only writes a source together
// with a group replication policy, and an older vtctld's SetKeyspaceDurabilityPolicy, which does not
// know the source, can switch the policy next to it: the keyspace's shards that run a group are then
// read as asynchronous shards by the components that do not know the source either.
func sourceNextToAsyncPolicyWarning(ki *topo.KeyspaceInfo) string {
	source := ki.GetMigrationSourceDurabilityPolicy()
	if source == "" {
		return ""
	}
	if durability, err := policy.GetDurabilityPolicy(ki.GetDurabilityPolicy()); err == nil && policy.IsGroupReplication(durability) {
		return ""
	}
	return fmt.Sprintf("keyspace %s has the migration source %s next to durability policy %q, which is not a group replication policy: an older vtctld probably changed it with SetKeyspaceDurabilityPolicy; run MigrateReplicationMode to a group replication policy again to name it in the keyspace record again, or back to %s to reverse the migration",
		ki.KeyspaceName(), source, ki.GetDurabilityPolicy(), source)
}

// checkDurabilityPolicyChange returns a FAILED_PRECONDITION error if the keyspace's durability
// policy may not be set to durabilityPolicy outside MigrateReplicationMode: while a migration converts
// the keyspace (it has a migration source), or when the change switches the replication mode and a
// shard of the keyspace is initialized. Switched by the keyspace record alone, the shards' tablets
// would apply the other mode's rules to MySQL that runs the first one: a semi-sync primary would
// stop serving as a group primary that has no group, and a group's members would be repaired as
// asynchronous replicas. The caller holds the keyspace lock, which a new shard's creation takes too.
func checkDurabilityPolicyChange(ctx context.Context, ts *topo.Server, ki *topo.KeyspaceInfo, durabilityPolicy string) error {
	keyspace := ki.KeyspaceName()
	if source := ki.GetMigrationSourceDurabilityPolicy(); source != "" {
		if warning := sourceNextToAsyncPolicyWarning(ki); warning != "" {
			log.Warn(warning)
			return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, warning)
		}
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"MigrateReplicationMode is converting keyspace %s from %s to %s: run MigrateReplicationMode again to finish the migration, or to convert the keyspace back",
			keyspace, source, ki.GetDurabilityPolicy())
	}
	current := cmp.Or(ki.GetDurabilityPolicy(), policy.DurabilityNone)
	currentMode, known := replicationModeOf(current)
	targetMode, _ := replicationModeOf(durabilityPolicy)
	if known && currentMode == targetMode {
		return nil
	}
	shards, err := ts.GetShardNames(ctx, keyspace)
	if err != nil {
		return vterrors.Wrapf(err, "failed to list the shards of keyspace %s", keyspace)
	}
	slices.Sort(shards)
	for _, shard := range shards {
		si, err := ts.GetShard(ctx, keyspace, shard)
		if err != nil {
			return vterrors.Wrapf(err, "failed to read shard %s/%s", keyspace, shard)
		}
		if !shardInitialized(si.Shard) {
			continue
		}
		from := currentMode.String()
		if !known {
			from = "an unknown replication mode"
		}
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"cannot change the durability policy of keyspace %s from %s to %s: the replication mode would change from %s to %s, and shard %s/%s is initialized; use MigrateReplicationMode, which converts the shards one at a time",
			keyspace, current, durabilityPolicy, from, targetMode, keyspace, shard)
	}
	return nil
}

// replicationModeOf returns the replication mode of the durability policy, and whether the policy
// is known.
func replicationModeOf(durabilityPolicy string) (policy.ReplicationMode, bool) {
	durability, err := policy.GetDurabilityPolicy(durabilityPolicy)
	if err != nil {
		return policy.ReplicationModeAsync, false
	}
	return policy.GetReplicationMode(durability), true
}

// shardInitialized returns whether the shard had a primary, has a replication group's state, or has
// its own durability policy.
func shardInitialized(shard *topodatapb.Shard) bool {
	return shard.GetPrimaryAlias() != nil || shard.GetPrimaryTermStartTime() != nil ||
		len(shard.GetGroupReplicationVoters()) > 0 || shard.GetGroupReplicationIncarnation() != "" ||
		shard.GetGroupReplicationBootstrapIntent() != nil || shard.GetDurabilityPolicy() != ""
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

	if toGroup && !r.opts.DryRun {
		if err := r.checkKeyspaceNamesGroupPolicy(ctx); err != nil {
			return err
		}
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
	s := &migrationShard{
		run: r, shard: shard, recordedVoters: si.GroupReplicationVoters, voters: si.GroupReplicationVoters,
		recordedIncarnation: si.GroupReplicationIncarnation, recordedPolicy: si.GetDurabilityPolicy(),
	}
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

// shardConverted returns whether the shard is converted to the target: its durability policy,
// its own or the keyspace's, is the target policy, and, to Group Replication, its voters were
// selected, its primary is the primary of its group and every voter is ONLINE in it, or, back to
// asynchronous replication, none of its tablets is an active group member.
func (r *migrationRun) shardConverted(ctx context.Context, shard string, toGroup bool) (bool, error) {
	s, err := r.readShard(ctx, shard)
	if err != nil {
		return false, err
	}
	if !s.hasTargetPolicy() {
		return false, nil
	}
	if !toGroup {
		for _, tablet := range s.tablets {
			if s.status(tablet).isActiveMember() {
				return false, nil
			}
		}
		return true, nil
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

// hasTargetPolicy returns whether the shard's durability policy, as the shard record read by the
// migration resolves it, is the target policy.
func (s *migrationShard) hasTargetPolicy() bool {
	return topo.ShardDurabilityPolicy(s.run.keyspaceRecord, &topodatapb.Shard{DurabilityPolicy: s.recordedPolicy}) == s.run.opts.DurabilityPolicy
}

// setShardPolicy makes the target policy the shard's durability policy: it stores it as the
// shard's own policy, unless the shard has it already, its own or through the keyspace.
func (s *migrationShard) setShardPolicy(ctx context.Context) error {
	target := s.run.opts.DurabilityPolicy
	if s.hasTargetPolicy() {
		s.record(MigrationActionSetShardDurabilityPolicy, nil, MigrationStepSkipped, fmt.Sprintf("shard %s/%s already has durability policy %s", s.run.keyspace, s.shard, target))
		return nil
	}
	return s.do(ctx, MigrationActionSetShardDurabilityPolicy, nil, fmt.Sprintf("store durability policy %s in the shard record", target), func(ctx context.Context) error {
		if err := writeShardDurabilityPolicy(ctx, s.run.m.ts, s.run.keyspace, s.shard, target); err != nil {
			return err
		}
		s.recordedPolicy = target
		return nil
	})
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

// hasGroupReplicationPolicy returns whether the shard's durability policy, as the shard record read by
// the migration resolves it, is a group replication policy: the shard is managed as a group's shard.
func (s *migrationShard) hasGroupReplicationPolicy() bool {
	durability, err := policy.GetDurabilityPolicy(topo.ShardDurabilityPolicy(s.run.keyspaceRecord, &topodatapb.Shard{DurabilityPolicy: s.recordedPolicy}))
	return err == nil && policy.IsGroupReplication(durability)
}

// groupRuns returns whether a tablet of the shard is an active member of a group.
func (s *migrationShard) groupRuns() bool {
	for _, tablet := range s.tablets {
		if s.status(tablet).isActiveMember() {
			return true
		}
	}
	return false
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
	if s.hasGroupReplicationPolicy() && s.groupRuns() && len(s.recordedVoters) > 0 && !votersEqual(s.recordedVoters, s.voters) {
		// A converted shard is managed by its group replication policy: VTOrc maintains its voters
		// (GroupVotersOutOfDate), under the rule that keeps a view without a majority of the current
		// voters from holding a majority of the new list, and with the re-check right before its
		// write. The migration's own selection applies neither: it keeps the recorded voters.
		s.selectedVoters = s.voters
		s.voters = s.recordedVoters
	}
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
		} else if problem := shardDurabilityPolicyProblem(tablet, s.status(tablet)); problem != "" {
			problems = append(problems, problem)
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

// shardDurabilityPolicyProblem describes why the tablet's vttablet cannot take part in a
// migration, "" if it can: a vttablet that does not know the shard's own durability policy
// (Shard.durability_policy) manages its shard by the keyspace's policy, which is not the shard's
// while the keyspace is half migrated. It would not apply the serving invariant of a converted
// shard, nor rejoin its group, and would stop serving while a shard converted back shrinks its
// group.
func shardDurabilityPolicyProblem(tablet *topodatapb.Tablet, res *fullStatusResult) string {
	if res.status.GetShardDurabilityPolicySupported() {
		return ""
	}
	return fmt.Sprintf("%v: vttablet does not apply the shard's own durability policy; upgrade it, and vtctld and VTOrc, before the migration",
		topoproto.TabletAliasString(tablet.Alias))
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
//  6. Store the target policy as the shard's own durability policy. Until then, the shard has
//     the keyspace's policy, which is not a group replication policy: the primary serves with
//     semi-sync while its group grows from one member, and neither the tablets nor VTOrc apply
//     the rules of a group replication policy (the serving invariant, rejoins, bootstraps, voter
//     replacement) to a group that is still being formed. From then on, they do, whatever the
//     keyspace policy: the shard runs its group with all its voters ONLINE, its incarnation
//     recorded, and semi-sync superseded.
func (s *migrationShard) toGroupReplication(ctx context.Context) error {
	if err := s.preflightToGroupReplication(ctx); err != nil {
		return err
	}
	primary := s.primary
	primaryAlias := topoproto.TabletAliasString(primary.Alias)

	// The voters are stored before the group exists, so that every tablet and VTOrc agree
	// on them from the first join.
	if s.selectedVoters != nil {
		s.record(MigrationActionSetVoters, nil, MigrationStepSkipped, fmt.Sprintf(
			"the group of shard %s/%s runs under a group replication policy: VTOrc maintains the voters of a converted shard, and the migration keeps the recorded voters %s (its own selection would be %s)",
			s.run.keyspace, s.shard, votersString(s.voters), votersString(s.selectedVoters)))
	} else if votersEqual(s.recordedVoters, s.voters) {
		s.record(MigrationActionSetVoters, nil, MigrationStepSkipped, "the shard record already lists the voters "+votersString(s.voters))
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
			// The voters are stored: a migration back that switches the keyspace from now on sees
			// them, and one that switched it before is seen here.
			if err := s.run.checkKeyspaceNamesGroupPolicy(ctx); err != nil {
				return err
			}
			startCtx, cancel := context.WithTimeout(ctx, s.run.opts.WaitTimeout)
			defer cancel()
			if _, err := s.run.m.tmc.StartGroupReplication(startCtx, primary, &tabletmanagerdatapb.StartGroupReplicationRequest{Bootstrap: true}); err != nil {
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
		s.record(MigrationActionSetIncarnation, primary, MigrationStepSkipped, "the shard record already lists the group incarnation "+incarnation)
	} else {
		err := s.do(ctx, MigrationActionSetIncarnation, primary, fmt.Sprintf("record the incarnation of the group of primary %v in the shard record", primaryAlias), func(ctx context.Context) error {
			_, err := RecordGroupReplicationIncarnation(ctx, s.run.m.ts, s.run.m.tmc, s.run.keyspace, s.shard, primary, s.recordedIncarnation)
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
				if _, err := s.run.m.tmc.StartGroupReplication(startCtx, tablet, &tabletmanagerdatapb.StartGroupReplicationRequest{}); err != nil {
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
	return s.setShardPolicy(ctx)
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
	// The members manage the shard by its own policy while the keyspace has another one.
	for _, tablet := range s.tablets {
		if res := s.status(tablet); (res.isActiveMember() || policy.IsVoter(s.voters, tablet.Alias)) && s.run.keyspacePolicy != s.run.opts.DurabilityPolicy {
			if problem := shardDurabilityPolicyProblem(tablet, res); problem != "" {
				problems = append(problems, problem)
			}
		}
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
// replication:
//
//  0. Store the target policy as the shard's own durability policy, before anything changes.
//     The tablets and VTOrc then manage the shard by it: its primary re-enables semi-sync, and
//     does not stop serving, as its group shrinks below a majority of the voters, and serves on
//     its own once it left the group. Active groups supersede semi-sync, so this changes nothing
//     until the group shrinks.
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
	if err := s.setShardPolicy(ctx); err != nil {
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
			return WriteGroupReplicationIncarnation(ctx, s.run.m.ts, s.run.keyspace, s.shard, s.recordedIncarnation, "")
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
