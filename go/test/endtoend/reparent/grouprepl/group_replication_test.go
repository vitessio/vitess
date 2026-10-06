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

// Package grouprepl tests MySQL Group Replication as a Vitess replication mode: the online
// migration from cross-cell semi-sync, planned and unplanned failovers, and the migration
// back.
package grouprepl

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

const (
	keyspaceName = "ks"
	shardName    = "0"
	schemaSQL    = `create table if not exists writes (id bigint not null auto_increment, val varchar(64), primary key (id)) engine=InnoDB`
	waitTimeout  = 90 * time.Second
	pollInterval = 250 * time.Millisecond
)

var cells = []string{"zone1", "zone2", "zone3"}

type testCluster struct {
	*cluster.LocalProcessCluster
	// replicas are the REPLICA-type tablets. replicas[0] is the first primary.
	replicas []*cluster.Vttablet
	// rdonly, if set, replicates asynchronously from the primary in both modes.
	rdonly *cluster.Vttablet
}

// clusterOptions describe the shard a test starts.
type clusterOptions struct {
	vtorc          cluster.VTOrcConfiguration
	vtorcExtraArgs []string
	// replicaCells has the cell of every REPLICA tablet, in order.
	replicaCells []string
	// rdonly adds an RDONLY tablet in the first cell.
	rdonly bool
	// pollingLag makes the tablets track replication lag by polling MySQL
	// (--enable-replication-reporter, which the cluster framework passes to every tablet)
	// instead of with heartbeats.
	pollingLag bool
	// cellsAlias puts all cells in one cell alias, so that vtgate, in the first cell, routes
	// replica reads to the REPLICA tablets of the other cells too: it only routes them to
	// tablets of its own cell or cell alias.
	cellsAlias bool
	// noBuffer runs vtgate without --enable-buffer.
	noBuffer bool
}

// defaultClusterOptions is the recommended layout: one REPLICA tablet in each of three cells,
// plus an RDONLY tablet.
func defaultClusterOptions() clusterOptions {
	return clusterOptions{vtorc: vtorcConfig, replicaCells: cells, rdonly: true}
}

// vtorcConfig is the VTOrc configuration of the tests: it polls every second, like the
// Vitess examples.
var vtorcConfig = cluster.VTOrcConfiguration{
	InstancePollTime:               "1s",
	RecoveryPollDuration:           "1s",
	TopoInformationRefreshDuration: "2s",
}

// setupCluster starts a shard with the given layout, using cross-cell semi-sync, and a VTOrc.
func setupCluster(t *testing.T, opts clusterOptions) *testCluster {
	clusterInstance := cluster.NewCluster(cells[0], "localhost")
	t.Cleanup(clusterInstance.Teardown)
	tc := &testCluster{LocalProcessCluster: clusterInstance}

	require.NoError(t, clusterInstance.StartTopo())
	for _, cell := range cells[1:] {
		require.NoError(t, clusterInstance.VtctldClientProcess.AddCellInfo(cell))
	}

	var tablets []*cluster.Vttablet
	perCell := make(map[string]int)
	for _, cell := range opts.replicaCells {
		perCell[cell]++
		tablet := clusterInstance.NewVttabletInstance("replica", 100*(slices.Index(cells, cell)+1)+perCell[cell], cell)
		tc.replicas = append(tc.replicas, tablet)
		tablets = append(tablets, tablet)
	}
	if opts.rdonly {
		tc.rdonly = clusterInstance.NewVttabletInstance("rdonly", 190, cells[0])
		tablets = append(tablets, tc.rdonly)
	}

	clusterInstance.VtTabletExtraArgs = append(clusterInstance.VtTabletExtraArgs,
		"--lock-tables-timeout", "5s",
		"--queryserver-enable-online-ddl=false",
		"--group-replication-sync-interval", "500ms",
	)
	if !opts.pollingLag {
		// Heartbeats take precedence over the replication reporter.
		clusterInstance.VtTabletExtraArgs = append(clusterInstance.VtTabletExtraArgs,
			"--heartbeat-enable",
			"--heartbeat-interval", "250ms",
		)
	}
	keyspace := &cluster.Keyspace{Name: keyspaceName, SchemaSQL: schemaSQL}
	require.NoError(t, clusterInstance.SetupCluster(keyspace, []cluster.Shard{{Name: shardName, Vttablets: tablets}}))

	var procs []*exec.Cmd
	for _, tablet := range tablets {
		proc, err := tablet.MysqlctlProcess.StartProcess()
		require.NoError(t, err)
		procs = append(procs, proc)
	}
	for _, proc := range procs {
		require.NoError(t, proc.Wait())
	}

	out, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("SetKeyspaceDurabilityPolicy", keyspaceName, "--durability-policy="+policy.DurabilityCrossCell)
	require.NoError(t, err, out)

	for _, tablet := range tablets {
		tablet.VttabletProcess.SupportsBackup = false
		// RDONLY tablets never join the group, but enabling group replication on them is
		// harmless and keeps the setup uniform.
		tablet.VttabletProcess.ExtraArgs = append(tablet.VttabletProcess.ExtraArgs, "--enable-group-replication")
		require.NoError(t, tablet.VttabletProcess.Setup())
	}
	for _, tablet := range tablets {
		require.NoError(t, tablet.VttabletProcess.WaitForTabletStatuses([]string{"SERVING", "NOT_SERVING"}))
	}
	require.NoError(t, clusterInstance.VtctldClientProcess.InitializeShard(keyspaceName, shardName, cells[0], tc.replicas[0].TabletUID))
	_, err = tc.replicas[0].VttabletProcess.QueryTablet(schemaSQL, keyspaceName, true)
	require.NoError(t, err)

	if !opts.noBuffer {
		clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs,
			"--enable-buffer",
			"--buffer-window", "30s",
			"--buffer-max-failover-duration", "30s",
			"--buffer-min-time-between-failovers", "1s",
		)
	}
	if opts.cellsAlias {
		out, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("AddCellsAlias", "--cells", strings.Join(cells, ","), "all")
		require.NoError(t, err, out)
	}
	// vtgate must watch every cell: the group can elect a primary in any of them.
	vtgate := clusterInstance.NewVtgateInstance()
	vtgate.CellsToWatch = strings.Join(cells, ",")
	clusterInstance.VtgateProcess = *vtgate
	require.NoError(t, clusterInstance.VtgateProcess.Setup())

	vtorc := clusterInstance.NewVTOrcProcess(opts.vtorc, cells[0])
	vtorc.ExtraArgs = append(vtorc.ExtraArgs, opts.vtorcExtraArgs...)
	require.NoError(t, vtorc.Setup())
	clusterInstance.VTOrcProcesses = append(clusterInstance.VTOrcProcesses, vtorc)
	return tc
}

func fullStatus(t *testing.T, tc *testCluster, tablet *cluster.Vttablet) (*replicationdatapb.FullStatus, error) {
	out, err := tc.VtctldClientProcess.ExecuteCommandWithOutput("GetFullStatus", tablet.Alias)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", err, out)
	}
	status := &replicationdatapb.FullStatus{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal([]byte(out), status); err != nil {
		return nil, err
	}
	return status, nil
}

func shardPrimary(t *testing.T, tc *testCluster) string {
	shard, err := tc.VtctldClientProcess.GetShard(keyspaceName, shardName)
	require.NoError(t, err)
	if shard.Shard.PrimaryAlias == nil {
		return ""
	}
	return topoproto.TabletAliasString(shard.Shard.PrimaryAlias)
}

func shardVoters(t *testing.T, tc *testCluster) []string {
	shard, err := tc.VtctldClientProcess.GetShard(keyspaceName, shardName)
	require.NoError(t, err)
	var voters []string
	for _, alias := range shard.Shard.GroupReplicationVoters {
		voters = append(voters, topoproto.TabletAliasString(alias))
	}
	return voters
}

func aliasesOf(tablets []*cluster.Vttablet) []string {
	var aliases []string
	for _, tablet := range tablets {
		aliases = append(aliases, tablet.Alias)
	}
	return aliases
}

func tabletType(t *testing.T, tc *testCluster, tablet *cluster.Vttablet) topodatapb.TabletType {
	tab, err := tc.VtctldClientProcess.GetTablet(tablet.Alias)
	require.NoError(t, err)
	return tab.Type
}

// waitForGroup waits until the given tablets are the voters in the shard record and ONLINE
// members of one group whose primary is the given tablet, and the topology agrees.
func waitForGroup(t *testing.T, tc *testCluster, primary *cluster.Vttablet, members []*cluster.Vttablet) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		primaryStatus, err := fullStatus(t, tc, primary)
		require.NoError(c, err)
		require.NotNil(c, primaryStatus.GroupReplicationStatus)
		assert.True(c, mysql.IsGroupPrimary(primaryStatus.GroupReplicationStatus), "primary %s is not the group primary: %v", primary.Alias, primaryStatus.GroupReplicationStatus)
		for _, voter := range members {
			status, err := fullStatus(t, tc, voter)
			require.NoError(c, err)
			require.NotNil(c, status.GroupReplicationStatus, voter.Alias)
			assert.Equal(c, mysql.GroupMemberStateOnline, status.GroupReplicationStatus.MemberState, voter.Alias)
			assert.Equal(c, primaryStatus.ServerUuid, status.GroupReplicationStatus.PrimaryUuid, voter.Alias)
			assert.Equal(c, policy.GroupName(keyspaceName, shardName), status.GroupReplicationStatus.GroupName, voter.Alias)
		}
		assert.Equal(c, primary.Alias, shardPrimary(t, tc))
		assert.ElementsMatch(c, aliasesOf(members), shardVoters(t, tc))
		assert.Equal(c, topodatapb.TabletType_PRIMARY, tabletType(t, tc, primary))
	}, waitTimeout, pollInterval)
}

// vtgateWatcherLagError is the error with which vtgate fails a write that it routes after its health
// check saw the primary stop serving, but before its keyspace event watcher did: the gateway finds no
// serving primary, the watcher does not ask for buffering yet, and the gateway's retries, which do not
// wait, all fail the same way. The window is the watcher's processing lag behind the health check, and
// opens at every planned pause of the primary (see "The migration's pauses and vtgate's buffer" in the
// design). Upstream's TestInconsistentStateDetectedBuffering (go/vt/vtgate) reproduces it.
const vtgateWatcherLagError = "inconsistent state detected, primary is serving but initially found no available tablet"

// maxWatcherLagFailures is how many writes a planned pause of the primary may fail with
// vtgateWatcherLagError: the writers send one write at a time, and the window opens once per pause.
const maxWatcherLagFailures = 1

// span is a stretch of time: the call that makes a planned pause of the primary, or a write, from
// when it was sent until it failed.
type span struct{ start, end time.Time }

// overlaps returns whether the two spans share an instant.
func (s span) overlaps(o span) bool {
	return !s.end.Before(o.start) && !o.end.Before(s.start)
}

// timed runs f, a call that makes one planned pause of the primary, such as a planned reparent or a
// migration step, and returns its span: the pause starts and ends within it.
func timed(f func()) span {
	start := time.Now()
	f()
	return span{start: start, end: time.Now()}
}

// requireNoFailedWrites checks that no write failed during the planned pauses of the primary, other
// than in vtgate's watcher lag (vtgateWatcherLagError), which the design does not cover: such a
// failure must overlap the call that made one of the pauses, and each pause may cause at most
// maxWatcherLagFailures of them.
func requireNoFailedWrites(t *testing.T, fail int64, watcherLag []span, pauses []span, lastErr error, during string) {
	t.Helper()
	require.NotEmpty(t, pauses)
	assert.Zero(t, fail-int64(len(watcherLag)), "writes failed %s, last error: %v", during, lastErr)
	perPause := make([]int, len(pauses))
	for _, w := range watcherLag {
		i := slices.IndexFunc(pauses, w.overlaps)
		if !assert.NotEqual(t, -1, i, "a write failed %s in vtgate's watcher lag outside the planned pauses of the primary: sent %v, failed %v, pauses %v",
			during, w.start, w.end, pauses) {
			continue
		}
		perPause[i]++
	}
	for i, n := range perPause {
		assert.LessOrEqual(t, n, maxWatcherLagFailures, "writes failed %s in vtgate's watcher lag during pause %d of %d", during, i+1, len(pauses))
	}
	if len(watcherLag) > 0 {
		t.Logf("%d writes failed %s in vtgate's watcher lag (%d pauses): %s", len(watcherLag), during, len(pauses), vtgateWatcherLagError)
	}
}

// isWatcherLag returns whether a write failed in vtgate's watcher lag.
func isWatcherLag(err error) bool {
	return err != nil && strings.Contains(err.Error(), vtgateWatcherLagError)
}

// writer inserts rows through vtgate until stopped and counts the failures.
type writer struct {
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	ok, fail atomic.Int64
	// longest is the longest time a write took, in nanoseconds, whether it succeeded or not.
	longest atomic.Int64
	mu      sync.Mutex
	lastErr error
	// watcherLag holds the spans of the writes that failed in vtgate's watcher lag, of fail.
	watcherLag []span
}

func startWriter(t *testing.T, tc *testCluster) *writer {
	ctx, cancel := context.WithCancel(t.Context())
	w := &writer{cancel: cancel}
	params := mysql.ConnParams{Host: tc.Hostname, Port: tc.VtgateMySQLPort}
	w.wg.Go(func() {
		var conn *mysql.Conn
		for ctx.Err() == nil {
			if conn == nil {
				var err error
				start := time.Now()
				conn, err = mysql.Connect(ctx, &params)
				if err != nil {
					w.failed(span{start: start, end: time.Now()}, err)
					continue
				}
			}
			start := time.Now()
			_, err := conn.ExecuteFetch("insert into writes (val) values ('x')", 0, false)
			w.observe(time.Since(start))
			if err != nil {
				w.failed(span{start: start, end: time.Now()}, err)
				conn.Close()
				conn = nil
				continue
			}
			w.ok.Add(1)
			select {
			case <-ctx.Done():
			case <-time.After(20 * time.Millisecond):
			}
		}
		if conn != nil {
			conn.Close()
		}
	})
	return w
}

// observe records how long a write took.
func (w *writer) observe(d time.Duration) {
	for {
		longest := w.longest.Load()
		if int64(d) <= longest || w.longest.CompareAndSwap(longest, int64(d)) {
			return
		}
	}
}

// longestWrite returns the longest time a write took so far.
func (w *writer) longestWrite() time.Duration {
	return time.Duration(w.longest.Load())
}

// failed records a failed write, sent and failed within s.
func (w *writer) failed(s span, err error) {
	w.fail.Add(1)
	w.mu.Lock()
	if isWatcherLag(err) {
		w.watcherLag = append(w.watcherLag, s)
	}
	w.lastErr = err
	w.mu.Unlock()
	time.Sleep(50 * time.Millisecond)
}

// watcherLagFailures returns the spans of the writes that failed in vtgate's watcher lag.
func (w *writer) watcherLagFailures() []span {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.watcherLag)
}

func (w *writer) stop() (ok, fail int64, lastErr error) {
	w.cancel()
	w.wg.Wait()
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.ok.Load(), w.fail.Load(), w.lastErr
}

// migrationMaxWriteStall bounds how long a write may take during a migration step. vtgate buffers
// the writes while the primary pauses for a change of its MySQL's replication, and releases them as
// soon as it serves again; the buffering would otherwise last --buffer-max-failover-duration (30s).
// The longest pause is the primary's leave of its group: MySQL's STOP GROUP_REPLICATION takes about
// 3.6s, and the first commit after it waits up to about 1s for a semi-sync acknowledgement, since
// the replicas reconnect.
const migrationMaxWriteStall = 8 * time.Second

// bufferStats returns vtgate's buffering starts for the test's shard, and its buffering stops by
// reason.
func bufferStats(t *testing.T, tc *testCluster) (starts int, stops map[string]int) {
	t.Helper()
	vars := tc.VtgateProcess.GetVars()
	require.NotNil(t, vars)
	key := keyspaceName + "." + shardName
	if m, ok := vars["BufferStarts"].(map[string]any); ok {
		if v, ok := m[key].(float64); ok {
			starts = int(v)
		}
	}
	stops = make(map[string]int)
	if m, ok := vars["BufferStops"].(map[string]any); ok {
		for k, v := range m {
			if reason, ok := strings.CutPrefix(k, key+"."); ok {
				stops[reason] = int(v.(float64))
			}
		}
	}
	return starts, stops
}

// checkMigrationWrites checks that no write failed during a migration step, and that none waited
// for long: vtgate buffered the writes only while the primary paused, and never until
// --buffer-max-failover-duration.
func checkMigrationWrites(t *testing.T, tc *testCluster, w *writer, migration span, startsBefore int, stopsBefore map[string]int) {
	t.Helper()
	ok, fail, lastErr := w.stop()
	assert.Positive(t, ok)
	requireNoFailedWrites(t, fail, w.watcherLagFailures(), []span{migration}, lastErr, "during the migration")
	starts, stops := bufferStats(t, tc)
	t.Logf("longest write %v, %d writes, vtgate buffered %d times, stopped buffering: %v (before: %v)",
		w.longestWrite(), ok, starts-startsBefore, stops, stopsBefore)
	assert.Less(t, w.longestWrite(), migrationMaxWriteStall, "a write waited for too long during the migration")
	assert.Equal(t, stopsBefore["MaxDurationExceeded"], stops["MaxDurationExceeded"], "vtgate buffered until --buffer-max-failover-duration")
}

// killMysqld kills the tablet's mysqld without letting it leave its group cleanly.
func killMysqld(t *testing.T, tablet *cluster.Vttablet) {
	pidFile := path.Join(os.Getenv("VTDATAROOT"), fmt.Sprintf("vt_%010d", tablet.TabletUID), "mysql.pid")
	data, err := os.ReadFile(pidFile)
	require.NoError(t, err)
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	require.NoError(t, err)
	require.NoError(t, syscall.Kill(pid, syscall.SIGKILL))
}

// rowCount returns the number of rows on the tablet's MySQL.
func rowCount(t *testing.T, tablet *cluster.Vttablet) (int, error) {
	qr, err := tablet.VttabletProcess.QueryTablet("select count(*) from writes", keyspaceName, true)
	if err != nil {
		return 0, err
	}
	return qr.Rows[0][0].ToInt()
}

// superReadOnlyActionEnabled returns whether the member action mysql_disable_super_read_only_if_primary
// is enabled in the configuration of the tablet's MySQL: its group's while it is in a group.
func superReadOnlyActionEnabled(tablet *cluster.Vttablet) (bool, error) {
	qr, err := tablet.VttabletProcess.QueryTablet("SELECT ENABLED FROM performance_schema.replication_group_member_actions "+
		"WHERE NAME = 'mysql_disable_super_read_only_if_primary' AND EVENT = 'AFTER_PRIMARY_ELECTION'", keyspaceName, false)
	if err != nil {
		return false, err
	}
	if len(qr.Rows) != 1 {
		return false, fmt.Errorf("unexpected member actions of %s: %v", tablet.Alias, qr.Rows)
	}
	enabled, err := qr.Rows[0][0].ToInt()
	return enabled != 0, err
}

// requireSuperReadOnlyActionDisabled checks that every voter's MySQL has the member action
// mysql_disable_super_read_only_if_primary disabled: Group Replication leaves the primary of every
// election super_read_only, and only the tablet's decision that it may serve makes it writable.
func requireSuperReadOnlyActionDisabled(t *testing.T, tc *testCluster) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		for _, voter := range tc.replicas {
			enabled, err := superReadOnlyActionEnabled(voter)
			require.NoError(c, err, voter.Alias)
			assert.False(c, enabled, "%s has mysql_disable_super_read_only_if_primary enabled", voter.Alias)
		}
	}, waitTimeout, pollInterval)
}

func waitForRowCounts(t *testing.T, tc *testCluster, primary *cluster.Vttablet) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		want, err := rowCount(t, primary)
		require.NoError(c, err)
		tablets := append([]*cluster.Vttablet{}, tc.replicas...)
		if tc.rdonly != nil {
			tablets = append(tablets, tc.rdonly)
		}
		for _, tablet := range tablets {
			got, err := rowCount(t, tablet)
			require.NoError(c, err)
			assert.Equal(c, want, got, tablet.Alias)
		}
	}, waitTimeout, pollInterval)
}

// TestGroupReplicationLifecycle converts a cross-cell semi-sync shard to Group Replication
// online, fails over with PRS and by killing the primary, and converts it back. vtgate buffers the
// writes while the primary pauses for a migration step, and no write fails in either direction.
func TestGroupReplicationLifecycle(t *testing.T) {
	tc := setupCluster(t, defaultClusterOptions())
	primary := tc.replicas[0]

	t.Run("migrate from semi-sync to group replication without failing writes", func(t *testing.T) {
		startsBefore, stopsBefore := bufferStats(t, tc)
		w := startWriter(t, tc)
		var out string
		var err error
		migration := timed(func() {
			out, err = tc.VtctldClientProcess.ExecuteCommandWithOutput("MigrateReplicationMode",
				"--durability-policy", policy.DurabilityGroupReplicationCrossCell, keyspaceName)
		})
		require.NoError(t, err, out)
		waitForGroup(t, tc, primary, tc.replicas)
		// The primary pauses once, while its MySQL bootstraps the group.
		checkMigrationWrites(t, tc, w, migration, startsBefore, stopsBefore)

		ks, err := tc.VtctldClientProcess.GetKeyspace(keyspaceName)
		require.NoError(t, err)
		assert.Equal(t, policy.DurabilityGroupReplicationCrossCell, ks.Keyspace.DurabilityPolicy)

		// The group, not semi-sync, makes the primary's commits durable now. The primary is the
		// group's only consensus leader.
		status, err := fullStatus(t, tc, primary)
		require.NoError(t, err)
		assert.False(t, status.SemiSyncPrimaryEnabled)
		assert.True(t, status.GroupReplicationStatus.PaxosSingleLeader)

		// The RDONLY tablet is not a member; it replicates asynchronously from the primary.
		status, err = fullStatus(t, tc, tc.rdonly)
		require.NoError(t, err)
		assert.False(t, mysql.IsGroupMemberActive(status.GroupReplicationStatus))
		require.NotNil(t, status.ReplicationStatus)
		assert.Equal(t, int32(primary.MySQLPort), status.ReplicationStatus.SourcePort)
		waitForRowCounts(t, tc, primary)

		// Every member has the member action that makes the primary of an election writable
		// disabled: the group took the configuration of the primary that bootstrapped it, and every
		// tablet disables it before it starts Group Replication.
		requireSuperReadOnlyActionDisabled(t, tc)
	})

	t.Run("planned reparent switches the group primary", func(t *testing.T) {
		w := startWriter(t, tc)
		newPrimary := tc.replicas[1]
		var err error
		reparent := timed(func() {
			err = tc.VtctldClientProcess.PlannedReparentShard(keyspaceName, shardName, newPrimary.Alias)
		})
		require.NoError(t, err)
		waitForGroup(t, tc, newPrimary, tc.replicas)
		primary = newPrimary
		ok, fail, lastErr := w.stop()
		// vtgate buffers writes while the primary switches.
		requireNoFailedWrites(t, fail, w.watcherLagFailures(), []span{reparent}, lastErr, "during the planned reparent")
		assert.Positive(t, ok)
		waitForRowCounts(t, tc, primary)
	})

	t.Run("the group elects a new primary when the primary's mysqld dies", func(t *testing.T) {
		w := startWriter(t, tc)
		oldPrimary := primary
		killed := time.Now()
		killMysqld(t, oldPrimary)

		var newPrimary *cluster.Vttablet
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			alias := shardPrimary(t, tc)
			assert.NotEqual(c, oldPrimary.Alias, alias)
			for _, voter := range tc.replicas {
				if voter.Alias == alias {
					newPrimary = voter
				}
			}
			require.NotNil(c, newPrimary)
			status, err := fullStatus(t, tc, newPrimary)
			require.NoError(c, err)
			assert.True(c, mysql.IsGroupPrimary(status.GroupReplicationStatus))
		}, waitTimeout, pollInterval)
		failover := time.Since(killed)
		t.Logf("%s was the primary in topo %v after the old primary's mysqld was killed", newPrimary.Alias, failover)
		// With group_replication_member_expel_timeout=0 the group replaces a failed primary
		// about 7 seconds after it fails; with MySQL's default of 5 seconds it takes about 22.
		assert.Less(t, failover, 15*time.Second)
		primary = newPrimary

		// Writes resume on the new primary.
		before := w.ok.Load()
		require.Eventually(t, func() bool { return w.ok.Load() > before+10 }, waitTimeout, pollInterval)
		_, _, _ = w.stop()

		// mysqld_safe restarts the killed mysqld, and the old primary's tablet makes it rejoin
		// the group as a secondary.
		waitForGroup(t, tc, primary, tc.replicas)
		waitForRowCounts(t, tc, primary)
		// The member that joined the group again has the action disabled too, and the primary that
		// the group elected became writable only through its tablet.
		requireSuperReadOnlyActionDisabled(t, tc)
		status, err := fullStatus(t, tc, primary)
		require.NoError(t, err)
		assert.False(t, status.ReadOnly)
	})

	t.Run("migrate back to semi-sync without failing writes", func(t *testing.T) {
		startsBefore, stopsBefore := bufferStats(t, tc)
		w := startWriter(t, tc)
		var out string
		var err error
		migration := timed(func() {
			out, err = tc.VtctldClientProcess.ExecuteCommandWithOutput("MigrateReplicationMode",
				"--durability-policy", policy.DurabilityCrossCell, keyspaceName)
		})
		require.NoError(t, err, out)

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			for _, voter := range tc.replicas {
				status, err := fullStatus(t, tc, voter)
				require.NoError(c, err)
				assert.False(c, mysql.IsGroupMemberActive(status.GroupReplicationStatus), voter.Alias)
				if voter == primary {
					assert.True(c, status.SemiSyncPrimaryEnabled)
					assert.False(c, status.ReadOnly)
					continue
				}
				require.NotNil(c, status.ReplicationStatus, voter.Alias)
				assert.Equal(c, int32(primary.MySQLPort), status.ReplicationStatus.SourcePort, voter.Alias)
			}
		}, waitTimeout, pollInterval)
		// The primary pauses once, while its MySQL leaves the group.
		checkMigrationWrites(t, tc, w, migration, startsBefore, stopsBefore)
		waitForRowCounts(t, tc, primary)
	})
}
