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

	clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs,
		"--enable-buffer",
		"--buffer-window", "30s",
		"--buffer-max-failover-duration", "30s",
		"--buffer-min-time-between-failovers", "1s",
	)
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

// writer inserts rows through vtgate until stopped and counts the failures.
type writer struct {
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	ok, fail atomic.Int64
	mu       sync.Mutex
	lastErr  error
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
				conn, err = mysql.Connect(ctx, &params)
				if err != nil {
					w.record(err)
					continue
				}
			}
			if _, err := conn.ExecuteFetch("insert into writes (val) values ('x')", 0, false); err != nil {
				w.record(err)
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

func (w *writer) record(err error) {
	w.fail.Add(1)
	w.mu.Lock()
	w.lastErr = err
	w.mu.Unlock()
	time.Sleep(50 * time.Millisecond)
}

func (w *writer) stop() (ok, fail int64, lastErr error) {
	w.cancel()
	w.wg.Wait()
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.ok.Load(), w.fail.Load(), w.lastErr
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
// online, fails over with PRS and by killing the primary, and converts it back.
func TestGroupReplicationLifecycle(t *testing.T) {
	tc := setupCluster(t, defaultClusterOptions())
	primary := tc.replicas[0]

	t.Run("migrate from semi-sync to group replication without failing writes", func(t *testing.T) {
		w := startWriter(t, tc)
		out, err := tc.VtctldClientProcess.ExecuteCommandWithOutput("MigrateReplicationMode",
			"--durability-policy", policy.DurabilityGroupReplicationCrossCell, keyspaceName)
		require.NoError(t, err, out)
		waitForGroup(t, tc, primary, tc.replicas)
		ok, fail, lastErr := w.stop()
		assert.Zero(t, fail, "writes failed during the migration, last error: %v", lastErr)
		assert.Positive(t, ok)

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
	})

	t.Run("planned reparent switches the group primary", func(t *testing.T) {
		w := startWriter(t, tc)
		newPrimary := tc.replicas[1]
		require.NoError(t, tc.VtctldClientProcess.PlannedReparentShard(keyspaceName, shardName, newPrimary.Alias))
		waitForGroup(t, tc, newPrimary, tc.replicas)
		primary = newPrimary
		ok, fail, lastErr := w.stop()
		// vtgate buffers writes while the primary switches.
		assert.Zero(t, fail, "writes failed during the planned reparent, last error: %v", lastErr)
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
	})

	t.Run("migrate back to semi-sync without data loss", func(t *testing.T) {
		w := startWriter(t, tc)
		out, err := tc.VtctldClientProcess.ExecuteCommandWithOutput("MigrateReplicationMode",
			"--durability-policy", policy.DurabilityCrossCell, keyspaceName)
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
		ok, fail, lastErr := w.stop()
		assert.Positive(t, ok)
		// Leaving the group on the primary makes MySQL read-only for a moment; vtgate buffers
		// most of it, a few writes may still fail.
		assert.LessOrEqual(t, fail, int64(5), "too many writes failed during the migration back, last error: %v", lastErr)
		waitForRowCounts(t, tc, primary)
	})
}
