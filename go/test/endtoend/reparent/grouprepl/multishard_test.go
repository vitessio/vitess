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

package grouprepl

import (
	"context"
	"fmt"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	msKeyspace = "msks"
	msSchema   = `create table if not exists writes (id bigint not null, val varchar(64), primary key (id)) engine=InnoDB`
	msVSchema  = `{"sharded": true, "vindexes": {"hash": {"type": "hash"}}, "tables": {"writes": {"column_vindexes": [{"column": "id", "name": "hash"}]}}}`
)

// msShards are the shards of the keyspace, in the order the test converts them.
var msShards = []string{"-80", "80-"}

// msBufferCooldown is longer than vtgate's --buffer-min-time-between-failovers (1s): vtgate does not
// buffer a shard's writes again sooner after it buffered them, and a migration's pause right after a
// reparent of the same shard would fail writes instead of buffering them (see "The migration's
// pauses and vtgate's buffer" in doc/design-docs/GroupReplication.md).
const msBufferCooldown = 2 * time.Second

// msCluster is a keyspace of two shards, each with one REPLICA tablet in each of three cells.
type msCluster struct {
	*cluster.LocalProcessCluster
	// tablets are the tablets of each shard; tablets[shard][0] is its first primary.
	tablets map[string][]*cluster.Vttablet
	// lastBuffering is when the last step that may have made vtgate buffer ended.
	lastBuffering time.Time
	// lastID is the last id that a writer used: every write of the test has its own id.
	lastID atomic.Int64
}

// waitBufferCooldown waits until vtgate buffers the writes of a shard again, after the last step
// that may have made it buffer them (msBufferCooldown).
func (mc *msCluster) waitBufferCooldown(t *testing.T) {
	require.Eventually(t, func() bool { return time.Since(mc.lastBuffering) > msBufferCooldown }, waitTimeout, pollInterval)
}

// setupMultiShardCluster starts the keyspace with cross-cell semi-sync, like setupCluster does for
// a single shard, with a buffering vtgate and a VTOrc.
func setupMultiShardCluster(t *testing.T) *msCluster {
	clusterInstance := cluster.NewCluster(cells[0], "localhost")
	t.Cleanup(clusterInstance.Teardown)
	mc := &msCluster{LocalProcessCluster: clusterInstance, tablets: make(map[string][]*cluster.Vttablet)}

	require.NoError(t, clusterInstance.StartTopo())
	for _, cell := range cells[1:] {
		require.NoError(t, clusterInstance.VtctldClientProcess.AddCellInfo(cell))
	}

	var shards []cluster.Shard
	var all []*cluster.Vttablet
	for i, shard := range msShards {
		for j, cell := range cells {
			tablet := clusterInstance.NewVttabletInstance("replica", 100*(j+1)+10*(i+1), cell)
			mc.tablets[shard] = append(mc.tablets[shard], tablet)
			all = append(all, tablet)
		}
		shards = append(shards, cluster.Shard{Name: shard, Vttablets: mc.tablets[shard]})
	}

	clusterInstance.VtTabletExtraArgs = append(clusterInstance.VtTabletExtraArgs,
		"--lock-tables-timeout", "5s",
		"--queryserver-enable-online-ddl=false",
		"--group-replication-sync-interval", "500ms",
		"--heartbeat-enable",
		"--heartbeat-interval", "250ms",
	)
	require.NoError(t, clusterInstance.SetupCluster(&cluster.Keyspace{Name: msKeyspace, SchemaSQL: msSchema}, shards))

	var procs []*exec.Cmd
	for _, tablet := range all {
		proc, err := tablet.MysqlctlProcess.StartProcess()
		require.NoError(t, err)
		procs = append(procs, proc)
	}
	for _, proc := range procs {
		require.NoError(t, proc.Wait())
	}

	out, err := clusterInstance.VtctldClientProcess.ExecuteCommandWithOutput("SetKeyspaceDurabilityPolicy", msKeyspace, "--durability-policy="+policy.DurabilityCrossCell)
	require.NoError(t, err, out)

	for _, tablet := range all {
		tablet.VttabletProcess.SupportsBackup = false
		tablet.VttabletProcess.ExtraArgs = append(tablet.VttabletProcess.ExtraArgs, "--enable-group-replication")
		require.NoError(t, tablet.VttabletProcess.Setup())
	}
	for _, tablet := range all {
		require.NoError(t, tablet.VttabletProcess.WaitForTabletStatuses([]string{"SERVING", "NOT_SERVING"}))
	}
	for _, shard := range msShards {
		primary := mc.tablets[shard][0]
		require.NoError(t, clusterInstance.VtctldClientProcess.InitializeShard(msKeyspace, shard, cells[0], primary.TabletUID))
		_, err = primary.VttabletProcess.QueryTablet(msSchema, msKeyspace, true)
		require.NoError(t, err)
	}
	require.NoError(t, clusterInstance.VtctldClientProcess.ApplyVSchema(msKeyspace, msVSchema))

	clusterInstance.VtGateExtraArgs = append(clusterInstance.VtGateExtraArgs,
		"--enable-buffer",
		"--buffer-window", "30s",
		"--buffer-max-failover-duration", "30s",
		"--buffer-min-time-between-failovers", "1s",
	)
	vtgate := clusterInstance.NewVtgateInstance()
	vtgate.CellsToWatch = strings.Join(cells, ",")
	clusterInstance.VtgateProcess = *vtgate
	require.NoError(t, clusterInstance.VtgateProcess.Setup())

	vtorc := clusterInstance.NewVTOrcProcess(vtorcConfig, cells[0])
	require.NoError(t, vtorc.Setup())
	clusterInstance.VTOrcProcesses = append(clusterInstance.VTOrcProcesses, vtorc)
	return mc
}

// shardRecord returns the shard record of the shard.
func (mc *msCluster) shardRecord(t *testing.T, shard string) *topodatapb.Shard {
	si, err := mc.VtctldClientProcess.GetShard(msKeyspace, shard)
	require.NoError(t, err)
	return si.Shard
}

// keyspacePolicy returns the keyspace's durability policy.
func (mc *msCluster) keyspacePolicy(t *testing.T) string {
	ks, err := mc.VtctldClientProcess.GetKeyspace(msKeyspace)
	require.NoError(t, err)
	return ks.Keyspace.DurabilityPolicy
}

// tabletType returns the type of the tablet in its tablet record.
func (mc *msCluster) tabletType(t *testing.T, tablet *cluster.Vttablet) topodatapb.TabletType {
	tab, err := mc.VtctldClientProcess.GetTablet(tablet.Alias)
	require.NoError(t, err)
	return tab.Type
}

// migrate runs MigrateReplicationMode on one shard.
func (mc *msCluster) migrate(t *testing.T, durability, shard string) {
	out, err := mc.VtctldClientProcess.ExecuteCommandWithOutput("MigrateReplicationMode", "--durability-policy", durability, msKeyspace+"/"+shard)
	require.NoError(t, err, out)
}

// waitForShardGroup waits until every tablet of the shard is a voter in the shard record and an
// ONLINE member of the shard's group, whose primary is the given tablet, and the topology agrees.
func (mc *msCluster) waitForShardGroup(t *testing.T, shard string, primary *cluster.Vttablet) {
	t.Helper()
	tc := &testCluster{LocalProcessCluster: mc.LocalProcessCluster}
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		primaryStatus, err := fullStatus(t, tc, primary)
		require.NoError(c, err)
		require.NotNil(c, primaryStatus.GroupReplicationStatus)
		assert.True(c, mysql.IsGroupPrimary(primaryStatus.GroupReplicationStatus), "%s is not the group primary", primary.Alias)
		for _, tablet := range mc.tablets[shard] {
			status, err := fullStatus(t, tc, tablet)
			require.NoError(c, err)
			require.NotNil(c, status.GroupReplicationStatus, tablet.Alias)
			assert.Equal(c, mysql.GroupMemberStateOnline, status.GroupReplicationStatus.MemberState, tablet.Alias)
			assert.Equal(c, primaryStatus.ServerUuid, status.GroupReplicationStatus.PrimaryUuid, tablet.Alias)
			assert.Equal(c, policy.GroupName(msKeyspace, shard), status.GroupReplicationStatus.GroupName, tablet.Alias)
		}
		si := mc.shardRecord(t, shard)
		assert.Equal(c, primary.Alias, topoproto.TabletAliasString(si.PrimaryAlias))
		var voters []string
		for _, alias := range si.GroupReplicationVoters {
			voters = append(voters, topoproto.TabletAliasString(alias))
		}
		assert.ElementsMatch(c, aliasesOf(mc.tablets[shard]), voters)
		assert.Equal(c, topodatapb.TabletType_PRIMARY, mc.tabletType(t, primary))
	}, waitTimeout, pollInterval)
}

// waitForSemiSyncShard waits until no tablet of the shard is a group member, the given tablet is
// its writable primary with semi-sync, and the other tablets replicate from it.
func (mc *msCluster) waitForSemiSyncShard(t *testing.T, shard string, primary *cluster.Vttablet) {
	t.Helper()
	tc := &testCluster{LocalProcessCluster: mc.LocalProcessCluster}
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		for _, tablet := range mc.tablets[shard] {
			status, err := fullStatus(t, tc, tablet)
			require.NoError(c, err)
			assert.False(c, mysql.IsGroupMemberActive(status.GroupReplicationStatus), tablet.Alias)
			if tablet == primary {
				assert.True(c, status.SemiSyncPrimaryEnabled, tablet.Alias)
				assert.False(c, status.ReadOnly, tablet.Alias)
				continue
			}
			require.NotNil(c, status.ReplicationStatus, tablet.Alias)
			assert.Equal(c, int32(primary.MySQLPort), status.ReplicationStatus.SourcePort, tablet.Alias)
			assert.True(c, status.SemiSyncReplicaEnabled, tablet.Alias)
		}
		assert.Equal(c, primary.Alias, topoproto.TabletAliasString(mc.shardRecord(t, shard).PrimaryAlias))
	}, waitTimeout, pollInterval)
}

// shardPrimaryTablet returns the tablet that the shard record names as the shard's primary.
func (mc *msCluster) shardPrimaryTablet(t *testing.T, shard string) *cluster.Vttablet {
	alias := topoproto.TabletAliasString(mc.shardRecord(t, shard).PrimaryAlias)
	for _, tablet := range mc.tablets[shard] {
		if tablet.Alias == alias {
			return tablet
		}
	}
	require.FailNow(t, "the shard primary is not a tablet of the shard", "%s: %s", shard, alias)
	return nil
}

// idWriter inserts rows with increasing ids through vtgate, into both shards, and remembers the ids
// of the writes that were acknowledged.
type idWriter struct {
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	fail    atomic.Int64
	mu      sync.Mutex
	acked   []int64
	lastErr error
}

func startIDWriter(t *testing.T, mc *msCluster) *idWriter {
	ctx, cancel := context.WithCancel(t.Context())
	w := &idWriter{cancel: cancel}
	t.Cleanup(func() { w.stop() })
	params := mysql.ConnParams{Host: mc.Hostname, Port: mc.VtgateMySQLPort}
	w.wg.Go(func() {
		var conn *mysql.Conn
		for ctx.Err() == nil {
			if conn == nil {
				var err error
				if conn, err = mysql.Connect(ctx, &params); err != nil {
					w.failed(err)
					continue
				}
			}
			id := mc.lastID.Add(1)
			if _, err := conn.ExecuteFetch(fmt.Sprintf("insert into writes (id, val) values (%d, 'x')", id), 0, false); err != nil {
				w.failed(err)
				conn.Close()
				conn = nil
				continue
			}
			w.mu.Lock()
			w.acked = append(w.acked, id)
			w.mu.Unlock()
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

func (w *idWriter) failed(err error) {
	w.fail.Add(1)
	w.mu.Lock()
	w.lastErr = err
	w.mu.Unlock()
	time.Sleep(50 * time.Millisecond)
}

// ackedCount returns the number of acknowledged writes so far.
func (w *idWriter) ackedCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.acked)
}

// stop stops the writer and returns the ids of the acknowledged writes, the number of failed writes
// and the last error.
func (w *idWriter) stop() ([]int64, int64, error) {
	w.cancel()
	w.wg.Wait()
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.acked), w.fail.Load(), w.lastErr
}

// requireNoLostWrites checks that every acknowledged write is in the keyspace, read through vtgate
// from the shards' current primaries.
func requireNoLostWrites(t *testing.T, mc *msCluster, acked []int64) {
	t.Helper()
	require.NotEmpty(t, acked)
	conn, err := mysql.Connect(t.Context(), &mysql.ConnParams{Host: mc.Hostname, Port: mc.VtgateMySQLPort})
	require.NoError(t, err)
	defer conn.Close()
	var present map[int64]bool
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		qr, err := conn.ExecuteFetch("select id from writes", 1000000, false)
		require.NoError(c, err)
		present = make(map[int64]bool, len(qr.Rows))
		for _, row := range qr.Rows {
			id, err := row[0].ToInt64()
			require.NoError(c, err)
			present[id] = true
		}
	}, waitTimeout, pollInterval)
	var lost []int64
	for _, id := range acked {
		if !present[id] {
			lost = append(lost, id)
		}
	}
	assert.Empty(t, lost, "acknowledged writes are missing after the failover")
}

// TestGroupReplicationMigratesShardByShard converts a keyspace of two shards to Group Replication
// one shard at a time, and back. While the keyspace's policy is still semi-sync, the converted
// shard is managed as a Group Replication shard: when its primary's mysqld dies, the group elects
// a new primary, and the old primary rejoins the group as a voter rather than being made an
// asynchronous replica of it, and PRS takes the Group Replication path on it while it takes the
// semi-sync path on the other shard. The keyspace policy switches once both shards are converted.
// Converted back one at a time, the shard that left its group fails over like any semi-sync shard
// while the keyspace's policy is still Group Replication.
func TestGroupReplicationMigratesShardByShard(t *testing.T) {
	mc := setupMultiShardCluster(t)
	first, second := msShards[0], msShards[1]
	gr, semiSync := policy.DurabilityGroupReplicationCrossCell, policy.DurabilityCrossCell

	t.Run("migrate the first shard only", func(t *testing.T) {
		mc.waitBufferCooldown(t)
		defer func() { mc.lastBuffering = time.Now() }()
		w := startIDWriter(t, mc)
		mc.migrate(t, gr, first)
		mc.waitForShardGroup(t, first, mc.tablets[first][0])
		acked, fail, lastErr := w.stop()
		assert.Zero(t, fail, "writes failed during the migration, last error: %v", lastErr)
		requireNoLostWrites(t, mc, acked)

		assert.Equal(t, semiSync, mc.keyspacePolicy(t), "the keyspace keeps its policy while a shard is not converted")
		assert.Equal(t, gr, mc.shardRecord(t, first).DurabilityPolicy)
		assert.Empty(t, mc.shardRecord(t, second).DurabilityPolicy)
		mc.waitForSemiSyncShard(t, second, mc.tablets[second][0])
	})

	t.Run("the group replaces the primary of the converted shard while the keyspace is semi-sync", func(t *testing.T) {
		mc.waitBufferCooldown(t)
		defer func() { mc.lastBuffering = time.Now() }()
		w := startIDWriter(t, mc)
		require.Eventually(t, func() bool { return w.ackedCount() > 20 }, waitTimeout, pollInterval)
		oldPrimary := mc.shardPrimaryTablet(t, first)
		killed := time.Now()
		killMysqld(t, oldPrimary)

		var newPrimary *cluster.Vttablet
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			newPrimary = mc.shardPrimaryTablet(t, first)
			assert.NotEqual(c, oldPrimary.Alias, newPrimary.Alias)
		}, waitTimeout, pollInterval)
		t.Logf("%s was the primary of %s in topo %v after the old primary's mysqld was killed", newPrimary.Alias, first, time.Since(killed))

		before := w.ackedCount()
		require.Eventually(t, func() bool { return w.ackedCount() > before+20 }, waitTimeout, pollInterval)
		// mysqld_safe restarts the killed mysqld, and the old primary rejoins the group as a voter.
		mc.waitForShardGroup(t, first, newPrimary)
		acked, fail, lastErr := w.stop()
		t.Logf("%d writes acknowledged, %d failed during the failover, last error: %v", len(acked), fail, lastErr)
		requireNoLostWrites(t, mc, acked)

		// The old primary is a member of the group, not an asynchronous replica of it.
		status, err := fullStatus(t, &testCluster{LocalProcessCluster: mc.LocalProcessCluster}, oldPrimary)
		require.NoError(t, err)
		if status.ReplicationStatus != nil {
			assert.Empty(t, status.ReplicationStatus.SourceHost, "the old primary must not replicate asynchronously next to its group")
		}
		assert.Equal(t, semiSync, mc.keyspacePolicy(t))
	})

	t.Run("planned reparents take the path of each shard's policy", func(t *testing.T) {
		mc.waitBufferCooldown(t)
		defer func() { mc.lastBuffering = time.Now() }()
		w := startIDWriter(t, mc)
		for _, shard := range msShards {
			current := mc.shardPrimaryTablet(t, shard)
			target := mc.tablets[shard][0]
			if target == current {
				target = mc.tablets[shard][1]
			}
			require.NoError(t, mc.VtctldClientProcess.PlannedReparentShard(msKeyspace, shard, target.Alias))
			if shard == first {
				mc.waitForShardGroup(t, shard, target)
			} else {
				mc.waitForSemiSyncShard(t, shard, target)
			}
		}
		acked, fail, lastErr := w.stop()
		assert.Zero(t, fail, "writes failed during the planned reparents, last error: %v", lastErr)
		requireNoLostWrites(t, mc, acked)
	})

	t.Run("migrate the second shard: the keyspace policy switches", func(t *testing.T) {
		mc.waitBufferCooldown(t)
		defer func() { mc.lastBuffering = time.Now() }()
		w := startIDWriter(t, mc)
		mc.migrate(t, gr, second)
		mc.waitForShardGroup(t, second, mc.shardPrimaryTablet(t, second))
		acked, fail, lastErr := w.stop()
		assert.Zero(t, fail, "writes failed during the migration, last error: %v", lastErr)
		requireNoLostWrites(t, mc, acked)

		assert.Equal(t, gr, mc.keyspacePolicy(t))
		for _, shard := range msShards {
			assert.Empty(t, mc.shardRecord(t, shard).DurabilityPolicy, "the keyspace's policy applies to %s", shard)
		}
		mc.waitForShardGroup(t, first, mc.shardPrimaryTablet(t, first))
	})

	t.Run("migrate back the first shard, which then fails over like a semi-sync shard", func(t *testing.T) {
		mc.waitBufferCooldown(t)
		defer func() { mc.lastBuffering = time.Now() }()
		w := startIDWriter(t, mc)
		primary := mc.shardPrimaryTablet(t, first)
		mc.migrate(t, semiSync, first)
		mc.waitForSemiSyncShard(t, first, primary)
		acked, fail, lastErr := w.stop()
		assert.Zero(t, fail, "writes failed during the migration back, last error: %v", lastErr)
		requireNoLostWrites(t, mc, acked)

		assert.Equal(t, gr, mc.keyspacePolicy(t), "the keyspace keeps its policy while a shard runs its group")
		assert.Equal(t, semiSync, mc.shardRecord(t, first).DurabilityPolicy)
		assert.Empty(t, mc.shardRecord(t, first).GroupReplicationVoters)
		mc.waitForShardGroup(t, second, mc.shardPrimaryTablet(t, second))

		// The primary's host dies: VTOrc's emergency reparent takes the semi-sync path, although
		// the keyspace's policy is still Group Replication.
		mc.lastBuffering = time.Now()
		mc.waitBufferCooldown(t)
		w = startIDWriter(t, mc)
		require.Eventually(t, func() bool { return w.ackedCount() > 20 }, waitTimeout, pollInterval)
		killed := time.Now()
		killHost(t, primary)
		var newPrimary *cluster.Vttablet
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			newPrimary = mc.shardPrimaryTablet(t, first)
			assert.NotEqual(c, primary.Alias, newPrimary.Alias)
		}, waitTimeout, pollInterval)
		t.Logf("%s was the primary of %s in topo %v after the old primary's host died", newPrimary.Alias, first, time.Since(killed))
		before := w.ackedCount()
		require.Eventually(t, func() bool { return w.ackedCount() > before+20 }, waitTimeout, pollInterval)

		// The old primary's host comes back, and replicates from the new primary. No group is
		// formed again.
		primary.MysqlctlProcess.InitMysql = false
		require.NoError(t, primary.MysqlctlProcess.Start())
		require.NoError(t, primary.VttabletProcess.Setup())
		mc.waitForSemiSyncShard(t, first, newPrimary)
		acked, fail, lastErr = w.stop()
		t.Logf("%d writes acknowledged, %d failed during the failover, last error: %v", len(acked), fail, lastErr)
		requireNoLostWrites(t, mc, acked)
		assert.Empty(t, mc.shardRecord(t, first).GroupReplicationVoters)
		assert.Empty(t, mc.shardRecord(t, first).GroupReplicationIncarnation)
	})

	t.Run("migrate back the second shard: the keyspace policy switches back", func(t *testing.T) {
		mc.waitBufferCooldown(t)
		defer func() { mc.lastBuffering = time.Now() }()
		w := startIDWriter(t, mc)
		primary := mc.shardPrimaryTablet(t, second)
		mc.migrate(t, semiSync, second)
		mc.waitForSemiSyncShard(t, second, primary)
		acked, fail, lastErr := w.stop()
		assert.Zero(t, fail, "writes failed during the migration back, last error: %v", lastErr)
		requireNoLostWrites(t, mc, acked)

		assert.Equal(t, semiSync, mc.keyspacePolicy(t))
		for _, shard := range msShards {
			assert.Empty(t, mc.shardRecord(t, shard).DurabilityPolicy, "the keyspace's policy applies to %s", shard)
		}
	})
}
