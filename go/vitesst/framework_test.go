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

// Framework self-tests exercise supervisor lifecycle, restart handles, and
// comparison MySQL against real Docker containers. Run them with:
//
//	make vitesst_images && go test -count=1 -v ./go/vitesst/
package vitesst_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vitesst"
)

const selfTestSchema = `create table t1(
	id bigint not null,
	val varchar(64),
	primary key(id)
) Engine=InnoDB;`

const selfTestVSchema = `{
	"sharded": true,
	"vindexes": {"hash": {"type": "hash"}},
	"tables": {"t1": {"column_vindexes": [{"column": "id", "name": "hash"}]}}
}`

func TestClusterBootstrap(t *testing.T) {
	t.Parallel()

	c, err := vitesst.NewCluster(t,
		vitesst.WithCells("zone1", "zone2"),
		vitesst.WithKeyspace("uks").
			WithReplicas(1).
			WithSchema(selfTestSchema),
		vitesst.WithKeyspace("sks").
			WithShards(2).
			WithSchema(selfTestSchema).
			WithVSchema(selfTestVSchema),
	)
	require.NoError(t, err)
	cleanup, err := c.Start(t, t.Context())
	t.Cleanup(func() {
		ctx := context.WithoutCancel(t.Context())
		if err := cleanup(ctx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})
	require.NoError(t, err)

	conn := c.Connect(t)
	defer conn.Close()

	// The vtgate advertises the image's MySQL version.
	assert.Equal(t, c.MySQLVersion()+"-vitess", conn.ServerVersion)

	// Unsharded keyspace serves reads and writes through vtgate.
	_, err = conn.ExecuteFetch("insert into uks.t1(id, val) values (1, 'one')", 1, false)
	require.NoError(t, err)
	qr, err := conn.ExecuteFetch("select val from uks.t1 where id = 1", 1, false)
	require.NoError(t, err)
	require.Len(t, qr.Rows, 1)
	assert.Equal(t, "one", qr.Rows[0][0].ToString())

	// Sharded keyspace routes by the hash vindex.
	for i := 1; i <= 10; i++ {
		_, err := conn.ExecuteFetch(fmt.Sprintf("insert into sks.t1(id, val) values (%d, 'row')", i), 1, false)
		require.NoError(t, err)
	}
	qr, err = conn.ExecuteFetch("select count(*) from sks.t1", 1, false)
	require.NoError(t, err)
	assert.Equal(t, "10", qr.Rows[0][0].ToString())

	// Runtime accessors reflect the topology.
	uks := c.Keyspace("uks")
	require.NotNil(t, uks)
	require.Len(t, uks.Shards(), 1)
	unshardedShard := uks.Shard("-")
	require.NotNil(t, unshardedShard)
	require.NotNil(t, unshardedShard.Primary())
	assert.Len(t, unshardedShard.Replicas(), 1)
	assert.Empty(t, unshardedShard.RDOnly())

	sks := c.Keyspace("sks")
	require.NotNil(t, sks)
	shards := sks.Shards()
	require.Len(t, shards, 2)
	assert.Equal(t, "-80", shards[0].Name)
	assert.Equal(t, "80-", shards[1].Name)

	// The rows really live on the shard primaries' mysqlds.
	var total int64
	for _, shard := range shards {
		require.NotNil(t, shard.Primary())
		res, err := shard.Primary().QueryTablet(t.Context(), "select count(*) from t1")
		require.NoError(t, err)
		count, err := res.Rows[0][0].ToInt64()
		require.NoError(t, err)
		total += count
	}
	assert.EqualValues(t, 10, total)

	// Tablets are spread across both cells.
	cells := make(map[string]int)
	for _, tablet := range c.Tablets() {
		cells[tablet.Cell]++
	}
	assert.Positive(t, cells["zone1"])
	assert.Positive(t, cells["zone2"])

	// Component observability works.
	vars, err := c.VTGate().GetVars(t.Context())
	require.NoError(t, err)
	assert.NotEmpty(t, vars)
}

// bootstrapTopoFlavor proves a full cluster bring-up and a query round trip
// on a non-default topology flavor.
func bootstrapTopoFlavor(t *testing.T, flavor string) {
	t.Helper()

	c, err := vitesst.NewCluster(t,
		vitesst.WithTopo(flavor),
		vitesst.WithKeyspace("ks").WithSchema(selfTestSchema),
	)
	require.NoError(t, err)
	cleanup, err := c.Start(t, t.Context())
	t.Cleanup(func() {
		ctx := context.WithoutCancel(t.Context())
		if err := cleanup(ctx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})
	require.NoError(t, err)

	conn := c.Connect(t)
	defer conn.Close()
	_, err = conn.ExecuteFetch("insert into ks.t1(id, val) values (1, 'one')", 1, false)
	require.NoError(t, err)
	qr, err := conn.ExecuteFetch("select val from ks.t1 where id = 1", 1, false)
	require.NoError(t, err)
	require.Len(t, qr.Rows, 1)
	assert.Equal(t, "one", qr.Rows[0][0].ToString())
}

func TestClusterBootstrapConsul(t *testing.T) {
	t.Parallel()
	bootstrapTopoFlavor(t, "consul")
}

func TestClusterBootstrapZookeeper(t *testing.T) {
	t.Parallel()
	bootstrapTopoFlavor(t, "zk2")
}

func TestTabletProcessLifecycle(t *testing.T) {
	t.Parallel()

	c, err := vitesst.NewCluster(t,
		vitesst.WithKeyspace("ks").
			WithReplicas(1).
			WithSchema(selfTestSchema),
	)
	require.NoError(t, err)
	cleanup, err := c.Start(t, t.Context())
	t.Cleanup(func() {
		ctx := context.WithoutCancel(t.Context())
		if err := cleanup(ctx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})
	require.NoError(t, err)
	ctx := t.Context()

	replica := c.Keyspace("ks").Shard("-").Replicas()[0]

	// Stopping vttablet leaves mysqld and the container running.
	require.NoError(t, replica.StopVttablet(ctx))
	_, _, err = replica.MakeAPICall(ctx, "/debug/vars")
	assert.Error(t, err, "vttablet HTTP endpoint should be down after StopVttablet")
	_, err = replica.QueryTablet(ctx, "select 1")
	assert.NoError(t, err, "mysqld should stay up after StopVttablet")
	assert.True(t, replica.IsRunning(), "container should stay up after StopVttablet")

	// Restarting with new flags applies them, with data intact.
	require.NoError(t, replica.StartVttablet(ctx, "--queryserver-config-pool-size", "13"))
	vars, err := replica.GetVars(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 13, vars["ConnPoolCapacity"], "restart should apply the new flag")

	// A killed vttablet stays dead until restarted.
	require.NoError(t, replica.KillVttablet(ctx))
	_, _, err = replica.MakeAPICall(ctx, "/debug/vars")
	assert.Error(t, err, "vttablet HTTP endpoint should be down after KillVttablet")
	require.NoError(t, replica.StartVttablet(ctx))

	// mysqld can be stopped and started under the live vttablet.
	require.NoError(t, replica.StopMySQL(ctx))
	_, err = replica.QueryTablet(ctx, "select 1")
	assert.Error(t, err, "mysqld should be down after StopMySQL")
	require.NoError(t, replica.StartMySQL(ctx))
	assert.Eventually(t, func() bool {
		_, err := replica.QueryTablet(ctx, "select 1")
		return err == nil
	}, 60*time.Second, 500*time.Millisecond, "mysqld should accept queries again after StartMySQL")

	// mysqld crash and recovery.
	require.NoError(t, replica.KillMySQL(ctx))
	assert.Eventually(t, func() bool {
		_, err := replica.QueryTablet(ctx, "select 1")
		return err != nil
	}, 30*time.Second, 500*time.Millisecond, "mysqld should be down after KillMySQL")
	require.NoError(t, replica.StartMySQL(ctx))
	assert.Eventually(t, func() bool {
		_, err := replica.QueryTablet(ctx, "select 1")
		return err == nil
	}, 60*time.Second, 500*time.Millisecond, "mysqld should recover after KillMySQL + StartMySQL")
}

// TestAddShard proves that resharding targets can join a keyspace on a
// running cluster: new shards get their own tablets, an elected primary, and
// the keyspace's schema, while the source shard keeps serving.
func TestAddShard(t *testing.T) {
	t.Parallel()

	c, err := vitesst.NewCluster(t,
		vitesst.WithKeyspace("ks").
			WithShardNames("0").
			WithSchema(selfTestSchema).
			WithVSchema(selfTestVSchema),
	)
	require.NoError(t, err)
	cleanup, err := c.Start(t, t.Context())
	t.Cleanup(func() {
		ctx := context.WithoutCancel(t.Context())
		if err := cleanup(ctx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})
	require.NoError(t, err)

	ctx := t.Context()
	conn := c.Connect(t)
	defer conn.Close()
	_, err = conn.ExecuteFetch("insert into ks.t1(id, val) values (1, 'source')", 1, false)
	require.NoError(t, err)

	var targets []*vitesst.Shard
	for _, name := range []string{"-80", "80-"} {
		shard, err := c.AddShard(t, ctx, "ks", name, 1, 0)
		require.NoError(t, err)
		require.NotNil(t, shard.Primary(), "shard %s should have an elected primary", name)
		require.Len(t, shard.Replicas(), 1)
		targets = append(targets, shard)
	}
	assert.Len(t, c.Keyspace("ks").Shards(), 3)

	// A keyspace-wide DDL reaches every shard, including the new ones.
	require.NoError(t, c.Vtctld().ExecuteCommand(ctx,
		"ApplySchema", "--sql", "create table t2(id bigint primary key) Engine=InnoDB", "ks"))
	for _, shard := range targets {
		_, err = shard.Primary().QueryTablet(ctx, "select count(*) from t2")
		require.NoError(t, err, "shard %s should carry the new table", shard.Name)
	}

	// The source shard keeps serving through vtgate.
	qr, err := conn.ExecuteFetch("select val from ks.t1 where id = 1", 1, false)
	require.NoError(t, err)
	require.Len(t, qr.Rows, 1)
	assert.Equal(t, "source", qr.Rows[0][0].ToString())
}

// TestBackupCycle proves the shared backup volume: vtctld takes a backup from
// a replica, vtbackup takes another, both are listed, and a replica restores
// from them.
func TestBackupCycle(t *testing.T) {
	t.Parallel()

	c, err := vitesst.NewCluster(t,
		vitesst.WithBackupStorage(),
		vitesst.WithKeyspace("ks").
			WithReplicas(1).
			WithSchema(selfTestSchema),
	)
	require.NoError(t, err)
	cleanup, err := c.Start(t, t.Context())
	t.Cleanup(func() {
		ctx := context.WithoutCancel(t.Context())
		if err := cleanup(ctx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})
	require.NoError(t, err)

	ctx := t.Context()
	conn := c.Connect(t)
	defer conn.Close()
	_, err = conn.ExecuteFetch("insert into ks.t1(id, val) values (1, 'backed-up')", 1, false)
	require.NoError(t, err)

	shard := c.Keyspace("ks").Shard("-")
	replica := shard.Replicas()[0]

	// vtctld takes a backup of the replica.
	require.NoError(t, c.Vtctld().ExecuteCommand(ctx, "Backup", replica.Alias()))
	backups, err := c.ListBackups(ctx, "ks", "-")
	require.NoError(t, err)
	require.Len(t, backups, 1, "vtctld Backup should store one backup")

	// vtbackup takes another one, reading the same volume.
	_, err = c.RunVtbackup(t, ctx, vitesst.VtbackupSpec{Keyspace: "ks", Shard: "-"})
	require.NoError(t, err)
	backups, err = c.ListBackups(ctx, "ks", "-")
	require.NoError(t, err)
	assert.Len(t, backups, 2, "vtbackup should store a second backup")

	// Killing the container discards the tablet's tmpfs data, so its restart
	// initializes a fresh mysqld and vttablet restores from the backup.
	require.NoError(t, replica.KillContainer(ctx))
	require.NoError(t, replica.StartContainer(ctx))

	assert.Eventually(t, func() bool {
		res, err := replica.QueryTablet(ctx, "select val from t1 where id = 1")
		return err == nil && len(res.Rows) == 1 && res.Rows[0][0].ToString() == "backed-up"
	}, 3*time.Minute, 2*time.Second, "replica should serve the restored row")

	require.NoError(t, c.RemoveAllBackups(ctx, "ks", "-"))
	backups, err = c.ListBackups(ctx, "ks", "-")
	require.NoError(t, err)
	assert.Empty(t, backups)
}

func TestVTGateRestart(t *testing.T) {
	t.Parallel()

	c, err := vitesst.NewCluster(t,
		vitesst.WithKeyspace("ks").WithSchema(selfTestSchema),
	)
	require.NoError(t, err)
	cleanup, err := c.Start(t, t.Context())
	t.Cleanup(func() {
		ctx := context.WithoutCancel(t.Context())
		if err := cleanup(ctx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})
	require.NoError(t, err)

	conn := c.Connect(t)
	_, err = conn.ExecuteFetch("insert into ks.t1(id, val) values (7, 'before-restart')", 1, false)
	require.NoError(t, err)
	conn.Close()

	require.NoError(t, c.VTGate().Restart(t, t.Context(), "--mysql-server-version", "9.9.9-vitesst-test"))

	// A fresh connection re-resolves the new mapped port, sees the new flag,
	// and the data written before the restart.
	conn = c.Connect(t)
	defer conn.Close()
	assert.Equal(t, "9.9.9-vitesst-test", conn.ServerVersion, "restart should apply the new flag")
	qr, err := conn.ExecuteFetch("select val from ks.t1 where id = 7", 1, false)
	require.NoError(t, err)
	require.Len(t, qr.Rows, 1)
	assert.Equal(t, "before-restart", qr.Rows[0][0].ToString())
}

func TestNewMySQLComparison(t *testing.T) {
	t.Parallel()

	c, err := vitesst.NewCluster(t,
		vitesst.WithKeyspace("ks").WithSchema(selfTestSchema),
	)
	require.NoError(t, err)
	cleanup, err := c.Start(t, t.Context())
	t.Cleanup(func() {
		ctx := context.WithoutCancel(t.Context())
		if err := cleanup(ctx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})
	require.NoError(t, err)

	mysqlParams, mysqlCleanup, err := vitesst.NewMySQL(t, t.Context(), c, "ks", selfTestSchema)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := mysqlCleanup(context.WithoutCancel(t.Context())); err != nil {
			t.Logf("comparison mysqld teardown: %v", err)
		}
	})

	mysqlConn, err := mysql.Connect(t.Context(), &mysqlParams)
	require.NoError(t, err)
	defer mysqlConn.Close()
	_, err = mysqlConn.ExecuteFetch("select 1", 1, false)
	require.NoError(t, err)

	// The ported MySQLCompare harness runs the same statement on both sides
	// and compares results.
	mcmp, err := vitesst.NewMySQLCompare(t.Context(), t, c.VTParams(t.Context(), "ks"), mysqlParams)
	require.NoError(t, err)
	defer mcmp.Close()

	mcmp.Exec("insert into t1(id, val) values (42, 'both')")
	mcmp.AssertMatches("select id, val from t1 where id = 42", `[[INT64(42) VARCHAR("both")]]`)
}
