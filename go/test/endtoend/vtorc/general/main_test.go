/*
Copyright 2021 The Vitess Authors.

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

package general

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/topo"
	tmc "vitess.io/vitess/go/vt/vttablet/grpctmclient"

	// Register the etcd2 topo implementation for direct topo access.
	_ "vitess.io/vitess/go/vt/topo/etcd2topo"
	// Register the grpc queryservice dialer so StreamHealth works.
	_ "vitess.io/vitess/go/vt/vttablet/grpctabletconn"
)

const (
	keyspaceName    = "ks"
	shardName       = "0"
	cell1           = "zone1"
	cell2           = "zone2"
	tabletMySQLPort = 3306
)

// vtorcCluster bundles a vitesst cluster with the bookkeeping the VTOrc tests
// need: the shard's tablet pools, a host-side topo client, and the currently
// running VTOrc instances.
type vtorcCluster struct {
	cluster  *vitesst.Cluster
	etcdAddr string
	ts       *topo.Server

	replicaPool []*vitesst.Tablet
	rdonlyPool  []*vitesst.Tablet

	active        []*vitesst.Tablet
	vtorcs        []*vitesst.VTOrc
	lastUsedValue int
	tmClient      *tmc.Client

	cleanup func(context.Context) error
}

func setupVtorcCluster(t *testing.T) *vtorcCluster {
	t.Helper()

	vc, err := createClusterAndStartTopo(t.Context())
	if vc != nil {
		vc.tmClient = tmc.NewClient()
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
			defer cancel()

			printVTOrcLogsOnFailure(t, ctx, vc)
			vc.tmClient.Close()
			vc.teardown(t, ctx)
		})
	}
	require.NoError(t, err)
	return vc
}

// createClusterAndStartTopo starts a cluster: one keyspace with four
// replica tablets and one rdonly tablet in zone1, with a second empty cell
// registered, and no primary elected.
func createClusterAndStartTopo(ctx context.Context) (*vtorcCluster, error) {
	nextUID := 100
	keyspace := vitesst.WithKeyspace(keyspaceName).
		WithShardNames(shardName).
		WithReplicas(3).
		WithRDOnly(1).
		WithoutPrimaryElection().
		WithTabletSpec(func(spec *vitesst.TabletSpec) {
			spec.Cell = cell1
			spec.UID = nextUID
			nextUID++
		})

	cluster, err := vitesst.NewCluster(
		vitesst.WithCells(cell1, cell2),
		vitesst.WithoutVTGate(),
		vitesst.WithVTTabletArgs("--lock-tables-timeout", "5s"),
		keyspace,
	)
	if err != nil {
		return nil, err
	}

	cleanup, err := cluster.Start(ctx)
	vc := &vtorcCluster{cluster: cluster, cleanup: cleanup, lastUsedValue: 100}
	if err != nil {
		return vc, err
	}

	vc.etcdAddr, err = cluster.EtcdAddr(ctx)
	if err != nil {
		return vc, err
	}
	vc.ts, err = topo.OpenServer("etcd2", vc.etcdAddr, "/vitess/global")
	if err != nil {
		return vc, err
	}

	shard := cluster.Keyspace(keyspaceName).Shard(shardName)
	vc.replicaPool = shard.Replicas()
	vc.rdonlyPool = shard.RDOnly()
	vc.active = append(append([]*vitesst.Tablet{}, vc.replicaPool...), vc.rdonlyPool...)
	return vc, nil
}

func (vc *vtorcCluster) teardown(t *testing.T, ctx context.Context) {
	t.Helper()
	vc.stopVTOrcs(ctx)
	if vc.ts != nil {
		vc.ts.Close()
	}
	if vc.cleanup != nil {
		if err := vc.cleanup(ctx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	}
}

// setupVttabletsAndVTOrcs resets the shard to the requested number of replica
// and rdonly tablets, sets the durability policy, and starts the requested
// number of VTOrcs, letting them elect a primary.
func (vc *vtorcCluster) setupVttabletsAndVTOrcs(t *testing.T, numReplicas, numRdonly int, orcExtraArgs []string, preventCrossCell bool, vtorcCount int, durability string) {
	t.Helper()
	ctx := t.Context()

	if vtorcCount == 0 {
		vtorcCount = 1
	}

	vc.stopVTOrcs(ctx)

	require.LessOrEqual(t, numReplicas, len(vc.replicaPool), "more than available replica tablets requested")
	require.LessOrEqual(t, numRdonly, len(vc.rdonlyPool), "more than available rdonly tablets requested")

	// Reset the shard primary before removing tablet records so DeleteTablets
	// does not refuse to remove the previously elected primary.
	vc.resetShardPrimary(t)
	for _, tablet := range append(append([]*vitesst.Tablet{}, vc.replicaPool...), vc.rdonlyPool...) {
		vc.deactivateTablet(ctx, tablet)
	}
	vc.active = nil

	for _, tablet := range vc.replicaPool[:numReplicas] {
		vc.activateTablet(t, tablet)
	}
	for _, tablet := range vc.rdonlyPool[:numRdonly] {
		vc.activateTablet(t, tablet)
	}

	for _, tablet := range vc.active {
		require.NoError(t, tablet.WaitForTabletStatus(ctx, 30*time.Second, "SERVING", "NOT_SERVING"))
	}
	for _, tablet := range vc.active {
		require.NoError(t, tablet.WaitForTabletType(ctx, 30*time.Second, "replica", "rdonly"))
	}

	if durability == "" {
		durability = "none"
	}
	out, err := vc.cluster.Vtctld().ExecuteCommandWithOutput(ctx, "SetKeyspaceDurabilityPolicy", keyspaceName, "--durability-policy="+durability)
	require.NoError(t, err, out)

	// VTOrc also reads the shard record, so clear the primary again.
	_, err = vc.ts.UpdateShardFields(ctx, keyspaceName, shardName, func(info *topo.ShardInfo) error {
		info.PrimaryTermStartTime = nil
		info.PrimaryAlias = nil
		return nil
	})
	require.NoError(t, err)

	vc.startVTOrcs(t, orcExtraArgs, preventCrossCell, vtorcCount)
}

// resetShardPrimary clears the shard's primary alias in the global topo.
func (vc *vtorcCluster) resetShardPrimary(t *testing.T) {
	t.Helper()
	_, err := vc.ts.UpdateShardFields(t.Context(), keyspaceName, shardName, func(si *topo.ShardInfo) error {
		si.PrimaryAlias = nil
		return nil
	})
	require.NoError(t, err)
}

// deactivateTablet stops the tablet's vttablet, removes its topology record,
// and detaches its mysqld from replication so the still-running mysqld does not
// linger as a replica without a tablet record. Every step is best-effort so an
// already-stopped, already-deleted, or unreachable tablet is a no-op.
func (vc *vtorcCluster) deactivateTablet(ctx context.Context, tablet *vitesst.Tablet) {
	_ = tablet.StopVttablet(ctx)
	_ = vc.cluster.Vtctld().ExecuteCommand(ctx, "DeleteTablets", tablet.Alias())
	if conn := connectTablet(ctx, tablet, ""); conn != nil {
		// Disable semi-sync so the later DROP DATABASE during activation does
		// not block forever waiting for acks when this mysqld was a semi-sync
		// primary with no connected acker.
		_, _ = conn.ExecuteFetch("SET GLOBAL rpl_semi_sync_source_enabled = 0, GLOBAL rpl_semi_sync_replica_enabled = 0", 1, false)
		_, _ = conn.ExecuteFetch("STOP REPLICA", 1, false)
		_, _ = conn.ExecuteFetch("RESET REPLICA ALL", 1, false)
		conn.Close()
	}
}

// activateTablet cleans the tablet's mysqld and starts its vttablet, which
// re-registers the tablet in the topology.
func (vc *vtorcCluster) activateTablet(t *testing.T, tablet *vitesst.Tablet) {
	t.Helper()
	ctx := t.Context()

	_, err := runSQL(ctx, t, "SET GLOBAL super_read_only = OFF", tablet, "")
	require.NoError(t, err)
	_, err = runSQL(ctx, t, "DROP DATABASE IF EXISTS vt_ks", tablet, "")
	require.NoError(t, err)
	_, err = runSQL(ctx, t, "DROP DATABASE IF EXISTS _vt", tablet, "")
	require.NoError(t, err)
	_, err = runSQL(ctx, t, "STOP REPLICA", tablet, "")
	require.NoError(t, err)
	resetCmd, err := resetBinaryLogsCommand(ctx, tablet)
	require.NoError(t, err)
	_, err = runSQL(ctx, t, resetCmd, tablet, "")
	require.NoError(t, err)
	_, err = runSQL(ctx, t, "SET GLOBAL read_only = ON", tablet, "")
	require.NoError(t, err)

	require.NoError(t, tablet.StartVttablet(ctx))
	vc.active = append(vc.active, tablet)
}

// startVTOrcs starts the requested number of VTOrc instances in zone1.
func (vc *vtorcCluster) startVTOrcs(t *testing.T, orcExtraArgs []string, preventCrossCell bool, count int) {
	t.Helper()
	ctx := t.Context()

	args := append([]string{}, orcExtraArgs...)
	if preventCrossCell {
		args = append(args, "--prevent-cross-cell-failover")
	}
	for range count {
		vtorc, err := vc.cluster.AddVTOrc(ctx, cell1, args...)
		require.NoError(t, err)
		vc.vtorcs = append(vc.vtorcs, vtorc)
	}
}

// stopVTOrcs stops all running VTOrc instances.
func (vc *vtorcCluster) stopVTOrcs(ctx context.Context) {
	for _, vtorc := range vc.vtorcs {
		_ = vtorc.StopContainer(ctx, 10*time.Second)
	}
	vc.vtorcs = nil
}

// setupNewClusterSemiSync sets up a separate cluster with three replica tablets
// and one rdonly tablet, with semi-sync durability and no primary elected.
func setupNewClusterSemiSync(t *testing.T) *vtorcCluster {
	t.Helper()
	ctx := t.Context()

	nextUID := 100
	keyspace := vitesst.WithKeyspace(keyspaceName).
		WithShardNames(shardName).
		WithReplicas(2).
		WithRDOnly(1).
		WithDurabilityPolicy("semi_sync").
		WithoutPrimaryElection().
		WithTabletSpec(func(spec *vitesst.TabletSpec) {
			spec.Cell = cell1
			spec.UID = nextUID
			nextUID++
		})

	cluster, err := vitesst.NewCluster(
		vitesst.WithoutVTGate(),
		vitesst.WithVTTabletArgs("--lock-tables-timeout", "5s"),
		keyspace,
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(ctx)
	require.NoError(t, err)

	vc := &vtorcCluster{cluster: cluster, cleanup: cleanup, lastUsedValue: 100}
	vc.tmClient = tmc.NewClient()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()

		printVTOrcLogsOnFailure(t, ctx, vc)
		vc.tmClient.Close()
		vc.teardown(t, ctx)
	})
	vc.etcdAddr, err = cluster.EtcdAddr(ctx)
	require.NoError(t, err)
	vc.ts, err = topo.OpenServer("etcd2", vc.etcdAddr, "/vitess/global")
	require.NoError(t, err)

	shard := cluster.Keyspace(keyspaceName).Shard(shardName)
	vc.replicaPool = shard.Replicas()
	vc.rdonlyPool = shard.RDOnly()
	vc.active = append(append([]*vitesst.Tablet{}, vc.replicaPool...), vc.rdonlyPool...)
	return vc
}

// addSemiSyncKeyspace adds a second keyspace with three replica tablets and
// semi-sync durability to the shared cluster, without electing a primary.
func addSemiSyncKeyspace(t *testing.T, vc *vtorcCluster) *vitesst.Keyspace {
	t.Helper()
	ctx := t.Context()

	keyspaceSemiSyncName := "ks2"
	nextUID := 300
	keyspace := vitesst.WithKeyspace(keyspaceSemiSyncName).
		WithShardNames(shardName).
		WithReplicas(2).
		WithDurabilityPolicy("semi_sync").
		WithoutPrimaryElection().
		WithTabletSpec(func(spec *vitesst.TabletSpec) {
			spec.Cell = cell1
			spec.UID = nextUID
			nextUID++
		})

	ks, err := vc.cluster.AddKeyspace(ctx, keyspace)
	require.NoError(t, err)
	return ks
}
