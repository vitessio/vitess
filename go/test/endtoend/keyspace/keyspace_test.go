/*
Copyright 2019 The Vitess Authors.

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

package sequence

import (
	"context"
	"encoding/binary"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/topo"
	_ "vitess.io/vitess/go/vt/topo/etcd2topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	clusterForKSTest      *vitesst.Cluster
	keyspaceShardedName   = "test_ks_sharded"
	keyspaceUnshardedName = "test_ks_unsharded"
	cell                  = "zone1"
	cell2                 = "zone2"
	servedTypes           = map[topodatapb.TabletType]bool{topodatapb.TabletType_PRIMARY: true, topodatapb.TabletType_REPLICA: true, topodatapb.TabletType_RDONLY: true}
	sqlSchema             = `create table vt_insert_test (
								id bigint auto_increment,
								msg varchar(64),
								keyspace_id bigint(20) unsigned NOT NULL,
								primary key (id)
								) Engine=InnoDB`
	vSchema = `{
      "sharded": true,
      "vindexes": {
        "hash_index": {
          "type": "hash"
        }
      },
      "tables": {
        "vt_insert_test": {
           "column_vindexes": [
            {
              "column": "keyspace_id",
              "name": "hash_index"
            }
          ]
        }
      }
	}`
	shardKIdMap = map[string][]uint64{
		"-80": {
			527875958493693904, 626750931627689502,
			345387386794260318, 332484755310826578,
			1842642426274125671, 1326307661227634652,
			1761124146422844620, 1661669973250483744,
			3361397649937244239, 2444880764308344533,
		},
		"80-": {
			9767889778372766922, 9742070682920810358,
			10296850775085416642, 9537430901666854108,
			10440455099304929791, 11454183276974683945,
			11185910247776122031, 10460396697869122981,
			13379616110062597001, 12826553979133932576,
		},
	}
)

func setup(t *testing.T) {
	t.Helper()
	ctx := t.Context()

	cluster, err := vitesst.NewCluster(
		vitesst.WithCells(cell, cell2),
		vitesst.WithKeyspace(keyspaceShardedName).
			WithShardNames("-80", "80-").
			WithReplicas(1).
			WithSchema(sqlSchema).
			WithVSchema(vSchema),
		vitesst.WithKeyspace(keyspaceUnshardedName).
			WithShardNames(keyspaceUnshardedName).
			WithReplicas(1).
			WithSchema(sqlSchema),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(ctx)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
		defer cancel()
		if t.Failed() {
			cluster.DumpDiagnostics(cleanupCtx, t.Logf)
		}
		if err := cleanup(cleanupCtx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})
	require.NoError(t, err)

	clusterForKSTest = cluster
	require.NoError(t, cluster.Vtctld().ExecuteCommand(ctx, "RebuildKeyspaceGraph", keyspaceShardedName))
	require.NoError(t, cluster.Vtctld().ExecuteCommand(ctx, "RebuildKeyspaceGraph", keyspaceUnshardedName))
}

// TestDurabilityPolicyField tests that the DurabilityPolicy field of a keyspace can be set during creation, read and updated later
// from vtctld server and the vtctl binary
func TestDurabilityPolicyField(t *testing.T) {
	setup(t)
	ctx := t.Context()

	out, err := clusterForKSTest.Vtctld().ExecuteCommandWithOutput(ctx, "CreateKeyspace", "ks_durability", "--durability-policy=semi_sync")
	require.NoError(t, err, out)
	checkDurabilityPolicy(t, policy.DurabilitySemiSync)

	out, err = clusterForKSTest.Vtctld().ExecuteCommandWithOutput(ctx, "SetKeyspaceDurabilityPolicy", "ks_durability", "--durability-policy=none")
	require.NoError(t, err, out)
	checkDurabilityPolicy(t, policy.DurabilityNone)

	out, err = clusterForKSTest.Vtctld().ExecuteCommandWithOutput(ctx, "DeleteKeyspace", "ks_durability")
	require.NoError(t, err, out)

	out, err = clusterForKSTest.Vtctld().ExecuteCommandWithOutput(ctx, "CreateKeyspace", "--durability-policy=semi_sync", "ks_durability")
	require.NoError(t, err, out)
	checkDurabilityPolicy(t, policy.DurabilitySemiSync)

	out, err = clusterForKSTest.Vtctld().ExecuteCommandWithOutput(ctx, "DeleteKeyspace", "ks_durability")
	require.NoError(t, err, out)
}

func checkDurabilityPolicy(t *testing.T, durabilityPolicy string) {
	ks, err := getKeyspace(t, "ks_durability")
	require.NoError(t, err)
	require.Equal(t, ks.Keyspace.DurabilityPolicy, durabilityPolicy)
}

func TestGetSrvKeyspaceNames(t *testing.T) {
	setup(t)
	data, err := clusterForKSTest.Vtctld().ExecuteCommandWithOutput(t.Context(), "GetSrvKeyspaceNames", cell)
	require.Nil(t, err)

	namesByCell := map[string]*vtctldatapb.GetSrvKeyspaceNamesResponse_NameList{}
	err = json2.Unmarshal([]byte(data), &namesByCell)
	require.NoError(t, err)

	assert.Contains(t, namesByCell[cell].Names, keyspaceUnshardedName)
	assert.Contains(t, namesByCell[cell].Names, keyspaceShardedName)
}

func TestGetSrvKeyspacePartitions(t *testing.T) {
	setup(t)
	shardedSrvKeyspace := getSrvKeyspace(t, cell, keyspaceShardedName)
	otherShardRefFound := false
	for _, partition := range shardedSrvKeyspace.Partitions {
		if servedTypes[partition.ServedType] {
			for _, shardRef := range partition.ShardReferences {
				assert.True(t, shardRef.Name == "-80" || shardRef.Name == "80-")
			}
		} else {
			otherShardRefFound = true
		}
	}
	assert.True(t, !otherShardRefFound)

	unShardedSrvKeyspace := getSrvKeyspace(t, cell, keyspaceUnshardedName)
	otherShardRefFound = false
	for _, partition := range unShardedSrvKeyspace.Partitions {
		if servedTypes[partition.ServedType] {
			for _, shardRef := range partition.ShardReferences {
				assert.True(t, shardRef.Name == keyspaceUnshardedName)
			}
		} else {
			otherShardRefFound = true
		}
	}
	assert.True(t, !otherShardRefFound)
}

func TestShardNames(t *testing.T) {
	setup(t)
	output, err := getSrvKeyspaces(t, keyspaceShardedName, cell)
	require.NoError(t, err)
	require.NotNil(t, output[cell], "no srvkeyspace for cell %s", cell)
}

func TestGetKeyspace(t *testing.T) {
	setup(t)
	_, err := getKeyspace(t, keyspaceUnshardedName)
	require.Nil(t, err)
}

func TestDeleteKeyspace(t *testing.T) {
	setup(t)
	ctx := t.Context()
	vtctld := clusterForKSTest.Vtctld()

	_ = vtctld.ExecuteCommand(ctx, "CreateKeyspace", "test_delete_keyspace", "--sidecar-db-name", sidecar.DefaultName)
	_ = vtctld.ExecuteCommand(ctx, "CreateShard", "test_delete_keyspace/0")
	_ = initTablet(ctx, cell, 999, topodatapb.TabletType_PRIMARY, 0, "test_delete_keyspace", "0")

	// Can't delete keyspace if there are shards present.
	err := vtctld.ExecuteCommand(ctx, "DeleteKeyspace", "test_delete_keyspace")
	require.Error(t, err)

	// Can't delete shard if there are tablets present.
	err = vtctld.ExecuteCommand(ctx, "DeleteShards", "--even-if-serving", "test_delete_keyspace/0")
	require.Error(t, err)

	// Use recursive DeleteShard to remove tablets.
	_ = vtctld.ExecuteCommand(ctx, "DeleteShards", "--even-if-serving", "--recursive", "test_delete_keyspace/0")
	// Now non-recursive DeleteKeyspace should work.
	_ = vtctld.ExecuteCommand(ctx, "DeleteKeyspace", "test_delete_keyspace")

	// Start over and this time use recursive DeleteKeyspace to do everything.
	_ = vtctld.ExecuteCommand(ctx, "CreateKeyspace", "test_delete_keyspace", "--sidecar-db-name", sidecar.DefaultName)
	_ = vtctld.ExecuteCommand(ctx, "CreateShard", "test_delete_keyspace/0")
	_ = initTablet(ctx, cell, 999, topodatapb.TabletType_PRIMARY, 1234, "test_delete_keyspace", "0")

	// Create the serving/replication entries and check that they exist,
	//  so we can later check they're deleted.
	_ = vtctld.ExecuteCommand(ctx, "RebuildKeyspaceGraph", "test_delete_keyspace")
	_, _ = vtctld.ExecuteCommandWithOutput(ctx, "GetShardReplication", "test_delete_keyspace/0", cell)
	_ = vtctld.ExecuteCommand(ctx, "GetSrvKeyspace", cell, "test_delete_keyspace")

	// Recursive DeleteKeyspace
	_ = vtctld.ExecuteCommand(ctx, "DeleteKeyspace", "--recursive", "test_delete_keyspace")

	// Check that everything is gone.
	err = vtctld.ExecuteCommand(ctx, "GetKeyspace", "test_delete_keyspace")
	require.Error(t, err)
	err = vtctld.ExecuteCommand(ctx, "GetShard", "test_delete_keyspace/0")
	require.Error(t, err)
	err = vtctld.ExecuteCommand(ctx, "GetTablet", "zone1-0000000999")
	require.Error(t, err)
	_, err = vtctld.ExecuteCommandWithOutput(ctx, "GetShardReplication", "test_delete_keyspace/0", cell)
	require.Error(t, err)
	err = vtctld.ExecuteCommand(ctx, "GetSrvKeyspace", cell, "test_delete_keyspace")
	require.Error(t, err)
	ksMap, err := getSrvKeyspaces(t, "test_delete_keyspace", cell)
	require.NoError(t, err)
	require.Empty(t, ksMap[cell])
}

func TestShardCountForAllKeyspaces(t *testing.T) {
	setup(t)
	testShardCountForKeyspace(t, keyspaceUnshardedName, 1)
	testShardCountForKeyspace(t, keyspaceShardedName, 2)
}

func testShardCountForKeyspace(t *testing.T, keyspace string, count int) {
	srvKeyspace := getSrvKeyspace(t, cell, keyspace)

	// for each served type PRIMARY REPLICA RDONLY, the shard ref count should match
	for _, partition := range srvKeyspace.Partitions {
		if servedTypes[partition.ServedType] {
			assert.Equal(t, len(partition.ShardReferences), count)
		}
	}
}

func TestShardNameForAllKeyspaces(t *testing.T) {
	setup(t)
	testShardNameForKeyspace(t, keyspaceUnshardedName, []string{"test_ks_unsharded"})
	testShardNameForKeyspace(t, keyspaceShardedName, []string{"-80", "80-"})
}

func testShardNameForKeyspace(t *testing.T, keyspace string, shardNames []string) {
	srvKeyspace := getSrvKeyspace(t, cell, keyspace)

	// for each served type PRIMARY REPLICA RDONLY, the shard ref count should match
	for _, partition := range srvKeyspace.Partitions {
		if servedTypes[partition.ServedType] {
			for _, shardRef := range partition.ShardReferences {
				assert.Contains(t, shardNames, shardRef.Name)
			}
		}
	}
}

func TestKeyspaceToShardName(t *testing.T) {
	setup(t)
	var id []byte
	srvKeyspace := getSrvKeyspace(t, cell, keyspaceShardedName)

	// for each served type PRIMARY REPLICA RDONLY, the shard ref count should match
	for _, partition := range srvKeyspace.Partitions {
		if partition.ServedType == topodatapb.TabletType_PRIMARY {
			for _, shardRef := range partition.ShardReferences {
				shardKIDs := shardKIdMap[shardRef.Name]
				for _, kid := range shardKIDs {
					id = packKeyspaceID(kid)
					assert.True(t, key.Compare(shardRef.KeyRange.Start, id) <= 0 &&
						(key.Empty(shardRef.KeyRange.End) || key.Compare(id, shardRef.KeyRange.End) < 0))
				}
			}
		}
	}

	srvKeyspace = getSrvKeyspace(t, cell, keyspaceUnshardedName)

	for _, partition := range srvKeyspace.Partitions {
		if partition.ServedType == topodatapb.TabletType_PRIMARY {
			for _, shardRef := range partition.ShardReferences {
				assert.Equal(t, shardRef.Name, keyspaceUnshardedName)
			}
		}
	}
}

// packKeyspaceID packs this into big-endian and returns byte[] to do a byte-wise comparison.
func packKeyspaceID(keyspaceID uint64) []byte {
	var keybytes [8]byte
	binary.BigEndian.PutUint64(keybytes[:], keyspaceID)
	return keybytes[:]
}

func getSrvKeyspace(t *testing.T, cell string, ksname string) *topodatapb.SrvKeyspace {
	output, err := getSrvKeyspaces(t, ksname, cell)
	require.NoError(t, err)

	srvKeyspace := output[cell]
	require.NotNil(t, srvKeyspace, "no srvkeyspace for cell %s", cell)

	return srvKeyspace
}

// getSrvKeyspaces returns a mapping of cell to srv keyspace for the given keyspace.
func getSrvKeyspaces(t *testing.T, keyspace string, cells ...string) (map[string]*topodatapb.SrvKeyspace, error) {
	args := append([]string{"GetSrvKeyspaces", keyspace}, cells...)
	out, err := clusterForKSTest.Vtctld().ExecuteCommandWithOutput(t.Context(), args...)
	if err != nil {
		return nil, err
	}

	ksMap := map[string]*topodatapb.SrvKeyspace{}
	err = json2.Unmarshal([]byte(out), &ksMap)
	return ksMap, err
}

// getKeyspace fetches a keyspace record and parses the response.
func getKeyspace(t *testing.T, keyspace string) (*vtctldatapb.Keyspace, error) {
	data, err := clusterForKSTest.Vtctld().ExecuteCommandWithOutput(t.Context(), "GetKeyspace", keyspace)
	if err != nil {
		return nil, err
	}

	var ks vtctldatapb.Keyspace
	if err := json2.UnmarshalPB([]byte(data), &ks); err != nil {
		return nil, err
	}
	return &ks, nil
}

// initTablet creates a bare tablet record in the topology server, without a
// running mysqld or vttablet. The record and its shard replication entry live
// in the cell's own topo root, which the Vitess components reach through the
// in-network cell address. That address is not reachable from the host, so the
// shared etcd is opened at the cell's data root directly and the records are
// written there.
func initTablet(ctx context.Context, cell string, uid uint32, tabletType topodatapb.TabletType, httpPort int32, keyspace, shard string) error {
	etcdAddr, err := clusterForKSTest.EtcdAddr(ctx)
	if err != nil {
		return err
	}
	ts, err := topo.OpenServer("etcd2", etcdAddr, "/vitess/"+cell)
	if err != nil {
		return err
	}
	defer ts.Close()

	conn, err := ts.ConnForCell(ctx, topo.GlobalCell)
	if err != nil {
		return err
	}

	alias := &topodatapb.TabletAlias{
		Cell: cell,
		Uid:  uid,
	}
	tablet := &topodatapb.Tablet{
		Alias:    alias,
		Hostname: "localhost",
		Type:     tabletType,
		PortMap: map[string]int32{
			"vt": httpPort,
		},
		Keyspace: keyspace,
		Shard:    shard,
	}

	data, err := tablet.MarshalVT()
	if err != nil {
		return err
	}
	tabletPath := path.Join(topo.TabletsPath, topoproto.TabletAliasString(alias), topo.TabletFile)
	if _, err := conn.Create(ctx, tabletPath, data); err != nil {
		return err
	}

	return ts.UpdateShardReplicationFields(ctx, topo.GlobalCell, keyspace, shard,
		func(sr *topodatapb.ShardReplication) error {
			sr.Nodes = append(sr.Nodes, &topodatapb.ShardReplication_Node{TabletAlias: alias})
			return nil
		})
}
