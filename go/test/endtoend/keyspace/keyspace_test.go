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
	"encoding/binary"
	"flag"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/json2"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/key"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

var (
	clusterForKSTest      *cluster.LocalProcessCluster
	keyspaceShardedName   = "test_ks_sharded"
	keyspaceUnshardedName = "test_ks_unsharded"
	cell                  = "zone1"
	cell2                 = "zone2"
	hostname              = "localhost"
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
		"-80": {527875958493693904, 626750931627689502,
			345387386794260318, 332484755310826578,
			1842642426274125671, 1326307661227634652,
			1761124146422844620, 1661669973250483744,
			3361397649937244239, 2444880764308344533},
		"80-": {9767889778372766922, 9742070682920810358,
			10296850775085416642, 9537430901666854108,
			10440455099304929791, 11454183276974683945,
			11185910247776122031, 10460396697869122981,
			13379616110062597001, 12826553979133932576},
	}
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterForKSTest = cluster.NewCluster(cell, hostname)
		defer clusterForKSTest.Teardown()

		// Start topo server
		if err := clusterForKSTest.StartTopo(); err != nil {
			return 1
		}

		if err := clusterForKSTest.TopoProcess.ManageTopoDir("mkdir", "/vitess/"+cell2); err != nil {
			return 1
		}

		if err := clusterForKSTest.VtctlProcess.AddCellInfo(cell2); err != nil {
			return 1
		}

		// Start sharded keyspace
		keyspaceSharded := &cluster.Keyspace{
			Name:      keyspaceShardedName,
			SchemaSQL: sqlSchema,
			VSchema:   vSchema,
		}
		if err := clusterForKSTest.StartKeyspace(*keyspaceSharded, []string{"-80", "80-"}, 1, false); err != nil {
			return 1
		}
		if err := clusterForKSTest.VtctldClientProcess.ExecuteCommand("RebuildKeyspaceGraph", keyspaceShardedName); err != nil {
			return 1
		}

		// Start unsharded keyspace
		keyspaceUnsharded := &cluster.Keyspace{
			Name:      keyspaceUnshardedName,
			SchemaSQL: sqlSchema,
		}
		if err := clusterForKSTest.StartKeyspace(*keyspaceUnsharded, []string{keyspaceUnshardedName}, 1, false); err != nil {
			return 1
		}
		if err := clusterForKSTest.VtctldClientProcess.ExecuteCommand("RebuildKeyspaceGraph", keyspaceUnshardedName); err != nil {
			return 1
		}

		// Start vtgate
		if err := clusterForKSTest.StartVtgate(); err != nil {
			return 1
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}

// TestDurabilityPolicyField tests that the DurabilityPolicy field of a keyspace can be set during creation, read and updated later
// from vtctld server and the vtctl binary
func TestDurabilityPolicyField(t *testing.T) {
	vtctldClientProcess := cluster.VtctldClientProcessInstance("localhost", clusterForKSTest.VtctldProcess.GrpcPort, clusterForKSTest.TmpDirectory)

	out, err := vtctldClientProcess.ExecuteCommandWithOutput("CreateKeyspace", "ks_durability", "--durability-policy=semi_sync")
	require.NoError(t, err, out)
	checkDurabilityPolicy(t, "semi_sync")

	out, err = vtctldClientProcess.ExecuteCommandWithOutput("SetKeyspaceDurabilityPolicy", "ks_durability", "--durability-policy=none")
	require.NoError(t, err, out)
	checkDurabilityPolicy(t, "none")

	out, err = vtctldClientProcess.ExecuteCommandWithOutput("DeleteKeyspace", "ks_durability")
	require.NoError(t, err, out)

	out, err = clusterForKSTest.VtctldClientProcess.ExecuteCommandWithOutput("CreateKeyspace", "--durability-policy=semi_sync", "ks_durability")
	require.NoError(t, err, out)
	checkDurabilityPolicy(t, "semi_sync")

	out, err = clusterForKSTest.VtctldClientProcess.ExecuteCommandWithOutput("DeleteKeyspace", "ks_durability")
	require.NoError(t, err, out)
}

func checkDurabilityPolicy(t *testing.T, durabilityPolicy string) {
	ks, err := clusterForKSTest.VtctldClientProcess.GetKeyspace("ks_durability")
	require.NoError(t, err)
	require.Equal(t, ks.Keyspace.DurabilityPolicy, durabilityPolicy)
}

func TestGetSrvKeyspaceNames(t *testing.T) {
	defer cluster.PanicHandler(t)
	data, err := clusterForKSTest.VtctldClientProcess.ExecuteCommandWithOutput("GetSrvKeyspaceNames", cell)
	require.Nil(t, err)

	var namesByCell = map[string]*vtctldatapb.GetSrvKeyspaceNamesResponse_NameList{}
	err = json2.Unmarshal([]byte(data), &namesByCell)
	require.NoError(t, err)

	assert.Contains(t, namesByCell[cell].Names, keyspaceUnshardedName)
	assert.Contains(t, namesByCell[cell].Names, keyspaceShardedName)
}

func TestGetSrvKeyspacePartitions(t *testing.T) {
	defer cluster.PanicHandler(t)
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
	defer cluster.PanicHandler(t)
	output, err := clusterForKSTest.VtctldClientProcess.GetSrvKeyspaces(keyspaceShardedName, cell)
	require.NoError(t, err)
	require.NotNil(t, output[cell], "no srvkeyspace for cell %s", cell)
}

func TestGetKeyspace(t *testing.T) {
	defer cluster.PanicHandler(t)
	_, err := clusterForKSTest.VtctldClientProcess.GetKeyspace(keyspaceUnshardedName)
	require.Nil(t, err)
}

func TestDeleteKeyspace(t *testing.T) {
	defer cluster.PanicHandler(t)
	_ = clusterForKSTest.VtctldClientProcess.CreateKeyspace("test_delete_keyspace", sidecar.DefaultName)
	_ = clusterForKSTest.VtctldClientProcess.ExecuteCommand("CreateShard", "test_delete_keyspace/0")
	_ = clusterForKSTest.InitTablet(&cluster.Vttablet{
		Type:      "primary",
		TabletUID: 100,
		Cell:      "zone1",
	}, "test_delete_keyspace", "0")

	// Can't delete keyspace if there are shards present.
	err := clusterForKSTest.VtctldClientProcess.ExecuteCommand("DeleteKeyspace", "test_delete_keyspace")
	require.Error(t, err)

	// Can't delete shard if there are tablets present.
	err = clusterForKSTest.VtctldClientProcess.ExecuteCommand("DeleteShards", "--even-if-serving", "test_delete_keyspace/0")
	require.Error(t, err)

	// Use recursive DeleteShard to remove tablets.
	_ = clusterForKSTest.VtctldClientProcess.ExecuteCommand("DeleteShards", "--even-if-serving", "--recursive", "test_delete_keyspace/0")
	// Now non-recursive DeleteKeyspace should work.
	_ = clusterForKSTest.VtctldClientProcess.ExecuteCommand("DeleteKeyspace", "test_delete_keyspace")

	// Start over and this time use recursive DeleteKeyspace to do everything.
	_ = clusterForKSTest.VtctldClientProcess.CreateKeyspace("test_delete_keyspace", sidecar.DefaultName)
	_ = clusterForKSTest.VtctldClientProcess.ExecuteCommand("CreateShard", "test_delete_keyspace/0")
	_ = clusterForKSTest.InitTablet(&cluster.Vttablet{
		Type:      "primary",
		TabletUID: 100,
		Cell:      "zone1",
		HTTPPort:  1234,
	}, "test_delete_keyspace", "0")

	// Create the serving/replication entries and check that they exist,
	//  so we can later check they're deleted.
	_ = clusterForKSTest.VtctldClientProcess.ExecuteCommand("RebuildKeyspaceGraph", "test_delete_keyspace")
	_, _ = clusterForKSTest.VtctldClientProcess.GetShardReplication("test_delete_keyspace", "0", cell)
	_ = clusterForKSTest.VtctldClientProcess.ExecuteCommand("GetSrvKeyspace", cell, "test_delete_keyspace")

	// Recursive DeleteKeyspace
	_ = clusterForKSTest.VtctldClientProcess.ExecuteCommand("DeleteKeyspace", "--recursive", "test_delete_keyspace")

	// Check that everything is gone.
	err = clusterForKSTest.VtctldClientProcess.ExecuteCommand("GetKeyspace", "test_delete_keyspace")
	require.Error(t, err)
	err = clusterForKSTest.VtctldClientProcess.ExecuteCommand("GetShard", "test_delete_keyspace/0")
	require.Error(t, err)
	err = clusterForKSTest.VtctldClientProcess.ExecuteCommand("GetTablet", "zone1-0000000100")
	require.Error(t, err)
	_, err = clusterForKSTest.VtctldClientProcess.GetShardReplication("test_delete_keyspace", "0", cell)
	require.Error(t, err)
	err = clusterForKSTest.VtctldClientProcess.ExecuteCommand("GetSrvKeyspace", cell, "test_delete_keyspace")
	require.Error(t, err)
	ksMap, err := clusterForKSTest.VtctldClientProcess.GetSrvKeyspaces("test_delete_keyspace", cell)
	require.NoError(t, err)
	require.Empty(t, ksMap[cell])
}

// TODO: Fix this test, not running in CI
// TODO: (ajm188) if this test gets fixed, the flags need to be updated to comply with VEP-4 as well.
// tells that in zone2 after deleting shard, there is no shard #264 and in zone1 there is only 1 #269
/*func RemoveKeyspaceCell(t *testing.T) {
	_ = clusterForKSTest.VtctlclientProcess.ExecuteCommand("CreateKeyspace", "test_delete_keyspace_removekscell")
	_ = clusterForKSTest.VtctlclientProcess.ExecuteCommand("CreateShard", "test_delete_keyspace_removekscell/0")
	_ = clusterForKSTest.VtctlclientProcess.ExecuteCommand("CreateShard", "test_delete_keyspace_removekscell/1")
	_ = clusterForKSTest.VtctlclientProcess.ExecuteCommand("InitTablet", "--port=1234", "--bind-address=127.0.0.1", "-keyspace=test_delete_keyspace_removekscell", "--shard=0", "zone1-0000000100", "primary")
	_ = clusterForKSTest.VtctlclientProcess.ExecuteCommand("InitTablet", "--port=1234", "--bind-address=127.0.0.1", "-keyspace=test_delete_keyspace_removekscell", "--shard=1", "zone1-0000000101", "primary")
	_ = clusterForKSTest.VtctlclientProcess.ExecuteCommand("InitTablet", "--port=1234", "--bind-address=127.0.0.1", "-keyspace=test_delete_keyspace_removekscell", "--shard=0", "zone2-0000000100", "replica")
	_ = clusterForKSTest.VtctlclientProcess.ExecuteCommand("InitTablet", "--port=1234", "--bind-address=127.0.0.1", "-keyspace=test_delete_keyspace_removekscell", "--shard=1", "zone2-0000000101", "replica")

	// Create the serving/replication entries and check that they exist,  so we can later check they're deleted.
	_ = clusterForKSTest.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", "test_delete_keyspace_removekscell")
	_ = clusterForKSTest.VtctlclientProcess.ExecuteCommand("GetShardReplication", "zone2", "test_delete_keyspace_removekscell/0")
	_ = clusterForKSTest.VtctlclientProcess.ExecuteCommand("GetShardReplication", "zone2", "test_delete_keyspace_removekscell/1")
	_ = clusterForKSTest.VtctlclientProcess.ExecuteCommand("GetSrvKeyspace", "zone2", "test_delete_keyspace_removekscell")
	_ = clusterForKSTest.VtctlclientProcess.ExecuteCommand("GetSrvKeyspace", "zone1", "test_delete_keyspace_removekscell")

	// Just remove the shard from one cell (including tablets),
	// but leaving the global records and other cells/shards alone.
	_ = clusterForKSTest.VtctlclientProcess.ExecuteCommand("RemoveShardCell", "--recursive", "test_delete_keyspace_removekscell/0", "zone2")

	//Check that the shard is gone from zone2.
	srvKeyspaceZone2 := getSrvKeyspace(t, cell2, "test_delete_keyspace_removekscell")

	for _, partition := range srvKeyspaceZone2.Partitions {
		assert.Equal(t, len(partition.ShardReferences), 1)
	}

	srvKeyspaceZone1 := getSrvKeyspace(t, cell, "test_delete_keyspace_removekscell")
	for _, partition := range srvKeyspaceZone1.Partitions {
		assert.Equal(t, len(partition.ShardReferences), 2)
	}

	_ = clusterForKSTest.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", "test_delete_keyspace_removekscell")
	_ = clusterForKSTest.VtctlclientProcess.ExecuteCommand("GetKeyspace", "test_delete_keyspace_removekscell")
	_ = clusterForKSTest.VtctlclientProcess.ExecuteCommand("GetShard", "test_delete_keyspace_removekscell/0")

	_ = clusterForKSTest.VtctlclientProcess.ExecuteCommand("GetTablet", "zone1-0000000100")

	err := clusterForKSTest.VtctlclientProcess.ExecuteCommand("GetTablet", "zone2-0000000100")
	require.Error(t, err)

	_ = clusterForKSTest.VtctlclientProcess.ExecuteCommand("GetTablet", "zone2-0000000101")
	_ = clusterForKSTest.VtctlclientProcess.ExecuteCommand("GetShardReplication", "zone1", "test_delete_keyspace_removekscell/0")

	err = clusterForKSTest.VtctlclientProcess.ExecuteCommand("GetShardReplication", "zone2", "test_delete_keyspace_removekscell/0")
	require.Error(t, err)

	_ = clusterForKSTest.VtctlclientProcess.ExecuteCommand("GetShardReplication", "zone2", "test_delete_keyspace_removekscell/1")
	_ = clusterForKSTest.VtctlclientProcess.ExecuteCommand("GetSrvKeyspace", "zone2", "test_delete_keyspace_removekscell")

	// Add it back to do another test.
	_ = clusterForKSTest.VtctlclientProcess.ExecuteCommand("InitTablet", "--port=1234", "--keyspace=test_delete_keyspace_removekscell", "--shard=0", "zone2-0000000100", "replica")
	_ = clusterForKSTest.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", "test_delete_keyspace_removekscell")
	_ = clusterForKSTest.VtctlclientProcess.ExecuteCommand("GetShardReplication", "zone2", "test_delete_keyspace_removekscell/0")

	// Now use RemoveKeyspaceCell to remove all shards.
	_ = clusterForKSTest.VtctlclientProcess.ExecuteCommand("RemoveKeyspaceCell", "-recursive", "test_delete_keyspace_removekscell", "zone2")
	_ = clusterForKSTest.VtctlclientProcess.ExecuteCommand("RebuildKeyspaceGraph", "test_delete_keyspace_removekscell")
	_ = clusterForKSTest.VtctlclientProcess.ExecuteCommand("GetShardReplication", "zone1", "test_delete_keyspace_removekscell/0")

	err = clusterForKSTest.VtctlclientProcess.ExecuteCommand("GetShardReplication", "zone2", "test_delete_keyspace_removekscell/0")
	require.Error(t, err)

	err = clusterForKSTest.VtctlclientProcess.ExecuteCommand("GetShardReplication", "zone2", "test_delete_keyspace_removekscell/1")
	require.Error(t, err)

	// Clean up
	_ = clusterForKSTest.VtctlclientProcess.ExecuteCommand("DeleteKeyspace", "-recursive", "test_delete_keyspace_removekscell")
} */

func TestShardCountForAllKeyspaces(t *testing.T) {
	defer cluster.PanicHandler(t)
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
	defer cluster.PanicHandler(t)
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
	defer cluster.PanicHandler(t)
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
	return (keybytes[:])
}

func getSrvKeyspace(t *testing.T, cell string, ksname string) *topodatapb.SrvKeyspace {
	output, err := clusterForKSTest.VtctldClientProcess.GetSrvKeyspaces(ksname, cell)
	require.NoError(t, err)

	srvKeyspace := output[cell]
	require.NotNil(t, srvKeyspace, "no srvkeyspace for cell %s", cell)

	return srvKeyspace
}
