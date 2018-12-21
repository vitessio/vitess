/*
 Copyright 2018 The Vitess Authors.

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

package topotools

import (
	"golang.org/x/net/context"
	"reflect"
	"testing"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestRebuildKeyspace(t *testing.T) {
	ts := createSetup(context.Background(), t)
	err := RebuildKeyspace(
		context.Background(),
		logutil.NewConsoleLogger(),
		ts,
		"test_keyspace",
		[]string{"test_cell"},
	)

	if err != nil {
		t.Fatalf("Expected error to be nil got: %v", err)
	}

	expectedSrvKeyspace := &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			&topodatapb.SrvKeyspace_KeyspacePartition{
				ServedType: topodatapb.TabletType_MASTER,
				ShardReferences: []*topodatapb.ShardReference{
					&topodatapb.ShardReference{Name: "0"},
				},
			},
			&topodatapb.SrvKeyspace_KeyspacePartition{
				ServedType: topodatapb.TabletType_REPLICA,
				ShardReferences: []*topodatapb.ShardReference{
					&topodatapb.ShardReference{Name: "0"},
				},
			},
			// Note: even though there are no rdonly tablets, this is
			// a served type in the shard, so it should show up in the
			// srvkeyspace
			&topodatapb.SrvKeyspace_KeyspacePartition{
				ServedType: topodatapb.TabletType_RDONLY,
				ShardReferences: []*topodatapb.ShardReference{
					&topodatapb.ShardReference{Name: "0"},
				},
			},
		},
	}

	srvKeyspace, err := ts.GetSrvKeyspace(context.Background(), "test_cell", "test_keyspace")
	if err != nil {
		t.Fatalf("Expected error to be nil got: %v", err)
	}
	if !reflect.DeepEqual(srvKeyspace, expectedSrvKeyspace) {
		t.Errorf("SrvKeyspace: invalid output expected %v, got :%v", srvKeyspace, expectedSrvKeyspace)
	}

	// It should fail to get a srvkeyspace that hasn't been generated
	srvKeyspace, err = ts.GetSrvKeyspace(context.Background(), "test_cell_2", "test_keyspace")
	if err == nil {
		t.Fatalf("Expected error, got nil")
	}

	err = RebuildKeyspace(
		context.Background(),
		logutil.NewConsoleLogger(),
		ts,
		"test_keyspace",
		[]string{"test_cell_2"},
	)
	if err != nil {
		t.Fatalf("Expected error to be nil got: %v", err)
	}

	// It should generate srvkeyspace for cells that have no tablets
	srvKeyspace, err = ts.GetSrvKeyspace(context.Background(), "test_cell_2", "test_keyspace")
	if err != nil {
		t.Fatalf("Expected error to be nil got: %v", err)
	}
	if !reflect.DeepEqual(srvKeyspace, expectedSrvKeyspace) {
		t.Errorf("SrvKeyspace: invalid output expected %v, got :%v", srvKeyspace, expectedSrvKeyspace)
	}

}

func createSetup(ctx context.Context, t *testing.T) *topo.Server {
	// Create an in memory toposerver
	ts := memorytopo.NewServer("test_cell", "test_cell_2")

	// create a keyspace and a couple tablets
	if err := ts.CreateKeyspace(ctx, "test_keyspace", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("cannot create keyspace: %v", err)
	}
	if err := ts.CreateShard(ctx, "test_keyspace", "0"); err != nil {
		t.Fatalf("cannot create shard: %v", err)
	}
	if _, err := ts.UpdateShardFields(ctx, "test_keyspace", "0", func(si *topo.ShardInfo) error {
		si.Cells = []string{}
		return nil
	}); err != nil {
		t.Fatalf("cannot update shard: %v", err)
	}
	tablet1 := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "test_cell",
			Uid:  123,
		},
		Hostname:      "masterhost",
		MysqlHostname: "masterhost",
		PortMap: map[string]int32{
			"vt":   8101,
			"gprc": 8102,
		},
		Keyspace:       "test_keyspace",
		Shard:          "0",
		Type:           topodatapb.TabletType_MASTER,
		DbNameOverride: "",
		KeyRange:       nil,
	}
	topoproto.SetMysqlPort(tablet1, 3306)
	if err := ts.CreateTablet(ctx, tablet1); err != nil {
		t.Fatalf("cannot create master tablet: %v", err)
	}
	tablet2 := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "test_cell",
			Uid:  234,
		},
		PortMap: map[string]int32{
			"vt":   8101,
			"grpc": 8102,
		},
		Hostname:      "slavehost",
		MysqlHostname: "slavehost",

		Keyspace:       "test_keyspace",
		Shard:          "0",
		Type:           topodatapb.TabletType_REPLICA,
		DbNameOverride: "",
		KeyRange:       nil,
	}
	topoproto.SetMysqlPort(tablet2, 3306)
	if err := ts.CreateTablet(ctx, tablet2); err != nil {
		t.Fatalf("cannot create slave tablet: %v", err)
	}
	return ts
}
