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

package controller

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/logutil"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
	"vitess.io/vitess/go/vt/vtgr/config"
)

func TestRefreshTabletsInShard(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	ts := memorytopo.NewServer("test_cell")
	defer ts.Close()
	ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{})
	ts.CreateShard(ctx, "ks", "0")
	tablet1 := buildTabletInfo(uint32(0), testHost, testPort0, topodatapb.TabletType_MASTER, time.Time{})
	tablet2 := buildTabletInfo(uint32(1), testHost, testPort1, topodatapb.TabletType_SPARE, time.Time{})
	tablet3 := buildTabletInfo(uint32(2), testHost, 0, topodatapb.TabletType_REPLICA, time.Time{})
	testutil.AddTablet(ctx, t, ts, tablet1.Tablet, nil)
	testutil.AddTablet(ctx, t, ts, tablet2.Tablet, nil)
	testutil.AddTablet(ctx, t, ts, tablet3.Tablet, nil)
	cfg := &config.VTGRConfig{GroupSize: 3, MinNumReplica: 0, BackoffErrorWaitTimeSeconds: 1, BootstrapWaitTimeSeconds: 1}
	shard := NewGRShard("ks", "0", nil, nil, ts, nil, cfg, testPort0)
	assert.Equal(t, "ks", shard.shardStatusCollector.status.Keyspace)
	assert.Equal(t, "0", shard.shardStatusCollector.status.Shard)
	shard.refreshTabletsInShardLocked(context.Background())
	instances := shard.instances
	// only have 2 instances here because we filter out the spare tablet
	assert.Equal(t, 2, len(instances))
	sort.Slice(instances[:], func(i, j int) bool {
		return instances[i].alias < instances[j].alias
	})
	assert.Equal(t, testHost, instances[0].tablet.Hostname)
	assert.Equal(t, int32(testPort0), instances[0].tablet.MysqlPort)
	assert.Equal(t, topodatapb.TabletType_MASTER, instances[0].tablet.Type)
	// host 3 is missing mysql host but we still put it in the instances list here
	assert.Equal(t, testHost, instances[1].instanceKey.Hostname)
	assert.Equal(t, int32(0), instances[1].tablet.MysqlPort)
	assert.Equal(t, topodatapb.TabletType_REPLICA, instances[1].tablet.Type)
}

func TestRefreshWithCells(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2", "cell3")
	defer ts.Close()
	ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{})
	ts.CreateShard(ctx, "ks", "0")
	tablet1 := buildTabletInfoWithCell(uint32(0), testHost, "cell1", testPort0, topodatapb.TabletType_REPLICA, time.Time{})
	tablet2 := buildTabletInfoWithCell(uint32(1), testHost, "cell2", testPort1, topodatapb.TabletType_REPLICA, time.Time{})
	tablet3 := buildTabletInfoWithCell(uint32(2), testHost, "cell3", testPort2, topodatapb.TabletType_REPLICA, time.Time{})
	testutil.AddTablet(ctx, t, ts, tablet1.Tablet, nil)
	testutil.AddTablet(ctx, t, ts, tablet2.Tablet, nil)
	testutil.AddTablet(ctx, t, ts, tablet3.Tablet, nil)
	cfg := &config.VTGRConfig{GroupSize: 3, MinNumReplica: 0, BackoffErrorWaitTimeSeconds: 1, BootstrapWaitTimeSeconds: 1}
	shard := NewGRShard("ks", "0", []string{"cell1", "cell3"}, nil, ts, nil, cfg, testPort0)
	shard.refreshTabletsInShardLocked(context.Background())
	instances := shard.instances
	// only have 2 instances here because we are not watching cell2
	assert.Equal(t, 2, len(instances))
	sort.Slice(instances[:], func(i, j int) bool {
		return instances[i].alias < instances[j].alias
	})
	assert.Equal(t, "cell1-0000000000", instances[0].alias)
	assert.Equal(t, "cell3-0000000002", instances[1].alias)
}

func TestRefreshWithEmptyCells(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2", "cell3")
	defer ts.Close()
	ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{})
	ts.CreateShard(ctx, "ks", "0")
	tablet1 := buildTabletInfoWithCell(uint32(0), testHost, "cell1", testPort0, topodatapb.TabletType_REPLICA, time.Time{})
	tablet2 := buildTabletInfoWithCell(uint32(1), testHost, "cell2", testPort1, topodatapb.TabletType_REPLICA, time.Time{})
	tablet3 := buildTabletInfoWithCell(uint32(2), testHost, "cell3", testPort2, topodatapb.TabletType_REPLICA, time.Time{})
	testutil.AddTablet(ctx, t, ts, tablet1.Tablet, nil)
	testutil.AddTablet(ctx, t, ts, tablet2.Tablet, nil)
	testutil.AddTablet(ctx, t, ts, tablet3.Tablet, nil)
	cfg := &config.VTGRConfig{GroupSize: 3, MinNumReplica: 0, BackoffErrorWaitTimeSeconds: 1, BootstrapWaitTimeSeconds: 1}
	shard := NewGRShard("ks", "0", nil, nil, ts, nil, cfg, testPort0)
	shard.refreshTabletsInShardLocked(context.Background())
	instances := shard.instances
	// nil cell will return everything
	assert.Equal(t, 3, len(instances))
	sort.Slice(instances[:], func(i, j int) bool {
		return instances[i].alias < instances[j].alias
	})
	assert.Equal(t, "cell1-0000000000", instances[0].alias)
	assert.Equal(t, "cell2-0000000001", instances[1].alias)
	assert.Equal(t, "cell3-0000000002", instances[2].alias)
}

func buildTabletInfo(id uint32, host string, mysqlPort int, ttype topodatapb.TabletType, masterTermTime time.Time) *topo.TabletInfo {
	return buildTabletInfoWithCell(id, host, "test_cell", mysqlPort, ttype, masterTermTime)
}

func buildTabletInfoWithCell(id uint32, host, cell string, mysqlPort int, ttype topodatapb.TabletType, masterTermTime time.Time) *topo.TabletInfo {
	alias := &topodatapb.TabletAlias{Cell: cell, Uid: id}
	return &topo.TabletInfo{Tablet: &topodatapb.Tablet{
		Alias:               alias,
		Hostname:            host,
		MysqlHostname:       host,
		MysqlPort:           int32(mysqlPort),
		Keyspace:            "ks",
		Shard:               "0",
		Type:                ttype,
		MasterTermStartTime: logutil.TimeToProto(masterTermTime),
		Tags:                map[string]string{"hostname": fmt.Sprintf("host_%d", id)},
	}}
}
