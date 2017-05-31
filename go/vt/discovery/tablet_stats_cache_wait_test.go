/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package discovery

import (
	"reflect"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestFindAllKeyspaceShards(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")

	// no keyspace / shards
	ks, err := findAllKeyspaceShards(ctx, ts, "cell1")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(ks) > 0 {
		t.Errorf("why did I get anything? %v", ks)
	}

	// add one
	if err := ts.UpdateSrvKeyspace(ctx, "cell1", "test_keyspace", &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_MASTER,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name: "test_shard0",
					},
				},
			},
		},
	}); err != nil {
		t.Fatalf("can't add srvKeyspace: %v", err)
	}

	// get it
	ks, err = findAllKeyspaceShards(ctx, ts, "cell1")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(ks, map[keyspaceShard]bool{
		{
			keyspace: "test_keyspace",
			shard:    "test_shard0",
		}: true,
	}) {
		t.Errorf("got wrong value: %v", ks)
	}

	// add another one
	if err := ts.UpdateSrvKeyspace(ctx, "cell1", "test_keyspace2", &topodatapb.SrvKeyspace{
		Partitions: []*topodatapb.SrvKeyspace_KeyspacePartition{
			{
				ServedType: topodatapb.TabletType_MASTER,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name: "test_shard1",
					},
				},
			},
			{
				ServedType: topodatapb.TabletType_MASTER,
				ShardReferences: []*topodatapb.ShardReference{
					{
						Name: "test_shard2",
					},
				},
			},
		},
	}); err != nil {
		t.Fatalf("can't add srvKeyspace: %v", err)
	}

	// get it
	ks, err = findAllKeyspaceShards(ctx, ts, "cell1")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(ks, map[keyspaceShard]bool{
		{
			keyspace: "test_keyspace",
			shard:    "test_shard0",
		}: true,
		{
			keyspace: "test_keyspace2",
			shard:    "test_shard1",
		}: true,
		{
			keyspace: "test_keyspace2",
			shard:    "test_shard2",
		}: true,
	}) {
		t.Errorf("got wrong value: %v", ks)
	}
}

func TestWaitForTablets(t *testing.T) {
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer shortCancel()
	waitAvailableTabletInterval = 20 * time.Millisecond

	tablet := topo.NewTablet(0, "cell", "a")
	tablet.PortMap["vt"] = 1
	input := make(chan *querypb.StreamHealthResponse)
	createFakeConn(tablet, input)

	hc := NewHealthCheck(1*time.Millisecond, 1*time.Millisecond, 1*time.Hour)
	tsc := NewTabletStatsCache(hc, "cell")
	hc.AddTablet(tablet, "")

	// this should time out
	if err := tsc.WaitForTablets(shortCtx, "cell", "keyspace", "shard", []topodatapb.TabletType{topodatapb.TabletType_REPLICA}); err != context.DeadlineExceeded {
		t.Errorf("got wrong error: %v", err)
	}

	// this should fail, but return a non-timeout error
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := tsc.WaitForTablets(cancelledCtx, "cell", "keyspace", "shard", []topodatapb.TabletType{topodatapb.TabletType_REPLICA}); err == nil || err == context.DeadlineExceeded {
		t.Errorf("want: non-timeout error, got: %v", err)
	}

	// send the tablet in
	shr := &querypb.StreamHealthResponse{
		Target: &querypb.Target{
			Keyspace:   "keyspace",
			Shard:      "shard",
			TabletType: topodatapb.TabletType_REPLICA,
		},
		Serving:       true,
		RealtimeStats: &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}
	input <- shr

	// and ask again, with longer time outs so it's not flaky
	longCtx, longCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer longCancel()
	waitAvailableTabletInterval = 10 * time.Millisecond
	if err := tsc.WaitForTablets(longCtx, "cell", "keyspace", "shard", []topodatapb.TabletType{topodatapb.TabletType_REPLICA}); err != nil {
		t.Errorf("got error: %v", err)
	}
}
