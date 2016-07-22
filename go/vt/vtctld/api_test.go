package vtctld

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/zktopo/zktestserver"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func compactJSON(in []byte) string {
	buf := &bytes.Buffer{}
	json.Compact(buf, in)
	return buf.String()
}

func TestAPI(t *testing.T) {
	ctx := context.Background()
	cells := []string{"cell1", "cell2"}
	ts := zktestserver.New(t, cells)
	actionRepo := NewActionRepository(ts)
	server := httptest.NewServer(nil)
	defer server.Close()

	// Populate topo.
	ts.CreateKeyspace(ctx, "ks1", &topodatapb.Keyspace{ShardingColumnName: "shardcol"})
	ts.Impl.CreateShard(ctx, "ks1", "-80", &topodatapb.Shard{
		Cells:    cells,
		KeyRange: &topodatapb.KeyRange{Start: nil, End: []byte{0x80}},
	})
	ts.Impl.CreateShard(ctx, "ks1", "80-", &topodatapb.Shard{
		Cells:    cells,
		KeyRange: &topodatapb.KeyRange{Start: []byte{0x80}, End: nil},
	})

	tablet1 := topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "cell1", Uid: 100},
		Keyspace: "ks1",
		Shard:    "-80",
		Type:     topodatapb.TabletType_REPLICA,
		KeyRange: &topodatapb.KeyRange{Start: nil, End: []byte{0x80}},
		PortMap:  map[string]int32{"vt": 100},
	}
	ts.CreateTablet(ctx, &tablet1)

	tablet2 := topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "cell2", Uid: 200},
		Keyspace: "ks1",
		Shard:    "-80",
		Type:     topodatapb.TabletType_REPLICA,
		KeyRange: &topodatapb.KeyRange{Start: nil, End: []byte{0x80}},
		PortMap:  map[string]int32{"vt": 200},
	}
	ts.CreateTablet(ctx, &tablet2)

	// Populate fake actions.
	actionRepo.RegisterKeyspaceAction("TestKeyspaceAction",
		func(ctx context.Context, wr *wrangler.Wrangler, keyspace string, r *http.Request) (string, error) {
			return "TestKeyspaceAction Result", nil
		})
	actionRepo.RegisterShardAction("TestShardAction",
		func(ctx context.Context, wr *wrangler.Wrangler, keyspace, shard string, r *http.Request) (string, error) {
			return "TestShardAction Result", nil
		})
	actionRepo.RegisterTabletAction("TestTabletAction", "",
		func(ctx context.Context, wr *wrangler.Wrangler, tabletAlias *topodatapb.TabletAlias, r *http.Request) (string, error) {
			return "TestTabletAction Result", nil
		})

	realtimeStats := newRealtimeStatsForTesting()
	initAPI(ctx, ts, actionRepo, realtimeStats)

	target := &querypb.Target{
		Keyspace:   "ks1",
		Shard:      "-80",
		TabletType: topodatapb.TabletType_REPLICA,
	}
	stats := &querypb.RealtimeStats{
		HealthError:         "",
		SecondsBehindMaster: 2,
		BinlogPlayersCount:  0,
		CpuUsage:            12.1,
		Qps:                 5.6,
	}
	tabletStats := &discovery.TabletStats{
		Tablet:  &tablet1,
		Target:  target,
		Up:      true,
		Serving: true,
		TabletExternallyReparentedTimestamp: 5,
		Stats:     stats,
		LastError: nil,
	}
	realtimeStats.tabletStats.StatsUpdate(tabletStats)

	// Test cases.
	table := []struct {
		method, path, want string
	}{
		// Cells
		{"GET", "cells", `["cell1","cell2"]`},

		// Keyspaces
		{"GET", "keyspaces", `["ks1"]`},
		{"GET", "keyspaces/ks1", `{
				"sharding_column_name": "shardcol"
			}`},
		{"POST", "keyspaces/ks1?action=TestKeyspaceAction", `{
				"Name": "TestKeyspaceAction",
				"Parameters": "ks1",
				"Output": "TestKeyspaceAction Result",
				"Error": false
			}`},

		// Shards
		{"GET", "shards/ks1/", `["-80","80-"]`},
		{"GET", "shards/ks1/-80", `{
				"key_range": {"end":"gA=="},
				"cells": ["cell1", "cell2"]
			}`},
		{"POST", "shards/ks1/-80?action=TestShardAction", `{
				"Name": "TestShardAction",
				"Parameters": "ks1/-80",
				"Output": "TestShardAction Result",
				"Error": false
			}`},

		// Tablets
		{"GET", "tablets/?shard=ks1%2F-80", `[
				{"cell":"cell1","uid":100},
				{"cell":"cell2","uid":200}
			]`},
		{"GET", "tablets/?cell=cell1", `[
				{"cell":"cell1","uid":100}
			]`},
		{"GET", "tablets/?shard=ks1%2F-80&cell=cell2", `[
				{"cell":"cell2","uid":200}
			]`},
		{"GET", "tablets/cell1-100", `{
				"alias": {"cell": "cell1", "uid": 100},
				"port_map": {"vt": 100},
				"keyspace": "ks1",
				"shard": "-80",
				"key_range": {"end": "gA=="},
				"type": 2
			}`},
		{"POST", "tablets/cell1-100?action=TestTabletAction", `{
				"Name": "TestTabletAction",
				"Parameters": "cell1-0000000100",
				"Output": "TestTabletAction Result",
				"Error": false
			}`},

		//Tablet Updates
		{"GET", "tablet_statuses/cell1/ks1/-80/REPLICA", `{"100":{"Tablet":{"alias":{"cell":"cell1","uid":100},"port_map":{"vt":100},"keyspace":"ks1","shard":"-80","key_range":{"end":"gA=="},"type":2},"Name":"","Target":{"keyspace":"ks1","shard":"-80","tablet_type":2},"Up":true,"Serving":true,"TabletExternallyReparentedTimestamp":5,"Stats":{"seconds_behind_master":2,"cpu_usage":12.1,"qps":5.6},"LastError":null}}`},
		{"GET", "tablet_statuses/cell1/ks1/replica", "can't get tablet_statuses: invalid target path: \"cell1/ks1/replica\"  expected path: <cell>/<keyspace>/<shard>/<type>"},
		{"GET", "tablet_statuses/cell1/ks1/-80/hello", "can't get tablet_statuses: invalid tablet type: hello"},
	}

	for _, in := range table {
		var resp *http.Response
		var err error

		switch in.method {
		case "GET":
			resp, err = http.Get(server.URL + apiPrefix + in.path)
		case "POST":
			resp, err = http.Post(server.URL+apiPrefix+in.path, "", nil)
		default:
			t.Errorf("[%v] unknown method: %v", in.path, in.method)
			continue
		}

		if err != nil {
			t.Errorf("[%v] http error: %v", in.path, err)
			continue
		}

		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()

		if err != nil {
			t.Errorf("[%v] ioutil.ReadAll(resp.Body) error: %v", in.path, err)
			continue
		}

		got := compactJSON(body)
		want := compactJSON([]byte(in.want))
		if want == "" {
			// want is no valid JSON. Fallback to a string comparison.
			want = in.want
			// For unknown reasons errors have a trailing "\n\t\t". Remove it.
			got = strings.TrimSpace(string(body))
		}
		if got != want {
			t.Errorf("[%v] got '%v', want '%v'", in.path, got, want)
			continue
		}
	}
}
