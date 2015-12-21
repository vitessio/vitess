package vtctld

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topotools"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/zktopo"

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
	ts := zktopo.NewTestServer(t, cells)
	actionRepo := NewActionRepository(ts)
	initAPI(ctx, ts, actionRepo)

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

	ts.CreateTablet(ctx, &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "cell1", Uid: 100},
		Keyspace: "ks1",
		Shard:    "-80",
		Type:     topodatapb.TabletType_REPLICA,
		KeyRange: &topodatapb.KeyRange{Start: nil, End: []byte{0x80}},
		PortMap:  map[string]int32{"vt": 100},
	})
	ts.CreateTablet(ctx, &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "cell2", Uid: 200},
		Keyspace: "ks1",
		Shard:    "-80",
		Type:     topodatapb.TabletType_REPLICA,
		KeyRange: &topodatapb.KeyRange{Start: nil, End: []byte{0x80}},
		PortMap:  map[string]int32{"vt": 200},
	})
	topotools.RebuildShard(ctx, logutil.NewConsoleLogger(), ts, "ks1", "-80", cells)

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

		// EndPoints
		{"GET", "endpoints/cell1/ks1/-80/", `[2]`},
		{"GET", "endpoints/cell1/ks1/-80/replica", `{
				"entries": [{
						"uid": 100,
						"port_map": {"vt": 100}
					}]
			}`},
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
		if got, want := compactJSON(body), compactJSON([]byte(in.want)); got != want {
			t.Errorf("[%v] got %v, want %v", in.path, got, want)
		}
	}
}
