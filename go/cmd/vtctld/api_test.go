// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topotools"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/zktopo"

	pb "github.com/youtube/vitess/go/vt/proto/topodata"
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
	actionRepo = NewActionRepository(ts)
	initAPI(ctx, ts, actionRepo)

	server := httptest.NewServer(nil)
	defer server.Close()

	// Populate topo.
	ts.CreateKeyspace(ctx, "ks1", &pb.Keyspace{ShardingColumnName: "shardcol"})
	ts.CreateShard(ctx, "ks1", "-80", &pb.Shard{
		Cells:    cells,
		KeyRange: &pb.KeyRange{Start: nil, End: []byte{0x80}},
	})
	ts.CreateShard(ctx, "ks1", "80-", &pb.Shard{
		Cells:    cells,
		KeyRange: &pb.KeyRange{Start: []byte{0x80}, End: nil},
	})

	topo.CreateTablet(ctx, ts, &topo.Tablet{
		Alias:    topo.TabletAlias{Cell: "cell1", Uid: 100},
		Keyspace: "ks1",
		Shard:    "-80",
		Type:     topo.TYPE_REPLICA,
		KeyRange: key.KeyRange{Start: "", End: "\x80"},
		Portmap:  map[string]int{"vt": 100},
	})
	topo.CreateTablet(ctx, ts, &topo.Tablet{
		Alias:    topo.TabletAlias{Cell: "cell2", Uid: 200},
		Keyspace: "ks1",
		Shard:    "-80",
		Type:     topo.TYPE_REPLICA,
		KeyRange: key.KeyRange{Start: "", End: "\x80"},
		Portmap:  map[string]int{"vt": 200},
	})
	topotools.RebuildShard(ctx, logutil.NewConsoleLogger(), ts, "ks1", "-80", cells, 10*time.Second)

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
		func(ctx context.Context, wr *wrangler.Wrangler, tabletAlias topo.TabletAlias, r *http.Request) (string, error) {
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
				{"Cell":"cell1","Uid":100},
				{"Cell":"cell2","Uid":200}
			]`},
		{"GET", "tablets/?cell=cell1", `[
				{"Cell":"cell1","Uid":100}
			]`},
		{"GET", "tablets/?shard=ks1%2F-80&cell=cell2", `[
				{"Cell":"cell2","Uid":200}
			]`},
		{"GET", "tablets/cell1-100", `{
				"Alias": {"Cell": "cell1", "Uid": 100},
				"Hostname": "",
				"IPAddr": "",
				"Portmap": {"vt": 100},
				"Tags": null,
				"Health": null,
				"Keyspace": "ks1",
				"Shard": "-80",
				"Type": "replica",
				"DbNameOverride": "",
				"KeyRange": {"Start": "", "End": "80"}
			}`},
		{"POST", "tablets/cell1-100?action=TestTabletAction", `{
				"Name": "TestTabletAction",
				"Parameters": "cell1-0000000100",
				"Output": "TestTabletAction Result",
				"Error": false
			}`},

		// EndPoints
		{"GET", "endpoints/cell1/ks1/-80/", `["replica"]`},
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
