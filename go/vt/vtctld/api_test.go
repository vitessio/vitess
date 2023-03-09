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

package vtctld

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
)

func compactJSON(in []byte) string {
	buf := &bytes.Buffer{}
	json.Compact(buf, in)
	return strings.ReplaceAll(buf.String(), "\n", "")
}

func TestAPI(t *testing.T) {
	ctx := context.Background()
	cells := []string{"cell1", "cell2"}
	ts := memorytopo.NewServer(cells...)
	actionRepo := NewActionRepository(ts)
	server := httptest.NewServer(nil)
	defer server.Close()

	// Populate topo. Remove ServedTypes from shards to avoid ordering issues.
	ts.CreateKeyspace(ctx, "ks1", &topodatapb.Keyspace{DurabilityPolicy: "semi_sync"})
	ts.CreateShard(ctx, "ks1", "-80")
	ts.CreateShard(ctx, "ks1", "80-")

	// SaveVSchema to test that creating a snapshot keyspace copies VSchema
	vs := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"name1": {
				Type: "hash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"table1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{
					{
						Column: "column1",
						Name:   "name1",
					},
				},
			},
		},
	}
	ts.SaveVSchema(ctx, "ks1", vs)

	tablet1 := topodatapb.Tablet{
		Alias:         &topodatapb.TabletAlias{Cell: "cell1", Uid: 100},
		Keyspace:      "ks1",
		Shard:         "-80",
		Type:          topodatapb.TabletType_REPLICA,
		KeyRange:      &topodatapb.KeyRange{Start: nil, End: []byte{0x80}},
		PortMap:       map[string]int32{"vt": 100},
		Hostname:      "mysql1-cell1.test.net",
		MysqlHostname: "mysql1-cell1.test.net",
		MysqlPort:     int32(3306),
	}
	ts.CreateTablet(ctx, &tablet1)

	tablet2 := topodatapb.Tablet{
		Alias:         &topodatapb.TabletAlias{Cell: "cell2", Uid: 200},
		Keyspace:      "ks1",
		Shard:         "-80",
		Type:          topodatapb.TabletType_REPLICA,
		KeyRange:      &topodatapb.KeyRange{Start: nil, End: []byte{0x80}},
		PortMap:       map[string]int32{"vt": 200},
		Hostname:      "mysql2-cell2.test.net",
		MysqlHostname: "mysql2-cell2.test.net",
		MysqlPort:     int32(3306),
	}
	ts.CreateTablet(ctx, &tablet2)

	// Populate fake actions.
	actionRepo.RegisterKeyspaceAction("TestKeyspaceAction",
		func(ctx context.Context, wr *wrangler.Wrangler, keyspace string) (string, error) {
			return "TestKeyspaceAction Result", nil
		})
	actionRepo.RegisterShardAction("TestShardAction",
		func(ctx context.Context, wr *wrangler.Wrangler, keyspace, shard string) (string, error) {
			return "TestShardAction Result", nil
		})
	actionRepo.RegisterTabletAction("TestTabletAction", "",
		func(ctx context.Context, wr *wrangler.Wrangler, tabletAlias *topodatapb.TabletAlias) (string, error) {
			return "TestTabletAction Result", nil
		})

	initAPI(ctx, ts, actionRepo)

	// all-tablets response for keyspace/ks1/tablets/ endpoints
	keyspaceKs1AllTablets := `[
		{
			"alias": {
				"cell": "cell1",
				"uid": 100
			},
			"hostname": "mysql1-cell1.test.net",
			"port_map": {
				"vt": 100
			},
			"keyspace": "ks1",
			"shard": "-80",
			"key_range": {
				"end": "gA=="
			},
			"type": 2,
			"mysql_hostname": "mysql1-cell1.test.net",
			"mysql_port": 3306,
			"url": "http://mysql1-cell1.test.net:100"
		},
		{
			"alias": {
				"cell": "cell2",
				"uid": 200
			},
			"hostname": "mysql2-cell2.test.net",
			"port_map": {
				"vt": 200
			},
			"keyspace": "ks1",
			"shard": "-80",
			"key_range": {
				"end": "gA=="
			},
			"type": 2,
			"mysql_hostname": "mysql2-cell2.test.net",
			"mysql_port": 3306,
			"url": "http://mysql2-cell2.test.net:200"
		}
	]`

	// Test cases.
	table := []struct {
		method, path, body, want string
		statusCode               int
	}{
		// Create snapshot keyspace with durability policy specified
		{"POST", "vtctl/", `["CreateKeyspace", "--keyspace_type=SNAPSHOT", "--base_keyspace=ks1", "--snapshot_time=2006-01-02T15:04:05+00:00", "--durability-policy=semi_sync", "ks3"]`, `{
  "Error": "durability-policy cannot be specified while creating a snapshot keyspace"`, http.StatusOK},
		// Create snapshot keyspace using API
		{"POST", "vtctl/", `["CreateKeyspace", "--keyspace_type=SNAPSHOT", "--base_keyspace=ks1", "--snapshot_time=2006-01-02T15:04:05+00:00", "ks3"]`, `{
		   "Error": "",
		   "Output": ""
		}`, http.StatusOK},

		// Cells
		{"GET", "cells", "", `["cell1","cell2"]`, http.StatusOK},

		// Keyspace
		{"GET", "keyspace/doesnt-exist/tablets/", "", ``, http.StatusNotFound},
		{"GET", "keyspace/ks1/tablets/", "", keyspaceKs1AllTablets, http.StatusOK},
		{"GET", "keyspace/ks1/tablets/-80", "", keyspaceKs1AllTablets, http.StatusOK},
		{"GET", "keyspace/ks1/tablets/80-", "", `[]`, http.StatusOK},
		{"GET", "keyspace/ks1/tablets/?cells=cell1,cell2", "", keyspaceKs1AllTablets, http.StatusOK},
		{"GET", "keyspace/ks1/tablets/?cells=cell1", "", `[
			{
				"alias": {
					"cell": "cell1",
					"uid": 100
				},
				"hostname": "mysql1-cell1.test.net",
				"port_map": {
					"vt": 100
				},
				"keyspace": "ks1",
				"shard": "-80",
				"key_range": {
					"end": "gA=="
				},
				"type": 2,
				"mysql_hostname": "mysql1-cell1.test.net",
				"mysql_port": 3306,
				"url": "http://mysql1-cell1.test.net:100"
			}
		]`, http.StatusOK},
		{"GET", "keyspace/ks1/tablets/?cells=cell3", "", `[]`, http.StatusOK},
		{"GET", "keyspace/ks1/tablets/?cell=cell2", "", `[
			{
				"alias": {
					"cell": "cell2",
					"uid": 200
				},
				"hostname": "mysql2-cell2.test.net",
				"port_map": {
					"vt": 200
				},
				"keyspace": "ks1",
				"shard": "-80",
				"key_range": {
					"end": "gA=="
				},
				"type": 2,
				"mysql_hostname": "mysql2-cell2.test.net",
				"mysql_port": 3306,
				"url": "http://mysql2-cell2.test.net:200"
			}
		]`, http.StatusOK},
		{"GET", "keyspace/ks1/tablets/?cell=cell3", "", `[]`, http.StatusOK},

		// Keyspaces
		{"GET", "keyspaces", "", `["ks1", "ks3"]`, http.StatusOK},
		{"GET", "keyspaces/ks1", "", `{
				"served_froms": [],
				"keyspace_type":0,
				"base_keyspace":"",
				"snapshot_time":null,
				"durability_policy":"semi_sync",
				"throttler_config": null
			}`, http.StatusOK},
		{"GET", "keyspaces/nonexistent", "", "404 page not found", http.StatusNotFound},
		{"POST", "keyspaces/ks1?action=TestKeyspaceAction", "", `{
				"Name": "TestKeyspaceAction",
				"Parameters": "ks1",
				"Output": "TestKeyspaceAction Result",
				"Error": false
			}`, http.StatusOK},

		// Shards
		{"GET", "shards/ks1/", "", `["-80","80-"]`, http.StatusOK},
		{"GET", "shards/ks1/-80", "", `{
				"primary_alias": null,
				"primary_term_start_time":null,
				"key_range": {
					"start": "",
					"end":"gA=="
				},
				"source_shards": [],
				"tablet_controls": [],
				"is_primary_serving": true
			}`, http.StatusOK},
		{"GET", "shards/ks1/-DEAD", "", "404 page not found", http.StatusNotFound},
		{"POST", "shards/ks1/-80?action=TestShardAction", "", `{
				"Name": "TestShardAction",
				"Parameters": "ks1/-80",
				"Output": "TestShardAction Result",
				"Error": false
			}`, http.StatusOK},

		// Tablets
		{"GET", "tablets/?shard=ks1%2F-80", "", `[
				{"cell":"cell1","uid":100},
				{"cell":"cell2","uid":200}
			]`, http.StatusOK},
		{"GET", "tablets/?cell=cell1", "", `[
				{"cell":"cell1","uid":100}
			]`, http.StatusOK},
		{"GET", "tablets/?shard=ks1%2F-80&cell=cell2", "", `[
				{"cell":"cell2","uid":200}
			]`, http.StatusOK},
		{"GET", "tablets/?shard=ks1%2F80-&cell=cell1", "", `[]`, http.StatusOK},
		{"GET", "tablets/cell1-100", "", `{
				"alias": {"cell": "cell1", "uid": 100},
				"hostname": "mysql1-cell1.test.net",
				"port_map": {"vt": 100},
				"keyspace": "ks1",
				"shard": "-80",
				"key_range": {
					"end": "gA=="
				},
				"type": 2,
				"mysql_hostname": "mysql1-cell1.test.net",
				"mysql_port": 3306,
				"url":"http://mysql1-cell1.test.net:100"
			}`, http.StatusOK},
		{"GET", "tablets/nonexistent-999", "", "404 page not found", http.StatusNotFound},
		{"POST", "tablets/cell1-100?action=TestTabletAction", "", `{
				"Name": "TestTabletAction",
				"Parameters": "cell1-0000000100",
				"Output": "TestTabletAction Result",
				"Error": false
			}`, http.StatusOK},

		// Tablet Updates
		{"GET", "tablet_statuses/?keyspace=all&cell=all&type=all&metric=lag", "", "404 page not found", http.StatusNotFound},
		{"GET", "tablet_statuses/cell1/REPLICA/lag", "", "404 page not found", http.StatusNotFound},
		{"GET", "tablet_statuses/?keyspace=ks1&cell=cell1&type=hello&metric=lag", "", "404 page not found", http.StatusNotFound},

		// Tablet Health
		{"GET", "tablet_health/cell1/100", "", "404 page not found", http.StatusNotFound},
		{"GET", "tablet_health/cell1", "", "404 page not found", http.StatusNotFound},
		{"GET", "tablet_health/cell1/gh", "", "404 page not found", http.StatusNotFound},

		// Topology Info
		{"GET", "topology_info/?keyspace=all&cell=all", "", "404 page not found", http.StatusNotFound},

		// vtctl RunCommand
		{"POST", "vtctl/", `["GetKeyspace","ks1"]`, `{
		   "Error": "",
		   "Output": "{\n  \"served_froms\": [],\n  \"keyspace_type\": 0,\n  \"base_keyspace\": \"\",\n  \"snapshot_time\": null,\n  \"durability_policy\": \"semi_sync\",\n  \"throttler_config\": null\n}\n\n"
		}`, http.StatusOK},
		{"POST", "vtctl/", `["GetKeyspace","ks3"]`, `{
		   "Error": "",
		   "Output": "{\n  \"served_froms\": [],\n  \"keyspace_type\": 1,\n  \"base_keyspace\": \"ks1\",\n  \"snapshot_time\": {\n    \"seconds\": \"1136214245\",\n    \"nanoseconds\": 0\n  },\n  \"durability_policy\": \"none\",\n  \"throttler_config\": null\n}\n\n"
		}`, http.StatusOK},
		{"POST", "vtctl/", `["GetVSchema","ks3"]`, `{
		   "Error": "",
		   "Output": "{\n  \"sharded\": true,\n  \"vindexes\": {\n    \"name1\": {\n      \"type\": \"hash\"\n    }\n  },\n  \"tables\": {\n    \"table1\": {\n      \"columnVindexes\": [\n        {\n          \"column\": \"column1\",\n          \"name\": \"name1\"\n        }\n      ]\n    }\n  },\n  \"requireExplicitRouting\": true\n}\n\n"
		}`, http.StatusOK},
		{"POST", "vtctl/", `["GetKeyspace","does_not_exist"]`, `{
			"Error": "node doesn't exist: keyspaces/does_not_exist/Keyspace",
		   "Output": ""
		}`, http.StatusOK},
		{"POST", "vtctl/", `["Panic"]`, `uncaught panic: this command panics on purpose`, http.StatusInternalServerError},
	}
	for _, in := range table {
		t.Run(in.method+in.path, func(t *testing.T) {
			var resp *http.Response
			var err error

			switch in.method {
			case "GET":
				resp, err = http.Get(server.URL + apiPrefix + in.path)
				require.NoError(t, err)
				defer resp.Body.Close()
			case "POST":
				resp, err = http.Post(server.URL+apiPrefix+in.path, "application/json", strings.NewReader(in.body))
				require.NoError(t, err)
				defer resp.Body.Close()
			default:
				t.Fatalf("[%v] unknown method: %v", in.path, in.method)
			}

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, in.statusCode, resp.StatusCode)

			got := compactJSON(body)
			want := compactJSON([]byte(in.want))
			if want == "" {
				// want is not valid JSON. Fallback to a string comparison.
				want = in.want
				// For unknown reasons errors have a trailing "\n\t\t". Remove it.
				got = strings.TrimSpace(string(body))
			}
			if !strings.HasPrefix(got, want) {
				t.Fatalf("For path [%v] got\n'%v', want\n'%v'", in.path, got, want)
				return
			}
		})

	}
}
