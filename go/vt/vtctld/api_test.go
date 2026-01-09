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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/vt/servenv/testutils"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtorcdatapb "vitess.io/vitess/go/vt/proto/vtorcdata"
	"vitess.io/vitess/go/vt/proto/vttime"
)

func compactJSON(in []byte) string {
	buf := &bytes.Buffer{}
	json.Compact(buf, in)
	return strings.ReplaceAll(buf.String(), "\n", "")
}

// unmarshalProto unmarshals JSON data into a proto message
func unmarshalProto(data []byte, msg proto.Message) error {
	unmarshaler := protojson.UnmarshalOptions{
		DiscardUnknown: true,
	}
	return unmarshaler.Unmarshal(data, msg)
}

// unmarshalProtoSlice unmarshals JSON array data into a slice of proto messages
func unmarshalProtoSlice(data []byte, msgType proto.Message) ([]proto.Message, error) {
	var jsonArray []json.RawMessage
	if err := json.Unmarshal(data, &jsonArray); err != nil {
		return nil, err
	}

	var result []proto.Message
	for _, item := range jsonArray {
		msg := proto.Clone(msgType)
		if err := unmarshalProto(item, msg); err != nil {
			return nil, err
		}
		result = append(result, msg)
	}
	return result, nil
}

// compareProtoResponse compares the actual JSON response with expected proto objects
func compareProtoResponse(t *testing.T, actualJSON []byte, expected proto.Message, path string) {
	actual := proto.Clone(expected)
	proto.Reset(actual)

	if err := unmarshalProto(actualJSON, actual); err != nil {
		t.Fatalf("Failed to unmarshal response for %s: %v\nResponse: %s", path, err, string(actualJSON))
	}

	if !proto.Equal(actual, expected) {
		actualJSON, _ := protojson.MarshalOptions{Multiline: true}.Marshal(actual)
		expectedJSON, _ := protojson.MarshalOptions{Multiline: true}.Marshal(expected)
		t.Fatalf("Proto comparison failed for %s:\nActual: %s\nExpected: %s", path, string(actualJSON), string(expectedJSON))
	}
}

// compareProtoSliceResponse compares the actual JSON array response with expected proto objects
func compareProtoSliceResponse(t *testing.T, actualJSON []byte, expected []proto.Message, msgType proto.Message, path string) {
	actual, err := unmarshalProtoSlice(actualJSON, msgType)
	if err != nil {
		t.Fatalf("Failed to unmarshal response for %s: %v\nResponse: %s", path, err, string(actualJSON))
	}

	if len(actual) != len(expected) {
		t.Fatalf("Length mismatch for %s: got %d, want %d", path, len(actual), len(expected))
	}

	for i, actualItem := range actual {
		if !proto.Equal(actualItem, expected[i]) {
			actualJSON, _ := protojson.MarshalOptions{Multiline: true}.Marshal(actualItem)
			expectedJSON, _ := protojson.MarshalOptions{Multiline: true}.Marshal(expected[i])
			t.Fatalf("Proto comparison failed for %s[%d]:\nActual: %s\nExpected: %s", path, i, string(actualJSON), string(expectedJSON))
		}
	}
}

// compareTabletWithURLResponse compares the actual JSON response with expected TabletWithURL objects
func compareTabletWithURLResponse(t *testing.T, actualJSON []byte, expected *TabletWithURL, path string) {
	var actual TabletWithURL
	if err := json.Unmarshal(actualJSON, &actual); err != nil {
		t.Fatalf("Failed to unmarshal response for %s: %v\nResponse: %s", path, err, string(actualJSON))
	}

	// Compare the structs by marshaling them to JSON for comparison
	actualJSON2, _ := json.Marshal(actual)
	expectedJSON, _ := json.Marshal(expected)

	if string(actualJSON2) != string(expectedJSON) {
		t.Fatalf("TabletWithURL comparison failed for %s:\nActual: %s\nExpected: %s", path, string(actualJSON2), string(expectedJSON))
	}
}

// compareTabletWithURLSliceResponse compares the actual JSON array response with expected TabletWithURL objects
func compareTabletWithURLSliceResponse(t *testing.T, actualJSON []byte, expected []*TabletWithURL, path string) {
	var actual []TabletWithURL
	if err := json.Unmarshal(actualJSON, &actual); err != nil {
		t.Fatalf("Failed to unmarshal response for %s: %v\nResponse: %s", path, err, string(actualJSON))
	}

	if len(actual) != len(expected) {
		t.Fatalf("Length mismatch for %s: got %d, want %d", path, len(actual), len(expected))
	}

	for i, actualItem := range actual {
		actualJSON2, _ := json.Marshal(actualItem)
		expectedJSON, _ := json.Marshal(expected[i])
		if string(actualJSON2) != string(expectedJSON) {
			t.Fatalf("TabletWithURL comparison failed for %s[%d]:\nActual: %s\nExpected: %s", path, i, string(actualJSON2), string(expectedJSON))
		}
	}
}

func TestAPI(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cells := []string{"cell1", "cell2"}
	ts := memorytopo.NewServer(ctx, cells...)
	defer ts.Close()
	actionRepo := NewActionRepository(vtenv.NewTestEnv(), ts)
	server := testutils.HTTPTestServer()
	defer server.Close()

	ks1 := &topodatapb.Keyspace{
		DurabilityPolicy: policy.DurabilitySemiSync,
		SidecarDbName:    "_vt_sidecar_ks1",
		VtorcState: &vtorcdatapb.Keyspace{
			DisableEmergencyReparent: true,
		},
	}

	// Populate topo. Remove ServedTypes from shards to avoid ordering issues.
	ts.CreateKeyspace(ctx, "ks1", ks1)
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
	ts.SaveVSchema(ctx, &topo.KeyspaceVSchemaInfo{
		Name:     "ks1",
		Keyspace: vs,
	})

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

	// Define expected proto objects for responses
	expectedTablet1 := &TabletWithURL{
		Alias:         &topodatapb.TabletAlias{Cell: "cell1", Uid: 100},
		Hostname:      "mysql1-cell1.test.net",
		PortMap:       map[string]int32{"vt": 100},
		Keyspace:      "ks1",
		Shard:         "-80",
		KeyRange:      &topodatapb.KeyRange{Start: nil, End: []byte{0x80}},
		Type:          topodatapb.TabletType_REPLICA,
		MysqlHostname: "mysql1-cell1.test.net",
		MysqlPort:     3306,
		URL:           "http://mysql1-cell1.test.net:100",
	}

	expectedTablet2 := &TabletWithURL{
		Alias:         &topodatapb.TabletAlias{Cell: "cell2", Uid: 200},
		Hostname:      "mysql2-cell2.test.net",
		PortMap:       map[string]int32{"vt": 200},
		Keyspace:      "ks1",
		Shard:         "-80",
		KeyRange:      &topodatapb.KeyRange{Start: nil, End: []byte{0x80}},
		Type:          topodatapb.TabletType_REPLICA,
		MysqlHostname: "mysql2-cell2.test.net",
		MysqlPort:     3306,
		URL:           "http://mysql2-cell2.test.net:200",
	}

	expectedKeyspace1 := &topodatapb.Keyspace{
		KeyspaceType:     topodatapb.KeyspaceType_NORMAL,
		BaseKeyspace:     "",
		SnapshotTime:     nil,
		DurabilityPolicy: policy.DurabilitySemiSync,
		ThrottlerConfig:  nil,
		SidecarDbName:    "_vt_sidecar_ks1",
		VtorcState: &vtorcdatapb.Keyspace{
			DisableEmergencyReparent: true,
		},
	}

	expectedKeyspace2 := &topodatapb.Keyspace{
		KeyspaceType:     topodatapb.KeyspaceType_SNAPSHOT,
		BaseKeyspace:     "ks1",
		SnapshotTime:     &vttime.Time{Seconds: 1136214245},
		DurabilityPolicy: policy.DurabilityNone,
		ThrottlerConfig:  nil,
		SidecarDbName:    "_vt",
	}

	expectedShard := &topodatapb.Shard{
		PrimaryAlias:         nil,
		PrimaryTermStartTime: nil,
		KeyRange: &topodatapb.KeyRange{
			Start: nil,
			End:   []byte{0x80},
		},
		SourceShards:     []*topodatapb.Shard_SourceShard{},
		TabletControls:   []*topodatapb.Shard_TabletControl{},
		IsPrimaryServing: true,
		VtorcState:       nil,
	}

	// Test cases with proto-based expectations
	type testCase struct {
		method     string
		path       string
		body       string
		statusCode int
		// For proto-based tests
		expectedProto     proto.Message
		expectedProtoList []proto.Message
		msgType           proto.Message
		// For TabletWithURL-based tests
		expectedTabletWithURL     *TabletWithURL
		expectedTabletWithURLList []*TabletWithURL
		// For vtctl POST Output proto comparison
		expectedOutputProto proto.Message
		// For string-based tests (fallback)
		expectedString string
		useStringMatch bool
	}

	table := []testCase{
		// Create snapshot keyspace with durability policy specified
		{
			method:         "POST",
			path:           "vtctl/",
			body:           `["CreateKeyspace", "--keyspace_type=SNAPSHOT", "--base_keyspace=ks1", "--snapshot_time=2006-01-02T15:04:05+00:00", "--durability-policy=semi_sync", "--sidecar-db-name=_vt_sidecar_ks3", "ks3"]`,
			expectedString: `{"Error": "durability-policy cannot be specified while creating a snapshot keyspace","Output": ""}`,
			statusCode:     http.StatusOK,
			useStringMatch: true,
		},
		// Create snapshot keyspace using API
		{
			method:         "POST",
			path:           "vtctl/",
			body:           `["CreateKeyspace", "--keyspace_type=SNAPSHOT", "--base_keyspace=ks1", "--snapshot_time=2006-01-02T15:04:05+00:00", "ks3"]`,
			expectedString: `{"Error": "","Output": ""}`,
			statusCode:     http.StatusOK,
			useStringMatch: true,
		},

		// Cells
		{
			method:         "GET",
			path:           "cells",
			expectedString: `["cell1","cell2"]`,
			statusCode:     http.StatusOK,
			useStringMatch: true,
		},

		// Keyspace tablets - all tablets
		{
			method:                    "GET",
			path:                      "keyspace/ks1/tablets/",
			expectedTabletWithURLList: []*TabletWithURL{expectedTablet1, expectedTablet2},
			statusCode:                http.StatusOK,
		},
		// Keyspace tablets - specific shard
		{
			method:                    "GET",
			path:                      "keyspace/ks1/tablets/-80",
			expectedTabletWithURLList: []*TabletWithURL{expectedTablet1, expectedTablet2},
			statusCode:                http.StatusOK,
		},
		// Keyspace tablets - empty shard
		{
			method:         "GET",
			path:           "keyspace/ks1/tablets/80-",
			expectedString: `[]`,
			statusCode:     http.StatusOK,
			useStringMatch: true,
		},
		// Keyspace tablets - filtered by cells
		{
			method:                    "GET",
			path:                      "keyspace/ks1/tablets/?cells=cell1",
			expectedTabletWithURLList: []*TabletWithURL{expectedTablet1},
			statusCode:                http.StatusOK,
		},
		// Keyspace tablets - filtered by single cell
		{
			method:                    "GET",
			path:                      "keyspace/ks1/tablets/?cell=cell2",
			expectedTabletWithURLList: []*TabletWithURL{expectedTablet2},
			statusCode:                http.StatusOK,
		},

		// Keyspaces
		{
			method:         "GET",
			path:           "keyspaces",
			expectedString: `["ks1", "ks3"]`,
			statusCode:     http.StatusOK,
			useStringMatch: true,
		},
		{
			method:        "GET",
			path:          "keyspaces/ks1",
			expectedProto: expectedKeyspace1,
			statusCode:    http.StatusOK,
		},
		{
			method:         "GET",
			path:           "keyspaces/nonexistent",
			expectedString: "404 page not found",
			statusCode:     http.StatusNotFound,
			useStringMatch: true,
		},
		{
			method:         "POST",
			path:           "keyspaces/ks1?action=TestKeyspaceAction",
			expectedString: `{"Name": "TestKeyspaceAction","Parameters": "ks1","Output": "TestKeyspaceAction Result","Error": false}`,
			statusCode:     http.StatusOK,
			useStringMatch: true,
		},

		// Shards
		{
			method:         "GET",
			path:           "shards/ks1/",
			expectedString: `["-80","80-"]`,
			statusCode:     http.StatusOK,
			useStringMatch: true,
		},
		{
			method:        "GET",
			path:          "shards/ks1/-80",
			expectedProto: expectedShard,
			statusCode:    http.StatusOK,
		},
		{
			method:         "GET",
			path:           "shards/ks1/-DEAD",
			expectedString: "404 page not found",
			statusCode:     http.StatusNotFound,
			useStringMatch: true,
		},
		{
			method:         "POST",
			path:           "shards/ks1/-80?action=TestShardAction",
			expectedString: `{"Name": "TestShardAction","Parameters": "ks1/-80","Output": "TestShardAction Result","Error": false}`,
			statusCode:     http.StatusOK,
			useStringMatch: true,
		},

		// Tablets
		{
			method:         "GET",
			path:           "tablets/?shard=ks1%2F-80",
			expectedString: `[{"cell":"cell1","uid":100},{"cell":"cell2","uid":200}]`,
			statusCode:     http.StatusOK,
			useStringMatch: true,
		},
		{
			method:         "GET",
			path:           "tablets/?cell=cell1",
			expectedString: `[{"cell":"cell1","uid":100}]`,
			statusCode:     http.StatusOK,
			useStringMatch: true,
		},
		{
			method:         "GET",
			path:           "tablets/?shard=ks1%2F-80&cell=cell2",
			expectedString: `[{"cell":"cell2","uid":200}]`,
			statusCode:     http.StatusOK,
			useStringMatch: true,
		},
		{
			method:         "GET",
			path:           "tablets/?shard=ks1%2F80-&cell=cell1",
			expectedString: `[]`,
			statusCode:     http.StatusOK,
			useStringMatch: true,
		},
		{
			method:                "GET",
			path:                  "tablets/cell1-100",
			expectedTabletWithURL: expectedTablet1,
			statusCode:            http.StatusOK,
		},
		{
			method:         "GET",
			path:           "tablets/nonexistent-999",
			expectedString: "404 page not found",
			statusCode:     http.StatusNotFound,
			useStringMatch: true,
		},
		{
			method:         "POST",
			path:           "tablets/cell1-100?action=TestTabletAction",
			expectedString: `{"Name": "TestTabletAction","Parameters": "cell1-0000000100","Output": "TestTabletAction Result","Error": false}`,
			statusCode:     http.StatusOK,
			useStringMatch: true,
		},

		// vtctl RunCommand
		{
			method:              "POST",
			path:                "vtctl/",
			body:                `["GetKeyspace","ks1"]`,
			expectedOutputProto: expectedKeyspace1,
			statusCode:          http.StatusOK,
		},
		{
			method:              "POST",
			path:                "vtctl/",
			body:                `["GetKeyspace","ks3"]`,
			expectedOutputProto: expectedKeyspace2,
			statusCode:          http.StatusOK,
		},
		{
			method: "POST",
			path:   "vtctl/",
			body:   `["GetVSchema","ks3"]`,
			expectedOutputProto: &vschemapb.Keyspace{
				Sharded: true,
				Vindexes: map[string]*vschemapb.Vindex{
					"name1": {Type: "hash"},
				},
				Tables: map[string]*vschemapb.Table{
					"table1": {
						ColumnVindexes: []*vschemapb.ColumnVindex{{Column: "column1", Name: "name1"}},
					},
				},
				RequireExplicitRouting: true,
			},
			statusCode: http.StatusOK,
		},
		{
			method:         "POST",
			path:           "vtctl/",
			body:           `["GetKeyspace","does_not_exist"]`,
			expectedString: `{"Error": "node doesn't exist: keyspaces/does_not_exist/Keyspace","Output": ""}`,
			statusCode:     http.StatusOK,
			useStringMatch: true,
		},
		{
			method:         "POST",
			path:           "vtctl/",
			body:           `["Panic"]`,
			expectedString: `uncaught panic: this command panics on purpose`,
			statusCode:     http.StatusInternalServerError,
			useStringMatch: true,
		},
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

			if in.useStringMatch {
				// Fallback to string comparison for non-proto responses
				got := compactJSON(body)
				want := compactJSON([]byte(in.expectedString))
				if want == "" {
					// want is not valid JSON. Fallback to a string comparison.
					want = in.expectedString
					// For unknown reasons errors have a trailing "\n\t\t". Remove it.
					got = strings.TrimSpace(string(body))
				}
				// Use contains instead of prefix for more flexible matching
				if !strings.Contains(got, want) {
					t.Fatalf("For path [%v] got\n'%v', want to contain\n'%v'", in.path, got, want)
				}
			} else if in.expectedProto != nil {
				// Compare single proto object
				compareProtoResponse(t, body, in.expectedProto, in.path)
			} else if in.expectedProtoList != nil {
				// Compare list of proto objects
				compareProtoSliceResponse(t, body, in.expectedProtoList, in.msgType, in.path)
			} else if in.expectedTabletWithURL != nil {
				// Compare TabletWithURL object
				compareTabletWithURLResponse(t, body, in.expectedTabletWithURL, in.path)
			} else if in.expectedTabletWithURLList != nil {
				// Compare list of TabletWithURL objects
				compareTabletWithURLSliceResponse(t, body, in.expectedTabletWithURLList, in.path)
			} else if in.expectedOutputProto != nil {
				// Compare vtctl POST Output field as proto
				compareOutputProto(t, body, in.expectedOutputProto, in.path)
			}
		})
	}
}

// mustMarshalProto marshals a proto message to JSON, panicking on error
func mustMarshalProto(msg proto.Message) []byte {
	data, err := protojson.MarshalOptions{EmitUnpopulated: true, UseProtoNames: true, Multiline: true, UseEnumNumbers: true}.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return data
}

// Helper to compare Output field as proto
func compareOutputProto(t *testing.T, body []byte, expected proto.Message, path string) {
	var resp struct {
		Error  string `json:"Error"`
		Output string `json:"Output"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("Failed to unmarshal vtctl response for %s: %v\nResponse: %s", path, err, string(body))
	}
	if resp.Error != "" {
		t.Fatalf("Expected no error for %s, got: %s", path, resp.Error)
	}
	actual := proto.Clone(expected)
	proto.Reset(actual)
	if err := protojson.Unmarshal([]byte(resp.Output), actual); err != nil {
		t.Fatalf("Failed to unmarshal Output as proto for %s: %v\nOutput: %s", path, err, resp.Output)
	}
	if !proto.Equal(actual, expected) {
		actualJSON, _ := protojson.MarshalOptions{Multiline: true}.Marshal(actual)
		expectedJSON, _ := protojson.MarshalOptions{Multiline: true}.Marshal(expected)
		t.Fatalf("Proto Output comparison failed for %s:\nActual: %s\nExpected: %s", path, string(actualJSON), string(expectedJSON))
	}
}
