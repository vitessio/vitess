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
	"net/http"
	"path"
	"reflect"
	"testing"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestHandleExplorerRedirect(t *testing.T) {
	ctx := context.Background()

	ts := memorytopo.NewServer("cell1")
	if err := ts.CreateTablet(ctx, &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  123,
		},
		Keyspace: "test_keyspace",
		Shard:    "123-456",
	}); err != nil {
		t.Fatalf("CreateTablet failed: %v", err)
	}

	table := map[string]string{
		"/explorers/redirect?type=keyspace&keyspace=test_keyspace":                         "/app/#/keyspaces/",
		"/explorers/redirect?type=shard&keyspace=test_keyspace&shard=-80":                  "/app/#/shard/test_keyspace/-80",
		"/explorers/redirect?type=srv_keyspace&keyspace=test_keyspace&cell=cell1":          "/app/#/keyspaces/",
		"/explorers/redirect?type=tablet&alias=cell1-123":                                  "/app/#/shard/test_keyspace/123-456",
		"/explorers/redirect?type=replication&keyspace=test_keyspace&shard=-80&cell=cell1": "/app/#/shard/test_keyspace/-80",
	}

	for input, want := range table {
		request, err := http.NewRequest("GET", input, nil)
		if err != nil {
			t.Fatalf("NewRequest error: %v", err)
		}
		if err := request.ParseForm(); err != nil {
			t.Fatalf("ParseForm error: %v", err)
		}
		got, err := handleExplorerRedirect(ctx, ts, request)
		if err != nil {
			t.Fatalf("handleExplorerRedirect error: %v", err)
		}
		if got != want {
			t.Errorf("handlExplorerRedirect(%#v) = %#v, want %#v", input, got, want)
		}
	}
}

// Test the explorer using MemoryTopo as a backend.
func TestHandlePathRoot(t *testing.T) {
	input := "/"
	cells := []string{"cell1", "cell2", "cell3"}
	want := []string{topo.GlobalCell, "cell1", "cell2", "cell3"}

	ts := memorytopo.NewServer(cells...)
	ex := newBackendExplorer(ts)
	result := ex.HandlePath(input, nil)
	if got := result.Children; !reflect.DeepEqual(got, want) {
		t.Errorf("HandlePath(%q) = %v, want %v", input, got, want)
	}
	if got := result.Error; got != "" {
		t.Errorf("HandlePath(%q).Error = %v", input, got)
	}
}

func TestHandlePathKeyspace(t *testing.T) {
	cells := []string{"cell1", "cell2", "cell3"}
	keyspace := &topodatapb.Keyspace{
		ShardingColumnName: "keyspace_id",
	}

	ctx := context.Background()
	ts := memorytopo.NewServer(cells...)
	if err := ts.CreateKeyspace(ctx, "test_keyspace", keyspace); err != nil {
		t.Fatalf("CreateKeyspace error: %v", err)
	}
	if err := ts.CreateShard(ctx, "test_keyspace", "10-20"); err != nil {
		t.Fatalf("CreateShard error: %v", err)
	}
	if err := ts.CreateShard(ctx, "test_keyspace", "20-30"); err != nil {
		t.Fatalf("CreateShard error: %v", err)
	}

	ex := newBackendExplorer(ts)

	// Test the Keyspace object itself.
	input := path.Join("/global", "keyspaces", "test_keyspace", "Keyspace")
	want := "sharding_column_name: \"keyspace_id\"\n"
	result := ex.HandlePath(input, nil)
	if got := result.Data; got != want {
		t.Errorf("HandlePath(%q) = %q, want %q", input, got, want)
	}
	if got, want := result.Children, []string(nil); !reflect.DeepEqual(got, want) {
		t.Errorf("Children = %v, want %v", got, want)
	}
	if got := result.Error; got != "" {
		t.Errorf("HandlePath(%q).Error = %v", input, got)
	}

	// Test the shards path.
	input = path.Join("/global", "keyspaces", "test_keyspace", "shards")
	result = ex.HandlePath(input, nil)
	want = ""
	if got := result.Data; got != want {
		t.Errorf("HandlePath(%q) = %q, want %q", input, got, want)
	}
	if got, want := result.Children, []string{"10-20", "20-30"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Children = %v, want %v", got, want)
	}
	if got := result.Error; got != "" {
		t.Errorf("HandlePath(%q).Error = %v", input, got)
	}
}

func TestHandlePathShard(t *testing.T) {
	input := path.Join("/global", "keyspaces", "test_keyspace", "shards", "-80", "Shard")
	cells := []string{"cell1", "cell2", "cell3"}
	keyspace := &topodatapb.Keyspace{}
	want := "is_master_serving: true\n"

	ctx := context.Background()
	ts := memorytopo.NewServer(cells...)
	if err := ts.CreateKeyspace(ctx, "test_keyspace", keyspace); err != nil {
		t.Fatalf("CreateKeyspace error: %v", err)
	}
	if err := ts.CreateShard(ctx, "test_keyspace", "-80"); err != nil {
		t.Fatalf("CreateShard error: %v", err)
	}
	if _, err := ts.UpdateShardFields(ctx, "test_keyspace", "-80", func(si *topo.ShardInfo) error {
		// Set cells, reset other fields so printout is easier to compare.
		si.KeyRange = nil
		return nil
	}); err != nil {
		t.Fatalf("UpdateShardFields error: %v", err)
	}

	ex := newBackendExplorer(ts)
	result := ex.HandlePath(input, nil)
	if got := result.Data; got != want {
		t.Errorf("HandlePath(%q) = %q, want %q", input, got, want)
	}
	if got, want := result.Children, []string(nil); !reflect.DeepEqual(got, want) {
		t.Errorf("Children = %v, want %v", got, want)
	}
	if got := result.Error; got != "" {
		t.Errorf("HandlePath(%q).Error = %v", input, got)
	}
}

func TestHandlePathTablet(t *testing.T) {
	input := path.Join("/cell1", "tablets", "cell1-0000000123", "Tablet")
	cells := []string{"cell1", "cell2", "cell3"}
	tablet := &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "cell1", Uid: 123},
		Hostname: "example.com",
		PortMap:  map[string]int32{"vt": 4321},
	}
	want := "alias: <\n  cell: \"cell1\"\n  uid: 123\n>\nhostname: \"example.com\"\nport_map: <\n  key: \"vt\"\n  value: 4321\n>\n"

	ctx := context.Background()
	ts := memorytopo.NewServer(cells...)
	if err := ts.CreateTablet(ctx, tablet); err != nil {
		t.Fatalf("CreateTablet error: %v", err)
	}

	ex := newBackendExplorer(ts)
	result := ex.HandlePath(input, nil)
	if got := result.Data; got != want {
		t.Errorf("HandlePath(%q) = %q, want %q", input, got, want)
	}
	if got, want := result.Children, []string(nil); !reflect.DeepEqual(got, want) {
		t.Errorf("Children = %v, want %v", got, want)
	}
	if got := result.Error; got != "" {
		t.Errorf("HandlePath(%q).Error = %v", input, got)
	}
}

func TestHandleBadPath(t *testing.T) {
	input := "/foo"
	cells := []string{"cell1", "cell2", "cell3"}
	want := "Invalid cell: node doesn't exist: cells/foo/CellInfo"

	ts := memorytopo.NewServer(cells...)
	ex := newBackendExplorer(ts)
	result := ex.HandlePath(input, nil)
	if got := result.Error; !reflect.DeepEqual(got, want) {
		t.Errorf("HandlePath(%q) = %v, want %v", input, got, want)
	}
}
