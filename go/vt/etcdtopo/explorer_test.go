/*
Copyright 2017 Google Inc.

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

package etcdtopo

import (
	"encoding/json"
	"path"
	"reflect"
	"strings"
	"testing"

	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/topo"
)

func toJSON(t *testing.T, value interface{}) string {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatalf("cannot JSON encode: %v", err)
	}
	return string(data)
}

func TestSplitCellPath(t *testing.T) {
	table := map[string][]string{
		"/cell-a":            {"cell-a", "/"},
		"/cell-b/x":          {"cell-b", "/x"},
		"/cell1/other/stuff": {"cell1", "/other/stuff"},
	}
	for input, want := range table {
		cell, rest, err := splitCellPath(input)
		if err != nil {
			t.Errorf("splitCellPath error: %v", err)
		}
		if cell != want[0] || rest != want[1] {
			t.Errorf("splitCellPath(%q) = (%q, %q), want (%q, %q)",
				input, cell, rest, want[0], want[1])
		}
	}
}

func TestSplitShardDirPath(t *testing.T) {
	// Make sure keyspace/shard names are preserved through a "round-trip".
	input := shardDirPath("my-keyspace", "my-shard")
	keyspace, shard, err := splitShardDirPath(input)
	if err != nil {
		t.Errorf("splitShardDirPath error: %v", err)
	}
	if keyspace != "my-keyspace" || shard != "my-shard" {
		t.Errorf("splitShardDirPath(%q) = (%q, %q), want (%q, %q)",
			input, keyspace, shard, "my-keyspace", "my-shard")
	}
}

func TestHandlePathInvalid(t *testing.T) {
	// Don't panic!
	ex := NewExplorer(nil)
	result := ex.HandlePath("xxx", nil)
	if want := "invalid"; !strings.Contains(result.Error, want) {
		t.Errorf("HandlePath returned wrong error: got %q, want %q", result.Error, want)
	}
}

func testHandlePathRoot(t *testing.T, ts *Server) {
	t.Log("=== testHandlePathRoot")
	input := "/"
	want := []string{topo.GlobalCell, "cell1", "cell2", "cell3"}

	ex := NewExplorer(ts)
	result := ex.HandlePath(input, nil)
	if got := result.Children; !reflect.DeepEqual(got, want) {
		t.Errorf("HandlePath(%q) = %v, want %v", input, got, want)
	}
}

func testHandlePathKeyspace(t *testing.T, ts *Server) {
	t.Log("=== testHandlePathKeyspace")
	input := path.Join("/global", keyspaceDirPath("test_keyspace"))
	keyspace := &topodatapb.Keyspace{}
	shard := &topodatapb.Shard{}
	want := toJSON(t, keyspace)

	ctx := context.Background()
	if err := ts.CreateKeyspace(ctx, "test_keyspace", keyspace); err != nil {
		t.Fatalf("CreateKeyspace error: %v", err)
	}
	if err := ts.CreateShard(ctx, "test_keyspace", "10-20", shard); err != nil {
		t.Fatalf("CreateShard error: %v", err)
	}
	if err := ts.CreateShard(ctx, "test_keyspace", "20-30", shard); err != nil {
		t.Fatalf("CreateShard error: %v", err)
	}

	ex := NewExplorer(ts)
	result := ex.HandlePath(input, nil)
	if got := result.Data; got != want {
		t.Errorf("HandlePath(%q) = %q, want %q", input, got, want)
	}
	if got, want := result.Children, []string{"10-20", "20-30"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Children = %v, want %v", got, want)
	}
}

func testHandlePathShard(t *testing.T, ts *Server) {
	t.Log("=== testHandlePathShard")
	input := path.Join("/global", shardDirPath("test_keyspace", "-80"))
	keyspace := &topodatapb.Keyspace{}
	shard := &topodatapb.Shard{}
	want := toJSON(t, shard)

	ctx := context.Background()
	if err := ts.CreateKeyspace(ctx, "test_keyspace", keyspace); err != nil {
		t.Fatalf("CreateKeyspace error: %v", err)
	}
	if err := ts.CreateShard(ctx, "test_keyspace", "-80", shard); err != nil {
		t.Fatalf("CreateShard error: %v", err)
	}

	ex := NewExplorer(ts)
	result := ex.HandlePath(input, nil)
	if got := result.Data; got != want {
		t.Errorf("HandlePath(%q) = %q, want %q", input, got, want)
	}
}

func testHandlePathTablet(t *testing.T, ts *Server) {
	t.Log("=== testHandlePathTablet")
	input := path.Join("/cell1", path.Join(tabletsDirPath, "cell1-0000000123"))
	tablet := &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "cell1", Uid: 123},
		Hostname: "example.com",
		PortMap:  map[string]int32{"vt": 4321},
	}
	want := toJSON(t, tablet)

	ctx := context.Background()
	if err := ts.CreateTablet(ctx, tablet); err != nil {
		t.Fatalf("CreateTablet error: %v", err)
	}

	ex := NewExplorer(ts)
	result := ex.HandlePath(input, nil)
	if got := result.Data; got != want {
		t.Errorf("HandlePath(%q) = %q, want %q", input, got, want)
	}
}
