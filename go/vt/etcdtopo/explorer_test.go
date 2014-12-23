// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcdtopo

import (
	"html/template"
	"net/http"
	"path"
	"reflect"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/jscfg"
	"github.com/youtube/vitess/go/vt/topo"
)

func TestSplitCellPath(t *testing.T) {
	table := map[string][]string{
		"/cell-a":            []string{"cell-a", "/"},
		"/cell-b/x":          []string{"cell-b", "/x"},
		"/cell1/other/stuff": []string{"cell1", "/other/stuff"},
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
	result := ex.HandlePath(nil, "xxx", nil)
	exResult := result.(*explorerResult)
	if want := "invalid"; !strings.Contains(exResult.Error, want) {
		t.Errorf("HandlePath returned wrong error: got %q, want %q", exResult.Error, want)
	}
}

func TestHandlePathRoot(t *testing.T) {
	input := explorerRoot
	cells := []string{"cell1", "cell2", "cell3"}
	want := []string{"global", "cell1", "cell2", "cell3"}

	ts := newTestServer(t, cells)
	ex := NewExplorer(ts)
	result := ex.HandlePath(nil, input, nil)
	exResult := result.(*explorerResult)
	if got := exResult.Children; !reflect.DeepEqual(got, want) {
		t.Errorf("HandlePath(%q) = %v, want %v", input, got, want)
	}
}

func TestHandlePathKeyspace(t *testing.T) {
	input := path.Join(explorerRoot, "global", keyspaceDirPath("test_keyspace"))
	cells := []string{"cell1", "cell2", "cell3"}
	keyspace := &topo.Keyspace{}
	shard := &topo.Shard{}
	want := jscfg.ToJson(keyspace)

	ts := newTestServer(t, cells)
	if err := ts.CreateKeyspace("test_keyspace", keyspace); err != nil {
		t.Fatalf("CreateKeyspace error: %v", err)
	}
	if err := ts.CreateShard("test_keyspace", "10-20", shard); err != nil {
		t.Fatalf("CreateShard error: %v", err)
	}
	if err := ts.CreateShard("test_keyspace", "20-30", shard); err != nil {
		t.Fatalf("CreateShard error: %v", err)
	}

	m := &mockActionRepo{}
	ex := NewExplorer(ts)
	result := ex.HandlePath(m, input, nil)
	exResult := result.(*explorerResult)
	if got := exResult.Data; got != want {
		t.Errorf("HandlePath(%q) = %q, want %q", input, got, want)
	}
	if got, want := exResult.Children, []string{"10-20", "20-30"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Children = %v, want %v", got, want)
	}
	if m.keyspaceActions == nil {
		t.Errorf("ActionRepository.PopulateKeyspaceActions not called")
	}
	if m.keyspace != "test_keyspace" {
		t.Errorf("ActionRepository called with keyspace %q, want %q", m.keyspace, "test_keyspace")
	}
}

func TestHandlePathShard(t *testing.T) {
	input := path.Join(explorerRoot, "global", shardDirPath("test_keyspace", "-80"))
	cells := []string{"cell1", "cell2", "cell3"}
	keyspace := &topo.Keyspace{}
	shard := &topo.Shard{}
	want := jscfg.ToJson(shard)

	ts := newTestServer(t, cells)
	if err := ts.CreateKeyspace("test_keyspace", keyspace); err != nil {
		t.Fatalf("CreateKeyspace error: %v", err)
	}
	if err := ts.CreateShard("test_keyspace", "-80", shard); err != nil {
		t.Fatalf("CreateShard error: %v", err)
	}

	m := &mockActionRepo{}
	ex := NewExplorer(ts)
	result := ex.HandlePath(m, input, nil)
	exResult := result.(*explorerResult)
	if got := exResult.Data; got != want {
		t.Errorf("HandlePath(%q) = %q, want %q", input, got, want)
	}
	if m.shardActions == nil {
		t.Errorf("ActionRepository.PopulateShardActions not called")
	}
	if m.keyspace != "test_keyspace" {
		t.Errorf("ActionRepository called with keyspace %q, want %q", m.keyspace, "test_keyspace")
	}
	if m.shard != "-80" {
		t.Errorf("ActionRepository called with shard %q, want %q", m.shard, "-80")
	}
}

func TestHandlePathTablet(t *testing.T) {
	input := path.Join(explorerRoot, "cell1", tabletDirPath("cell1-0000000123"))
	cells := []string{"cell1", "cell2", "cell3"}
	tablet := &topo.Tablet{
		Alias:    topo.TabletAlias{Cell: "cell1", Uid: 123},
		Hostname: "example.com",
		Portmap:  map[string]int{"vt": 4321},
	}
	want := jscfg.ToJson(tablet)

	ts := newTestServer(t, cells)
	if err := ts.CreateTablet(tablet); err != nil {
		t.Fatalf("CreateTablet error: %v", err)
	}

	m := &mockActionRepo{}
	ex := NewExplorer(ts)
	result := ex.HandlePath(m, input, nil)
	exResult := result.(*explorerResult)
	if got := exResult.Data; got != want {
		t.Errorf("HandlePath(%q) = %q, want %q", input, got, want)
	}
	wantLinks := map[string]template.URL{
		"status": template.URL("http://example.com:4321/debug/status"),
	}
	for k, want := range wantLinks {
		if got := exResult.Links[k]; got != want {
			t.Errorf("Links[%q] = %v, want %v", k, got, want)
		}
	}
	if m.tabletActions == nil {
		t.Errorf("ActionRepository.PopulateTabletActions not called")
	}
	if m.tablet != "cell1-0000000123" {
		t.Errorf("ActionRepository called with tablet %q, want %q", m.tablet, "cell1-0000000123")
	}
}

type mockActionRepo struct {
	keyspace, shard, tablet                      string
	keyspaceActions, shardActions, tabletActions map[string]template.URL
}

func (m *mockActionRepo) PopulateKeyspaceActions(actions map[string]template.URL, keyspace string) {
	m.keyspace = keyspace
	m.keyspaceActions = actions
}
func (m *mockActionRepo) PopulateShardActions(actions map[string]template.URL, keyspace, shard string) {
	m.keyspace = keyspace
	m.shard = shard
	m.shardActions = actions
}
func (m *mockActionRepo) PopulateTabletActions(actions map[string]template.URL, tablet string, r *http.Request) {
	m.tablet = tablet
	m.tabletActions = actions
}
