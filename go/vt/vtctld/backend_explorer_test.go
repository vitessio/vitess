package vtctld

import (
	"context"
	"path"
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// Test the explorer using MemoryTopo as a backend.

func TestHandlePathRoot(t *testing.T) {
	input := "/"
	cells := []string{"cell1", "cell2", "cell3"}
	want := []string{topo.GlobalCell, "cell1", "cell2", "cell3"}

	ts := memorytopo.New(cells...)
	ex := NewBackendExplorer(ts)
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
	shard := &topodatapb.Shard{}

	ctx := context.Background()
	ts := memorytopo.New(cells...)
	if err := ts.CreateKeyspace(ctx, "test_keyspace", keyspace); err != nil {
		t.Fatalf("CreateKeyspace error: %v", err)
	}
	if err := ts.CreateShard(ctx, "test_keyspace", "10-20", shard); err != nil {
		t.Fatalf("CreateShard error: %v", err)
	}
	if err := ts.CreateShard(ctx, "test_keyspace", "20-30", shard); err != nil {
		t.Fatalf("CreateShard error: %v", err)
	}

	ex := NewBackendExplorer(ts)

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
	shard := &topodatapb.Shard{
		Cells: []string{"cell1", "cell2", "cell3"},
	}
	want := "cells: \"cell1\"\ncells: \"cell2\"\ncells: \"cell3\"\n"

	ctx := context.Background()
	ts := memorytopo.New(cells...)
	if err := ts.CreateKeyspace(ctx, "test_keyspace", keyspace); err != nil {
		t.Fatalf("CreateKeyspace error: %v", err)
	}
	if err := ts.CreateShard(ctx, "test_keyspace", "-80", shard); err != nil {
		t.Fatalf("CreateShard error: %v", err)
	}

	ex := NewBackendExplorer(ts)
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
	ts := memorytopo.New(cells...)
	if err := ts.CreateTablet(ctx, tablet); err != nil {
		t.Fatalf("CreateTablet error: %v", err)
	}

	ex := NewBackendExplorer(ts)
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
	want := "node doesn't exist"

	ts := memorytopo.New(cells...)
	ex := NewBackendExplorer(ts)
	result := ex.HandlePath(input, nil)
	if got := result.Error; !reflect.DeepEqual(got, want) {
		t.Errorf("HandlePath(%q) = %v, want %v", input, got, want)
	}
}
