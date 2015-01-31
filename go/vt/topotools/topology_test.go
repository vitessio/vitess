// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package topotools

import (
	"reflect"
	"sort"
	"testing"

	"github.com/youtube/vitess/go/vt/topo"
)

func TestTabletNodeShortName(t *testing.T) {
	table := map[TabletNode]string{
		TabletNode{Host: "hostname", Port: 0}:          "hostname",
		TabletNode{Host: "hostname", Port: 123}:        "hostname:123",
		TabletNode{Host: "hostname.domain", Port: 456}: "hostname:456",
		TabletNode{Host: "12.34.56.78", Port: 555}:     "12.34.56.78:555",
		TabletNode{Host: "::1", Port: 789}:             "[::1]:789",
	}
	for input, want := range table {
		if got := input.ShortName(); got != want {
			t.Errorf("ShortName(%v:%v) = %q, want %q", input.Host, input.Port, got, want)
		}
	}
}

func TestNumericShardNodesList(t *testing.T) {
	input := numericShardNodesList{
		&ShardNodes{Name: "3"},
		&ShardNodes{Name: "5"},
		&ShardNodes{Name: "4"},
		&ShardNodes{Name: "7"},
		&ShardNodes{Name: "10"},
		&ShardNodes{Name: "1"},
		&ShardNodes{Name: "0"},
	}
	want := numericShardNodesList{
		&ShardNodes{Name: "0"},
		&ShardNodes{Name: "1"},
		&ShardNodes{Name: "3"},
		&ShardNodes{Name: "4"},
		&ShardNodes{Name: "5"},
		&ShardNodes{Name: "7"},
		&ShardNodes{Name: "10"},
	}
	sort.Sort(input)
	if !reflect.DeepEqual(input, want) {
		t.Errorf("Sort(numericShardNodesList) failed")
	}
}

func TestRangeShardNodesList(t *testing.T) {
	input := rangeShardNodesList{
		&ShardNodes{Name: "50-60"},
		&ShardNodes{Name: "80-"},
		&ShardNodes{Name: "70-80"},
		&ShardNodes{Name: "30-40"},
		&ShardNodes{Name: "-10"},
	}
	want := rangeShardNodesList{
		&ShardNodes{Name: "-10"},
		&ShardNodes{Name: "30-40"},
		&ShardNodes{Name: "50-60"},
		&ShardNodes{Name: "70-80"},
		&ShardNodes{Name: "80-"},
	}
	sort.Sort(input)
	if !reflect.DeepEqual(input, want) {
		t.Errorf("Sort(rangeShardNodesList) failed")
	}
}

func TestKeyspaceNodesTabletTypes(t *testing.T) {
	input := KeyspaceNodes{
		ShardNodes: []*ShardNodes{
			&ShardNodes{TabletNodes: TabletNodesByType{topo.TYPE_REPLICA: nil}},
			&ShardNodes{TabletNodes: TabletNodesByType{topo.TYPE_MASTER: nil, topo.TYPE_REPLICA: nil}},
		},
	}
	want := topo.MakeStringTypeList([]topo.TabletType{topo.TYPE_REPLICA, topo.TYPE_MASTER})
	got := topo.MakeStringTypeList(input.TabletTypes())
	if !reflect.DeepEqual(got, want) {
		t.Errorf("KeyspaceNodes.TabletTypes() = %v, want %v", got, want)
	}
}
