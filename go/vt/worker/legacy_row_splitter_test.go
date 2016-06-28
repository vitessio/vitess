// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

// TODO(mberlin): Remove this file when SplitClone supports merge-sorting
// primary key columns based on the MySQL collation.

import (
	"encoding/hex"
	"fmt"
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/topo"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func hki(hexValue string) []byte {
	k, err := hex.DecodeString(hexValue)
	if err != nil {
		panic(err)
	}
	return k
}

func si(start, end string) *topo.ShardInfo {
	s := hki(start)
	e := hki(end)
	return topo.NewShardInfo("keyspace", start+"-"+end, &topodatapb.Shard{
		KeyRange: &topodatapb.KeyRange{
			Start: s,
			End:   e,
		},
	}, 0)
}

func TestRowSplitterUint64(t *testing.T) {
	shards := []*topo.ShardInfo{
		si("", "40"),
		si("40", "c0"),
		si("c0", ""),
	}
	ks := &topodatapb.Keyspace{ShardingColumnType: topodatapb.KeyspaceIdType_UINT64}
	ki := &topo.KeyspaceInfo{Keyspace: ks}
	resolver := &v2Resolver{ki, 1}
	rs := NewRowSplitter(shards, resolver)

	// rows in different shards
	row0 := []sqltypes.Value{
		sqltypes.MakeString([]byte("Ignored Value")),
		sqltypes.MakeString([]byte(fmt.Sprintf("%v", 0x1000000000000000))),
	}
	row1 := []sqltypes.Value{
		sqltypes.MakeString([]byte("Ignored Value")),
		sqltypes.MakeString([]byte(fmt.Sprintf("%v", 0x6000000000000000))),
	}
	row2 := []sqltypes.Value{
		sqltypes.MakeString([]byte("Ignored Value")),
		sqltypes.MakeString([]byte(fmt.Sprintf("%v", uint64(0xe000000000000000)))),
	}

	// basic split
	rows := [][]sqltypes.Value{row0, row1, row2, row2, row1, row2, row0}
	result := rs.StartSplit()
	if err := rs.Split(result, rows); err != nil {
		t.Fatalf("Split failed: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("Bad column count: %v", rows)
	}
	if !reflect.DeepEqual(result[0], [][]sqltypes.Value{row0, row0}) {
		t.Fatalf("Bad result[0]: %v", result[0])
	}
	if !reflect.DeepEqual(result[1], [][]sqltypes.Value{row1, row1}) {
		t.Fatalf("Bad result[1]: %v", result[1])
	}
	if !reflect.DeepEqual(result[2], [][]sqltypes.Value{row2, row2, row2}) {
		t.Fatalf("Bad result[2]: %v", result[2])
	}
}

func siBytes(start, end string) *topo.ShardInfo {
	s := hex.EncodeToString([]byte(start))
	e := hex.EncodeToString([]byte(end))
	return topo.NewShardInfo("keyspace", s+"-"+e, &topodatapb.Shard{
		KeyRange: &topodatapb.KeyRange{
			Start: []byte(start),
			End:   []byte(end),
		},
	}, 0)
}

func TestRowSplitterString(t *testing.T) {
	shards := []*topo.ShardInfo{
		siBytes("", "E"),
		siBytes("E", "L"),
		siBytes("L", ""),
	}
	ks := &topodatapb.Keyspace{ShardingColumnType: topodatapb.KeyspaceIdType_BYTES}
	ki := &topo.KeyspaceInfo{Keyspace: ks}
	resolver := &v2Resolver{ki, 1}
	rs := NewRowSplitter(shards, resolver)

	// rows in different shards
	row0 := []sqltypes.Value{
		sqltypes.MakeString([]byte("Ignored Value")),
		sqltypes.MakeString([]byte("A")),
	}
	row1 := []sqltypes.Value{
		sqltypes.MakeString([]byte("Ignored Value")),
		sqltypes.MakeString([]byte("G")),
	}
	row2 := []sqltypes.Value{
		sqltypes.MakeString([]byte("Ignored Value")),
		sqltypes.MakeString([]byte("Q")),
	}

	// basic split
	rows := [][]sqltypes.Value{row0, row1, row2, row2, row1, row2, row0}
	result := rs.StartSplit()
	if err := rs.Split(result, rows); err != nil {
		t.Fatalf("Split failed: %v", err)
	}
	if len(result) != 3 {
		t.Fatalf("Bad column count: %v", rows)
	}
	if !reflect.DeepEqual(result[0], [][]sqltypes.Value{row0, row0}) {
		t.Fatalf("Bad result[0]: %v", result[0])
	}
	if !reflect.DeepEqual(result[1], [][]sqltypes.Value{row1, row1}) {
		t.Fatalf("Bad result[1]: %v", result[1])
	}
	if !reflect.DeepEqual(result[2], [][]sqltypes.Value{row2, row2, row2}) {
		t.Fatalf("Bad result[2]: %v", result[2])
	}
}
