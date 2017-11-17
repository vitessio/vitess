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
	}, nil)
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
		sqltypes.NewVarBinary("Ignored Value"),
		sqltypes.NewVarBinary(fmt.Sprintf("%v", 0x1000000000000000)),
	}
	row1 := []sqltypes.Value{
		sqltypes.NewVarBinary("Ignored Value"),
		sqltypes.NewVarBinary(fmt.Sprintf("%v", 0x6000000000000000)),
	}
	row2 := []sqltypes.Value{
		sqltypes.NewVarBinary("Ignored Value"),
		sqltypes.NewVarBinary(fmt.Sprintf("%v", uint64(0xe000000000000000))),
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
	}, nil)
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
		sqltypes.NewVarBinary("Ignored Value"),
		sqltypes.NewVarBinary("A"),
	}
	row1 := []sqltypes.Value{
		sqltypes.NewVarBinary("Ignored Value"),
		sqltypes.NewVarBinary("G"),
	}
	row2 := []sqltypes.Value{
		sqltypes.NewVarBinary("Ignored Value"),
		sqltypes.NewVarBinary("Q"),
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
