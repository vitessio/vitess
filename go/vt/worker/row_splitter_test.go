// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"
)

func hki(hexValue string) key.KeyspaceId {
	k, err := key.HexKeyspaceId(hexValue).Unhex()
	if err != nil {
		panic(err)
	}
	return k
}

func si(start, end string) *topo.ShardInfo {
	s := hki(start)
	e := hki(end)
	return topo.NewShardInfo("keyspace", s.String()+"-"+e.String(), &topo.Shard{
		KeyRange: key.KeyRange{
			Start: s,
			End:   e,
		},
	}, 0)
}

func TestRowSplitter(t *testing.T) {
	shards := []*topo.ShardInfo{
		si("", "40"),
		si("40", "c0"),
		si("c0", ""),
	}
	rs := NewRowSplitter(shards, key.KIT_UINT64, 1)

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
	result, err := rs.Split(rows)
	if err != nil {
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
