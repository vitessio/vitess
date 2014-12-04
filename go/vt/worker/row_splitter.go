// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"fmt"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"
)

// RowSplitter is a helper class to split rows into multiple
// subsets targeted to different shards.
type RowSplitter struct {
	Type       key.KeyspaceIdType
	ValueIndex int
	KeyRanges  []key.KeyRange
}

// NewRowSplitter returns a new row splitter for the given shard distribution.
func NewRowSplitter(shardInfos []*topo.ShardInfo, typ key.KeyspaceIdType, valueIndex int) *RowSplitter {
	result := &RowSplitter{
		Type:       typ,
		ValueIndex: valueIndex,
		KeyRanges:  make([]key.KeyRange, len(shardInfos)),
	}
	for i, si := range shardInfos {
		result.KeyRanges[i] = si.KeyRange
	}
	return result
}

// Split will split the rows into subset for each distribution
func (rs *RowSplitter) Split(rows [][]sqltypes.Value) ([][][]sqltypes.Value, error) {
	result := make([][][]sqltypes.Value, len(rs.KeyRanges))
	if rs.Type == key.KIT_UINT64 {
		for _, row := range rows {
			v := sqltypes.MakeNumeric(row[rs.ValueIndex].Raw())
			i, err := v.ParseUint64()
			if err != nil {
				return nil, fmt.Errorf("Non numerical value: %v", err)
			}
			k := key.Uint64Key(i).KeyspaceId()
			for i, kr := range rs.KeyRanges {
				if kr.Contains(k) {
					result[i] = append(result[i], row)
					break
				}
			}
		}
	} else {
		for _, row := range rows {
			k := key.KeyspaceId(row[rs.ValueIndex].Raw())
			for i, kr := range rs.KeyRanges {
				if kr.Contains(k) {
					result[i] = append(result[i], row)
					break
				}
			}
		}
	}
	return result, nil
}
