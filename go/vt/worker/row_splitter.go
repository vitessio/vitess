// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"fmt"

	mproto "github.com/youtube/vitess/go/mysql/proto"
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

// StartSplit starts a new split. Split can then be called multiple times.
func (rs *RowSplitter) StartSplit() [][][]sqltypes.Value {
	return make([][][]sqltypes.Value, len(rs.KeyRanges))
}

// Split will split the rows into subset for each distribution
func (rs *RowSplitter) Split(result [][][]sqltypes.Value, rows [][]sqltypes.Value) error {
	if rs.Type == key.KIT_UINT64 {
		for _, row := range rows {
			v := sqltypes.MakeNumeric(row[rs.ValueIndex].Raw())
			i, err := v.ParseUint64()
			if err != nil {
				return fmt.Errorf("Non numerical value: %v", err)
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
	return nil
}

// Send will send the rows to the list of channels. Returns true if aborted.
func (rs *RowSplitter) Send(fields []mproto.Field, result [][][]sqltypes.Value, baseCmd string, insertChannels []chan string, abort <-chan struct{}) bool {
	for i, c := range insertChannels {
		// one of the chunks might be empty, so no need
		// to send data in that case
		if len(result[i]) > 0 {
			cmd := baseCmd + makeValueString(fields, result[i])
			// also check on abort, so we don't wait forever
			select {
			case c <- cmd:
			case <-abort:
				return true
			}
		}
	}
	return false
}
