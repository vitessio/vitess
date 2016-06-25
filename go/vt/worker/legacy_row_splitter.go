// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

// TODO(mberlin): Remove this file when SplitClone supports merge-sorting
// primary key columns based on the MySQL collation.

import (
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// RowSplitter is a helper class to split rows into multiple
// subsets targeted to different shards.
type RowSplitter struct {
	KeyResolver keyspaceIDResolver
	KeyRanges   []*topodatapb.KeyRange
}

// NewRowSplitter returns a new row splitter for the given shard distribution.
func NewRowSplitter(shardInfos []*topo.ShardInfo, keyResolver keyspaceIDResolver) *RowSplitter {
	result := &RowSplitter{
		KeyResolver: keyResolver,
		KeyRanges:   make([]*topodatapb.KeyRange, len(shardInfos)),
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
	for _, row := range rows {
		k, err := rs.KeyResolver.keyspaceID(row)
		if err != nil {
			return err
		}
		for i, kr := range rs.KeyRanges {
			if key.KeyRangeContains(kr, k) {
				result[i] = append(result[i], row)
				break
			}
		}
	}
	return nil
}

// Send will send the rows to the list of channels. Returns true if aborted.
func (rs *RowSplitter) Send(fields []*querypb.Field, result [][][]sqltypes.Value, baseCmds []string, insertChannels []chan string, abort <-chan struct{}) bool {
	for i, c := range insertChannels {
		// one of the chunks might be empty, so no need
		// to send data in that case
		if len(result[i]) > 0 {
			cmd := baseCmds[i] + makeValueString(fields, result[i])
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
