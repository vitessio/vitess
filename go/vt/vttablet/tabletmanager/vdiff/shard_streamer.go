/*
Copyright 2022 The Vitess Authors.

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

package vdiff

import (
	"context"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

// shardStreamer streams rows from one shard.
// shardStreamer satisfies engine.StreamExecutor, and can be added to Primitives of engine.MergeSort.
// Each tableDiffer has one shardStreamer per source and one for the target.
type shardStreamer struct {
	tablet *topodatapb.Tablet // tablet currently picked to stream from
	shard  string

	snapshotPosition string                // gtid set of the current snapshot which is being streamed from
	result           chan *sqltypes.Result // the next row is sent to this channel and received by the comparator
	err              error
}

// StreamExecute implements the StreamExecutor interface of the Primitive executor and
// it simply waits for a result to be available for this shard and sends it to the merge sorter.
func (sm *shardStreamer) StreamExecute(ctx context.Context, vcursor engine.VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	for result := range sm.result {
		if err := callback(result); err != nil {
			return err
		}
	}
	return sm.err
}
