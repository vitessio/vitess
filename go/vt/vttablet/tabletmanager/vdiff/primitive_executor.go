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
	"vitess.io/vitess/go/vt/vterrors"
	vtgateEngine "vitess.io/vitess/go/vt/vtgate/engine"
)

/*
	VDiff gets data from multiple sources. There is a global ordering of all the data (based on PKs). However, that
	data is potentially spread across shards. The distribution is selected by the vindex (sharding key) and hence
	we need to globally sort the data across the shards to match the order on the target: we need the rows to be in
	order so that we can compare the data correctly.

	We leverage the merge sorting functionality that vtgate uses to return sorted data from a scatter query.
		* Merge sorter engine.Primitives are set up, one each for the source and target shards.
		* This merge sorter is embedded by a primitiveExecutor, which also contains a query result channel,
			and a list of rows, sorted for the specific shard. These rows have already been popped
			from the query result, but not yet compared since they do not yet contain the "topmost" row.
		* The result channel is populated by the shardStreamer, which satisfies the engine.StreamExecutor interface.
			The shardStreamer gets data using VStreamRows for that shard.
*/

// primitiveExecutor starts execution on the top level primitive
// and provides convenience functions for row-by-row iteration.
type primitiveExecutor struct {
	prim     vtgateEngine.Primitive
	rows     [][]sqltypes.Value
	resultch chan *sqltypes.Result
	err      error

	name string // for debug purposes only
}

func newPrimitiveExecutor(ctx context.Context, prim vtgateEngine.Primitive, name string) *primitiveExecutor {
	pe := &primitiveExecutor{
		prim:     prim,
		resultch: make(chan *sqltypes.Result, 1),
		name:     name,
	}
	vcursor := &contextVCursor{ctx: ctx}

	// handles each callback from the merge sorter, waits for a result set from the shard streamer and pushes it on the result channel
	go func() {
		defer close(pe.resultch)
		pe.err = vcursor.StreamExecutePrimitive(ctx, pe.prim, make(map[string]*querypb.BindVariable), true, func(qr *sqltypes.Result) error {

			select {
			case pe.resultch <- qr:
			case <-ctx.Done():
				return vterrors.Wrap(ctx.Err(), "Outer Stream")
			}
			return nil
		})
	}()
	return pe
}

// next gets the next row in the stream for this shard, if there's currently no rows to process in the stream then wait on the
// result channel for the shard streamer to produce them.
func (pe *primitiveExecutor) next() ([]sqltypes.Value, error) {
	for len(pe.rows) == 0 {
		qr, ok := <-pe.resultch
		if !ok {
			return nil, pe.err
		}
		pe.rows = qr.Rows
	}

	row := pe.rows[0]
	pe.rows = pe.rows[1:]
	return row, nil
}

// drain fastforward's a shard to process (and ignore) everything from its results stream and return a count of the
// discarded rows.
func (pe *primitiveExecutor) drain(ctx context.Context) (int64, error) {
	var count int64
	for {
		row, err := pe.next()
		if err != nil {
			return 0, err
		}
		if row == nil {
			return count, nil
		}
		count++
	}
}
