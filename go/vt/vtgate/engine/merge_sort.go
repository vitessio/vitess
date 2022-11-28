/*
Copyright 2019 The Vitess Authors.

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

package engine

import (
	"container/heap"
	"context"
	"io"

	"vitess.io/vitess/go/mysql"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// StreamExecutor is a subset of Primitive that MergeSort
// requires its inputs to satisfy.
type StreamExecutor interface {
	StreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error
}

var _ Primitive = (*MergeSort)(nil)

// MergeSort performs a merge-sort of rows returned by each Input. This should
// only be used for StreamExecute. One row from each stream is added to the
// merge-sorter heap. Every time a value is pulled out of the heap,
// a new value is added to it from the stream that was the source of the value that
// was pulled out. Since the input streams are sorted the same way that the heap is
// sorted, this guarantees that the merged stream will also be sorted the same way.
// MergeSort only supports the StreamExecute function of a Primitive. So, it cannot
// be used like other Primitives in VTGate. However, it satisfies the Primitive API
// so that vdiff can use it. In that situation, only StreamExecute is used.
type MergeSort struct {
	Primitives              []StreamExecutor
	OrderBy                 []OrderByParams
	ScatterErrorsAsWarnings bool
	noInputs
	noTxNeeded
}

// RouteType satisfies Primitive.
func (ms *MergeSort) RouteType() string { return "MergeSort" }

// GetKeyspaceName satisfies Primitive.
func (ms *MergeSort) GetKeyspaceName() string { return "" }

// GetTableName satisfies Primitive.
func (ms *MergeSort) GetTableName() string { return "" }

// TryExecute is not supported.
func (ms *MergeSort) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] Execute is not reachable")
}

// GetFields is not supported.
func (ms *MergeSort) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] GetFields is not reachable")
}

// TryStreamExecute performs a streaming exec.
func (ms *MergeSort) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()
	gotFields := wantfields
	handles := make([]*streamHandle, len(ms.Primitives))
	for i, input := range ms.Primitives {
		handles[i] = runOneStream(ctx, vcursor, input, bindVars, gotFields)
		if !ms.ScatterErrorsAsWarnings {
			// we only need the fields from the first input, unless we allow ScatterErrorsAsWarnings.
			// in that case, we need to ask all the inputs for fields - we don't know which will return anything
			gotFields = false
		}
	}

	if wantfields {
		err := ms.getStreamingFields(handles, callback)
		if err != nil {
			return err
		}
	}

	comparers := extractSlices(ms.OrderBy)
	sh := &scatterHeap{
		rows:      make([]streamRow, 0, len(handles)),
		comparers: comparers,
	}

	var errs []error
	// Prime the heap. One element must be pulled from
	// each stream.
	for i, handle := range handles {
		select {
		case row, ok := <-handle.row:
			if !ok {
				if handle.err != nil {
					if ms.ScatterErrorsAsWarnings {
						errs = append(errs, handle.err)
						break
					}
					return handle.err
				}
				// It's possible that a stream returns no rows.
				// If so, don't add anything to the heap.
				continue
			}
			sh.rows = append(sh.rows, streamRow{row: row, id: i})
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	heap.Init(sh)
	if sh.err != nil {
		return sh.err
	}

	// Iterate one row at a time:
	// Pop a row from the heap and send it out.
	// Then pull the next row from the stream the popped
	// row came from and push it into the heap.
	for len(sh.rows) != 0 {
		sr := heap.Pop(sh).(streamRow)
		if sh.err != nil {
			// Unreachable: This should never fail.
			return sh.err
		}
		if err := callback(&sqltypes.Result{Rows: [][]sqltypes.Value{sr.row}}); err != nil {
			return err
		}

		select {
		case row, ok := <-handles[sr.id].row:
			if !ok {
				if handles[sr.id].err != nil {
					return handles[sr.id].err
				}
				continue
			}
			sr.row = row
			heap.Push(sh, sr)
			if sh.err != nil {
				return sh.err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	err := vterrors.Aggregate(errs)
	if err != nil && ms.ScatterErrorsAsWarnings && len(errs) < len(handles) {
		// we got errors, but not all shards failed, so we can hide the error and just warn instead
		partialSuccessScatterQueries.Add(1)
		sErr := mysql.NewSQLErrorFromError(err).(*mysql.SQLError)
		vcursor.Session().RecordWarning(&querypb.QueryWarning{Code: uint32(sErr.Num), Message: err.Error()})
		return nil
	}
	return err
}

func (ms *MergeSort) getStreamingFields(handles []*streamHandle, callback func(*sqltypes.Result) error) error {
	var fields []*querypb.Field

	if ms.ScatterErrorsAsWarnings {
		for _, handle := range handles {
			// Fetch field info from just one stream.
			fields = <-handle.fields
			// If fields is nil, it means there was an error.
			if fields != nil {
				break
			}
		}
	} else {
		// Fetch field info from just one stream.
		fields = <-handles[0].fields
	}
	if fields == nil {
		// something went wrong. need to figure out where the error can be
		if !ms.ScatterErrorsAsWarnings {
			return handles[0].err
		}

		var errs []error
		for _, handle := range handles {
			errs = append(errs, handle.err)
		}
		return vterrors.Aggregate(errs)
	}

	if err := callback(&sqltypes.Result{Fields: fields}); err != nil {
		return err
	}
	return nil
}

func (ms *MergeSort) description() PrimitiveDescription {
	other := map[string]any{
		"OrderBy": ms.OrderBy,
	}
	return PrimitiveDescription{
		OperatorType: "Sort",
		Variant:      "Merge",
		Other:        other,
	}
}

// streamHandle is the rendez-vous point between each stream and the merge-sorter.
// The fields channel is used by the stream to transmit the field info, which
// is the first packet. Following this, the stream sends each row to the row
// channel. At the end of the stream, fields and row are closed. If there
// was an error, err is set before the channels are closed. The MergeSort
// routine that pulls the rows out of each streamHandle can abort the stream
// by calling canceling the context.
type streamHandle struct {
	fields chan []*querypb.Field
	row    chan []sqltypes.Value
	err    error
}

// runOnestream starts a streaming query on one shard, and returns a streamHandle for it.
func runOneStream(ctx context.Context, vcursor VCursor, input StreamExecutor, bindVars map[string]*querypb.BindVariable, wantfields bool) *streamHandle {
	handle := &streamHandle{
		fields: make(chan []*querypb.Field, 1),
		row:    make(chan []sqltypes.Value, 10),
	}

	go func() {
		defer close(handle.fields)
		defer close(handle.row)

		handle.err = input.StreamExecute(ctx, vcursor, bindVars, wantfields, func(qr *sqltypes.Result) error {
			if len(qr.Fields) != 0 {
				select {
				case handle.fields <- qr.Fields:
				case <-ctx.Done():
					return io.EOF
				}
			}

			for _, row := range qr.Rows {
				select {
				case handle.row <- row:
				case <-ctx.Done():
					return io.EOF
				}
			}
			return nil
		})
	}()

	return handle
}

// A streamRow represents a row identified by the stream
// it came from. It is used as an element in scatterHeap.
type streamRow struct {
	row []sqltypes.Value
	id  int
}

// scatterHeap is the heap that is used for merge-sorting.
// You can push streamRow elements into it. Popping an
// element will return the one with the lowest value
// as defined by the orderBy criteria. If a comparison
// yielded an error, err is set. This must be checked
// after every heap operation.
type scatterHeap struct {
	rows      []streamRow
	err       error
	comparers []*comparer
}

// Len satisfies sort.Interface and heap.Interface.
func (sh *scatterHeap) Len() int {
	return len(sh.rows)
}

// Less satisfies sort.Interface and heap.Interface.
func (sh *scatterHeap) Less(i, j int) bool {
	for _, c := range sh.comparers {
		if sh.err != nil {
			return true
		}
		// First try to compare the columns that we want to order
		cmp, err := c.compare(sh.rows[i].row, sh.rows[j].row)
		if err != nil {
			sh.err = err
			return true
		}
		if cmp == 0 {
			continue
		}
		return cmp < 0
	}
	return true
}

// Swap satisfies sort.Interface and heap.Interface.
func (sh *scatterHeap) Swap(i, j int) {
	sh.rows[i], sh.rows[j] = sh.rows[j], sh.rows[i]
}

// Push satisfies heap.Interface.
func (sh *scatterHeap) Push(x any) {
	sh.rows = append(sh.rows, x.(streamRow))
}

// Pop satisfies heap.Interface.
func (sh *scatterHeap) Pop() any {
	n := len(sh.rows)
	x := sh.rows[n-1]
	sh.rows = sh.rows[:n-1]
	return x
}
