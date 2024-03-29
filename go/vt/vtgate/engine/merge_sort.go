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
	"context"
	"io"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

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
	noInputs
	noTxNeeded

	Primitives              []StreamExecutor
	OrderBy                 evalengine.Comparison
	ScatterErrorsAsWarnings bool
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
func (ms *MergeSort) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) (err error) {
	defer evalengine.PanicHandler(&err)

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

	merge := &evalengine.Merger{
		Compare: ms.OrderBy,
	}

	if wantfields {
		fields, err := ms.getStreamingFields(handles)
		if err != nil {
			return err
		}
		if err := callback(&sqltypes.Result{Fields: fields}); err != nil {
			return err
		}
	}

	var errs []error
	// Prime the heap. One element must be pulled from each stream.
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
			merge.Push(row, i)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	merge.Init()

	// Iterate one row at a time:
	// Pop a row from the heap and send it out.
	// Then pull the next row from the stream the popped
	// row came from and push it into the heap.
	for merge.Len() != 0 {
		row, stream := merge.Pop()
		if err := callback(&sqltypes.Result{Rows: [][]sqltypes.Value{row}}); err != nil {
			return err
		}

		select {
		case row, ok := <-handles[stream].row:
			if !ok {
				if handles[stream].err != nil {
					return handles[stream].err
				}
				continue
			}
			merge.Push(row, stream)
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	err = vterrors.Aggregate(errs)
	if err != nil && ms.ScatterErrorsAsWarnings && len(errs) < len(handles) {
		// we got errors, but not all shards failed, so we can hide the error and just warn instead
		partialSuccessScatterQueries.Add(1)
		sErr := sqlerror.NewSQLErrorFromError(err).(*sqlerror.SQLError)
		vcursor.Session().RecordWarning(&querypb.QueryWarning{Code: uint32(sErr.Num), Message: err.Error()})
		return nil
	}
	return err
}

func (ms *MergeSort) getStreamingFields(handles []*streamHandle) ([]*querypb.Field, error) {
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
			return nil, handles[0].err
		}

		var errs []error
		for _, handle := range handles {
			errs = append(errs, handle.err)
		}
		return nil, vterrors.Aggregate(errs)
	}
	return fields, nil
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
