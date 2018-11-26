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

import (
	"container/heap"
	"fmt"
	"io"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// ResultSizeRows specifies how many rows should be merged together per
// returned Result. Higher values will improve the performance of the overall
// pipeline but increase the memory usage.
// The current value of 64 rows tries to aim at a total size of 4k bytes
// for the returned Result (i.e. we assume an average row size of 64 bytes).
const ResultSizeRows = 64

// ResultMerger returns a sorted stream of multiple ResultReader input streams.
// The output stream will be sorted by ascending primary key order.
// It implements the ResultReader interface.
type ResultMerger struct {
	inputs []ResultReader
	fields []*querypb.Field
	// output is the buffer of merged rows. Once it's full, we'll return it in
	// Next() (wrapped in a sqltypes.Result).
	output [][]sqltypes.Value
	// nextRowHeap is a priority queue whose min value always points to the next
	// row which should be merged into "output". See Next() for details.
	nextRowHeap *nextRowHeap
	// lastRowReaderDrained becomes true when there is only one input ResultReader
	// left and its respective RowReader stopped after the current Result. From
	// there on, we can switch to an "endgame" mode where we can read the Result
	// directly from the input instead of reading one row at a time.
	lastRowReaderDrained bool
}

// NewResultMerger returns a new ResultMerger.
func NewResultMerger(inputs []ResultReader, pkFieldCount int) (*ResultMerger, error) {
	if len(inputs) < 2 {
		panic("ResultMerger requires at least two ResultReaders as input")
	}

	fields := inputs[0].Fields()

	if err := checkFieldsEqual(fields, inputs); err != nil {
		return nil, err
	}

	err := CheckValidTypesForResultMerger(fields, pkFieldCount)
	if err != nil {
		return nil, fmt.Errorf("invalid PK types for ResultMerger. Use the vtworker LegacySplitClone command instead. %v", err.Error())
	}

	// Initialize the priority queue with all input ResultReader which have at
	// least one row.
	var activeInputs []ResultReader
	nextRowHeap := newNextRowHeap(fields, pkFieldCount)
	for i, input := range inputs {
		nextRow := newNextRow(input)
		if err := nextRow.next(); err != nil {
			if err == io.EOF {
				continue
			}
			return nil, vterrors.Wrapf(err, "failed to read from input at index: %v ResultReader: %v err", i, input)
		}
		activeInputs = append(activeInputs, input)
		heap.Push(nextRowHeap, nextRow)
	}

	rm := &ResultMerger{
		inputs:      activeInputs,
		fields:      fields,
		nextRowHeap: nextRowHeap,
	}
	rm.reset()
	return rm, nil
}

// CheckValidTypesForResultMerger returns an error if the provided fields are not compatible with how ResultMerger works
func CheckValidTypesForResultMerger(fields []*querypb.Field, pkFieldCount int) error {
	for i := 0; i < pkFieldCount; i++ {
		typ := fields[i].Type
		if !sqltypes.IsIntegral(typ) && !sqltypes.IsFloat(typ) && !sqltypes.IsBinary(typ) {
			return fmt.Errorf("unsupported type: %v cannot compare fields with this type", typ)
		}
	}
	return nil
}

// Fields returns the field information for the columns in the result.
// It is part of the ResultReader interface.
func (rm *ResultMerger) Fields() []*querypb.Field {
	return rm.fields
}

// Close closes all inputs
func (rm *ResultMerger) Close(ctx context.Context) {
	for _, i := range rm.inputs {
		i.Close(ctx)
	}
}

// Next returns the next Result in the sorted, merged stream.
// It is part of the ResultReader interface.
func (rm *ResultMerger) Next() (*sqltypes.Result, error) {
	// All input readers were consumed during a merge. Return end of stream.
	if len(rm.inputs) == 0 {
		return nil, io.EOF
	}
	// Endgame mode if there is exactly one input left.
	if len(rm.inputs) == 1 && rm.lastRowReaderDrained {
		return rm.inputs[0].Next()
	}

	// Current output buffer is not full and there is more than one input left.
	// Keep merging rows from the inputs using the priority queue.
	for len(rm.output) < ResultSizeRows && len(rm.inputs) != 0 {
		// Get the next smallest row.
		next := heap.Pop(rm.nextRowHeap).(*nextRow)
		// Add it to the output.
		rm.output = append(rm.output, next.row)
		// Check if the input we just popped has more rows.
		if err := next.next(); err != nil {
			if err == io.EOF {
				// No more rows. Delete this input ResultReader from "inputs".
				rm.deleteInput(next.input)

				if len(rm.inputs) == 1 {
					// We just deleted the second last ResultReader from "inputs" i.e.
					// only one input is left. Tell the last input's RowReader to stop
					// processing new Results after the current Result. Once we read the
					// remaining rows, we can switch to the endgame mode where we read the
					// Result directly from the input.
					lastNextRow := heap.Pop(rm.nextRowHeap).(*nextRow)
					lastNextRow.rowReader.StopAfterCurrentResult()
					heap.Push(rm.nextRowHeap, lastNextRow)
				}

				// Do not add back the deleted input to the priority queue.
				continue
			} else if err == ErrStoppedRowReader {
				// Endgame mode: We just consumed all rows from the last input.
				// Switch to a faster mode where we read Results from the input instead
				// of reading rows from the RowReader.
				rm.lastRowReaderDrained = true
				break
			} else {
				return nil, vterrors.Wrapf(err, "failed to read from input ResultReader: %v err", next.input)
			}
		}
		// Input has more rows. Add it back to the priority queue.
		heap.Push(rm.nextRowHeap, next)
	}

	result := &sqltypes.Result{
		Fields:       rm.fields,
		RowsAffected: uint64(len(rm.output)),
		Rows:         rm.output,
	}
	rm.reset()

	return result, nil
}

func (rm *ResultMerger) deleteInput(deleteMe ResultReader) {
	for i, input := range rm.inputs {
		if input == deleteMe {
			rm.inputs = append(rm.inputs[:i], rm.inputs[i+1:]...)
			break
		}
	}
}

func (rm *ResultMerger) reset() {
	// Allocate the full array at once.
	rm.output = make([][]sqltypes.Value, 0, ResultSizeRows)
}

func checkFieldsEqual(fields []*querypb.Field, inputs []ResultReader) error {
	for i := 1; i < len(inputs); i++ {
		otherFields := inputs[i].Fields()
		if len(fields) != len(otherFields) {
			return fmt.Errorf("input ResultReaders have conflicting Fields data: ResultReader[0]: %v != ResultReader[%d]: %v", fields, i, otherFields)
		}
		for j, field := range fields {
			otherField := otherFields[j]
			if !proto.Equal(field, otherField) {
				return fmt.Errorf("input ResultReaders have conflicting Fields data: ResultReader[0]: %v != ResultReader[%d]: %v", fields, i, otherFields)
			}
		}
	}
	return nil
}

// nextRow is an entry in the nextRowHeap priority queue.
// It stores the current row which should be used for sorting, the input and the
// input's RowReader which will allow to move forward to the next row.
type nextRow struct {
	input ResultReader
	// rowReader allows to consume "input" one row at a time.
	rowReader *RowReader
	row       []sqltypes.Value
}

func newNextRow(input ResultReader) *nextRow {
	return &nextRow{
		input:     input,
		rowReader: NewRowReader(input),
	}
}

// next reads the next row from the input. It returns io.EOF if the input stream
// has ended.
func (n *nextRow) next() error {
	var err error
	n.row, err = n.rowReader.Next()
	if err != nil {
		return err
	}
	if n.row == nil {
		return io.EOF
	}
	return nil
}

// nextRowHeap implements a priority queue which allows to sort multiple
// input ResultReader by their smallest row (in terms of primary key order).
// It's intended to be used as follows:
// - pop the smallest value
// - advance the popped input to the next row
// - push the input with the new row back (unless the stream has ended)
// See ResultMerger.Next() for details.
type nextRowHeap struct {
	// fields is required by the CompareRows function.
	fields []*querypb.Field
	// pkFieldCount is the number of columns in the primary key.
	// We assume that the first columns in the row belong to the primary key and
	// we only compare them.
	pkFieldCount int
	// nextRowByInputs is the underlying storage of the priority queue.
	nextRowByInputs []*nextRow
}

func newNextRowHeap(fields []*querypb.Field, pkFieldCount int) *nextRowHeap {
	return &nextRowHeap{
		fields:       fields,
		pkFieldCount: pkFieldCount,
	}
}

func (h nextRowHeap) Len() int { return len(h.nextRowByInputs) }
func (h nextRowHeap) Less(i, j int) bool {
	result, err := CompareRows(h.fields, h.pkFieldCount, h.nextRowByInputs[i].row, h.nextRowByInputs[j].row)
	if err != nil {
		panic(fmt.Sprintf("failed to compare two rows: %v vs. %v err: %v", h.nextRowByInputs[i].row, h.nextRowByInputs[j].row, err))
	}
	return result == -1
}
func (h nextRowHeap) Swap(i, j int) {
	h.nextRowByInputs[i], h.nextRowByInputs[j] = h.nextRowByInputs[j], h.nextRowByInputs[i]
}

// Push adds x as element Len().
// It is part of the container/heap.Interface interface.
func (h *nextRowHeap) Push(x interface{}) {
	h.nextRowByInputs = append(h.nextRowByInputs, x.(*nextRow))
}

// Push removes and returns element Len()-1.
// It is part of the container/heap.Interface interface.
func (h *nextRowHeap) Pop() interface{} {
	old := h.nextRowByInputs
	n := len(old)
	x := old[n-1]
	h.nextRowByInputs = old[0 : n-1]
	return x
}
