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
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*SimpleProjection)(nil)

// SimpleProjection selects which columns to keep from the input
type SimpleProjection struct {
	// Cols defines the column numbers from the underlying primitive
	// to be returned.
	Cols []int
	// ColNames are the column names to use for the columns.
	ColNames []string
	Input    Primitive
}

// NeedsTransaction implements the Primitive interface
func (sc *SimpleProjection) NeedsTransaction() bool {
	return sc.Input.NeedsTransaction()
}

// TryExecute performs a non-streaming exec.
func (sc *SimpleProjection) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	inner, err := vcursor.ExecutePrimitive(ctx, sc.Input, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	if err := sc.checkColumns(inner); err != nil {
		return nil, err
	}
	return sc.buildResult(inner), nil
}

// TryStreamExecute performs a streaming exec.
func (sc *SimpleProjection) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	return vcursor.StreamExecutePrimitive(ctx, sc.Input, bindVars, wantfields, func(inner *sqltypes.Result) error {
		if err := sc.checkColumns(inner); err != nil {
			return err
		}
		return callback(sc.buildResult(inner))
	})
}

// GetFields fetches the field info.
func (sc *SimpleProjection) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	inner, err := sc.Input.GetFields(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	if err := sc.checkColumns(inner); err != nil {
		return nil, err
	}
	return &sqltypes.Result{Fields: sc.buildFields(inner)}, nil
}

// Inputs returns the input to this primitive
func (sc *SimpleProjection) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{sc.Input}, nil
}

// buildResult builds a new result by pulling the necessary columns from
// the input in the requested order.
func (sc *SimpleProjection) buildResult(inner *sqltypes.Result) *sqltypes.Result {
	if sc.namesOnly() {
		sc.renameFields(inner.Fields)
		return inner
	}
	result := &sqltypes.Result{Fields: sc.buildFields(inner)}
	result.Rows = make([][]sqltypes.Value, 0, len(inner.Rows))
	for _, innerRow := range inner.Rows {
		row := make([]sqltypes.Value, 0, len(sc.Cols))
		for _, col := range sc.Cols {
			row = append(row, innerRow[col])
		}
		result.Rows = append(result.Rows, row)
	}
	result.RowsAffected = inner.RowsAffected
	return result
}

func (sc *SimpleProjection) namesOnly() bool {
	return sc.Cols == nil
}

// checkColumns verifies that every requested column exists in the input result.
// A projection that reads at or past the end of the input's columns can only
// come from a planning bug - for example, a projection above a union whose
// wider inputs were trimmed, resolving a select expression to one of the
// padding columns that Union.GetColumns added. Fail with a clean error instead
// of panicking with an index out of range deeper down: in OLTP mode the panic
// would be caught by the connection's recover and close the client connection,
// and in OLAP mode it would take down the whole vtgate process.
//
// Chunks that carry field info are validated against the fields. Streaming
// chunks after the first carry rows but no field info, so they are validated
// against the width of their rows instead: the Cols are indices into the input
// rows, so the highest index must exist in a row. This matters in OLAP mode,
// where Concatenate.parallelStreamExec sends fields only on the first chunk
// and clears them on every later chunk: once the fields chunk has failed with
// the error below, cancel() does not run until the failing arm's
// StreamExecutePrimitive returns (a scatter route waits for all of its shard
// streams to finish), and the other arms keep delivering row chunks in the
// meantime, which would otherwise panic in buildResult before the error could
// be returned. Rows of a chunk all come from the same source, so checking the
// first row keeps the overhead to one comparison per chunk. (A planner-level
// fix that rejects such projections outright would also be possible, but this
// runtime check guards every execution path.)
func (sc *SimpleProjection) checkColumns(inner *sqltypes.Result) error {
	if sc.namesOnly() {
		return nil
	}
	if len(inner.Fields) > 0 {
		for _, col := range sc.Cols {
			if col >= len(inner.Fields) {
				return errWrongNumberOfColumnsInSelect
			}
		}
	}
	if len(inner.Rows) > 0 {
		for _, col := range sc.Cols {
			if col >= len(inner.Rows[0]) {
				return errWrongNumberOfColumnsInSelect
			}
		}
	}
	return nil
}

func (sc *SimpleProjection) buildFields(inner *sqltypes.Result) []*querypb.Field {
	if len(inner.Fields) == 0 {
		return nil
	}
	fields := make([]*querypb.Field, 0, len(sc.ColNames))
	if sc.namesOnly() {
		for idx, field := range inner.Fields {
			if sc.ColNames[idx] != "" {
				field = proto.Clone(field).(*querypb.Field)
				field.Name = sc.ColNames[idx]
			}
			fields = append(fields, field)
		}
		return fields
	}
	for idx, col := range sc.Cols {
		field := inner.Fields[col]
		if sc.ColNames[idx] != "" {
			field = proto.Clone(field).(*querypb.Field)
			field.Name = sc.ColNames[idx]
		}
		fields = append(fields, field)
	}
	return fields
}

func (sc *SimpleProjection) renameFields(fields []*querypb.Field) {
	if len(fields) == 0 {
		return
	}
	for idx, name := range sc.ColNames {
		if sc.ColNames[idx] != "" {
			fields[idx].Name = name
		}
	}
}

func (sc *SimpleProjection) description() PrimitiveDescription {
	other := map[string]any{}
	if !sc.namesOnly() {
		other["Columns"] = strings.Join(slice.Map(sc.Cols, strconv.Itoa), ",")
	}

	var colNames []string
	for idx, cName := range sc.ColNames {
		if cName != "" {
			colNames = append(colNames, fmt.Sprintf("%d:%s", idx, cName))
		}
	}
	if colNames != nil {
		other["ColumnNames"] = colNames
	}

	return PrimitiveDescription{
		OperatorType: "SimpleProjection",
		Other:        other,
	}
}
