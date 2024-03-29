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
	"strings"
	"sync"
	"sync/atomic"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*Join)(nil)

// Join specifies the parameters for a join primitive.
type Join struct {
	Opcode JoinOpcode
	// Left and Right are the LHS and RHS primitives
	// of the Join. They can be any primitive.
	Left, Right Primitive `json:",omitempty"`

	// Cols defines which columns from the left
	// or right results should be used to build the
	// return result. For results coming from the
	// left query, the index values go as -1, -2, etc.
	// For the right query, they're 1, 2, etc.
	// If Cols is {-1, -2, 1, 2}, it means that
	// the returned result will be {Left0, Left1, Right0, Right1}.
	Cols []int `json:",omitempty"`

	// Vars defines the list of joinVars that need to
	// be built from the LHS result before invoking
	// the RHS subqquery.
	Vars map[string]int `json:",omitempty"`
}

// TryExecute performs a non-streaming exec.
func (jn *Join) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	joinVars := make(map[string]*querypb.BindVariable)
	lresult, err := vcursor.ExecutePrimitive(ctx, jn.Left, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	result := &sqltypes.Result{}
	if len(lresult.Rows) == 0 && wantfields {
		for k, col := range jn.Vars {
			joinVars[k] = bindvarForType(lresult.Fields[col].Type)
		}
		rresult, err := jn.Right.GetFields(ctx, vcursor, combineVars(bindVars, joinVars))
		if err != nil {
			return nil, err
		}
		result.Fields = joinFields(lresult.Fields, rresult.Fields, jn.Cols)
		return result, nil
	}
	for _, lrow := range lresult.Rows {
		for k, col := range jn.Vars {
			joinVars[k] = sqltypes.ValueBindVariable(lrow[col])
		}
		rresult, err := vcursor.ExecutePrimitive(ctx, jn.Right, combineVars(bindVars, joinVars), wantfields)
		if err != nil {
			return nil, err
		}
		if wantfields {
			wantfields = false
			result.Fields = joinFields(lresult.Fields, rresult.Fields, jn.Cols)
		}
		for _, rrow := range rresult.Rows {
			result.Rows = append(result.Rows, joinRows(lrow, rrow, jn.Cols))
		}
		if jn.Opcode == LeftJoin && len(rresult.Rows) == 0 {
			result.Rows = append(result.Rows, joinRows(lrow, nil, jn.Cols))
		}
		if vcursor.ExceedsMaxMemoryRows(len(result.Rows)) {
			return nil, fmt.Errorf("in-memory row count exceeded allowed limit of %d", vcursor.MaxMemoryRows())
		}
	}
	return result, nil
}

func bindvarForType(t querypb.Type) *querypb.BindVariable {
	bv := &querypb.BindVariable{
		Type:  t,
		Value: nil,
	}
	switch t {
	case querypb.Type_INT8, querypb.Type_UINT8, querypb.Type_INT16, querypb.Type_UINT16,
		querypb.Type_INT32, querypb.Type_UINT32, querypb.Type_INT64, querypb.Type_UINT64:
		bv.Value = []byte("0")
	case querypb.Type_FLOAT32, querypb.Type_FLOAT64:
		bv.Value = []byte("0e0")
	case querypb.Type_DECIMAL:
		bv.Value = []byte("0.0")
	default:
		return sqltypes.NullBindVariable
	}
	return bv
}

// TryStreamExecute performs a streaming exec.
func (jn *Join) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	var mu sync.Mutex
	// We need to use this atomic since we're also reading this
	// value outside of it being locked with the mu lock.
	// This is still racy, but worst case it means that we may
	// retrieve the right hand side fields twice instead of once.
	var fieldsSent atomic.Bool
	fieldsSent.Store(!wantfields)
	err := vcursor.StreamExecutePrimitive(ctx, jn.Left, bindVars, wantfields, func(lresult *sqltypes.Result) error {
		joinVars := make(map[string]*querypb.BindVariable)
		for _, lrow := range lresult.Rows {
			for k, col := range jn.Vars {
				joinVars[k] = sqltypes.ValueBindVariable(lrow[col])
			}
			var rowSent atomic.Bool
			err := vcursor.StreamExecutePrimitive(ctx, jn.Right, combineVars(bindVars, joinVars), !fieldsSent.Load(), func(rresult *sqltypes.Result) error {
				// This needs to be locking since it's not safe to just use
				// fieldsSent. This is because we can't have a race between
				// checking fieldsSent and then actually calling the callback
				// and in parallel another goroutine doing the same. That
				// can lead to out of order execution of the callback. So the callback
				// itself and the check need to be covered by the same lock.
				mu.Lock()
				defer mu.Unlock()
				result := &sqltypes.Result{}
				if fieldsSent.CompareAndSwap(false, true) {
					result.Fields = joinFields(lresult.Fields, rresult.Fields, jn.Cols)
				}
				for _, rrow := range rresult.Rows {
					result.Rows = append(result.Rows, joinRows(lrow, rrow, jn.Cols))
				}
				if len(rresult.Rows) != 0 {
					rowSent.Store(true)
				}
				return callback(result)
			})
			if err != nil {
				return err
			}
			if jn.Opcode == LeftJoin && !rowSent.Load() {
				result := &sqltypes.Result{}
				result.Rows = [][]sqltypes.Value{joinRows(
					lrow,
					nil,
					jn.Cols,
				)}
				return callback(result)
			}
		}
		// This needs to be locking since it's not safe to just use
		// fieldsSent. This is because we can't have a race between
		// checking fieldsSent and then actually calling the callback
		// and in parallel another goroutine doing the same. That
		// can lead to out of order execution of the callback. So the callback
		// itself and the check need to be covered by the same lock.
		mu.Lock()
		defer mu.Unlock()
		if fieldsSent.CompareAndSwap(false, true) {
			for k := range jn.Vars {
				joinVars[k] = sqltypes.NullBindVariable
			}
			result := &sqltypes.Result{}
			rresult, err := jn.Right.GetFields(ctx, vcursor, combineVars(bindVars, joinVars))
			if err != nil {
				return err
			}
			result.Fields = joinFields(lresult.Fields, rresult.Fields, jn.Cols)
			return callback(result)
		}
		return nil
	})
	return err
}

// GetFields fetches the field info.
func (jn *Join) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	joinVars := make(map[string]*querypb.BindVariable)
	lresult, err := jn.Left.GetFields(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	result := &sqltypes.Result{}
	for k := range jn.Vars {
		joinVars[k] = sqltypes.NullBindVariable
	}
	rresult, err := jn.Right.GetFields(ctx, vcursor, combineVars(bindVars, joinVars))
	if err != nil {
		return nil, err
	}
	result.Fields = joinFields(lresult.Fields, rresult.Fields, jn.Cols)
	return result, nil
}

// Inputs returns the input primitives for this join
func (jn *Join) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{jn.Left, jn.Right}, nil
}

func joinFields(lfields, rfields []*querypb.Field, cols []int) []*querypb.Field {
	fields := make([]*querypb.Field, len(cols))
	for i, index := range cols {
		if index < 0 {
			fields[i] = lfields[-index-1]
			continue
		}
		fields[i] = rfields[index-1]
	}
	return fields
}

func joinRows(lrow, rrow sqltypes.Row, cols []int) sqltypes.Row {
	row := make([]sqltypes.Value, len(cols))
	for i, index := range cols {
		if index < 0 {
			row[i] = lrow[-index-1]
			continue
		}
		// rrow can be nil on left joins
		if rrow != nil {
			row[i] = rrow[index-1]
		}
	}
	return row
}

// JoinOpcode is a number representing the opcode
// for the Join primitive.
type JoinOpcode int

// This is the list of JoinOpcode values.
const (
	InnerJoin = JoinOpcode(iota)
	LeftJoin
)

func (code JoinOpcode) String() string {
	if code == InnerJoin {
		return "Join"
	}
	return "LeftJoin"
}

// MarshalJSON serializes the JoinOpcode as a JSON string.
// It's used for testing and diagnostics.
func (code JoinOpcode) MarshalJSON() ([]byte, error) {
	return ([]byte)(fmt.Sprintf("\"%s\"", code.String())), nil
}

// RouteType returns a description of the query routing type used by the primitive
func (jn *Join) RouteType() string {
	return "Join"
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to.
func (jn *Join) GetKeyspaceName() string {
	if jn.Left.GetKeyspaceName() == jn.Right.GetKeyspaceName() {
		return jn.Left.GetKeyspaceName()
	}
	return jn.Left.GetKeyspaceName() + "_" + jn.Right.GetKeyspaceName()
}

// GetTableName specifies the table that this primitive routes to.
func (jn *Join) GetTableName() string {
	return jn.Left.GetTableName() + "_" + jn.Right.GetTableName()
}

// NeedsTransaction implements the Primitive interface
func (jn *Join) NeedsTransaction() bool {
	return jn.Right.NeedsTransaction() || jn.Left.NeedsTransaction()
}

func combineVars(bv1, bv2 map[string]*querypb.BindVariable) map[string]*querypb.BindVariable {
	out := make(map[string]*querypb.BindVariable)
	for k, v := range bv1 {
		out[k] = v
	}
	for k, v := range bv2 {
		out[k] = v
	}
	return out
}

func (jn *Join) description() PrimitiveDescription {
	other := map[string]any{
		"TableName":         jn.GetTableName(),
		"JoinColumnIndexes": jn.joinColsDescription(),
	}
	if len(jn.Vars) > 0 {
		other["JoinVars"] = orderedStringIntMap(jn.Vars)
	}
	return PrimitiveDescription{
		OperatorType: "Join",
		Variant:      jn.Opcode.String(),
		Other:        other,
	}
}

func (jn *Join) joinColsDescription() string {
	var joinCols []string
	for _, col := range jn.Cols {
		if col < 0 {
			joinCols = append(joinCols, fmt.Sprintf("L:%d", -col-1))
		} else {
			joinCols = append(joinCols, fmt.Sprintf("R:%d", col-1))
		}
	}
	joinColTxt := strings.Join(joinCols, ",")
	return joinColTxt
}
