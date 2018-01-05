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

package engine

import (
	"fmt"

	"github.com/youtube/vitess/go/sqltypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
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

// Execute performs a non-streaming exec.
func (jn *Join) Execute(vcursor VCursor, bindVars, joinVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	lresult, err := jn.Left.Execute(vcursor, bindVars, joinVars, wantfields)
	if err != nil {
		return nil, err
	}
	result := &sqltypes.Result{}
	if len(lresult.Rows) == 0 && wantfields {
		for k := range jn.Vars {
			joinVars[k] = sqltypes.NullBindVariable
		}
		rresult, err := jn.Right.GetFields(vcursor, bindVars, joinVars)
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
		rresult, err := jn.Right.Execute(vcursor, bindVars, joinVars, wantfields)
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
			result.RowsAffected++
		} else {
			result.RowsAffected += uint64(len(rresult.Rows))
		}
	}
	return result, nil
}

// StreamExecute performs a streaming exec.
func (jn *Join) StreamExecute(vcursor VCursor, bindVars, joinVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	err := jn.Left.StreamExecute(vcursor, bindVars, joinVars, wantfields, func(lresult *sqltypes.Result) error {
		for _, lrow := range lresult.Rows {
			for k, col := range jn.Vars {
				joinVars[k] = sqltypes.ValueBindVariable(lrow[col])
			}
			rowSent := false
			err := jn.Right.StreamExecute(vcursor, bindVars, joinVars, wantfields, func(rresult *sqltypes.Result) error {
				result := &sqltypes.Result{}
				if wantfields {
					wantfields = false
					result.Fields = joinFields(lresult.Fields, rresult.Fields, jn.Cols)
				}
				for _, rrow := range rresult.Rows {
					result.Rows = append(result.Rows, joinRows(lrow, rrow, jn.Cols))
				}
				if len(rresult.Rows) != 0 {
					rowSent = true
				}
				return callback(result)
			})
			if err != nil {
				return err
			}
			if wantfields {
				// TODO(sougou): remove after testing
				panic("unexptected")
			}
			if jn.Opcode == LeftJoin && !rowSent {
				result := &sqltypes.Result{}
				result.Rows = [][]sqltypes.Value{joinRows(
					lrow,
					nil,
					jn.Cols,
				)}
				return callback(result)
			}
		}
		if wantfields {
			wantfields = false
			for k := range jn.Vars {
				joinVars[k] = sqltypes.NullBindVariable
			}
			result := &sqltypes.Result{}
			rresult, err := jn.Right.GetFields(vcursor, bindVars, joinVars)
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
func (jn *Join) GetFields(vcursor VCursor, bindVars, joinVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	lresult, err := jn.Left.GetFields(vcursor, bindVars, joinVars)
	if err != nil {
		return nil, err
	}
	result := &sqltypes.Result{}
	for k := range jn.Vars {
		joinVars[k] = sqltypes.NullBindVariable
	}
	rresult, err := jn.Right.GetFields(vcursor, bindVars, joinVars)
	if err != nil {
		return nil, err
	}
	result.Fields = joinFields(lresult.Fields, rresult.Fields, jn.Cols)
	return result, nil
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

func joinRows(lrow, rrow []sqltypes.Value, cols []int) []sqltypes.Value {
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
	NormalJoin = JoinOpcode(iota)
	LeftJoin
)

func (code JoinOpcode) String() string {
	if code == NormalJoin {
		return "Join"
	}
	return "LeftJoin"
}

// MarshalJSON serializes the JoinOpcode as a JSON string.
// It's used for testing and diagnostics.
func (code JoinOpcode) MarshalJSON() ([]byte, error) {
	return ([]byte)(fmt.Sprintf("\"%s\"", code.String())), nil
}
