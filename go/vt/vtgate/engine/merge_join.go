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
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*MergeJoin)(nil)

// MergeJoin specifies the parameters for a join primitive.
type MergeJoin struct {
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

	// LeftJoinCols defines which columns from the lhs are part of the ON comparison
	LeftJoinCols []int `json:",omitempty"`

	// RightJoinCols defines which columns from the rhs are part of the ON comparison
	RightJoinCols []int `json:",omitempty"`
}

// Execute performs a non-streaming exec.
func (jn *MergeJoin) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	lresult, err := jn.Left.Execute(vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	rresult, err := jn.Right.Execute(vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	result := &sqltypes.Result{}

	for l, r := 0, 0; l < len(lresult.Rows) && r < len(rresult.Rows); {
		if wantfields {
			wantfields = false
			result.Fields = joinFields(lresult.Fields, rresult.Fields, jn.Cols)
		}

		lrow := lresult.Rows[l]
		rrow := rresult.Rows[r]
		cmp, err := jn.isMatch(lrow, rrow)
		if err != nil {
			return nil, err
		}

		switch cmp {
		case -1: // rhs is bigger, move lhs forward
			l++
		case 1: // lhs is bigger, move rhs forward
			r++
		case 0: // we have a match!
			l++
			r++
			result.Rows = append(result.Rows, jn.resultRow(lrow, rrow))
			result.RowsAffected++
		}
	}
	return result, nil
}

// StreamExecute performs a streaming exec.
func (jn *MergeJoin) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	panic("implement me")
}

// GetFields fetches the field info.
func (jn *MergeJoin) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	panic("implement me")
}

// RouteType returns a description of the query routing type used by the primitive
func (jn *MergeJoin) RouteType() string {
	return "MergeJoin"
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to.
func (jn *MergeJoin) GetKeyspaceName() string {
	if jn.Left.GetKeyspaceName() == jn.Right.GetKeyspaceName() {
		return jn.Left.GetKeyspaceName()
	}
	return jn.Left.GetKeyspaceName() + "_" + jn.Right.GetKeyspaceName()
}

// GetTableName specifies the table that this primitive routes to.
func (jn *MergeJoin) GetTableName() string {
	return jn.Left.GetTableName() + "_" + jn.Right.GetTableName()
}

func (jn *MergeJoin) isMatch(lhs, rhs []sqltypes.Value) (int, error) {
	for i, ljoinCol := range jn.LeftJoinCols {
		lval := lhs[ljoinCol]
		rval := rhs[jn.RightJoinCols[i]]
		cmp, err := sqltypes.NullsafeCompare(lval, rval)
		if err != nil {
			return 0, vterrors.Wrap(err, "tried to compare two values")
		}

		switch cmp {
		case -1:
			return -1, nil
		case 1:
			return 1, nil
		default: // this column matched, continue with the rest
		}
	}
	return 0, nil
}

func (jn *MergeJoin) resultRow(lhs, rhs []sqltypes.Value) []sqltypes.Value {
	row := make([]sqltypes.Value, len(jn.Cols))
	for i, index := range jn.Cols {
		if index < 0 {
			row[i] = lhs[-index-1]

		} else
		// rrow can be nil on left joins
		if rhs != nil {
			row[i] = rhs[index-1]
		}
	}
	return row
}
