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
	"vitess.io/vitess/go/vt/proto/vtrpc"
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

// a struct that allows for iterating over the rows in chunks (array of Value), where all rows in a chunk share the same
// join column value
type prefetchingChunkCursor struct {
	rows            [][]sqltypes.Value
	idx             int
	current         [][]sqltypes.Value
	currentJoinVals []sqltypes.Value
	keyCols         []int
}

func newCursor(rows [][]sqltypes.Value, keyCols []int) (*prefetchingChunkCursor, error) {
	c := &prefetchingChunkCursor{
		rows:    rows,
		idx:     0,
		keyCols: keyCols,
	}
	err := c.fetchNextChunk()
	if err != nil {
		return nil, err
	}
	return c, nil

}

// if the cursor has prefetched data, this will return true
func (c *prefetchingChunkCursor) hasData() bool {
	return c.current != nil
}

// fetchNextChunk fetches the next chunk of rows, with all rows having the same value for the key columns
func (c *prefetchingChunkCursor) fetchNextChunk() error {
	if c.idx >= len(c.rows) {
		// if we are at the end of the input, we are done.
		c.current = nil
		return nil
	}

	c.currentJoinVals = *c.getCurrentJoinValues()
	c.current = [][]sqltypes.Value{c.rows[c.idx]}
	c.idx++

	for c.idx < len(c.rows) {
		curr := c.getCurrentJoinValues()
		cmp, err := compareAll(&c.currentJoinVals, curr)
		if err != nil {
			return err
		}
		if cmp != 0 {
			return nil
		}
		c.current = append(c.current, c.rows[c.idx])
		c.idx++
	}

	return nil
}

// getCurrentJoinValues fetches the join values being evaluated
func (c *prefetchingChunkCursor) getCurrentJoinValues() *[]sqltypes.Value {
	curr := make([]sqltypes.Value, len(c.keyCols))
	for i, column := range c.keyCols {
		curr[i] = c.rows[c.idx][column]
	}
	return &curr
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
	lCursor, err := newCursor(lresult.Rows, jn.LeftJoinCols)
	if err != nil {
		return nil, err
	}

	rCursor, err := newCursor(rresult.Rows, jn.RightJoinCols)
	if err != nil {
		return nil, err
	}

	for lCursor.hasData() && rCursor.hasData() {
		if wantfields {
			wantfields = false
			result.Fields = joinFields(lresult.Fields, rresult.Fields, jn.Cols)
		}
		cmp, err := compareAll(&lCursor.currentJoinVals, &rCursor.currentJoinVals)
		if err != nil {
			return nil, err
		}

		switch cmp {
		case -1: // rhs is bigger, move lhs forward
			err := lCursor.fetchNextChunk()
			if err != nil {
				return nil, err
			}
		case 1: // lhs is bigger, move rhs forward
			err := rCursor.fetchNextChunk()
			if err != nil {
				return nil, err
			}
		case 0: // we have a match!
			intermediate := jn.multiply(lCursor.current, rCursor.current)
			result.Rows = append(result.Rows, intermediate...)
			result.RowsAffected += uint64(len(intermediate))

			// since the cursor guarantees that each chunk is unique,
			// we can move both sides forward
			err := rCursor.fetchNextChunk()
			if err != nil {
				return nil, err
			}
			err = lCursor.fetchNextChunk()
			if err != nil {
				return nil, err
			}

		}
	}
	return result, nil
}

// multiply takes two chunks and combines them to produce the result rows
func (jn *MergeJoin) multiply(lhs, rhs [][]sqltypes.Value) [][]sqltypes.Value {
	rhsSize := len(rhs)
	totalSize := len(lhs) * rhsSize
	result := make([][]sqltypes.Value, totalSize, totalSize)

	for i, aside := range lhs {
		for j, bside := range rhs {
			result[rhsSize*i+j] = jn.resultRow(aside, bside)
		}
	}
	return result
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

func (jn *MergeJoin) compareJoinCols(lhs, rhs []sqltypes.Value) (int, error) {
	for i, ljoinCol := range jn.LeftJoinCols {
		lval := lhs[ljoinCol]
		rval := rhs[jn.RightJoinCols[i]]
		cmp, err := sqltypes.NullsafeCompare(lval, rval)
		if err != nil {
			return 0, vterrors.Wrap(err, "tried to compare two values")
		}

		if cmp != 0 {
			return cmp, nil
		}
	}
	return 0, nil
}

func compareAll(lhs, rhs *[]sqltypes.Value) (int, error) {
	if len(*lhs) != len(*rhs) {
		return 0, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "inputs must have the same size")
	}

	for i, lval := range *lhs {
		rval := (*rhs)[i]
		cmp, err := sqltypes.NullsafeCompare(lval, rval)
		if err != nil {
			return 0, vterrors.Wrap(err, "tried to compare two values")
		}

		if cmp != 0 {
			return cmp, nil
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
