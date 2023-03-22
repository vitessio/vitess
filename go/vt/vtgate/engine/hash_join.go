/*
Copyright 2021 The Vitess Authors.

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

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

var _ Primitive = (*HashJoin)(nil)

// HashJoin specifies the parameters for a join primitive
// Hash joins work by fetch all the input from the LHS, and building a hash map, known as the probe table, for this input.
// The key to the map is the hashcode of the value for column that we are joining by.
// Then the RHS is fetched, and we can check if the rows from the RHS matches any from the LHS.
// When they match by hash code, we double-check that we are not working with a false positive by comparing the values.
type HashJoin struct {
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

	// The keys correspond to the column offset in the inputs where
	// the join columns can be found
	LHSKey, RHSKey int

	// The join condition. Used for plan descriptions
	ASTPred sqlparser.Expr

	// collation and type are used to hash the incoming values correctly
	Collation      collations.ID
	ComparisonType querypb.Type
}

// TryExecute implements the Primitive interface
func (hj *HashJoin) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	lresult, err := vcursor.ExecutePrimitive(ctx, hj.Left, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	// build the probe table from the LHS result
	probeTable, err := hj.buildProbeTable(lresult)
	if err != nil {
		return nil, err
	}

	rresult, err := vcursor.ExecutePrimitive(ctx, hj.Right, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	result := &sqltypes.Result{
		Fields: joinFields(lresult.Fields, rresult.Fields, hj.Cols),
	}

	for _, currentRHSRow := range rresult.Rows {
		joinVal := currentRHSRow[hj.RHSKey]
		if joinVal.IsNull() {
			continue
		}
		hashcode, err := evalengine.NullsafeHashcode(joinVal, hj.Collation, hj.ComparisonType)
		if err != nil {
			return nil, err
		}
		lftRows := probeTable[hashcode]
		for _, currentLHSRow := range lftRows {
			lhsVal := currentLHSRow[hj.LHSKey]
			// hash codes can give false positives, so we need to check with a real comparison as well
			cmp, err := evalengine.NullsafeCompare(joinVal, lhsVal, collations.Unknown)
			if err != nil {
				return nil, err
			}

			if cmp == 0 {
				// we have a match!
				result.Rows = append(result.Rows, joinRows(currentLHSRow, currentRHSRow, hj.Cols))
			}
		}
	}

	return result, nil
}

func (hj *HashJoin) buildProbeTable(lresult *sqltypes.Result) (map[evalengine.HashCode][]sqltypes.Row, error) {
	probeTable := map[evalengine.HashCode][]sqltypes.Row{}
	for _, current := range lresult.Rows {
		joinVal := current[hj.LHSKey]
		if joinVal.IsNull() {
			continue
		}
		hashcode, err := evalengine.NullsafeHashcode(joinVal, hj.Collation, hj.ComparisonType)
		if err != nil {
			return nil, err
		}
		probeTable[hashcode] = append(probeTable[hashcode], current)
	}
	return probeTable, nil
}

// TryStreamExecute implements the Primitive interface
func (hj *HashJoin) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	// build the probe table from the LHS result
	probeTable := map[evalengine.HashCode][]sqltypes.Row{}
	var lfields []*querypb.Field
	err := vcursor.StreamExecutePrimitive(ctx, hj.Left, bindVars, wantfields, func(result *sqltypes.Result) error {
		if len(lfields) == 0 && len(result.Fields) != 0 {
			lfields = result.Fields
		}
		for _, current := range result.Rows {
			joinVal := current[hj.LHSKey]
			if joinVal.IsNull() {
				continue
			}
			hashcode, err := evalengine.NullsafeHashcode(joinVal, hj.Collation, hj.ComparisonType)
			if err != nil {
				return err
			}
			probeTable[hashcode] = append(probeTable[hashcode], current)
		}
		return nil
	})
	if err != nil {
		return err
	}

	return vcursor.StreamExecutePrimitive(ctx, hj.Right, bindVars, wantfields, func(result *sqltypes.Result) error {
		// compare the results coming from the RHS with the probe-table
		res := &sqltypes.Result{}
		if len(result.Fields) != 0 {
			res = &sqltypes.Result{
				Fields: joinFields(lfields, result.Fields, hj.Cols),
			}
		}
		for _, currentRHSRow := range result.Rows {
			joinVal := currentRHSRow[hj.RHSKey]
			if joinVal.IsNull() {
				continue
			}
			hashcode, err := evalengine.NullsafeHashcode(joinVal, hj.Collation, hj.ComparisonType)
			if err != nil {
				return err
			}
			lftRows := probeTable[hashcode]
			for _, currentLHSRow := range lftRows {
				lhsVal := currentLHSRow[hj.LHSKey]
				// hash codes can give false positives, so we need to check with a real comparison as well
				cmp, err := evalengine.NullsafeCompare(joinVal, lhsVal, hj.Collation)
				if err != nil {
					return err
				}

				if cmp == 0 {
					// we have a match!
					res.Rows = append(res.Rows, joinRows(currentLHSRow, currentRHSRow, hj.Cols))
				}
			}
		}
		if len(res.Rows) != 0 || len(res.Fields) != 0 {
			return callback(res)
		}
		return nil
	})
}

// RouteType implements the Primitive interface
func (hj *HashJoin) RouteType() string {
	return "HashJoin"
}

// GetKeyspaceName implements the Primitive interface
func (hj *HashJoin) GetKeyspaceName() string {
	if hj.Left.GetKeyspaceName() == hj.Right.GetKeyspaceName() {
		return hj.Left.GetKeyspaceName()
	}
	return hj.Left.GetKeyspaceName() + "_" + hj.Right.GetKeyspaceName()
}

// GetTableName implements the Primitive interface
func (hj *HashJoin) GetTableName() string {
	return hj.Left.GetTableName() + "_" + hj.Right.GetTableName()
}

// GetFields implements the Primitive interface
func (hj *HashJoin) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	joinVars := make(map[string]*querypb.BindVariable)
	lresult, err := hj.Left.GetFields(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	result := &sqltypes.Result{}
	rresult, err := hj.Right.GetFields(ctx, vcursor, combineVars(bindVars, joinVars))
	if err != nil {
		return nil, err
	}
	result.Fields = joinFields(lresult.Fields, rresult.Fields, hj.Cols)
	return result, nil
}

// NeedsTransaction implements the Primitive interface
func (hj *HashJoin) NeedsTransaction() bool {
	return hj.Right.NeedsTransaction() || hj.Left.NeedsTransaction()
}

// Inputs implements the Primitive interface
func (hj *HashJoin) Inputs() []Primitive {
	return []Primitive{hj.Left, hj.Right}
}

// description implements the Primitive interface
func (hj *HashJoin) description() PrimitiveDescription {
	other := map[string]any{
		"TableName":         hj.GetTableName(),
		"JoinColumnIndexes": strings.Trim(strings.Join(strings.Fields(fmt.Sprint(hj.Cols)), ","), "[]"),
		"Predicate":         sqlparser.String(hj.ASTPred),
		"ComparisonType":    hj.ComparisonType.String(),
	}
	coll := hj.Collation.Get()
	if coll != nil {
		other["Collation"] = coll.Name()
	}
	return PrimitiveDescription{
		OperatorType: "Join",
		Variant:      "Hash" + hj.Opcode.String(),
		Other:        other,
	}
}
