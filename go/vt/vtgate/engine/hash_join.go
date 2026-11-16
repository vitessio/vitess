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
	"sync"
	"sync/atomic"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/resultcharset"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vthash"
)

var _ Primitive = (*HashJoin)(nil)

type (
	// HashJoin specifies the parameters for a join primitive
	// Hash joins work by fetch all the input from the LHS, and building a hash map, known as the probe table, for this input.
	// The key to the map is the hashcode of the value for column that we are joining by.
	// Then the RHS is fetched, and we can check if the rows from the RHS matches any from the LHS.
	// When they match by hash code, we double-check that we are not working with a false positive by comparing the values.
	HashJoin struct {
		Opcode JoinOpcode

		// Left and Right are the LHS and RHS primitives
		// of the Join. They can be any primitive.
		Left, Right Primitive

		// Cols defines which columns from the left
		// or right results should be used to build the
		// return result. For results coming from the
		// left query, the index values go as -1, -2, etc.
		// For the right query, they're 1, 2, etc.
		// If Cols is {-1, -2, 1, 2}, it means that
		// the returned result will be {Left0, Left1, Right0, Right1}.
		Cols []int

		// The keys correspond to the column offset in the inputs where
		// the join columns can be found
		LHSKey, RHSKey int

		// The join condition. Used for plan descriptions
		ASTPred sqlparser.Expr

		// collation and type are used to hash the incoming values correctly
		Collation      collations.ID
		ComparisonType querypb.Type

		CollationEnv *collations.Environment

		// Values for enum and set types
		Values *evalengine.EnumSetValues
	}

	hashJoinProbeTable struct {
		innerMap map[vthash.Hash]*probeTableEntry

		coll           collations.ID
		typ            querypb.Type
		lhsKey, rhsKey int
		// lhsText and rhsText convert the key of a row of the left and of the
		// right side before it is hashed, see HashJoin.keyText. They are nil
		// when the key is hashed as it is.
		lhsText, rhsText *resultcharset.Conversion
		cols             []int
		hasher           vthash.Hasher
		sqlmode          evalengine.SQLMode
		values           *evalengine.EnumSetValues
	}

	probeTableEntry struct {
		row  sqltypes.Row
		next *probeTableEntry
		seen bool
	}
)

// keyText returns the conversion of the key of the rows of one side of the
// join, the column key of rows with these fields, to the character set of the
// collation of the join, and nil when there is nothing to convert.
//
// The join has one collation, in which the keys of both sides are hashed. With
// --convert-result-charset the text of a row is in the character set of the
// collation in its field, which does not have to be the one of the join: for
// a latin1 column a.l and a utf8mb4 column d.u, a.l = d.u is compared in
// utf8mb4_0900_ai_ci, and the 'é' of a.l is the byte E9. Hashed as it is, it
// does not find the 'é' of d.u, C3 A9, which MySQL 8.0.46 joins it with. The
// key is converted for the hash only, the rows stay as they are. Without the
// flag every row is in the connection character set, and is hashed as before.
func (hj *HashJoin) keyText(vcursor VCursor, fields []*querypb.Field, key int) *resultcharset.Conversion {
	if !vcursor.ConvertResultCharset() || key >= len(fields) {
		return nil
	}
	conv := resultcharset.ToFields(fields[key:key+1], []*querypb.Field{{Type: fields[key].Type, Charset: uint32(hj.Collation)}})
	if !conv.ConvertsValues() {
		return nil
	}
	return conv
}

// TryExecute implements the Primitive interface
func (hj *HashJoin) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	// With --convert-result-charset the fields say which character set the
	// rows are in, see keyText.
	wantfields = wantfields || vcursor.ConvertResultCharset()
	lresult, err := vcursor.ExecutePrimitive(ctx, hj.Left, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	pt := newHashJoinProbeTable(hj.Collation, hj.ComparisonType, hj.LHSKey, hj.RHSKey, hj.Cols, hj.Values)
	pt.lhsText = hj.keyText(vcursor, lresult.Fields, hj.LHSKey)
	// build the probe table from the LHS result
	for _, row := range lresult.Rows {
		err := pt.addLeftRow(row)
		if err != nil {
			return nil, err
		}
	}

	rresult, err := vcursor.ExecutePrimitive(ctx, hj.Right, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	pt.rhsText = hj.keyText(vcursor, rresult.Fields, hj.RHSKey)

	result := &sqltypes.Result{
		Fields: joinFields(lresult.Fields, rresult.Fields, hj.Cols),
	}

	for _, currentRHSRow := range rresult.Rows {
		matches, err := pt.get(currentRHSRow)
		if err != nil {
			return nil, err
		}
		result.Rows = append(result.Rows, matches...)
	}

	if hj.Opcode == LeftJoin {
		result.Rows = append(result.Rows, pt.notFetched()...)
	}

	return result, nil
}

// TryStreamExecute implements the Primitive interface
func (hj *HashJoin) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	// build the probe table from the LHS result
	pt := newHashJoinProbeTable(hj.Collation, hj.ComparisonType, hj.LHSKey, hj.RHSKey, hj.Cols, hj.Values)
	var lfields []*querypb.Field
	var mu sync.Mutex
	// With --convert-result-charset the fields, which come before the rows,
	// say which character set the rows are in, see keyText.
	textFields := vcursor.ConvertResultCharset()
	err := vcursor.StreamExecutePrimitive(ctx, hj.Left, bindVars, wantfields || textFields, func(result *sqltypes.Result) error {
		mu.Lock()
		defer mu.Unlock()
		if len(lfields) == 0 && len(result.Fields) != 0 {
			lfields = result.Fields
			pt.lhsText = hj.keyText(vcursor, lfields, hj.LHSKey)
		}
		for _, current := range result.Rows {
			err := pt.addLeftRow(current)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	var sendFields atomic.Bool
	sendFields.Store(wantfields)

	var haveRightFields bool
	err = vcursor.StreamExecutePrimitive(ctx, hj.Right, bindVars, sendFields.Load() || textFields, func(result *sqltypes.Result) error {
		mu.Lock()
		defer mu.Unlock()
		if !haveRightFields && len(result.Fields) != 0 {
			haveRightFields = true
			pt.rhsText = hj.keyText(vcursor, result.Fields, hj.RHSKey)
		}
		// compare the results coming from the RHS with the probe-table
		res := &sqltypes.Result{}
		if len(result.Fields) != 0 && sendFields.CompareAndSwap(true, false) {
			res.Fields = joinFields(lfields, result.Fields, hj.Cols)
		}
		for _, currentRHSRow := range result.Rows {
			results, err := pt.get(currentRHSRow)
			if err != nil {
				return err
			}
			res.Rows = append(res.Rows, results...)
		}
		if len(res.Rows) != 0 || len(res.Fields) != 0 {
			return callback(res)
		}
		return nil
	})
	if err != nil {
		return err
	}

	if hj.Opcode == LeftJoin {
		res := &sqltypes.Result{}
		if sendFields.CompareAndSwap(true, false) {
			// If we still have not sent the fields, we need to fetch
			// the fields from the RHS to be able to build the result fields
			rres, err := hj.Right.GetFields(ctx, vcursor, bindVars)
			if err != nil {
				return err
			}
			res.Fields = joinFields(lfields, rres.Fields, hj.Cols)
		}
		// this will only be called when all the concurrent access to the pt has
		// ceased, so we don't need to lock it here
		res.Rows = pt.notFetched()
		return callback(res)
	}
	return nil
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
func (hj *HashJoin) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{hj.Left, hj.Right}, nil
}

// description implements the Primitive interface
func (hj *HashJoin) description() PrimitiveDescription {
	other := map[string]any{
		"JoinColumnIndexes": strings.Trim(strings.Join(strings.Fields(fmt.Sprint(hj.Cols)), ","), "[]"),
		"Predicate":         sqlparser.String(hj.ASTPred),
		"ComparisonType":    hj.ComparisonType.String(),
	}
	coll := hj.Collation
	if coll != collations.Unknown {
		other["Collation"] = hj.CollationEnv.LookupName(coll)
	}
	return PrimitiveDescription{
		OperatorType: "Join",
		Variant:      "Hash" + hj.Opcode.String(),
		Other:        other,
	}
}

func newHashJoinProbeTable(coll collations.ID, typ querypb.Type, lhsKey, rhsKey int, cols []int, values *evalengine.EnumSetValues) *hashJoinProbeTable {
	return &hashJoinProbeTable{
		innerMap: map[vthash.Hash]*probeTableEntry{},
		coll:     coll,
		typ:      typ,
		lhsKey:   lhsKey,
		rhsKey:   rhsKey,
		cols:     cols,
		hasher:   vthash.New(),
		values:   values,
	}
}

func (pt *hashJoinProbeTable) addLeftRow(r sqltypes.Row) error {
	key := r[pt.lhsKey]
	if pt.lhsText != nil {
		key = pt.lhsText.Value(0, key)
	}
	hash, err := pt.hash(key)
	if err != nil {
		return err
	}
	pt.innerMap[hash] = &probeTableEntry{
		row:  r,
		next: pt.innerMap[hash],
	}

	return nil
}

func (pt *hashJoinProbeTable) hash(val sqltypes.Value) (vthash.Hash, error) {
	err := evalengine.NullsafeHashcode128(&pt.hasher, val, pt.coll, pt.typ, pt.sqlmode, pt.values)
	if err != nil {
		return vthash.Hash{}, err
	}

	res := pt.hasher.Sum128()
	pt.hasher.Reset()
	return res, nil
}

func (pt *hashJoinProbeTable) get(rrow sqltypes.Row) (result []sqltypes.Row, err error) {
	val := rrow[pt.rhsKey]
	if val.IsNull() {
		return
	}

	if pt.rhsText != nil {
		val = pt.rhsText.Value(0, val)
	}
	hash, err := pt.hash(val)
	if err != nil {
		return nil, err
	}

	for e := pt.innerMap[hash]; e != nil; e = e.next {
		e.seen = true
		result = append(result, joinRows(e.row, rrow, pt.cols))
	}

	return
}

func (pt *hashJoinProbeTable) notFetched() (rows []sqltypes.Row) {
	for _, e := range pt.innerMap {
		for ; e != nil; e = e.next {
			if !e.seen {
				rows = append(rows, joinRows(e.row, nil, pt.cols))
			}
		}
	}
	return
}
