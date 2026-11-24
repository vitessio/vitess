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
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

var _ Primitive = (*MemorySort)(nil)

// MemorySort is a primitive that performs in-memory sorting.
type MemorySort struct {
	UpperLimit evalengine.Expr
	OrderBy    evalengine.Comparison
	Input      Primitive

	// CollationQueries resolve UNION expression collations and coercibilities
	// without reading arm rows. Result-field collations can instead describe
	// character_set_results and cannot establish a common UNION comparator.
	CollationQueries []Primitive

	// TruncateColumnCount specifies the number of columns to return
	// in the final result. Rest of the columns are truncated
	// from the result received. If 0, no truncation happens.
	TruncateColumnCount int
}

// TryExecute satisfies the Primitive interface.
func (ms *MemorySort) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	count, err := ms.fetchCount(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}

	resolved, arms, err := ms.resolveUnionCollations(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	input, layout, err := ms.unionSortInput(resolved, arms, vcursor.Environment().CollationEnv())
	if err != nil {
		return nil, err
	}
	result, err := vcursor.ExecutePrimitive(ctx, input, bindVars, wantfields || layout != nil)
	if err != nil {
		return nil, err
	}

	orderBy := ms.OrderBy
	width := 0
	if layout != nil {
		width = layout.width
	}
	if len(resolved) > 0 {
		orderBy, err = unionSortComparison(orderBy, result.Fields, layout, resolved, vcursor.Environment().CollationEnv())
		if err != nil {
			return nil, err
		}
	}
	if layout != nil {
		if err := layout.validate(result); err != nil {
			return nil, err
		}
	}
	if err = orderBy.SortResult(result); err != nil {
		return nil, err
	}
	if len(resolved) > 0 {
		result = result.Truncate(width)
	}
	if len(result.Rows) > count {
		result.Rows = result.Rows[:count]
	}
	return result.Truncate(ms.TruncateColumnCount), nil
}

// TryStreamExecute satisfies the Primitive interface.
func (ms *MemorySort) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) (err error) {
	defer evalengine.PanicHandler(&err)

	count, err := ms.fetchCount(ctx, vcursor, bindVars)
	if err != nil {
		return err
	}

	resolved, arms, err := ms.resolveUnionCollations(ctx, vcursor, bindVars)
	if err != nil {
		return err
	}
	input, layout, err := ms.unionSortInput(resolved, arms, vcursor.Environment().CollationEnv())
	if err != nil {
		return err
	}
	width := 0
	if layout != nil {
		width = layout.width
	}
	cb := func(qr *sqltypes.Result) error {
		if len(resolved) > 0 {
			qr = qr.Truncate(width)
		}
		return callback(qr.Truncate(ms.TruncateColumnCount))
	}

	sorter := &evalengine.Sorter{
		Compare: ms.OrderBy,
		Limit:   count,
	}

	var mu sync.Mutex
	err = vcursor.StreamExecutePrimitive(ctx, input, bindVars, wantfields || layout != nil, func(qr *sqltypes.Result) error {
		mu.Lock()
		defer mu.Unlock()
		if len(qr.Fields) != 0 {
			if len(resolved) > 0 {
				var err error
				sorter.Compare, err = unionSortComparison(ms.OrderBy, qr.Fields, layout, resolved, vcursor.Environment().CollationEnv())
				if err != nil {
					return err
				}
			}
			if err := cb(&sqltypes.Result{Fields: qr.Fields}); err != nil {
				return err
			}
		}
		if layout != nil {
			if err := layout.validate(qr); err != nil {
				return err
			}
		}
		for _, row := range qr.Rows {
			sorter.Push(row)
		}
		if vcursor.ExceedsMaxMemoryRows(sorter.Len()) {
			return fmt.Errorf("in-memory row count exceeded allowed limit of %d", vcursor.MaxMemoryRows())
		}
		return nil
	})
	if err != nil {
		return err
	}
	return cb(&sqltypes.Result{Rows: sorter.Sorted()})
}

// Concatenate can aggregate TEXT to Blob. The server-generated binary key
// captures the common collation independently of that result type and of any
// lossy character_set_results conversion. Numeric values keep numeric ordering.
func unionSortComparison(orderBy evalengine.Comparison, fields []*querypb.Field, layout *unionSortLayout, resolved []collations.TypedCollation, env *collations.Environment) (evalengine.Comparison, error) {
	if len(fields) != layout.width+len(layout.keys) || len(orderBy) != len(layout.keys) {
		return nil, vterrors.VT13001("UNION sort fields do not match its column layout")
	}
	orderBy = slices.Clone(orderBy)
	for i := range orderBy {
		order := &orderBy[i]
		if order.Col < 0 || order.Col >= layout.width {
			return nil, vterrors.VT13001("UNION ordering column is outside the result")
		}
		field := fields[order.Col]
		order.Type = evalengine.NewTypeFromField(field)
		order.WeightStringCol = -1
		order.PadSpaceWeight = nil
		if sqltypes.IsTextOrBinary(field.Type) {
			order.Col = layout.keys[i]
			order.Type = evalengine.NewType(sqltypes.VarBinary, collations.CollationBinaryID)
			var err error
			order.PadSpaceWeight, err = unionSortPadWeight(env, resolved[i].Collation)
			if err != nil {
				return nil, err
			}
		}
	}
	return orderBy, nil
}

// Padding follows the collation that produced the key, independently of the
// result's wire charset and of any common collation chosen for another sort.
func unionSortPadWeight(env *collations.Environment, id collations.ID) ([]byte, error) {
	pad, known := env.PadSpace(id)
	if !known {
		return nil, vterrors.VT12001("UNION collation padding is unknown")
	}
	if !pad {
		return nil, nil
	}
	collation := colldata.Lookup(id)
	if collation == nil {
		return nil, vterrors.VT12001("UNION arm ordering with unsupported collation " + env.LookupName(id))
	}
	var encoded [4]byte
	n := collation.Charset().EncodeRune(encoded[:], ' ')
	if n <= 0 {
		return nil, vterrors.VT13001("UNION collation cannot encode SPACE")
	}
	weight := collation.WeightString(nil, encoded[:n], 0)
	if len(weight) == 0 {
		return nil, vterrors.VT13001("UNION collation has no SPACE weight")
	}
	return weight, nil
}

// GetFields satisfies the Primitive interface.
func (ms *MemorySort) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return ms.Input.GetFields(ctx, vcursor, bindVars)
}

// Inputs returns the input to memory sort
func (ms *MemorySort) Inputs() ([]Primitive, []map[string]any) {
	inputs := append([]Primitive{ms.Input}, ms.CollationQueries...)
	return inputs, nil
}

// NeedsTransaction implements the Primitive interface
func (ms *MemorySort) NeedsTransaction() bool {
	return ms.Input.NeedsTransaction()
}

func (ms *MemorySort) fetchCount(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (int, error) {
	if ms.UpperLimit == nil {
		return math.MaxInt, nil
	}
	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	resolved, err := env.Evaluate(ms.UpperLimit)
	if err != nil {
		return 0, err
	}
	value := resolved.Value(vcursor.ConnCollation())
	if !value.IsIntegral() {
		return 0, sqltypes.ErrIncompatibleTypeCast
	}

	count, err := strconv.Atoi(value.RawStr())
	if err != nil || count < 0 {
		return 0, fmt.Errorf("requested limit is out of range: %v", value.RawStr())
	}
	return count, nil
}

func (ms *MemorySort) description() PrimitiveDescription {
	orderByIndexes := GenericJoin(ms.OrderBy, orderByParamsToString)
	other := map[string]any{"OrderBy": orderByIndexes}
	if ms.TruncateColumnCount > 0 {
		other["ResultColumns"] = ms.TruncateColumnCount
	}
	return PrimitiveDescription{
		OperatorType: "Sort",
		Variant:      "Memory",
		Other:        other,
	}
}

func orderByParamsToString(i any) string {
	obp := i.(evalengine.OrderByParams)
	return obp.String()
}

// GenericJoin will iterate over arrays, slices or maps, and executes the f function to get a
// string representation of each element, and then uses strings.Join() join all the strings into a single one
func GenericJoin(input any, f func(any) string) string {
	sl := reflect.ValueOf(input)
	var keys []string
	switch sl.Kind() {
	case reflect.Slice:
		for i := 0; i < sl.Len(); i++ {
			keys = append(keys, f(sl.Index(i).Interface()))
		}
	case reflect.Map:
		for _, k := range sl.MapKeys() {
			keys = append(keys, f(k.Interface()))
		}
	default:
		panic("GenericJoin doesn't know how to deal with " + sl.Kind().String())
	}
	return strings.Join(keys, ", ")
}
