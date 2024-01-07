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
	"strconv"
	"strings"
	"sync"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

var _ Primitive = (*MemorySort)(nil)

// MemorySort is a primitive that performs in-memory sorting.
type MemorySort struct {
	UpperLimit evalengine.Expr
	OrderBy    evalengine.Comparison
	Input      Primitive

	// TruncateColumnCount specifies the number of columns to return
	// in the final result. Rest of the columns are truncated
	// from the result received. If 0, no truncation happens.
	TruncateColumnCount int `json:",omitempty"`
}

// RouteType returns a description of the query routing type used by the primitive.
func (ms *MemorySort) RouteType() string {
	return ms.Input.RouteType()
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to.
func (ms *MemorySort) GetKeyspaceName() string {
	return ms.Input.GetKeyspaceName()
}

// GetTableName specifies the table that this primitive routes to.
func (ms *MemorySort) GetTableName() string {
	return ms.Input.GetTableName()
}

// SetTruncateColumnCount sets the truncate column count.
func (ms *MemorySort) SetTruncateColumnCount(count int) {
	ms.TruncateColumnCount = count
}

// TryExecute satisfies the Primitive interface.
func (ms *MemorySort) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	count, err := ms.fetchCount(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}

	result, err := vcursor.ExecutePrimitive(ctx, ms.Input, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	if err = ms.OrderBy.SortResult(result); err != nil {
		return nil, err
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

	cb := func(qr *sqltypes.Result) error {
		return callback(qr.Truncate(ms.TruncateColumnCount))
	}

	sorter := &evalengine.Sorter{
		Compare: ms.OrderBy,
		Limit:   count,
	}

	var mu sync.Mutex
	err = vcursor.StreamExecutePrimitive(ctx, ms.Input, bindVars, wantfields, func(qr *sqltypes.Result) error {
		mu.Lock()
		defer mu.Unlock()
		if len(qr.Fields) != 0 {
			if err := cb(&sqltypes.Result{Fields: qr.Fields}); err != nil {
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

// GetFields satisfies the Primitive interface.
func (ms *MemorySort) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return ms.Input.GetFields(ctx, vcursor, bindVars)
}

// Inputs returns the input to memory sort
func (ms *MemorySort) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{ms.Input}, nil
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
