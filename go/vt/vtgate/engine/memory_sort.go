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
	"container/heap"
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*MemorySort)(nil)

// MemorySort is a primitive that performs in-memory sorting.
type MemorySort struct {
	UpperLimit evalengine.Expr
	OrderBy    []OrderByParams
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
	sh := &sortHeap{
		rows:      result.Rows,
		comparers: extractSlices(ms.OrderBy),
	}
	sort.Sort(sh)
	if sh.err != nil {
		return nil, sh.err
	}
	result.Rows = sh.rows
	if len(result.Rows) > count {
		result.Rows = result.Rows[:count]
	}
	return result.Truncate(ms.TruncateColumnCount), nil
}

// TryStreamExecute satisfies the Primitive interface.
func (ms *MemorySort) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	count, err := ms.fetchCount(ctx, vcursor, bindVars)
	if err != nil {
		return err
	}

	cb := func(qr *sqltypes.Result) error {
		return callback(qr.Truncate(ms.TruncateColumnCount))
	}

	// You have to reverse the ordering because the highest values
	// must be dropped once the upper limit is reached.
	sh := &sortHeap{
		comparers: extractSlices(ms.OrderBy),
		reverse:   true,
	}
	err = vcursor.StreamExecutePrimitive(ctx, ms.Input, bindVars, wantfields, func(qr *sqltypes.Result) error {
		if len(qr.Fields) != 0 {
			if err := cb(&sqltypes.Result{Fields: qr.Fields}); err != nil {
				return err
			}
		}
		for _, row := range qr.Rows {
			heap.Push(sh, row)
			// Remove the highest element from the heap if the size is more than the count
			// This optimization means that the maximum size of the heap is going to be (count + 1)
			for len(sh.rows) > count {
				_ = heap.Pop(sh)
			}
		}
		if vcursor.ExceedsMaxMemoryRows(len(sh.rows)) {
			return fmt.Errorf("in-memory row count exceeded allowed limit of %d", vcursor.MaxMemoryRows())
		}
		return nil
	})
	if err != nil {
		return err
	}
	if sh.err != nil {
		return sh.err
	}
	// Set ordering to normal for the final ordering.
	sh.reverse = false
	sort.Sort(sh)
	if sh.err != nil {
		// Unreachable.
		return sh.err
	}
	return cb(&sqltypes.Result{Rows: sh.rows})
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
		return math.MaxInt64, nil
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
	return i.(OrderByParams).String()
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

// sortHeap is sorted based on the orderBy params.
// Implementation is similar to scatterHeap
type sortHeap struct {
	rows      [][]sqltypes.Value
	comparers []*comparer
	reverse   bool
	err       error
}

// Len satisfies sort.Interface and heap.Interface.
func (sh *sortHeap) Len() int {
	return len(sh.rows)
}

// Less satisfies sort.Interface and heap.Interface.
func (sh *sortHeap) Less(i, j int) bool {
	for _, c := range sh.comparers {
		if sh.err != nil {
			return true
		}
		cmp, err := c.compare(sh.rows[i], sh.rows[j])
		if err != nil {
			sh.err = err
			return true
		}
		if cmp == 0 {
			continue
		}
		if sh.reverse {
			cmp = -cmp
		}
		return cmp < 0
	}
	return true
}

// Swap satisfies sort.Interface and heap.Interface.
func (sh *sortHeap) Swap(i, j int) {
	sh.rows[i], sh.rows[j] = sh.rows[j], sh.rows[i]
}

// Push satisfies heap.Interface.
func (sh *sortHeap) Push(x any) {
	sh.rows = append(sh.rows, x.([]sqltypes.Value))
}

// Pop satisfies heap.Interface.
func (sh *sortHeap) Pop() any {
	n := len(sh.rows)
	x := sh.rows[n-1]
	sh.rows = sh.rows[:n-1]
	return x
}
