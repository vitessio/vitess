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
	"encoding/json"
	"fmt"
	"math"
	"sort"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*MemorySort)(nil)

// MemorySort is a primitive that performs in-memory sorting.
type MemorySort struct {
	UpperLimit sqltypes.PlanValue
	OrderBy    []OrderbyParams
	Input      Primitive

	// TruncateColumnCount specifies the number of columns to return
	// in the final result. Rest of the columns are truncated
	// from the result received. If 0, no truncation happens.
	TruncateColumnCount int `json:",omitempty"`
}

// MarshalJSON serializes the MemorySort into a JSON representation.
// It's used for testing and diagnostics.
func (ms *MemorySort) MarshalJSON() ([]byte, error) {
	marshalMemorySort := struct {
		Opcode  string
		MaxRows sqltypes.PlanValue
		OrderBy []OrderbyParams
		Input   Primitive
	}{
		Opcode:  "MemorySort",
		MaxRows: ms.UpperLimit,
		OrderBy: ms.OrderBy,
		Input:   ms.Input,
	}
	return json.Marshal(marshalMemorySort)
}

// RouteType returns a description of the query routing type used by the primitive.
func (ms *MemorySort) RouteType() string {
	return ms.Input.RouteType()
}

// KeyspaceTableNames specifies the table that this primitive routes to.
func (ms *MemorySort) KeyspaceTableNames() []*KeyspaceTableName {
	return ms.Input.KeyspaceTableNames()
}

// SetTruncateColumnCount sets the truncate column count.
func (ms *MemorySort) SetTruncateColumnCount(count int) {
	ms.TruncateColumnCount = count
}

// Execute satisfies the Primtive interface.
func (ms *MemorySort) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	count, err := ms.fetchCount(bindVars)
	if err != nil {
		return nil, err
	}

	result, err := ms.Input.Execute(vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	sh := &sortHeap{
		rows:    result.Rows,
		orderBy: ms.OrderBy,
	}
	sort.Sort(sh)
	if sh.err != nil {
		return nil, sh.err
	}
	result.Rows = sh.rows
	if len(result.Rows) > count {
		result.Rows = result.Rows[:count]
		result.RowsAffected = uint64(count)
	}
	return result.Truncate(ms.TruncateColumnCount), nil
}

// StreamExecute satisfies the Primtive interface.
func (ms *MemorySort) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	count, err := ms.fetchCount(bindVars)
	if err != nil {
		return err
	}

	cb := func(qr *sqltypes.Result) error {
		return callback(qr.Truncate(ms.TruncateColumnCount))
	}

	// You have to reverse the ordering because the highest values
	// must be dropped once the upper limit is reached.
	sh := &sortHeap{
		orderBy: ms.OrderBy,
		reverse: true,
	}
	err = ms.Input.StreamExecute(vcursor, bindVars, wantfields, func(qr *sqltypes.Result) error {
		if len(qr.Fields) != 0 {
			if err := cb(&sqltypes.Result{Fields: qr.Fields}); err != nil {
				return err
			}
		}
		for _, row := range qr.Rows {
			heap.Push(sh, row)
		}
		for len(sh.rows) > count {
			_ = heap.Pop(sh)
		}
		if len(sh.rows) > vcursor.MaxMemoryRows() {
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

// GetFields satisfies the Primtive interface.
func (ms *MemorySort) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return ms.Input.GetFields(vcursor, bindVars)
}

func (ms *MemorySort) fetchCount(bindVars map[string]*querypb.BindVariable) (int, error) {
	resolved, err := ms.UpperLimit.ResolveValue(bindVars)
	if err != nil {
		return 0, err
	}
	if resolved.IsNull() {
		return math.MaxInt64, nil
	}
	num, err := sqltypes.ToUint64(resolved)
	if err != nil {
		return 0, err
	}
	count := int(num)
	if count < 0 {
		return 0, fmt.Errorf("requested limit is out of range: %v", num)
	}
	return count, nil
}

// sortHeap is sorted based on the orderBy params.
// Implementation is similar to scatterHeap
type sortHeap struct {
	rows    [][]sqltypes.Value
	orderBy []OrderbyParams
	reverse bool
	err     error
}

// Len satisfies sort.Interface and heap.Interface.
func (sh *sortHeap) Len() int {
	return len(sh.rows)
}

// Less satisfies sort.Interface and heap.Interface.
func (sh *sortHeap) Less(i, j int) bool {
	for _, order := range sh.orderBy {
		if sh.err != nil {
			return true
		}
		cmp, err := sqltypes.NullsafeCompare(sh.rows[i][order.Col], sh.rows[j][order.Col])
		if err != nil {
			sh.err = err
			return true
		}
		if cmp == 0 {
			continue
		}
		// This is equivalent to:
		//if !sh.reverse {
		//	if order.Desc {
		//		cmp = -cmp
		//	}
		//} else {
		//	if !order.Desc {
		//		cmp = -cmp
		//	}
		//}
		if sh.reverse != order.Desc {
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
func (sh *sortHeap) Push(x interface{}) {
	sh.rows = append(sh.rows, x.([]sqltypes.Value))
}

// Pop satisfies heap.Interface.
func (sh *sortHeap) Pop() interface{} {
	n := len(sh.rows)
	x := sh.rows[n-1]
	sh.rows = sh.rows[:n-1]
	return x
}
