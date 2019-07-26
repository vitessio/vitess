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
	"encoding/json"
	"fmt"
	"io"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*Limit)(nil)

// Limit is a primitive that performs the LIMIT operation.
type Limit struct {
	Count  sqltypes.PlanValue
	Offset sqltypes.PlanValue
	Input  Primitive
}

// MarshalJSON serializes the Limit into a JSON representation.
// It's used for testing and diagnostics.
func (l *Limit) MarshalJSON() ([]byte, error) {
	marshalLimit := struct {
		Opcode string
		Count  sqltypes.PlanValue
		Offset sqltypes.PlanValue
		Input  Primitive
	}{
		Opcode: "Limit",
		Count:  l.Count,
		Offset: l.Offset,
		Input:  l.Input,
	}
	return json.Marshal(marshalLimit)
}

// RouteType returns a description of the query routing type used by the primitive
func (l *Limit) RouteType() string {
	return l.Input.RouteType()
}

// KeyspaceName specifies the Keyspace that this primitive routes to.
func (l *Limit) KeyspaceName() string {
	return l.Input.KeyspaceName()
}

// TableName specifies the table that this primitive routes to.
func (l *Limit) TableName() string {
	return l.Input.TableName()
}

// Execute satisfies the Primtive interface.
func (l *Limit) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	count, err := l.fetchCount(bindVars)
	if err != nil {
		return nil, err
	}
	offset, err := l.fetchOffset(bindVars)
	if err != nil {
		return nil, err
	}
	// When offset is present, we hijack the limit value so we can calculate
	// the offset in memory from the result of the scatter query with count + offset.
	bindVars["__upper_limit"] = sqltypes.Int64BindVariable(int64(count + offset))

	result, err := l.Input.Execute(vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	// There are more rows in the response than limit + offset
	if count+offset <= len(result.Rows) {
		result.Rows = result.Rows[offset : count+offset]
		result.RowsAffected = uint64(count)
		return result, nil
	}
	// Remove extra rows from response
	if offset <= len(result.Rows) {
		result.Rows = result.Rows[offset:]
		result.RowsAffected = uint64(len(result.Rows))
		return result, nil
	}
	// offset is beyond the result set
	result.Rows = nil
	result.RowsAffected = 0
	return result, nil
}

// StreamExecute satisfies the Primtive interface.
func (l *Limit) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	count, err := l.fetchCount(bindVars)
	if err != nil {
		return err
	}
	if !l.Offset.IsNull() {
		return fmt.Errorf("offset not supported for stream execute queries")
	}

	bindVars["__upper_limit"] = sqltypes.Int64BindVariable(int64(count))

	err = l.Input.StreamExecute(vcursor, bindVars, wantfields, func(qr *sqltypes.Result) error {
		if len(qr.Fields) != 0 {
			if err := callback(&sqltypes.Result{Fields: qr.Fields}); err != nil {
				return err
			}
		}
		if len(qr.Rows) == 0 {
			return nil
		}

		if count == 0 {
			// Unreachable: this is just a failsafe.
			return io.EOF
		}

		// reduce count till 0.
		result := &sqltypes.Result{Rows: qr.Rows}
		if count > len(result.Rows) {
			count -= len(result.Rows)
			return callback(result)
		}
		result.Rows = result.Rows[:count]
		count = 0
		if err := callback(result); err != nil {
			return err
		}
		return io.EOF
	})

	if err == io.EOF {
		// We may get back the EOF we returned in the callback.
		// If so, suppress it.
		return nil
	}
	if err != nil {
		return err
	}
	return nil
}

// GetFields satisfies the Primtive interface.
func (l *Limit) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return l.Input.GetFields(vcursor, bindVars)
}

func (l *Limit) fetchCount(bindVars map[string]*querypb.BindVariable) (int, error) {
	resolved, err := l.Count.ResolveValue(bindVars)
	if err != nil {
		return 0, err
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

func (l *Limit) fetchOffset(bindVars map[string]*querypb.BindVariable) (int, error) {
	if l.Offset.IsNull() {
		return 0, nil
	}
	resolved, err := l.Offset.ResolveValue(bindVars)
	if err != nil {
		return 0, err
	}
	num, err := sqltypes.ToUint64(resolved)
	if err != nil {
		return 0, err
	}
	offset := int(num)
	if offset < 0 {
		return 0, fmt.Errorf("requested limit is out of range: %v", num)
	}
	return offset, nil
}
