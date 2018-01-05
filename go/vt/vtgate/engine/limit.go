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

	"github.com/youtube/vitess/go/sqltypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

var _ Primitive = (*Limit)(nil)

// Limit is a primitive that performs the LIMIT operation.
// For now, it only supports count without offset.
type Limit struct {
	Count sqltypes.PlanValue
	Input Primitive
}

// MarshalJSON serializes the Limit into a JSON representation.
// It's used for testing and diagnostics.
func (l *Limit) MarshalJSON() ([]byte, error) {
	marshalLimit := struct {
		Opcode string
		Count  sqltypes.PlanValue
		Input  Primitive
	}{
		Opcode: "Limit",
		Count:  l.Count,
		Input:  l.Input,
	}
	return json.Marshal(marshalLimit)
}

// Execute satisfies the Primtive interface.
func (l *Limit) Execute(vcursor VCursor, bindVars, joinVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	count, err := l.fetchCount(bindVars, joinVars)
	if err != nil {
		return nil, err
	}

	result, err := l.Input.Execute(vcursor, bindVars, joinVars, wantfields)
	if err != nil {
		return nil, err
	}

	if count < len(result.Rows) {
		result.Rows = result.Rows[:count]
		result.RowsAffected = uint64(count)
	}
	return result, nil
}

// StreamExecute satisfies the Primtive interface.
func (l *Limit) StreamExecute(vcursor VCursor, bindVars, joinVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	count, err := l.fetchCount(bindVars, joinVars)
	if err != nil {
		return err
	}

	err = l.Input.StreamExecute(vcursor, bindVars, joinVars, wantfields, func(qr *sqltypes.Result) error {
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
func (l *Limit) GetFields(vcursor VCursor, bindVars, joinVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return l.Input.GetFields(vcursor, bindVars, joinVars)
}

func (l *Limit) fetchCount(bindVars, joinVars map[string]*querypb.BindVariable) (int, error) {
	// TODO(sougou): to avoid duplication, check if this can be done
	// by the supplier of joinVars instead.
	bindVars = combineVars(bindVars, joinVars)

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
