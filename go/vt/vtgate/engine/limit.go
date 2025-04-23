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
	"io"
	"strconv"
	"sync"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*Limit)(nil)

// Limit performs the LIMIT operation, restricting the number of rows returned.
type Limit struct {
	// Count specifies the maximum number of rows to return.
	Count evalengine.Expr

	// Offset specifies the number of rows to skip before returning results.
	Offset evalengine.Expr

	// RequireCompleteInput determines if all input rows must be fully retrieved.
	// - If true, all Result structs are passed through, and the total rows are limited.
	// - If false, Limit returns io.EOF once the limit is reached in streaming mode,
	//   signaling the tablet to stop sending data.
	RequireCompleteInput bool

	// Input provides the input rows.
	Input Primitive
}

var UpperLimitStr = "__upper_limit"

// TryExecute satisfies the Primitive interface.
func (l *Limit) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	count, offset, err := l.getCountAndOffset(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	// When offset is present, we hijack the limit value so we can calculate
	// the offset in memory from the result of the scatter query with count + offset.

	bindVars[UpperLimitStr] = sqltypes.Int64BindVariable(int64(count + offset))

	result, err := vcursor.ExecutePrimitive(ctx, l.Input, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	// There are more rows in the response than limit + offset
	if count+offset <= len(result.Rows) {
		result.Rows = result.Rows[offset : count+offset]
		return result, nil
	}
	// Remove extra rows from response
	if offset <= len(result.Rows) {
		result.Rows = result.Rows[offset:]
		return result, nil
	}
	// offset is beyond the result set
	result.Rows = nil
	return result, nil
}

func (l *Limit) mustRetrieveAll(vcursor VCursor) bool {
	return l.RequireCompleteInput || vcursor.Session().InTransaction()
}

// TryStreamExecute satisfies the Primitive interface.
func (l *Limit) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	count, offset, err := l.getCountAndOffset(ctx, vcursor, bindVars)
	if err != nil {
		return err
	}

	bindVars = copyBindVars(bindVars)

	// Adjust the upper limit so that the initial fetch includes both the offset and count.
	// We do this because we want to skip the first `offset` rows locally rather than on the server side.
	bindVars[UpperLimitStr] = sqltypes.Int64BindVariable(int64(count + offset))

	var mu sync.Mutex
	err = vcursor.StreamExecutePrimitive(ctx, l.Input, bindVars, wantfields, func(qr *sqltypes.Result) error {
		mu.Lock()
		defer mu.Unlock()

		inputSize := len(qr.Rows)
		// If we still need to skip `offset` rows before returning any to the client:
		if offset > 0 {
			if inputSize <= offset {
				// not enough to return anything yet, but we still want to pass on metadata such as last_insert_id
				offset -= inputSize
				if !wantfields && !l.mustRetrieveAll(vcursor) {
					return nil
				}
				if len(qr.Fields) > 0 {
					wantfields = false
				}
				qr.Rows = nil
				return callback(qr)
			}
			// Skip `offset` rows from this batch and reset offset to 0.
			qr.Rows = qr.Rows[offset:]
			offset = 0
		}

		// At this point, we've dealt with the offset. Now handle the count (limit).
		if count == 0 {
			// If count is zero, we've fetched everything we need.
			if !wantfields && !l.mustRetrieveAll(vcursor) {
				return io.EOF
			}
			if len(qr.Fields) > 0 {
				wantfields = false
			}

			// If we require the complete input, or we are in a transaction, we cannot return io.EOF early.
			// Instead, we return empty results as needed until input ends.
			qr.Rows = nil
			return callback(qr)
		}

		if len(qr.Fields) > 0 {
			wantfields = false
		}

		// reduce count till 0.
		resultSize := len(qr.Rows)
		if count > resultSize {
			count -= resultSize
			return callback(qr)
		}

		qr.Rows = qr.Rows[:count]
		count = 0
		if err := callback(qr); err != nil {
			return err
		}

		// If we required complete input or are in a transaction, we must not exit early.
		// We'll return empty batches until the input is done.
		if l.mustRetrieveAll(vcursor) {
			return nil
		}

		return io.EOF
	})

	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	return nil
}

// GetFields implements the Primitive interface.
func (l *Limit) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return l.Input.GetFields(ctx, vcursor, bindVars)
}

// Inputs returns the input to limit
func (l *Limit) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{l.Input}, nil
}

// NeedsTransaction implements the Primitive interface.
func (l *Limit) NeedsTransaction() bool {
	return l.Input.NeedsTransaction()
}

func (l *Limit) getCountAndOffset(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (count int, offset int, err error) {
	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	count, err = getIntFrom(env, vcursor, l.Count)
	if err != nil {
		return
	}
	offset, err = getIntFrom(env, vcursor, l.Offset)
	if err != nil {
		return
	}
	return
}

func getIntFrom(env *evalengine.ExpressionEnv, vcursor VCursor, expr evalengine.Expr) (int, error) {
	if expr == nil {
		return 0, nil
	}
	evalResult, err := env.Evaluate(expr)
	if err != nil {
		return 0, err
	}
	value := evalResult.Value(vcursor.ConnCollation())
	if value.IsNull() {
		return 0, nil
	}

	if !value.IsIntegral() {
		return 0, sqltypes.ErrIncompatibleTypeCast
	}

	count, err := strconv.Atoi(value.RawStr())
	if err != nil || count < 0 {
		return 0, fmt.Errorf("requested limit is out of range: %v", value.RawStr())
	}
	return count, nil
}

func (l *Limit) description() PrimitiveDescription {
	other := map[string]any{}

	if l.Count != nil {
		other["Count"] = sqlparser.String(l.Count)
	}
	if l.Offset != nil {
		other["Offset"] = sqlparser.String(l.Offset)
	}
	if l.RequireCompleteInput {
		other["RequireCompleteInput"] = true
	}

	return PrimitiveDescription{
		OperatorType: "Limit",
		Other:        other,
	}
}
