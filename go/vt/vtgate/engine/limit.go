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

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*Limit)(nil)

// Limit is a primitive that performs the LIMIT operation.
type Limit struct {
	Count  evalengine.Expr
	Offset evalengine.Expr
	Input  Primitive
}

// RouteType returns a description of the query routing type used by the primitive
func (l *Limit) RouteType() string {
	return l.Input.RouteType()
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to.
func (l *Limit) GetKeyspaceName() string {
	return l.Input.GetKeyspaceName()
}

// GetTableName specifies the table that this primitive routes to.
func (l *Limit) GetTableName() string {
	return l.Input.GetTableName()
}

// TryExecute satisfies the Primitive interface.
func (l *Limit) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	count, offset, err := l.getCountAndOffset(vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	// When offset is present, we hijack the limit value so we can calculate
	// the offset in memory from the result of the scatter query with count + offset.
	bindVars["__upper_limit"] = sqltypes.Int64BindVariable(int64(count + offset))

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

// TryStreamExecute satisfies the Primitive interface.
func (l *Limit) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	count, offset, err := l.getCountAndOffset(vcursor, bindVars)
	if err != nil {
		return err
	}

	bindVars = copyBindVars(bindVars)

	// When offset is present, we hijack the limit value so we can calculate
	// the offset in memory from the result of the scatter query with count + offset.
	bindVars["__upper_limit"] = sqltypes.Int64BindVariable(int64(count + offset))

	err = vcursor.StreamExecutePrimitive(ctx, l.Input, bindVars, wantfields, func(qr *sqltypes.Result) error {
		if len(qr.Fields) != 0 {
			if err := callback(&sqltypes.Result{Fields: qr.Fields}); err != nil {
				return err
			}
		}
		inputSize := len(qr.Rows)
		if inputSize == 0 {
			return nil
		}

		// we've still not seen all rows we need to see before we can return anything to the client
		if offset > 0 {
			if inputSize <= offset {
				// not enough to return anything yet
				offset -= inputSize
				return nil
			}
			qr.Rows = qr.Rows[offset:]
			offset = 0
		}

		if count == 0 {
			return io.EOF
		}

		// reduce count till 0.
		result := &sqltypes.Result{Rows: qr.Rows}
		resultSize := len(result.Rows)
		if count > resultSize {
			count -= resultSize
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

// GetFields implements the Primitive interface.
func (l *Limit) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return l.Input.GetFields(ctx, vcursor, bindVars)
}

// Inputs returns the input to limit
func (l *Limit) Inputs() []Primitive {
	return []Primitive{l.Input}
}

// NeedsTransaction implements the Primitive interface.
func (l *Limit) NeedsTransaction() bool {
	return l.Input.NeedsTransaction()
}

func (l *Limit) getCountAndOffset(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (count int, offset int, err error) {
	env := evalengine.NewExpressionEnv(bindVars, vcursor.TimeZone())
	count, err = getIntFrom(env, l.Count)
	if err != nil {
		return
	}
	offset, err = getIntFrom(env, l.Offset)
	if err != nil {
		return
	}
	return
}

func getIntFrom(env *evalengine.ExpressionEnv, expr evalengine.Expr) (int, error) {
	if expr == nil {
		return 0, nil
	}
	evalResult, err := env.Evaluate(expr)
	if err != nil {
		return 0, err
	}
	value := evalResult.Value()
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
		other["Count"] = evalengine.FormatExpr(l.Count)
	}
	if l.Offset != nil {
		other["Offset"] = evalengine.FormatExpr(l.Offset)
	}

	return PrimitiveDescription{
		OperatorType: "Limit",
		Other:        other,
	}
}
