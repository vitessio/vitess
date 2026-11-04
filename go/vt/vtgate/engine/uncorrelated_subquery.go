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
	"maps"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
)

var _ Primitive = (*UncorrelatedSubquery)(nil)

// UncorrelatedSubquery executes a subquery once and uses
// the result as a bind variable for the underlying primitive.
type UncorrelatedSubquery struct {
	Opcode opcode.PulloutOpcode

	// SubqueryResult and HasValues are used to send in the bindvar used in the query to the underlying primitive
	SubqueryResult string
	HasValues      string
	// RowColumns is how many columns each row of a PulloutIn or PulloutNotIn subquery has, when it is more than
	// one, as in `(a, b) in (select x, y ...)`. SubqueryResult is then bound to a list of tuples rather than of values.
	RowColumns int
	// RowResults holds one bind variable per column for a PulloutValue subquery whose row has more than one column,
	// as in `(a, b) < (select x, y ...)`. Each column is bound to its own variable, and SubqueryResult is not bound,
	// because outside of an IN list a bind variable can only hold a single value.
	RowResults []string

	Subquery Primitive
	Outer    Primitive
}

// Inputs returns the input primitives for this join
func (ps *UncorrelatedSubquery) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{ps.Subquery, ps.Outer}, []map[string]any{{
		inputName: "SubQuery",
	}, {
		inputName: "Outer",
	}}
}

// TryExecute satisfies the Primitive interface.
func (ps *UncorrelatedSubquery) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	combinedVars, err := ps.execSubquery(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	return vcursor.ExecutePrimitive(ctx, ps.Outer, combinedVars, wantfields)
}

// TryStreamExecute performs a streaming exec.
func (ps *UncorrelatedSubquery) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	combinedVars, err := ps.execSubquery(ctx, vcursor, bindVars)
	if err != nil {
		return err
	}
	return vcursor.StreamExecutePrimitive(ctx, ps.Outer, combinedVars, wantfields, callback)
}

// GetFields fetches the field info.
func (ps *UncorrelatedSubquery) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	combinedVars := make(map[string]*querypb.BindVariable, len(bindVars)+1)
	maps.Copy(combinedVars, bindVars)
	switch ps.Opcode {
	case opcode.PulloutValue:
		ps.bindNullValue(combinedVars)
	case opcode.PulloutIn, opcode.PulloutNotIn:
		combinedVars[ps.HasValues] = sqltypes.Int64BindVariable(0)
		combinedVars[ps.SubqueryResult] = ps.placeholderList()
	case opcode.PulloutExists:
		combinedVars[ps.HasValues] = sqltypes.Int64BindVariable(0)
	}
	return ps.Outer.GetFields(ctx, vcursor, combinedVars)
}

// NeedsTransaction implements the Primitive interface
func (ps *UncorrelatedSubquery) NeedsTransaction() bool {
	return ps.Subquery.NeedsTransaction() || ps.Outer.NeedsTransaction()
}

var errSqRow = vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "subquery returned more than one row")

func (ps *UncorrelatedSubquery) execSubquery(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (map[string]*querypb.BindVariable, error) {
	subqueryBindVars := make(map[string]*querypb.BindVariable, len(bindVars))
	maps.Copy(subqueryBindVars, bindVars)
	result, err := vcursor.ExecutePrimitive(ctx, ps.Subquery, subqueryBindVars, false)
	if err != nil {
		return nil, err
	}
	combinedVars := make(map[string]*querypb.BindVariable, len(bindVars)+1)
	maps.Copy(combinedVars, bindVars)
	width := max(ps.RowColumns, len(ps.RowResults))
	for _, row := range result.Rows {
		if len(row) < width {
			return nil, vterrors.VT13001(fmt.Sprintf("subquery returned %d columns for a row of %d", len(row), width))
		}
	}
	switch ps.Opcode {
	case opcode.PulloutValue:
		switch len(result.Rows) {
		case 0:
			ps.bindNullValue(combinedVars)
		case 1:
			if len(ps.RowResults) == 0 {
				combinedVars[ps.SubqueryResult] = sqltypes.ValueBindVariable(result.Rows[0][0])
				break
			}
			for i, name := range ps.RowResults {
				combinedVars[name] = sqltypes.ValueBindVariable(result.Rows[0][i])
			}
		default:
			return nil, errSqRow
		}
	case opcode.PulloutIn, opcode.PulloutNotIn:
		switch len(result.Rows) {
		case 0:
			combinedVars[ps.HasValues] = sqltypes.Int64BindVariable(0)
			// Add a bogus value. It will not be checked.
			combinedVars[ps.SubqueryResult] = ps.placeholderList()
		default:
			combinedVars[ps.HasValues] = sqltypes.Int64BindVariable(1)
			values := &querypb.BindVariable{
				Type:   querypb.Type_TUPLE,
				Values: make([]*querypb.Value, len(result.Rows)),
			}
			for i, v := range result.Rows {
				if ps.RowColumns > 1 {
					// The subquery can return more columns than the row, such as a weight_string for its ordering.
					values.Values[i] = sqltypes.TupleToProto(v[:ps.RowColumns])
				} else {
					values.Values[i] = sqltypes.ValueToProto(v[0])
				}
			}
			combinedVars[ps.SubqueryResult] = values
		}
	case opcode.PulloutExists:
		switch len(result.Rows) {
		case 0:
			combinedVars[ps.HasValues] = sqltypes.Int64BindVariable(0)
		default:
			combinedVars[ps.HasValues] = sqltypes.Int64BindVariable(1)
		}
	}
	return combinedVars, nil
}

// bindNullValue binds the result of a PulloutValue subquery that returned no rows: NULL, or a NULL for each
// column of its row, which is how MySQL compares a row with an empty row subquery.
func (ps *UncorrelatedSubquery) bindNullValue(bindVars map[string]*querypb.BindVariable) {
	if len(ps.RowResults) == 0 {
		bindVars[ps.SubqueryResult] = sqltypes.NullBindVariable
		return
	}
	for _, name := range ps.RowResults {
		bindVars[name] = sqltypes.NullBindVariable
	}
}

// placeholderList returns the list bound for a PulloutIn or PulloutNotIn subquery that returned no rows. The
// has-values guard keeps it from being compared, but MySQL still rejects it unless it is as wide as the row.
func (ps *UncorrelatedSubquery) placeholderList() *querypb.BindVariable {
	if ps.RowColumns <= 1 {
		return &querypb.BindVariable{
			Type:   querypb.Type_TUPLE,
			Values: []*querypb.Value{sqltypes.ValueToProto(sqltypes.NewInt64(0))},
		}
	}
	row := make([]sqltypes.Value, ps.RowColumns)
	for i := range row {
		row[i] = sqltypes.NewInt64(0)
	}
	return &querypb.BindVariable{
		Type:   querypb.Type_TUPLE,
		Values: []*querypb.Value{sqltypes.TupleToProto(row)},
	}
}

func (ps *UncorrelatedSubquery) description() PrimitiveDescription {
	other := map[string]any{}
	var pulloutVars []string
	if ps.HasValues != "" {
		pulloutVars = append(pulloutVars, ps.HasValues)
	}
	if len(ps.RowResults) > 0 {
		pulloutVars = append(pulloutVars, ps.RowResults...)
	} else if ps.SubqueryResult != "" {
		pulloutVars = append(pulloutVars, ps.SubqueryResult)
	}
	if len(pulloutVars) > 0 {
		other["PulloutVars"] = pulloutVars
	}
	if ps.RowColumns > 1 {
		other["RowColumns"] = ps.RowColumns
	}
	return PrimitiveDescription{
		OperatorType: "UncorrelatedSubquery",
		Variant:      ps.Opcode.String(),
		Other:        other,
	}
}
