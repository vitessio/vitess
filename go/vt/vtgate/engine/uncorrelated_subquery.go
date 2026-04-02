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
	"maps"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var _ Primitive = (*UncorrelatedSubquery)(nil)

// UncorrelatedSubquery executes a subquery once and produces three bind
// variables from the result: a scalar value, a tuple list, and a has-values
// flag. The outer query references whichever it needs.
type UncorrelatedSubquery struct {
	// ScalarResult is the bind var name for the scalar value (first row, first col, or NULL).
	ScalarResult string
	// ListResult is the bind var name for the tuple of all first-col values.
	ListResult string
	// HasValues is the bind var name for the boolean flag (0 = no rows, 1 = has rows).
	HasValues string
	// NeedsScalar is true when the outer query uses the scalar result.
	// When true, >1 row from the subquery is an error.
	NeedsScalar bool

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
	combinedVars := make(map[string]*querypb.BindVariable, len(bindVars)+3)
	maps.Copy(combinedVars, bindVars)
	if ps.ScalarResult != "" {
		combinedVars[ps.ScalarResult] = sqltypes.NullBindVariable
	}
	if ps.HasValues != "" {
		combinedVars[ps.HasValues] = sqltypes.Int64BindVariable(0)
	}
	if ps.ListResult != "" {
		combinedVars[ps.ListResult] = &querypb.BindVariable{
			Type:   querypb.Type_TUPLE,
			Values: []*querypb.Value{sqltypes.ValueToProto(sqltypes.NewInt64(0))},
		}
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
	combinedVars := make(map[string]*querypb.BindVariable, len(bindVars)+3)
	maps.Copy(combinedVars, bindVars)

	switch len(result.Rows) {
	case 0:
		if ps.ScalarResult != "" {
			combinedVars[ps.ScalarResult] = sqltypes.NullBindVariable
		}
		if ps.HasValues != "" {
			combinedVars[ps.HasValues] = sqltypes.Int64BindVariable(0)
		}
		if ps.ListResult != "" {
			combinedVars[ps.ListResult] = &querypb.BindVariable{
				Type:   querypb.Type_TUPLE,
				Values: []*querypb.Value{sqltypes.ValueToProto(sqltypes.NewInt64(0))},
			}
		}
	case 1:
		if ps.ScalarResult != "" {
			combinedVars[ps.ScalarResult] = sqltypes.ValueBindVariable(result.Rows[0][0])
		}
		if ps.HasValues != "" {
			combinedVars[ps.HasValues] = sqltypes.Int64BindVariable(1)
		}
		if ps.ListResult != "" {
			combinedVars[ps.ListResult] = &querypb.BindVariable{
				Type:   querypb.Type_TUPLE,
				Values: []*querypb.Value{sqltypes.ValueToProto(result.Rows[0][0])},
			}
		}
	default:
		if ps.NeedsScalar {
			return nil, errSqRow
		}
		if ps.ScalarResult != "" {
			combinedVars[ps.ScalarResult] = sqltypes.NullBindVariable
		}
		if ps.HasValues != "" {
			combinedVars[ps.HasValues] = sqltypes.Int64BindVariable(1)
		}
		if ps.ListResult != "" {
			values := &querypb.BindVariable{
				Type:   querypb.Type_TUPLE,
				Values: make([]*querypb.Value, len(result.Rows)),
			}
			for i, v := range result.Rows {
				values.Values[i] = sqltypes.ValueToProto(v[0])
			}
			combinedVars[ps.ListResult] = values
		}
	}

	return combinedVars, nil
}

func (ps *UncorrelatedSubquery) description() PrimitiveDescription {
	other := map[string]any{}
	var pulloutVars []string
	if ps.HasValues != "" {
		pulloutVars = append(pulloutVars, ps.HasValues)
	}
	if ps.ListResult != "" {
		pulloutVars = append(pulloutVars, ps.ListResult)
	}
	if ps.ScalarResult != "" {
		pulloutVars = append(pulloutVars, ps.ScalarResult)
	}
	if len(pulloutVars) > 0 {
		other["PulloutVars"] = pulloutVars
	}

	return PrimitiveDescription{
		OperatorType: "UncorrelatedSubquery",
		Variant:      "PulloutSubquery",
		Other:        other,
	}
}
