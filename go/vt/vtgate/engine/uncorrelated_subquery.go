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
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
)

var _ Primitive = (*UncorrelatedSubquery)(nil)

// SubqueryOutput describes one set of bind variables to produce from a subquery result.
type SubqueryOutput struct {
	Opcode         opcode.PulloutOpcode
	SubqueryResult string
	HasValues      string
}

// UncorrelatedSubquery executes a subquery once and uses
// the result as bind variables for the underlying primitive.
// Multiple outputs can be produced from a single execution when
// the same subquery is used in different contexts (e.g., scalar and IN).
type UncorrelatedSubquery struct {
	Outputs  []SubqueryOutput
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
	combinedVars := make(map[string]*querypb.BindVariable, len(bindVars)+len(ps.Outputs)*2)
	maps.Copy(combinedVars, bindVars)
	for _, out := range ps.Outputs {
		switch out.Opcode {
		case opcode.PulloutValue:
			combinedVars[out.SubqueryResult] = sqltypes.NullBindVariable
		case opcode.PulloutIn, opcode.PulloutNotIn:
			combinedVars[out.HasValues] = sqltypes.Int64BindVariable(0)
			combinedVars[out.SubqueryResult] = &querypb.BindVariable{
				Type:   querypb.Type_TUPLE,
				Values: []*querypb.Value{sqltypes.ValueToProto(sqltypes.NewInt64(0))},
			}
		case opcode.PulloutExists:
			combinedVars[out.HasValues] = sqltypes.Int64BindVariable(0)
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
	combinedVars := make(map[string]*querypb.BindVariable, len(bindVars)+len(ps.Outputs)*2)
	maps.Copy(combinedVars, bindVars)
	for _, out := range ps.Outputs {
		if err := ps.bindOutput(out, result, combinedVars); err != nil {
			return nil, err
		}
	}
	return combinedVars, nil
}

func (ps *UncorrelatedSubquery) bindOutput(out SubqueryOutput, result *sqltypes.Result, combinedVars map[string]*querypb.BindVariable) error {
	switch out.Opcode {
	case opcode.PulloutValue:
		switch len(result.Rows) {
		case 0:
			combinedVars[out.SubqueryResult] = sqltypes.NullBindVariable
		case 1:
			combinedVars[out.SubqueryResult] = sqltypes.ValueBindVariable(result.Rows[0][0])
		default:
			return errSqRow
		}
	case opcode.PulloutIn, opcode.PulloutNotIn:
		switch len(result.Rows) {
		case 0:
			combinedVars[out.HasValues] = sqltypes.Int64BindVariable(0)
			// Add a bogus value. It will not be checked.
			combinedVars[out.SubqueryResult] = &querypb.BindVariable{
				Type:   querypb.Type_TUPLE,
				Values: []*querypb.Value{sqltypes.ValueToProto(sqltypes.NewInt64(0))},
			}
		default:
			combinedVars[out.HasValues] = sqltypes.Int64BindVariable(1)
			values := &querypb.BindVariable{
				Type:   querypb.Type_TUPLE,
				Values: make([]*querypb.Value, len(result.Rows)),
			}
			for i, v := range result.Rows {
				values.Values[i] = sqltypes.ValueToProto(v[0])
			}
			combinedVars[out.SubqueryResult] = values
		}
	case opcode.PulloutExists:
		switch len(result.Rows) {
		case 0:
			combinedVars[out.HasValues] = sqltypes.Int64BindVariable(0)
		default:
			combinedVars[out.HasValues] = sqltypes.Int64BindVariable(1)
		}
	}
	return nil
}

func (ps *UncorrelatedSubquery) description() PrimitiveDescription {
	other := map[string]any{}
	var pulloutVars []string
	for _, out := range ps.Outputs {
		if out.HasValues != "" {
			pulloutVars = append(pulloutVars, out.HasValues)
		}
		if out.SubqueryResult != "" {
			pulloutVars = append(pulloutVars, out.SubqueryResult)
		}
	}
	if len(pulloutVars) > 0 {
		other["PulloutVars"] = pulloutVars
	}

	variant := ""
	if len(ps.Outputs) == 1 {
		variant = ps.Outputs[0].Opcode.String()
	} else {
		variant = "MultiOutput"
	}

	return PrimitiveDescription{
		OperatorType: "UncorrelatedSubquery",
		Variant:      variant,
		Other:        other,
	}
}
