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

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	. "vitess.io/vitess/go/vt/vtgate/engine/opcode"
)

var _ Primitive = (*UncorrelatedSubquery)(nil)

// UncorrelatedSubquery executes a subquery once and uses
// the result as a bind variable for the underlying primitive.
type UncorrelatedSubquery struct {
	Opcode PulloutOpcode

	// SubqueryResult and HasValues are used to send in the bindvar used in the query to the underlying primitive
	SubqueryResult string
	HasValues      string

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

// RouteType returns a description of the query routing type used by the primitive
func (ps *UncorrelatedSubquery) RouteType() string {
	return ps.Opcode.String()
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to.
func (ps *UncorrelatedSubquery) GetKeyspaceName() string {
	return ps.Outer.GetKeyspaceName()
}

// GetTableName specifies the table that this primitive routes to.
func (ps *UncorrelatedSubquery) GetTableName() string {
	return ps.Outer.GetTableName()
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
	for k, v := range bindVars {
		combinedVars[k] = v
	}
	switch ps.Opcode {
	case PulloutValue:
		combinedVars[ps.SubqueryResult] = sqltypes.NullBindVariable
	case PulloutIn, PulloutNotIn:
		combinedVars[ps.HasValues] = sqltypes.Int64BindVariable(0)
		combinedVars[ps.SubqueryResult] = &querypb.BindVariable{
			Type:   querypb.Type_TUPLE,
			Values: []*querypb.Value{sqltypes.ValueToProto(sqltypes.NewInt64(0))},
		}
	case PulloutExists:
		combinedVars[ps.HasValues] = sqltypes.Int64BindVariable(0)
	}
	return ps.Outer.GetFields(ctx, vcursor, combinedVars)
}

// NeedsTransaction implements the Primitive interface
func (ps *UncorrelatedSubquery) NeedsTransaction() bool {
	return ps.Subquery.NeedsTransaction() || ps.Outer.NeedsTransaction()
}

var (
	errSqRow    = vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "subquery returned more than one row")
	errSqColumn = vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "subquery returned more than one column")
)

func (ps *UncorrelatedSubquery) execSubquery(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (map[string]*querypb.BindVariable, error) {
	subqueryBindVars := make(map[string]*querypb.BindVariable, len(bindVars))
	for k, v := range bindVars {
		subqueryBindVars[k] = v
	}
	result, err := vcursor.ExecutePrimitive(ctx, ps.Subquery, subqueryBindVars, false)
	if err != nil {
		return nil, err
	}
	combinedVars := make(map[string]*querypb.BindVariable, len(bindVars)+1)
	for k, v := range bindVars {
		combinedVars[k] = v
	}
	switch ps.Opcode {
	case PulloutValue:
		switch len(result.Rows) {
		case 0:
			combinedVars[ps.SubqueryResult] = sqltypes.NullBindVariable
		case 1:
			combinedVars[ps.SubqueryResult] = sqltypes.ValueBindVariable(result.Rows[0][0])
		default:
			return nil, errSqRow
		}
	case PulloutIn, PulloutNotIn:
		switch len(result.Rows) {
		case 0:
			combinedVars[ps.HasValues] = sqltypes.Int64BindVariable(0)
			// Add a bogus value. It will not be checked.
			combinedVars[ps.SubqueryResult] = &querypb.BindVariable{
				Type:   querypb.Type_TUPLE,
				Values: []*querypb.Value{sqltypes.ValueToProto(sqltypes.NewInt64(0))},
			}
		default:
			combinedVars[ps.HasValues] = sqltypes.Int64BindVariable(1)
			values := &querypb.BindVariable{
				Type:   querypb.Type_TUPLE,
				Values: make([]*querypb.Value, len(result.Rows)),
			}
			for i, v := range result.Rows {
				values.Values[i] = sqltypes.ValueToProto(v[0])
			}
			combinedVars[ps.SubqueryResult] = values
		}
	case PulloutExists:
		switch len(result.Rows) {
		case 0:
			combinedVars[ps.HasValues] = sqltypes.Int64BindVariable(0)
		default:
			combinedVars[ps.HasValues] = sqltypes.Int64BindVariable(1)
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
	if ps.SubqueryResult != "" {
		pulloutVars = append(pulloutVars, ps.SubqueryResult)
	}
	if len(pulloutVars) > 0 {
		other["PulloutVars"] = pulloutVars
	}
	return PrimitiveDescription{
		OperatorType: "UncorrelatedSubquery",
		Variant:      ps.Opcode.String(),
		Other:        other,
	}
}
