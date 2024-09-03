/*
Copyright 2024 The Vitess Authors.

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
)

var _ Primitive = (*JoinValues)(nil)

// JoinValues is a primitive that joins two primitives by constructing a table from the rows of the LHS primitive.
// The table is passed in as a bind variable to the RHS primitive.
type JoinValues struct {
	// Left and Right are the LHS and RHS primitives
	// of the Join. They can be any primitive.
	// When Left is empty, the WhenLeftEmpty primitive will be executed.
	Left, Right, WhenLeftEmpty Primitive

	RowConstructorArg string
}

// TryExecute performs a non-streaming exec.
func (jv *JoinValues) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	lresult, err := vcursor.ExecutePrimitive(ctx, jv.Left, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	if len(lresult.Rows) == 0 && wantfields {
		return jv.WhenLeftEmpty.GetFields(ctx, vcursor, bindVars)
	}
	bv := &querypb.BindVariable{
		Type: querypb.Type_TUPLE,
	}
	for _, row := range lresult.Rows {
		bv.Values = append(bv.Values, sqltypes.TupleToProto(row))
	}
	bindVars[jv.RowConstructorArg] = bv
	return vcursor.ExecutePrimitive(ctx, jv.Right, bindVars, wantfields)
}

// TryStreamExecute performs a streaming exec.
func (jv *JoinValues) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	panic("implement me")
}

// GetFields fetches the field info.
func (jv *JoinValues) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return jv.WhenLeftEmpty.GetFields(ctx, vcursor, bindVars)
}

// Inputs returns the input primitives for this join
func (jv *JoinValues) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{jv.Left, jv.Right, jv.WhenLeftEmpty}, nil
}

// RouteType returns a description of the query routing type used by the primitive
func (jv *JoinValues) RouteType() string {
	return "JoinValues"
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to.
func (jv *JoinValues) GetKeyspaceName() string {
	if jv.Left.GetKeyspaceName() == jv.Right.GetKeyspaceName() {
		return jv.Left.GetKeyspaceName()
	}
	return jv.Left.GetKeyspaceName() + "_" + jv.Right.GetKeyspaceName()
}

// GetTableName specifies the table that this primitive routes to.
func (jv *JoinValues) GetTableName() string {
	return jv.Left.GetTableName() + "_" + jv.Right.GetTableName()
}

// NeedsTransaction implements the Primitive interface
func (jv *JoinValues) NeedsTransaction() bool {
	return jv.Right.NeedsTransaction() || jv.Left.NeedsTransaction()
}

func (jv *JoinValues) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "Join",
		Variant:      "Values",
		Other: map[string]any{
			"ValuesArg": jv.RowConstructorArg,
		},
	}
}
