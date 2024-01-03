/*
Copyright 2023 The Vitess Authors.

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

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*DeleteMulti)(nil)

const DM_VALS = "dm_vals"

// DeleteMulti represents the instructions to perform a delete.
type DeleteMulti struct {
	Delete Primitive
	Input  Primitive

	txNeeded
}

func (del *DeleteMulti) RouteType() string {
	return "DELETEMULTI"
}

func (del *DeleteMulti) GetKeyspaceName() string {
	return del.Input.GetKeyspaceName()
}

func (del *DeleteMulti) GetTableName() string {
	return del.Input.GetTableName()
}

func (del *DeleteMulti) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{del.Input, del.Delete}, nil
}

// TryExecute performs a non-streaming exec.
func (del *DeleteMulti) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, _ bool) (*sqltypes.Result, error) {
	inputRes, err := vcursor.ExecutePrimitive(ctx, del.Input, bindVars, false)
	if err != nil {
		return nil, err
	}

	bv := &querypb.BindVariable{
		Type: querypb.Type_TUPLE,
	}
	for _, row := range inputRes.Rows {
		bv.Values = append(bv.Values, sqltypes.TupleToProto(row))
	}
	return vcursor.ExecutePrimitive(ctx, del.Delete, map[string]*querypb.BindVariable{
		DM_VALS: bv,
	}, false)
}

// TryStreamExecute performs a streaming exec.
func (del *DeleteMulti) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	res, err := del.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(res)
}

// GetFields fetches the field info.
func (del *DeleteMulti) GetFields(context.Context, VCursor, map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.VT13001("unreachable code for MULTI DELETE")
}

func (del *DeleteMulti) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType:     "DeleteMulti",
		TargetTabletType: topodatapb.TabletType_PRIMARY,
	}
}
