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

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

var _ Primitive = (*Upsert)(nil)

// Upsert Primitive will execute the insert primitive first and
// if there is `Duplicate Key` error, it executes the update primitive.
type Upsert struct {
	updPrimitive Primitive
	insPrimitive Primitive

	txNeeded
}

func (u *Upsert) RouteType() string {
	return "UPSERT"
}

func (u *Upsert) GetKeyspaceName() string {
	return u.insPrimitive.GetKeyspaceName()
}

func (u *Upsert) GetTableName() string {
	return u.insPrimitive.GetTableName()
}

func (u *Upsert) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.VT13001("unexpected to receive GetFields call for insert on duplicate key update query")
}

func (u *Upsert) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	qr, err := vcursor.ExecutePrimitive(ctx, u.insPrimitive, bindVars, wantfields)
	if err == nil {
		return qr, nil
	}
	if vterrors.Code(err) != vtrpcpb.Code_ALREADY_EXISTS {
		return nil, err
	}
	return vcursor.ExecutePrimitive(ctx, u.updPrimitive, bindVars, wantfields)
}

func (u *Upsert) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	err := vcursor.StreamExecutePrimitive(ctx, u.insPrimitive, bindVars, wantfields, callback)
	if err == nil {
		return nil
	}
	if vterrors.Code(err) != vtrpcpb.Code_ALREADY_EXISTS {
		return err
	}
	return vcursor.StreamExecutePrimitive(ctx, u.updPrimitive, bindVars, wantfields, callback)
}

func (u *Upsert) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{u.updPrimitive, u.insPrimitive}, nil
}

func (u *Upsert) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType:     "Upsert",
		TargetTabletType: topodatapb.TabletType_PRIMARY,
	}
}
