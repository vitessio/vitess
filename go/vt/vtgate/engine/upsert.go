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
	"fmt"

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
	txNeeded
	Upserts []upsert
}

type upsert struct {
	Insert Primitive
	Update Primitive
}

// AddUpsert appends to the Upsert Primitive.
func (u *Upsert) AddUpsert(ins, upd Primitive) {
	u.Upserts = append(u.Upserts, upsert{
		Insert: ins,
		Update: upd,
	})
}

// RouteType implements Primitive interface type.
func (u *Upsert) RouteType() string {
	return "UPSERT"
}

// GetKeyspaceName implements Primitive interface type.
func (u *Upsert) GetKeyspaceName() string {
	if len(u.Upserts) > 0 {
		return u.Upserts[0].Insert.GetKeyspaceName()
	}
	return ""
}

// GetTableName implements Primitive interface type.
func (u *Upsert) GetTableName() string {
	if len(u.Upserts) > 0 {
		return u.Upserts[0].Insert.GetTableName()
	}
	return ""
}

// GetFields implements Primitive interface type.
func (u *Upsert) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.VT13001("unexpected to receive GetFields call for insert on duplicate key update query")
}

// TryExecute implements Primitive interface type.
func (u *Upsert) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	result := &sqltypes.Result{}
	for _, up := range u.Upserts {
		qr, err := execOne(ctx, vcursor, bindVars, wantfields, up)
		if err != nil {
			return nil, err
		}
		result.RowsAffected += qr.RowsAffected
	}
	return result, nil
}

func execOne(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, up upsert) (*sqltypes.Result, error) {
	insQr, err := vcursor.ExecutePrimitive(ctx, up.Insert, bindVars, wantfields)
	if err == nil {
		return insQr, nil
	}
	if vterrors.Code(err) != vtrpcpb.Code_ALREADY_EXISTS {
		return nil, err
	}
	updQr, err := vcursor.ExecutePrimitive(ctx, up.Update, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	// To match mysql, need to report +1 on rows affected if there is any change.
	if updQr.RowsAffected > 0 {
		updQr.RowsAffected += 1
	}
	return updQr, nil
}

// TryStreamExecute implements Primitive interface type.
func (u *Upsert) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	qr, err := u.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(qr)
}

// Inputs implements Primitive interface type.
func (u *Upsert) Inputs() ([]Primitive, []map[string]any) {
	var inputs []Primitive
	var inputsMap []map[string]any
	for i, up := range u.Upserts {
		inputs = append(inputs, up.Insert, up.Update)
		inputsMap = append(inputsMap,
			map[string]any{inputName: fmt.Sprintf("Insert-%d", i+1)},
			map[string]any{inputName: fmt.Sprintf("Update-%d", i+1)})
	}
	return inputs, inputsMap
}

func (u *Upsert) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType:     "Upsert",
		TargetTabletType: topodatapb.TabletType_PRIMARY,
	}
}
