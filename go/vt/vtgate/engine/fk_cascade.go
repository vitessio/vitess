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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type Child struct {
	BVName string
	Cols   []int // indexes
	P      Primitive
}

// FK_Cascade is a primitive that implements foreign key cascading using Input as values required to execute the Child Primitive.
// On success, it executes the Parent Primitive.
type FK_Cascade struct {
	Input  Primitive
	Child  []Child
	Parent Primitive

	txNeeded
}

// RouteType implements the Primitive interface.
func (fkc *FK_Cascade) RouteType() string {
	return "FK_CASCADE"
}

// GetKeyspaceName implements the Primitive interface.
func (fkc *FK_Cascade) GetKeyspaceName() string {
	return fkc.Parent.GetKeyspaceName()
}

// GetTableName implements the Primitive interface.
func (fkc *FK_Cascade) GetTableName() string {
	return fkc.Parent.GetTableName()
}

// GetFields implements the Primitive interface.
func (fkc *FK_Cascade) GetFields(_ context.Context, _ VCursor, _ map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] GetFields should not be called")
}

// TryExecute implements the Primitive interface.
func (fkc *FK_Cascade) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	inputRes, err := vcursor.ExecutePrimitive(ctx, fkc.Input, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	if len(inputRes.Rows) == 0 {
		return &sqltypes.Result{}, nil
	}

	for _, child := range fkc.Child {
		bv := &querypb.BindVariable{
			Type: querypb.Type_TUPLE,
		}
		for _, row := range inputRes.Rows {
			tuple := &querypb.Value{
				Type: querypb.Type_TUPLE,
			}
			for _, colIdx := range child.Cols {
				tuple.Values = append(tuple.Values,
					sqltypes.ValueToProto(row[colIdx]))
			}
			bv.Values = append(bv.Values, tuple)
		}
		bindVars[child.BVName] = bv
		_, err = vcursor.ExecutePrimitive(ctx, child.P, bindVars, wantfields)
		if err != nil {
			return nil, err
		}
		delete(bindVars, child.BVName)
	}

	return vcursor.ExecutePrimitive(ctx, fkc.Parent, bindVars, wantfields)
}

// TryStreamExecute implements the Primitive interface.
func (fkc *FK_Cascade) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	// TODO implement me
	panic("implement me")
}

// Inputs implements the Primitive interface.
func (fkc *FK_Cascade) Inputs() []Primitive {
	inputs := []Primitive{fkc.Input}
	for _, child := range fkc.Child {
		inputs = append(inputs, child.P)
	}
	inputs = append(inputs, fkc.Parent)
	return inputs
}

func (fkc *FK_Cascade) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: fkc.RouteType(),
	}
}

var _ Primitive = (*FK_Cascade)(nil)
