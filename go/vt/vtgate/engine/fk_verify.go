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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// Verify contains the verification primitve and its type i.e. parent or child
type Verify struct {
	Exec Primitive
	Typ  string
}

// FkVerify is a primitive that verifies that the foreign key constraints in parent tables are satisfied.
// It does this by executing a select distinct query on the parent table with the values that are being inserted/updated.
type FkVerify struct {
	txNeeded

	Verify []*Verify
	Exec   Primitive
}

// constants for verification type.
const (
	ParentVerify = "VerifyParent"
	ChildVerify  = "VerifyChild"
)

// RouteType implements the Primitive interface
func (f *FkVerify) RouteType() string {
	return "FKVerify"
}

// GetKeyspaceName implements the Primitive interface
func (f *FkVerify) GetKeyspaceName() string {
	return f.Exec.GetKeyspaceName()
}

// GetTableName implements the Primitive interface
func (f *FkVerify) GetTableName() string {
	return f.Exec.GetTableName()
}

// GetFields implements the Primitive interface
func (f *FkVerify) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] GetFields should not be called")
}

// TryExecute implements the Primitive interface
func (f *FkVerify) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	for _, v := range f.Verify {
		qr, err := vcursor.ExecutePrimitive(ctx, v.Exec, bindVars, wantfields)
		if err != nil {
			return nil, err
		}
		if len(qr.Rows) > 0 {
			return nil, getError(v.Typ)
		}
	}
	return vcursor.ExecutePrimitive(ctx, f.Exec, bindVars, wantfields)
}

// TryStreamExecute implements the Primitive interface
func (f *FkVerify) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	res, err := f.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(res)
}

// Inputs implements the Primitive interface
func (f *FkVerify) Inputs() ([]Primitive, []map[string]any) {
	var inputs []Primitive
	var inputsMap []map[string]any
	for idx, v := range f.Verify {
		inputsMap = append(inputsMap, map[string]any{
			inputName: fmt.Sprintf("%s-%d", v.Typ, idx+1),
		})
		inputs = append(inputs, v.Exec)
	}
	inputs = append(inputs, f.Exec)
	inputsMap = append(inputsMap, map[string]any{
		inputName: "PostVerify",
	})
	return inputs, inputsMap

}

func (f *FkVerify) description() PrimitiveDescription {
	return PrimitiveDescription{OperatorType: f.RouteType()}
}

var _ Primitive = (*FkVerify)(nil)

func getError(typ string) error {
	if typ == ParentVerify {
		return vterrors.NewErrorf(vtrpcpb.Code_FAILED_PRECONDITION, vterrors.NoReferencedRow2, "Cannot add or update a child row: a foreign key constraint fails")
	}
	return vterrors.NewErrorf(vtrpcpb.Code_FAILED_PRECONDITION, vterrors.RowIsReferenced2, "Cannot delete or update a parent row: a foreign key constraint fails")
}
