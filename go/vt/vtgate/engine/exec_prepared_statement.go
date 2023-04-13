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
	"strconv"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

var _ Primitive = (*ExecStmt)(nil)

type ExecStmt struct {
	Params []*sqlparser.Variable
	Input  Primitive

	noTxNeeded
}

func (e *ExecStmt) RouteType() string {
	return "EXECUTE"
}

func (e *ExecStmt) GetKeyspaceName() string {
	return e.Input.GetKeyspaceName()
}

func (e *ExecStmt) GetTableName() string {
	return e.Input.GetTableName()
}

func (e *ExecStmt) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.VT12001("prepare command on execute statement")
}

func (e *ExecStmt) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	bindVars = e.prepareBindVars(vcursor, bindVars)
	return vcursor.ExecutePrimitive(ctx, e.Input, bindVars, wantfields)
}

func (e *ExecStmt) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	bindVars = e.prepareBindVars(vcursor, bindVars)
	return vcursor.StreamExecutePrimitive(ctx, e.Input, bindVars, wantfields, callback)
}

func (e *ExecStmt) Inputs() []Primitive {
	return []Primitive{e.Input}
}

func (e *ExecStmt) description() PrimitiveDescription {
	var params []string
	for _, p := range e.Params {
		params = append(params, p.Name.Lowered())
	}
	return PrimitiveDescription{
		OperatorType: e.RouteType(),
		Other: map[string]any{
			"Parameters": params,
		},
	}
}

func (e *ExecStmt) prepareBindVars(vcursor VCursor, bindVars map[string]*querypb.BindVariable) map[string]*querypb.BindVariable {
	count := 1
	for _, p := range e.Params {
		bvName := "v" + strconv.Itoa(count)
		bv := vcursor.Session().GetUDV(p.Name.Lowered())
		if bv == nil {
			bv = sqltypes.NullBindVariable
		}
		bindVars[bvName] = bv
		count++
	}
	return bindVars
}
