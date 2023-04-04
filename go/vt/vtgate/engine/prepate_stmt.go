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
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/vterrors"
)

var _ Primitive = (*PrepareStmt)(nil)

type PrepareStmt struct {
	Name       string
	Query      string
	ParamCount int
	Input      Primitive

	noTxNeeded
}

func (p *PrepareStmt) RouteType() string {
	return "PREPARE"
}

func (p *PrepareStmt) GetKeyspaceName() string {
	return p.Input.GetKeyspaceName()
}

func (p *PrepareStmt) GetTableName() string {
	return p.Input.GetTableName()
}

func (p *PrepareStmt) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.VT12001("prepare command on prepare statement")
}

func (p *PrepareStmt) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	vcursor.Session().StorePrepareData(p.Name, &vtgatepb.PrepareData{
		PrepareStatement: p.Query,
		ParamsCount:      int32(p.ParamCount),
	})
	return &sqltypes.Result{}, nil
}

func (p *PrepareStmt) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	res, _ := p.TryExecute(ctx, vcursor, bindVars, wantfields)
	return callback(res)
}

func (p *PrepareStmt) Inputs() []Primitive {
	return []Primitive{p.Input}
}

func (p *PrepareStmt) description() PrimitiveDescription {
	other := map[string]any{
		"StatementName":    p.Name,
		"PrepareStatement": p.Query,
		"ParameterCount":   p.ParamCount,
	}

	return PrimitiveDescription{
		OperatorType: p.RouteType(),
		Other:        other,
	}
}
