/*
Copyright 2022 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
)

var _ Primitive = (*ShowExec)(nil)

// ShowExec is a primitive to call into executor via vcursor.
type ShowExec struct {
	Command    sqlparser.ShowCommandType
	ShowFilter *sqlparser.ShowFilter

	noInputs
	noTxNeeded
}

func (s *ShowExec) RouteType() string {
	return "ShowExec"
}

func (s *ShowExec) GetKeyspaceName() string {
	return ""
}

func (s *ShowExec) GetTableName() string {
	return ""
}

func (s *ShowExec) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*query.BindVariable) (*sqltypes.Result, error) {
	qr, err := s.TryExecute(ctx, vcursor, bindVars, true)
	if err != nil {
		return nil, err
	}
	qr.Rows = nil
	return qr, nil
}

func (s *ShowExec) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*query.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	return vcursor.ShowExec(ctx, s.Command, s.ShowFilter)
}

func (s *ShowExec) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*query.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	qr, err := s.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(qr)
}

func (s *ShowExec) description() PrimitiveDescription {
	other := map[string]any{}
	if s.ShowFilter != nil {
		other["Filter"] = sqlparser.String(s.ShowFilter)
	}
	return PrimitiveDescription{
		OperatorType: "ShowExec",
		Variant:      s.Command.ToString(),
		Other:        other,
	}
}
