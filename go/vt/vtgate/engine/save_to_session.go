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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

type SaveToSession struct {
	noTxNeeded

	Source Primitive
	Offset evalengine.Expr
}

var _ Primitive = (*SaveToSession)(nil)

func (s *SaveToSession) RouteType() string {
	return s.Source.RouteType()
}

func (s *SaveToSession) GetKeyspaceName() string {
	return s.Source.GetKeyspaceName()
}

func (s *SaveToSession) GetTableName() string {
	return s.Source.GetTableName()
}

func (s *SaveToSession) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return s.Source.GetFields(ctx, vcursor, bindVars)
}

// TryExecute on SaveToSession will execute the Source and save the last row's value of Offset into the session.
func (s *SaveToSession) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	result, err := s.Source.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	intEvalResult, ok, err := s.getUintFromOffset(ctx, vcursor, bindVars, result)
	if err != nil {
		return nil, err
	}
	if ok {
		vcursor.Session().SetLastInsertID(intEvalResult)
	}
	return result, nil
}

func (s *SaveToSession) getUintFromOffset(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, result *sqltypes.Result) (uint64, bool, error) {
	if len(result.Rows) == 0 {
		return 0, false, nil
	}

	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	env.Row = result.Rows[len(result.Rows)-1] // last row
	evalResult, err := env.Evaluate(s.Offset)
	if err != nil {
		return 0, false, err
	}
	value, err := evalResult.Value(vcursor.ConnCollation()).ToCastUint64()
	if err != nil {
		return 0, false, err
	}
	return value, true, nil
}

// TryStreamExecute on SaveToSession will execute the Source and save the last row's value of Offset into the session.
func (s *SaveToSession) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	var value *uint64

	f := func(qr *sqltypes.Result) error {
		v, ok, err := s.getUintFromOffset(ctx, vcursor, bindVars, qr)
		if err != nil {
			return err
		}
		if ok {
			value = &v
		}
		return callback(qr)
	}

	err := s.Source.TryStreamExecute(ctx, vcursor, bindVars, wantfields, f)
	if err != nil {
		return err
	}
	if value != nil {
		vcursor.Session().SetLastInsertID(*value)
	}

	return nil
}

func (s *SaveToSession) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{s.Source}, nil
}

func (s *SaveToSession) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "SaveToSession",
		Other: map[string]interface{}{
			"Offset": sqlparser.String(s.Offset),
		},
	}
}
