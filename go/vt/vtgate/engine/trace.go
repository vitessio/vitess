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

var _ Primitive = (*Trace)(nil)

type Trace struct {
	identifiablePrimitive
	Inner Primitive
}

func (t *Trace) RouteType() string {
	return t.Inner.RouteType()
}

func (t *Trace) GetKeyspaceName() string {
	return t.Inner.GetKeyspaceName()
}

func (t *Trace) GetTableName() string {
	return t.Inner.GetTableName()
}

func (t *Trace) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	vcursor.EnableOperatorTracing()
	return t.Inner.GetFields(ctx, vcursor, bindVars)
}

func (t *Trace) NeedsTransaction() bool {
	return t.Inner.NeedsTransaction()
}

func (t *Trace) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	vcursor.EnableOperatorTracing()
	return t.Inner.TryExecute(ctx, vcursor, bindVars, wantfields)
}

func (t *Trace) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	vcursor.EnableOperatorTracing()
	return t.Inner.TryStreamExecute(ctx, vcursor, bindVars, wantfields, callback)
}

func (t *Trace) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{t.Inner}, nil
}

func (t *Trace) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "Trace",
	}
}
