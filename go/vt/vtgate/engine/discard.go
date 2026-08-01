/*
Copyright 2026 The Vitess Authors.

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

var _ Primitive = (*Discard)(nil)

// Discard executes its Source for its side effects and discards the result.
type Discard struct {
	Source Primitive
}

func (d *Discard) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	if _, err := vcursor.ExecutePrimitive(ctx, d.Source, bindVars, false); err != nil {
		return nil, err
	}
	return &sqltypes.Result{}, nil
}

func (d *Discard) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	if _, err := vcursor.ExecutePrimitive(ctx, d.Source, bindVars, false); err != nil {
		return err
	}
	return callback(&sqltypes.Result{})
}

func (d *Discard) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return &sqltypes.Result{}, nil
}

func (d *Discard) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{d.Source}, nil
}

func (d *Discard) NeedsTransaction() bool {
	return d.Source.NeedsTransaction()
}

func (d *Discard) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "Discard",
	}
}
