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

// Discard executes Input for its side effects only and discards the
// result, reporting a plain OK with 0 rows affected. Used for DO, which
// evaluates its expressions purely for side effects and never returns a
// result set to the client.
type Discard struct {
	Input Primitive
}

func (d *Discard) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	_, err := vcursor.ExecutePrimitive(ctx, d.Input, bindVars, false)
	if err != nil {
		return nil, err
	}
	return &sqltypes.Result{}, nil
}

func (d *Discard) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	_, err := vcursor.ExecutePrimitive(ctx, d.Input, bindVars, false)
	if err != nil {
		return err
	}
	return callback(&sqltypes.Result{})
}

func (d *Discard) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return &sqltypes.Result{}, nil
}

func (d *Discard) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{d.Input}, nil
}

func (d *Discard) NeedsTransaction() bool {
	return d.Input.NeedsTransaction()
}

func (d *Discard) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "Discard",
	}
}
