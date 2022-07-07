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

package vdiff

import (
	"context"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

// contextVCursor satisfies VCursor, but only implements Context().
// MergeSort only requires Context to be implemented.
type contextVCursor struct {
	engine.VCursor
	ctx context.Context
}

func (vc *contextVCursor) ConnCollation() collations.ID {
	return collations.CollationBinaryID
}

func (vc *contextVCursor) ExecutePrimitive(ctx context.Context, primitive engine.Primitive, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	return primitive.TryExecute(ctx, vc, bindVars, wantfields)
}

func (vc *contextVCursor) StreamExecutePrimitive(ctx context.Context, primitive engine.Primitive, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	return primitive.TryStreamExecute(ctx, vc, bindVars, wantfields, callback)
}
