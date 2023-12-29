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
	"vitess.io/vitess/go/vt/vterrors"
)

var _ Primitive = (*NonCommittal)(nil)

// NonCommittal prevents changes from committing.
type NonCommittal struct {
	Primitive Primitive
}

func (n *NonCommittal) RouteType() string {
	return n.Primitive.RouteType()
}

func (n *NonCommittal) GetKeyspaceName() string {
	return n.Primitive.GetKeyspaceName()
}

func (n *NonCommittal) GetTableName() string {
	return n.Primitive.GetTableName()
}

func (n *NonCommittal) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return n.Primitive.GetFields(ctx, vcursor, bindVars)
}

func (n *NonCommittal) NeedsTransaction() bool {
	return n.Primitive.NeedsTransaction()
}

func (n *NonCommittal) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	switch t := n.Primitive.(type) {
	// For now, only allow primitive to execute if it is a read.
	case *Route:
		return t.TryExecute(ctx, vcursor, bindVars, wantfields)
	default:
		// Eventually, we can handle DML by cloning the vcursor and configuring
		// the session to rollback.
		return &sqltypes.Result{}, vterrors.VT12003()
	}
}

func (n *NonCommittal) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	switch t := n.Primitive.(type) {
	// For now, only allow primitive to execute if it is a read.
	case *Route:
		return t.TryStreamExecute(ctx, vcursor, bindVars, wantfields, callback)
	default:
		// Eventually, we can handle DML by cloning the vcursor and configuring
		// the session to rollback.
		return vterrors.VT12003()
	}
}

// Inputs is a slice containing the inputs to this Primitive.
// The returned map has additional information about the inputs, that is used in the description.
func (n *NonCommittal) Inputs() ([]Primitive, []map[string]any) {
	return n.Primitive.Inputs()
}

// description is the description, sans the inputs, of this Primitive.
// to get the plan description with all children, use PrimitiveToPlanDescription()
func (n *NonCommittal) description() PrimitiveDescription {
	return n.Primitive.description()
}
