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
	"sync"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*RePlan)(nil)

type RePlan struct {
	Input      Primitive
	replanOnce sync.Once
	replan     func()
}

// RouteType returns a description of the query routing type used by the primitive
func (rp *RePlan) RouteType() string {
	return rp.Input.RouteType()
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to.
func (rp *RePlan) GetKeyspaceName() string {
	return rp.Input.GetKeyspaceName()
}

// GetTableName specifies the table that this primitive routes to.
func (rp *RePlan) GetTableName() string {
	return rp.Input.GetTableName()
}

// TryExecute satisfies the Primitive interface.
func (rp *RePlan) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	defer func() {
		go rp.replanOnce.Do(rp.replan)
	}()
	r, err := rp.Input.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// TryStreamExecute satisfies the Primitive interface.
func (rp *RePlan) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	panic("not implemented")
}

// GetFields implements the Primitive interface.
func (rp *RePlan) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return rp.Input.GetFields(ctx, vcursor, bindVars)
}

// Inputs returns the input to limit
func (rp *RePlan) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{rp.Input}, nil
}

// NeedsTransaction implements the Primitive interface.
func (rp *RePlan) NeedsTransaction() bool {
	return rp.Input.NeedsTransaction()
}

func (rp *RePlan) description() PrimitiveDescription {
	other := map[string]any{}
	return PrimitiveDescription{
		Other: other,
	}
}
