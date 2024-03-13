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

var _ Primitive = (*Record)(nil)

type Record struct {
	Input  Primitive
	doOnce sync.Once
	record func(result *sqltypes.Result)
}

// RouteType returns a description of the query routing type used by the primitive
func (rc *Record) RouteType() string {
	return rc.Input.RouteType()
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to.
func (rc *Record) GetKeyspaceName() string {
	return rc.Input.GetKeyspaceName()
}

// GetTableName specifies the table that this primitive routes to.
func (rc *Record) GetTableName() string {
	return rc.Input.GetTableName()
}

// TryExecute satisfies the Primitive interface.
func (rc *Record) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	r, err := rc.Input.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	go rc.doOnce.Do(func() {
		rc.record(r)
	})
	return r, nil
}

// TryStreamExecute satisfies the Primitive interface.
func (rc *Record) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	panic("not implemented")
}

// GetFields implements the Primitive interface.
func (rc *Record) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return rc.Input.GetFields(ctx, vcursor, bindVars)
}

// Inputs returns the input to limit
func (rc *Record) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{rc.Input}, nil
}

// NeedsTransaction implements the Primitive interface.
func (rc *Record) NeedsTransaction() bool {
	return rc.Input.NeedsTransaction()
}

func (rc *Record) description() PrimitiveDescription {
	other := map[string]any{}
	return PrimitiveDescription{
		Other: other,
	}
}
