/*
Copyright 2020 The Vitess Authors.

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
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*Reserve)(nil)

//Reserve will mark the session as needing a
//reserved connection and then execute the inner Primitive
type Reserve struct {
	Input Primitive
}

// RouteType is part of the Primitive interface
func (r *Reserve) RouteType() string {
	return "reserve"
}

// GetKeyspaceName is part of the Primitive interface
func (r *Reserve) GetKeyspaceName() string {
	return r.Input.GetKeyspaceName()
}

// GetTableName is part of the Primitive interface
func (r *Reserve) GetTableName() string {
	return r.Input.GetTableName()
}

// Execute is part of the Primitive interface
func (r *Reserve) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	vcursor.Session().NeedsReservedConn()
	return r.Input.Execute(vcursor, bindVars, wantfields)
}

// StreamExecute is part of the Primitive interface
func (r *Reserve) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	vcursor.Session().NeedsReservedConn()
	return r.Input.StreamExecute(vcursor, bindVars, wantfields, callback)
}

// GetFields is part of the Primitive interface
func (r *Reserve) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return r.Input.GetFields(vcursor, bindVars)
}

// NeedsTransaction is part of the Primitive interface
func (r *Reserve) NeedsTransaction() bool {
	return r.Input.NeedsTransaction()
}

// Inputs is part of the Primitive interface
func (r *Reserve) Inputs() []Primitive {
	return []Primitive{}
}

func (r *Reserve) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "Reserve",
	}
}
