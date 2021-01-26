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
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ Primitive = (*CreateDatabase)(nil)

// CreateDatabase is just a container around custom database provisioning plugins
// the default behaviour is to just return an error
type CreateDatabase struct {
	Name string
	DoIt func(dbName string) error
	noInputs
	noTxNeeded
}

// RouteType implements the Primitive interface
func (c *CreateDatabase) RouteType() string {
	return "create database"
}

// GetKeyspaceName implements the Primitive interface
func (c *CreateDatabase) GetKeyspaceName() string {
	return c.Name
}

// GetTableName implements the Primitive interface
func (c *CreateDatabase) GetTableName() string {
	return ""
}

// Execute implements the Primitive interface
func (c *CreateDatabase) Execute(VCursor, map[string]*querypb.BindVariable, bool) (*sqltypes.Result, error) {
	err := c.DoIt(c.Name)
	if err != nil {
		return nil, err
	}

	return &sqltypes.Result{}, nil
}

// StreamExecute implements the Primitive interface
func (c *CreateDatabase) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	res, err := c.Execute(vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(res)
}

// GetFields implements the Primitive interface
func (c *CreateDatabase) GetFields(VCursor, map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return &sqltypes.Result{}, nil
}

// description implements the Primitive interface
func (c *CreateDatabase) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "CREATE DATABASE",
		Keyspace:     &vindexes.Keyspace{Name: c.Name},
	}
}
