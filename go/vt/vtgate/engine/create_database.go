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
	"strings"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ Primitive = (*DropCreateDatabase)(nil)

// DropCreateDatabase is just a container around custom database provisioning plugins
// The default behaviour is to just return an error
type DropCreateDatabase struct {
	name, verb string
	plugin     func() error
	noInputs
	noTxNeeded
}

// CreateDropCreateDatabase creates the engine primitive
func CreateDropCreateDatabase(dbName, verb string, plugin func() error) *DropCreateDatabase {
	return &DropCreateDatabase{
		name:   dbName,
		verb:   verb,
		plugin: plugin,
	}
}

// RouteType implements the Primitive interface
func (c *DropCreateDatabase) RouteType() string {
	return c.verb + " database"
}

// GetKeyspaceName implements the Primitive interface
func (c *DropCreateDatabase) GetKeyspaceName() string {
	return c.name
}

// GetTableName implements the Primitive interface
func (c *DropCreateDatabase) GetTableName() string {
	return ""
}

// Execute implements the Primitive interface
func (c *DropCreateDatabase) Execute(VCursor, map[string]*querypb.BindVariable, bool) (*sqltypes.Result, error) {
	err := c.plugin()
	if err != nil {
		return nil, err
	}

	return &sqltypes.Result{}, nil
}

// StreamExecute implements the Primitive interface
func (c *DropCreateDatabase) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	res, err := c.Execute(vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(res)
}

// GetFields implements the Primitive interface
func (c *DropCreateDatabase) GetFields(VCursor, map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return &sqltypes.Result{}, nil
}

// description implements the Primitive interface
func (c *DropCreateDatabase) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: strings.ToUpper(c.verb + " DATABASE"),
		Keyspace:     &vindexes.Keyspace{Name: c.name},
	}
}
