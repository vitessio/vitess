/*
Copyright 2021 The Vitess Authors.

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
	"fmt"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/srvtopo"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ Primitive = (*DBDDL)(nil)

//goland:noinspection GoVarAndConstTypeMayBeOmitted
var databaseCreatorPlugins = map[string]DBDDLPlugin{}

// DBDDLRegister registers a dbDDL plugin under the specified name.
// A duplicate plugin will generate a panic.
func DBDDLRegister(name string, plugin DBDDLPlugin) {
	if _, ok := databaseCreatorPlugins[name]; ok {
		panic(fmt.Sprintf("%s is already registered", name))
	}
	databaseCreatorPlugins[name] = plugin
}

// DBDDLPlugin is the interface that you need to implement to add a custom CREATE/DROP DATABASE handler
type DBDDLPlugin interface {
	CreateDatabase(ctx context.Context, name string) error
	DropDatabase(ctx context.Context, name string) error
}

// DBDDL is just a container around custom database provisioning plugins
// The default behaviour is to just return an error
type DBDDL struct {
	name         string
	create       bool
	queryTimeout int

	noInputs
	noTxNeeded
}

// NewDBDDL creates the engine primitive
// `create` will be true for CREATE, and false for DROP
func NewDBDDL(dbName string, create bool, timeout int) *DBDDL {
	return &DBDDL{
		name:         dbName,
		create:       create,
		queryTimeout: timeout,
	}
}

// RouteType implements the Primitive interface
func (c *DBDDL) RouteType() string {
	if c.create {
		return "CreateDB"
	}
	return "DropDB"
}

// GetKeyspaceName implements the Primitive interface
func (c *DBDDL) GetKeyspaceName() string {
	return c.name
}

// GetTableName implements the Primitive interface
func (c *DBDDL) GetTableName() string {
	return ""
}

// Execute implements the Primitive interface
func (c *DBDDL) Execute(vcursor VCursor, _ map[string]*querypb.BindVariable, _ bool) (*sqltypes.Result, error) {
	name := vcursor.GetDBDDLPluginName()
	plugin, ok := databaseCreatorPlugins[name]
	if !ok {
		log.Errorf("'%s' database ddl plugin is not registered. Falling back to default plugin", name)
		plugin = databaseCreatorPlugins[defaultDBDDLPlugin]
	}
	if c.queryTimeout != 0 {
		cancel := vcursor.SetContextTimeout(time.Duration(c.queryTimeout) * time.Millisecond)
		defer cancel()
	}

	if c.create {
		return c.createDatabase(vcursor, plugin)
	}

	return c.dropDatabase(vcursor, plugin)
}

func (c *DBDDL) createDatabase(vcursor VCursor, plugin DBDDLPlugin) (*sqltypes.Result, error) {
	ctx := vcursor.Context()
	err := plugin.CreateDatabase(ctx, c.name)
	if err != nil {
		return nil, err
	}
	var destinations []*srvtopo.ResolvedShard
	for {
		// loop until we have found a valid shard
		destinations, _, err = vcursor.ResolveDestinations(c.name, nil, []key.Destination{key.DestinationAllShards{}})
		if err == nil {
			break
		}
		select {
		case <-ctx.Done(): //context cancelled
			return nil, vterrors.Errorf(vtrpc.Code_DEADLINE_EXCEEDED, "could not validate create database: destination not resolved")
		case <-time.After(500 * time.Millisecond): //timeout
		}
	}
	var queries []*querypb.BoundQuery
	for range destinations {
		queries = append(queries, &querypb.BoundQuery{
			Sql:           "select 42 from dual where null",
			BindVariables: nil,
		})
	}

	for {
		_, errors := vcursor.ExecuteMultiShard(destinations, queries, false, true)

		noErr := true
		for _, err := range errors {
			if err != nil {
				noErr = false
				select {
				case <-ctx.Done(): //context cancelled
					return nil, vterrors.Errorf(vtrpc.Code_DEADLINE_EXCEEDED, "could not validate create database: tablets not healthy")
				case <-time.After(500 * time.Millisecond): //timeout
				}
				break
			}
		}
		if noErr {
			break
		}
	}
	return &sqltypes.Result{RowsAffected: 1}, nil
}

func (c *DBDDL) dropDatabase(vcursor VCursor, plugin DBDDLPlugin) (*sqltypes.Result, error) {
	ctx := vcursor.Context()
	err := plugin.DropDatabase(ctx, c.name)
	if err != nil {
		return nil, err
	}
	for vcursor.KeyspaceAvailable(c.name) {
		select {
		case <-ctx.Done(): //context cancelled
			return nil, vterrors.Errorf(vtrpc.Code_DEADLINE_EXCEEDED, "could not validate drop database: keyspace still available in vschema")
		case <-time.After(500 * time.Millisecond): //timeout
		}
	}

	return &sqltypes.Result{StatusFlags: sqltypes.ServerStatusDbDropped}, nil
}

// StreamExecute implements the Primitive interface
func (c *DBDDL) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	res, err := c.Execute(vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(res)
}

// GetFields implements the Primitive interface
func (c *DBDDL) GetFields(VCursor, map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return &sqltypes.Result{}, nil
}

// description implements the Primitive interface
func (c *DBDDL) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: strings.ToUpper(c.RouteType()),
		Keyspace:     &vindexes.Keyspace{Name: c.name},
	}
}
