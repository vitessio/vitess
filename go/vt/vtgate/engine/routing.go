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

package engine

import (
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// Opcode is a number representing the opcode
// for any engine primitve.
type Opcode int

// This is the list of Opcode values.
const (
	// None is used for queries which do not need routing
	None = Opcode(iota)
	// Unsharded is for routing a statement
	// to an unsharded keyspace.
	Unsharded
	// Any is for routing a statement
	// to any shard of a keyspace. e.g. - Reference tables
	Any
	// Equal is for routing a statement to a single shard.
	// Requires: A Vindex, and a single Value.
	Equal
	// In is for routing a statement to a multi shard.
	// Requires: A Vindex, and a multi Values.
	In
	// MultiEqual is used for routing queries with IN with tuple clause
	// Requires: A Vindex, and a multi Tuple Values.
	MultiEqual
	// Scatter is for routing a scattered statement.
	Scatter
	// ByDestination is to route explicitly to a given target destination.
	// Is used when the query explicitly sets a target destination:
	// in the clause e.g: UPDATE `keyspace[-]`.x1 SET foo=1
	ByDestination
	// DBA is used for routing DBA queries
	// e.g: Select * from information_schema.tables where schema_name = "a"
	DBA
)

type RoutingParameters struct {
	// Required for - All
	opcode   Opcode
	keyspace *vindexes.Keyspace

	// Required for - DBA
	sysTableTableSchema []evalengine.Expr
	sysTableTableName   map[string]evalengine.Expr
}

func (params *RoutingParameters) findRoute(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	switch params.opcode {
	case DBA:
		return params.systemQuery(vcursor, bindVars)
	case Unsharded:
		return params.unsharded(vcursor, bindVars)
	case Any:
		return params.anyShard(vcursor, bindVars)
	case None:
		return nil, nil, nil
	default:
		// Unreachable.
		return nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unsupported opcode: %v", params.opcode)
	}
}

func (params *RoutingParameters) systemQuery(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	destinations, err := params.routeInfoSchemaQuery(vcursor, bindVars)
	if err != nil {
		return nil, nil, err
	}

	return destinations, []map[string]*querypb.BindVariable{bindVars}, nil
}

func (params *RoutingParameters) routeInfoSchemaQuery(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, error) {
	defaultRoute := func() ([]*srvtopo.ResolvedShard, error) {
		ks := params.keyspace.Name
		destinations, _, err := vcursor.ResolveDestinations(ks, nil, []key.Destination{key.DestinationAnyShard{}})
		return destinations, vterrors.Wrapf(err, "failed to find information about keyspace `%s`", ks)
	}

	if len(params.sysTableTableName) == 0 && len(params.sysTableTableSchema) == 0 {
		return defaultRoute()
	}

	env := evalengine.EnvWithBindVars(bindVars)
	var specifiedKS string
	for _, tableSchema := range params.sysTableTableSchema {
		result, err := env.Evaluate(tableSchema)
		if err != nil {
			return nil, err
		}
		ks := result.Value().ToString()
		if specifiedKS == "" {
			specifiedKS = ks
		}
		if specifiedKS != ks {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "specifying two different database in the query is not supported")
		}
	}
	if specifiedKS != "" {
		bindVars[sqltypes.BvSchemaName] = sqltypes.StringBindVariable(specifiedKS)
	}

	tableNames := map[string]string{}
	for tblBvName, sysTableName := range params.sysTableTableName {
		val, err := env.Evaluate(sysTableName)
		if err != nil {
			return nil, err
		}
		tabName := val.Value().ToString()
		tableNames[tblBvName] = tabName
		bindVars[tblBvName] = sqltypes.StringBindVariable(tabName)
	}

	// if the table_schema is system schema, route to default keyspace.
	if sqlparser.SystemSchema(specifiedKS) {
		return defaultRoute()
	}

	// the use has specified a table_name - let's check if it's a routed table
	if len(tableNames) > 0 {
		rss, err := params.routedTable(vcursor, bindVars, specifiedKS, tableNames)
		if err != nil {
			// Only if keyspace is not found in vschema, we try with default keyspace.
			// As the in the table_schema predicates for a keyspace 'ks' it can contain 'vt_ks'.
			if vterrors.ErrState(err) == vterrors.BadDb {
				return defaultRoute()
			}
			return nil, err
		}
		if rss != nil {
			return rss, nil
		}
	}

	// it was not a routed table, and we dont have a schema name to look up. give up
	if specifiedKS == "" {
		return defaultRoute()
	}

	// we only have table_schema to work with
	destinations, _, err := vcursor.ResolveDestinations(specifiedKS, nil, []key.Destination{key.DestinationAnyShard{}})
	if err != nil {
		log.Errorf("failed to route information_schema query to keyspace [%s]", specifiedKS)
		bindVars[sqltypes.BvSchemaName] = sqltypes.StringBindVariable(specifiedKS)
		return defaultRoute()
	}
	setReplaceSchemaName(bindVars)
	return destinations, nil
}

func (params *RoutingParameters) routedTable(vcursor VCursor, bindVars map[string]*querypb.BindVariable, tableSchema string, tableNames map[string]string) ([]*srvtopo.ResolvedShard, error) {
	var routedKs *vindexes.Keyspace
	for tblBvName, tableName := range tableNames {
		tbl := sqlparser.TableName{
			Name:      sqlparser.NewTableIdent(tableName),
			Qualifier: sqlparser.NewTableIdent(tableSchema),
		}
		routedTable, err := vcursor.FindRoutedTable(tbl)
		if err != nil {
			return nil, err
		}

		if routedTable != nil {
			// if we were able to find information about this table, let's use it

			// check if the query is send to single keyspace.
			if routedKs == nil {
				routedKs = routedTable.Keyspace
			}
			if routedKs.Name != routedTable.Keyspace.Name {
				return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "cannot send the query to multiple keyspace due to different table_name: %s, %s", routedKs.Name, routedTable.Keyspace.Name)
			}

			shards, _, err := vcursor.ResolveDestinations(routedTable.Keyspace.Name, nil, []key.Destination{key.DestinationAnyShard{}})
			bindVars[tblBvName] = sqltypes.StringBindVariable(routedTable.Name.String())
			if tableSchema != "" {
				setReplaceSchemaName(bindVars)
			}
			return shards, err
		}
		// no routed table info found. we'll return nil and check on the outside if we can find the table_schema
		bindVars[tblBvName] = sqltypes.StringBindVariable(tableName)
	}
	return nil, nil
}

func setReplaceSchemaName(bindVars map[string]*querypb.BindVariable) {
	delete(bindVars, sqltypes.BvSchemaName)
	bindVars[sqltypes.BvReplaceSchemaName] = sqltypes.Int64BindVariable(1)
}

func (params *RoutingParameters) anyShard(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	rss, _, err := vcursor.ResolveDestinations(params.keyspace.Name, nil, []key.Destination{key.DestinationAnyShard{}})
	if err != nil {
		return nil, nil, err
	}
	multiBindVars := make([]map[string]*querypb.BindVariable, len(rss))
	for i := range multiBindVars {
		multiBindVars[i] = bindVars
	}
	return rss, multiBindVars, nil
}

func (params *RoutingParameters) unsharded(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	rss, _, err := vcursor.ResolveDestinations(params.keyspace.Name, nil, []key.Destination{key.DestinationAllShards{}})
	if err != nil {
		return nil, nil, err
	}
	if len(rss) != 1 {
		return nil, nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "cannot send query to multiple shards for un-sharded database: %v", rss)
	}
	multiBindVars := make([]map[string]*querypb.BindVariable, len(rss))
	for i := range multiBindVars {
		multiBindVars[i] = bindVars
	}
	return rss, multiBindVars, nil
}
