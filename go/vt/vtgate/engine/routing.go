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
	"encoding/json"

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
	// Unsharded is for routing a statement
	// to an unsharded keyspace.
	Unsharded = Opcode(iota)
	// EqualUnique is for routing a query to a single shard.
	// Requires: A Unique Vindex, and a single Value.
	EqualUnique
	// Equal is for routing a query using a non-unique vindex.
	// Requires: A Vindex, and a single Value.
	Equal
	// IN is for routing a statement to a multi shard.
	// Requires: A Vindex, and a multi Values.
	IN
	// MultiEqual is used for routing queries with IN with tuple clause
	// Requires: A Vindex, and a multi Tuple Values.
	MultiEqual
	// Scatter is for routing a scattered statement.
	Scatter
	// Next is for fetching from a sequence.
	Next
	// DBA is used for routing DBA queries
	// e.g: Select * from information_schema.tables where schema_name = "a"
	DBA
	// Reference is for fetching from a reference table.
	Reference
	// None is used for queries which do not need routing
	None
	// ByDestination is to route explicitly to a given target destination.
	// Is used when the query explicitly sets a target destination:
	// in the clause e.g: UPDATE `keyspace[-]`.x1 SET foo=1
	ByDestination
	// NumOpcodes is the number of opcodes
	NumOpcodes
)

var opName = map[Opcode]string{
	Unsharded:     "Unsharded",
	EqualUnique:   "EqualUnique",
	Equal:         "Equal",
	IN:            "IN",
	MultiEqual:    "MultiEqual",
	Scatter:       "Scatter",
	DBA:           "DBA",
	Next:          "Next",
	Reference:     "Reference",
	None:          "None",
	ByDestination: "ByDestination",
}

// MarshalJSON serializes the Opcode as a JSON string.
// It's used for testing and diagnostics.
func (code Opcode) MarshalJSON() ([]byte, error) {
	return json.Marshal(opName[code])
}

// String returns a string presentation of this opcode
func (code Opcode) String() string {
	return opName[code]
}

type RoutingParameters struct {
	// Opcode is the execution opcode.
	Opcode Opcode

	// Keyspace specifies the keyspace to send the query to.
	Keyspace *vindexes.Keyspace

	// The following two fields are used when routing information_schema queries
	SysTableTableSchema []evalengine.Expr
	SysTableTableName   map[string]evalengine.Expr

	// TargetDestination specifies an explicit target destination to send the query to.
	// This will bypass the routing logic.
	TargetDestination key.Destination

	// Vindex specifies the vindex to be used.
	Vindex vindexes.Vindex

	// Values specifies the vindex values to use for routing.
	Values []evalengine.Expr
}

func (rp *RoutingParameters) findRoutingInfo(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	switch rp.Opcode {
	case DBA:
		return rp.systemQuery(vcursor, bindVars)
	case Unsharded, Next:
		return rp.unsharded(vcursor, bindVars)
	case Reference:
		return rp.anyShard(vcursor, bindVars)
	case Scatter:
		return rp.byDestination(vcursor, bindVars, key.DestinationAllShards{})
	case ByDestination:
		return rp.byDestination(vcursor, bindVars, rp.TargetDestination)
	case Equal, EqualUnique:
		switch rp.Vindex.(type) {
		case vindexes.MultiColumn:
			return rp.paramsSelectEqualMultiCol(vcursor, bindVars)
		default:
			return rp.paramsSelectEqual(vcursor, bindVars)
		}
	case None:
		return nil, nil, nil
	default:
		// Unreachable.
		return nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unsupported opcode: %v", rp.Opcode)
	}
}

func (rp *RoutingParameters) systemQuery(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	destinations, err := rp.routeInfoSchemaQuery(vcursor, bindVars)
	if err != nil {
		return nil, nil, err
	}

	return destinations, []map[string]*querypb.BindVariable{bindVars}, nil
}

func (rp *RoutingParameters) routeInfoSchemaQuery(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, error) {
	defaultRoute := func() ([]*srvtopo.ResolvedShard, error) {
		ks := rp.Keyspace.Name
		destinations, _, err := vcursor.ResolveDestinations(ks, nil, []key.Destination{key.DestinationAnyShard{}})
		return destinations, vterrors.Wrapf(err, "failed to find information about keyspace `%s`", ks)
	}

	if len(rp.SysTableTableName) == 0 && len(rp.SysTableTableSchema) == 0 {
		return defaultRoute()
	}

	env := evalengine.EnvWithBindVars(bindVars)
	var specifiedKS string
	for _, tableSchema := range rp.SysTableTableSchema {
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
	for tblBvName, sysTableName := range rp.SysTableTableName {
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
		rss, err := rp.routedTable(vcursor, bindVars, specifiedKS, tableNames)
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

func (rp *RoutingParameters) routedTable(vcursor VCursor, bindVars map[string]*querypb.BindVariable, tableSchema string, tableNames map[string]string) ([]*srvtopo.ResolvedShard, error) {
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

func (rp *RoutingParameters) anyShard(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	rss, _, err := vcursor.ResolveDestinations(rp.Keyspace.Name, nil, []key.Destination{key.DestinationAnyShard{}})
	if err != nil {
		return nil, nil, err
	}
	multiBindVars := make([]map[string]*querypb.BindVariable, len(rss))
	for i := range multiBindVars {
		multiBindVars[i] = bindVars
	}
	return rss, multiBindVars, nil
}

func (rp *RoutingParameters) unsharded(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	rss, _, err := vcursor.ResolveDestinations(rp.Keyspace.Name, nil, []key.Destination{key.DestinationAllShards{}})
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

func (rp *RoutingParameters) byDestination(vcursor VCursor, bindVars map[string]*querypb.BindVariable, destination key.Destination) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	rss, _, err := vcursor.ResolveDestinations(rp.Keyspace.Name, nil, []key.Destination{destination})
	if err != nil {
		return nil, nil, err
	}
	multiBindVars := make([]map[string]*querypb.BindVariable, len(rss))
	for i := range multiBindVars {
		multiBindVars[i] = bindVars
	}
	return rss, multiBindVars, err
}

func (rp *RoutingParameters) paramsSelectEqual(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	env := evalengine.EnvWithBindVars(bindVars)
	value, err := env.Evaluate(rp.Values[0])
	if err != nil {
		return nil, nil, err
	}
	rss, _, err := resolveShards(vcursor, rp.Vindex.(vindexes.SingleColumn), rp.Keyspace, []sqltypes.Value{value.Value()})
	if err != nil {
		return nil, nil, err
	}
	multiBindVars := make([]map[string]*querypb.BindVariable, len(rss))
	for i := range multiBindVars {
		multiBindVars[i] = bindVars
	}
	return rss, multiBindVars, nil
}

func (rp *RoutingParameters) paramsSelectEqualMultiCol(vcursor VCursor, bindVars map[string]*querypb.BindVariable) ([]*srvtopo.ResolvedShard, []map[string]*querypb.BindVariable, error) {
	env := evalengine.EnvWithBindVars(bindVars)
	var rowValue []sqltypes.Value
	for _, rvalue := range rp.Values {
		v, err := env.Evaluate(rvalue)
		if err != nil {
			return nil, nil, err
		}
		rowValue = append(rowValue, v.Value())
	}

	rss, _, err := resolveShardsMultiCol(vcursor, rp.Vindex.(vindexes.MultiColumn), rp.Keyspace, [][]sqltypes.Value{rowValue}, false /* shardIdsNeeded */)
	if err != nil {
		return nil, nil, err
	}
	multiBindVars := make([]map[string]*querypb.BindVariable, len(rss))
	for i := range multiBindVars {
		multiBindVars[i] = bindVars
	}
	return rss, multiBindVars, nil
}
