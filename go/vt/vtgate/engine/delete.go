/*
Copyright 2018 Google Inc.

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
	"fmt"

	"vitess.io/vitess/go/jsonutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlannotation"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var _ Primitive = (*Delete)(nil)

// Delete represents the instructions to perform a delete.
type Delete struct {
	// Opcode is the execution opcode.
	Opcode DeleteOpcode

	// Keyspace specifies the keyspace to send the query to.
	Keyspace *vindexes.Keyspace

	// Query specifies the query to be executed.
	Query string

	// Vindex specifies the vindex to be used.
	Vindex vindexes.Vindex
	// Values specifies the vindex values to use for routing.
	// For now, only one value is specified.
	Values []sqltypes.PlanValue

	// Table specifies the table for the delete.
	Table *vindexes.Table

	// OwnedVindexQuery is used for deleting lookup vindex entries.
	OwnedVindexQuery string
}

// MarshalJSON serializes the Delete into a JSON representation.
// It's used for testing and diagnostics.
func (del *Delete) MarshalJSON() ([]byte, error) {
	var tname, vindexName string
	if del.Table != nil {
		tname = del.Table.Name.String()
	}
	if del.Vindex != nil {
		vindexName = del.Vindex.String()
	}
	marshalDelete := struct {
		Opcode           DeleteOpcode
		Keyspace         *vindexes.Keyspace   `json:",omitempty"`
		Query            string               `json:",omitempty"`
		Vindex           string               `json:",omitempty"`
		Values           []sqltypes.PlanValue `json:",omitempty"`
		Table            string               `json:",omitempty"`
		OwnedVindexQuery string               `json:",omitempty"`
	}{
		Opcode:           del.Opcode,
		Keyspace:         del.Keyspace,
		Query:            del.Query,
		Vindex:           vindexName,
		Values:           del.Values,
		Table:            tname,
		OwnedVindexQuery: del.OwnedVindexQuery,
	}
	return jsonutil.MarshalNoEscape(marshalDelete)
}

// DeleteOpcode is a number representing the opcode
// for the Delete primitve.
type DeleteOpcode int

// This is the list of DeleteOpcode values.
const (
	// DeleteUnsharded is for routing a delete statement
	// to an unsharded keyspace.
	DeleteUnsharded = DeleteOpcode(iota)
	// DeleteEqual is for routing a delete statement
	// to a single shard. Requires: A Vindex, a single
	// Value, and an OwnedVindexQuery, which will be used to
	// determine if lookup rows need to be deleted.
	DeleteEqual
	// DeleteSharded is for routing a scattered
	// delete statement.
	DeleteSharded
)

var delName = map[DeleteOpcode]string{
	DeleteUnsharded: "DeleteUnsharded",
	DeleteEqual:     "DeleteEqual",
	DeleteSharded:   "DeleteSharded",
}

// MarshalJSON serializes the DeleteOpcode as a JSON string.
// It's used for testing and diagnostics.
func (code DeleteOpcode) MarshalJSON() ([]byte, error) {
	return json.Marshal(delName[code])
}

// Execute performs a non-streaming exec.
func (del *Delete) Execute(vcursor VCursor, bindVars, joinVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	switch del.Opcode {
	case DeleteUnsharded:
		return del.execDeleteUnsharded(vcursor, bindVars)
	case DeleteEqual:
		return del.execDeleteEqual(vcursor, bindVars)
	case DeleteSharded:
		return del.execDeleteSharded(vcursor, bindVars)
	default:
		// Unreachable.
		return nil, fmt.Errorf("unsupported opcode: %v", del)
	}
}

// StreamExecute performs a streaming exec.
func (del *Delete) StreamExecute(vcursor VCursor, bindVars, joinVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	return fmt.Errorf("query %q cannot be used for streaming", del.Query)
}

// GetFields fetches the field info.
func (del *Delete) GetFields(vcursor VCursor, bindVars, joinVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, fmt.Errorf("BUG: unreachable code for %q", del.Query)
}

func (del *Delete) execDeleteUnsharded(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	ks, allShards, err := vcursor.GetKeyspaceShards(del.Keyspace)
	if err != nil {
		return nil, vterrors.Wrap(err, "execDeleteUnsharded")
	}
	if len(allShards) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "Keyspace does not have exactly one shard: %v", allShards)
	}
	return execShard(vcursor, del.Query, bindVars, ks, allShards[0].Name, true, true /* canAutocommit */)
}

func (del *Delete) execDeleteEqual(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	key, err := del.Values[0].ResolveValue(bindVars)
	if err != nil {
		return nil, vterrors.Wrap(err, "execDeleteEqual")
	}
	ks, shard, ksid, err := resolveSingleShard(vcursor, del.Vindex, del.Keyspace, bindVars, key)
	if err != nil {
		return nil, vterrors.Wrap(err, "execDeleteEqual")
	}
	if len(ksid) == 0 {
		return &sqltypes.Result{}, nil
	}
	if del.OwnedVindexQuery != "" {
		err = del.deleteVindexEntries(vcursor, bindVars, ks, shard, ksid)
		if err != nil {
			return nil, vterrors.Wrap(err, "execDeleteEqual")
		}
	}
	rewritten := sqlannotation.AddKeyspaceIDs(del.Query, [][]byte{ksid}, "")
	return execShard(vcursor, rewritten, bindVars, ks, shard, true /* isDML */, true /* canAutocommit */)
}

func (del *Delete) deleteVindexEntries(vcursor VCursor, bindVars map[string]*querypb.BindVariable, ks, shard string, ksid []byte) error {
	result, err := execShard(vcursor, del.OwnedVindexQuery, bindVars, ks, shard, false /* isDML */, false /* canAutocommit */)
	if err != nil {
		return err
	}
	if len(result.Rows) == 0 {
		return nil
	}
	colnum := 0
	for _, colVindex := range del.Table.Owned {
		ids := make([][]sqltypes.Value, len(result.Rows))
		for range colVindex.Columns {
			for rowIdx, row := range result.Rows {
				ids[rowIdx] = append(ids[rowIdx], row[colnum])
			}
			colnum++
		}
		if err = colVindex.Vindex.(vindexes.Lookup).Delete(vcursor, ids, ksid); err != nil {
			return err
		}
	}
	return nil
}

func (del *Delete) execDeleteSharded(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	ks, allShards, err := vcursor.GetKeyspaceShards(del.Keyspace)
	if err != nil {
		return nil, vterrors.Wrap(err, "execDeleteSharded")
	}

	shardQueries := make(map[string]*querypb.BoundQuery, len(allShards))
	sql := sqlannotation.AnnotateIfDML(del.Query, nil)
	for _, shard := range allShards {
		shardQueries[shard.Name] = &querypb.BoundQuery{
			Sql:           sql,
			BindVariables: bindVars,
		}
	}
	return vcursor.ExecuteMultiShard(ks, shardQueries, true /* isDML */, true /* canAutocommit */)
}
