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

var _ Primitive = (*Update)(nil)

// Update represents the instructions to perform an update.
type Update struct {
	// Opcode is the execution opcode.
	Opcode UpdateOpcode

	// Keyspace specifies the keyspace to send the query to.
	Keyspace *vindexes.Keyspace

	// Query specifies the query to be executed.
	Query string

	// Vindex specifies the vindex to be used.
	Vindex vindexes.Vindex
	// Values specifies the vindex values to use for routing.
	// For now, only one value is specified.
	Values []sqltypes.PlanValue

	// ChangedVindexValues contains values for updated Vindexes during an update statement.
	ChangedVindexValues map[string][]sqltypes.PlanValue

	// Table sepcifies the table for the update.
	Table *vindexes.Table

	// OwnedVindexQuery is used for updating changes in lookup vindexes.
	OwnedVindexQuery string
}

// MarshalJSON serializes the Update into a JSON representation.
// It's used for testing and diagnostics.
func (upd *Update) MarshalJSON() ([]byte, error) {
	var tname, vindexName string
	if upd.Table != nil {
		tname = upd.Table.Name.String()
	}
	if upd.Vindex != nil {
		vindexName = upd.Vindex.String()
	}
	marshalUpdate := struct {
		Opcode              UpdateOpcode
		Keyspace            *vindexes.Keyspace              `json:",omitempty"`
		Query               string                          `json:",omitempty"`
		Vindex              string                          `json:",omitempty"`
		Values              []sqltypes.PlanValue            `json:",omitempty"`
		ChangedVindexValues map[string][]sqltypes.PlanValue `json:",omitempty"`
		Table               string                          `json:",omitempty"`
		OwnedVindexQuery    string                          `json:",omitempty"`
	}{
		Opcode:              upd.Opcode,
		Keyspace:            upd.Keyspace,
		Query:               upd.Query,
		Vindex:              vindexName,
		Values:              upd.Values,
		ChangedVindexValues: upd.ChangedVindexValues,
		Table:               tname,
		OwnedVindexQuery:    upd.OwnedVindexQuery,
	}
	return jsonutil.MarshalNoEscape(marshalUpdate)
}

// UpdateOpcode is a number representing the opcode
// for the Update primitve.
type UpdateOpcode int

// This is the list of UpdateOpcode values.
const (
	// UpdateUnsharded is for routing an update statement
	// to an unsharded keyspace.
	UpdateUnsharded = UpdateOpcode(iota)
	// UpdateEqual is for routing an update statement
	// to a single shard: Requires: A Vindex, and
	// a single Value.
	UpdateEqual
)

var updName = map[UpdateOpcode]string{
	UpdateUnsharded: "UpdateUnsharded",
	UpdateEqual:     "UpdateEqual",
}

// MarshalJSON serializes the UpdateOpcode as a JSON string.
// It's used for testing and diagnostics.
func (code UpdateOpcode) MarshalJSON() ([]byte, error) {
	return json.Marshal(updName[code])
}

// Execute performs a non-streaming exec.
func (upd *Update) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	switch upd.Opcode {
	case UpdateUnsharded:
		return upd.execUpdateUnsharded(vcursor, bindVars)
	case UpdateEqual:
		return upd.execUpdateEqual(vcursor, bindVars)
	default:
		// Unreachable.
		return nil, fmt.Errorf("unsupported opcode: %v", upd)
	}
}

// StreamExecute performs a streaming exec.
func (upd *Update) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	return fmt.Errorf("query %q cannot be used for streaming", upd.Query)
}

// GetFields fetches the field info.
func (upd *Update) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, fmt.Errorf("BUG: unreachable code for %q", upd.Query)
}

func (upd *Update) execUpdateUnsharded(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	ks, allShards, err := vcursor.GetKeyspaceShards(upd.Keyspace)
	if err != nil {
		return nil, vterrors.Wrap(err, "execUpdateUnsharded")
	}
	if len(allShards) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "Keyspace does not have exactly one shard: %v", allShards)
	}
	return execShard(vcursor, upd.Query, bindVars, ks, allShards[0].Name, true, true /* canAutocommit */)
}

func (upd *Update) execUpdateEqual(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	key, err := upd.Values[0].ResolveValue(bindVars)
	if err != nil {
		return nil, vterrors.Wrap(err, "execUpdateEqual")
	}
	ks, shard, ksid, err := resolveSingleShard(vcursor, upd.Vindex, upd.Keyspace, bindVars, key)
	if err != nil {
		return nil, vterrors.Wrap(err, "execUpdateEqual")
	}
	if len(ksid) == 0 {
		return &sqltypes.Result{}, nil
	}
	if len(upd.ChangedVindexValues) != 0 {
		if err := upd.updateVindexEntries(vcursor, upd.OwnedVindexQuery, bindVars, ks, shard, ksid); err != nil {
			return nil, vterrors.Wrap(err, "execUpdateEqual")
		}
	}
	rewritten := sqlannotation.AddKeyspaceIDs(upd.Query, [][]byte{ksid}, "")
	return execShard(vcursor, rewritten, bindVars, ks, shard, true /* isDML */, true /* canAutocommit */)
}

// updateVindexEntries performs an update when a vindex is being modified
// by the statement.
// Note: the commit order may be different from the DML order because it's possible
// for DMLs to reuse existing transactions.
// Note 2: While changes are being committed, the changing row could be
// unreachable by either the new or old column values.
func (upd *Update) updateVindexEntries(vcursor VCursor, query string, bindVars map[string]*querypb.BindVariable, keyspace, shard string, ksid []byte) error {
	subQueryResult, err := execShard(vcursor, upd.OwnedVindexQuery, bindVars, keyspace, shard, false /* isDML */, false /* canAutocommit */)
	if err != nil {
		return err
	}
	if len(subQueryResult.Rows) == 0 {
		return nil
	}
	if len(subQueryResult.Rows) > 1 {
		return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: update changes multiple rows in the vindex")
	}
	colnum := 0
	for _, colVindex := range upd.Table.Owned {
		// Fetch the column values. colnum must keep incrementing.
		fromIds := make([]sqltypes.Value, 0, len(colVindex.Columns))
		for range colVindex.Columns {
			fromIds = append(fromIds, subQueryResult.Rows[0][colnum])
			colnum++
		}

		// Update columns only if they're being changed.
		if colValues, ok := upd.ChangedVindexValues[colVindex.Name]; ok {
			vindexColumnKeys := make([]sqltypes.Value, 0, len(colValues))
			for _, colValue := range colValues {
				resolvedVal, err := colValue.ResolveValue(bindVars)
				if err != nil {
					return err
				}
				vindexColumnKeys = append(vindexColumnKeys, resolvedVal)
			}

			if err := colVindex.Vindex.(vindexes.Lookup).Update(vcursor, fromIds, ksid, vindexColumnKeys); err != nil {
				return err
			}
		}
	}
	return nil
}
