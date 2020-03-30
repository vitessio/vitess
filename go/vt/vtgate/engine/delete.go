/*
Copyright 2019 The Vitess Authors.

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
	"fmt"
	"time"

	"vitess.io/vitess/go/jsonutil"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var _ Primitive = (*Delete)(nil)

// Delete represents the instructions to perform a delete.
type Delete struct {
	DML

	// Delete does not take inputs
	noInputs
}

// MarshalJSON serializes the Delete into a JSON representation.
// It's used for testing and diagnostics.
func (del *Delete) MarshalJSON() ([]byte, error) {
	var tname, vindexName, ksidVindexName string
	if del.Table != nil {
		tname = del.Table.Name.String()
	}
	if del.Vindex != nil {
		vindexName = del.Vindex.String()
	}
	if del.KsidVindex != nil {
		ksidVindexName = del.KsidVindex.String()
	}
	marshalDelete := struct {
		Opcode               string
		Keyspace             *vindexes.Keyspace   `json:",omitempty"`
		Query                string               `json:",omitempty"`
		Vindex               string               `json:",omitempty"`
		Values               []sqltypes.PlanValue `json:",omitempty"`
		Table                string               `json:",omitempty"`
		OwnedVindexQuery     string               `json:",omitempty"`
		KsidVindex           string               `json:",omitempty"`
		MultiShardAutocommit bool                 `json:",omitempty"`
		QueryTimeout         int                  `json:",omitempty"`
	}{
		Opcode:               del.RouteType(),
		Keyspace:             del.Keyspace,
		Query:                del.Query,
		Vindex:               vindexName,
		Values:               del.Values,
		Table:                tname,
		OwnedVindexQuery:     del.OwnedVindexQuery,
		KsidVindex:           ksidVindexName,
		MultiShardAutocommit: del.MultiShardAutocommit,
		QueryTimeout:         del.QueryTimeout,
	}
	return jsonutil.MarshalNoEscape(marshalDelete)
}

var delName = map[DMLOpcode]string{
	Unsharded:     "DeleteUnsharded",
	Equal:         "DeleteEqual",
	Scatter:       "DeleteScatter",
	ByDestination: "DeleteByDestination",
}

// RouteType returns a description of the query routing type used by the primitive
func (del *Delete) RouteType() string {
	return delName[del.Opcode]
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to.
func (del *Delete) GetKeyspaceName() string {
	return del.Keyspace.Name
}

// GetTableName specifies the table that this primitive routes to.
func (del *Delete) GetTableName() string {
	if del.Table != nil {
		return del.Table.Name.String()
	}
	return ""
}

// Execute performs a non-streaming exec.
func (del *Delete) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	if del.QueryTimeout != 0 {
		cancel := vcursor.SetContextTimeout(time.Duration(del.QueryTimeout) * time.Millisecond)
		defer cancel()
	}

	switch del.Opcode {
	case Unsharded:
		return del.execDeleteUnsharded(vcursor, bindVars)
	case Equal:
		return del.execDeleteEqual(vcursor, bindVars)
	case Scatter:
		return del.execDeleteByDestination(vcursor, bindVars, key.DestinationAllShards{})
	case ByDestination:
		return del.execDeleteByDestination(vcursor, bindVars, del.TargetDestination)
	default:
		// Unreachable.
		return nil, fmt.Errorf("unsupported opcode: %v", del)
	}
}

// StreamExecute performs a streaming exec.
func (del *Delete) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	return fmt.Errorf("query %q cannot be used for streaming", del.Query)
}

// GetFields fetches the field info.
func (del *Delete) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, fmt.Errorf("BUG: unreachable code for %q", del.Query)
}

func (del *Delete) execDeleteUnsharded(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	rss, _, err := vcursor.ResolveDestinations(del.Keyspace.Name, nil, []key.Destination{key.DestinationAllShards{}})
	if err != nil {
		return nil, vterrors.Wrap(err, "execDeleteUnsharded")
	}
	if len(rss) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "Keyspace does not have exactly one shard: %v", rss)
	}
	return execShard(vcursor, del.Query, bindVars, rss[0], true, true /* canAutocommit */)
}

func (del *Delete) execDeleteEqual(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	key, err := del.Values[0].ResolveValue(bindVars)
	if err != nil {
		return nil, vterrors.Wrap(err, "execDeleteEqual")
	}
	rs, ksid, err := resolveSingleShard(vcursor, del.Vindex, del.Keyspace, key)
	if err != nil {
		return nil, vterrors.Wrap(err, "execDeleteEqual")
	}
	if len(ksid) == 0 {
		return &sqltypes.Result{}, nil
	}
	if del.OwnedVindexQuery != "" {
		err = del.deleteVindexEntries(vcursor, bindVars, []*srvtopo.ResolvedShard{rs})
		if err != nil {
			return nil, vterrors.Wrap(err, "execDeleteEqual")
		}
	}
	return execShard(vcursor, del.Query, bindVars, rs, true /* rollbackOnError */, true /* canAutocommit */)
}

// deleteVindexEntries performs an delete if table owns vindex.
// Note: the commit order may be different from the DML order because it's possible
// for DMLs to reuse existing transactions.
func (del *Delete) deleteVindexEntries(vcursor VCursor, bindVars map[string]*querypb.BindVariable, rss []*srvtopo.ResolvedShard) error {
	queries := make([]*querypb.BoundQuery, len(rss))
	for i := range rss {
		queries[i] = &querypb.BoundQuery{Sql: del.OwnedVindexQuery, BindVariables: bindVars}
	}
	subQueryResults, errors := vcursor.ExecuteMultiShard(rss, queries, false, false)
	for _, err := range errors {
		if err != nil {
			return vterrors.Wrap(err, "deleteVindexEntries")
		}
	}

	if len(subQueryResults.Rows) == 0 {
		return nil
	}

	for _, row := range subQueryResults.Rows {
		colnum := 1
		ksid, err := resolveKeyspaceID(vcursor, del.KsidVindex, row[0])
		if err != nil {
			return err
		}
		for _, colVindex := range del.Table.Owned {
			// Fetch the column values. colnum must keep incrementing.
			fromIds := make([]sqltypes.Value, 0, len(colVindex.Columns))
			for range colVindex.Columns {
				fromIds = append(fromIds, row[colnum])
				colnum++
			}
			if err := colVindex.Vindex.(vindexes.Lookup).Delete(vcursor, [][]sqltypes.Value{fromIds}, ksid); err != nil {
				return err
			}
		}

	}

	return nil
}

func (del *Delete) execDeleteByDestination(vcursor VCursor, bindVars map[string]*querypb.BindVariable, dest key.Destination) (*sqltypes.Result, error) {
	rss, _, err := vcursor.ResolveDestinations(del.Keyspace.Name, nil, []key.Destination{dest})
	if err != nil {
		return nil, vterrors.Wrap(err, "execDeleteScatter")
	}

	queries := make([]*querypb.BoundQuery, len(rss))
	for i := range rss {
		queries[i] = &querypb.BoundQuery{
			Sql:           del.Query,
			BindVariables: bindVars,
		}
	}
	if len(del.Table.Owned) > 0 {
		err = del.deleteVindexEntries(vcursor, bindVars, rss)
		if err != nil {
			return nil, err
		}
	}
	autocommit := (len(rss) == 1 || del.MultiShardAutocommit) && vcursor.AutocommitApproval()
	res, errs := vcursor.ExecuteMultiShard(rss, queries, true /* rollbackOnError */, autocommit)
	return res, vterrors.Aggregate(errs)
}
