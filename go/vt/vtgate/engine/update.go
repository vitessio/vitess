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
	"vitess.io/vitess/go/vt/sqlannotation"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var _ Primitive = (*Update)(nil)

// Update represents the instructions to perform an update.
type Update struct {
	DML

	// ChangedVindexValues contains values for updated Vindexes during an update statement.
	ChangedVindexValues map[string][]sqltypes.PlanValue

	// Update does not take inputs
	noInputs
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
		Opcode               string
		Keyspace             *vindexes.Keyspace              `json:",omitempty"`
		Query                string                          `json:",omitempty"`
		Vindex               string                          `json:",omitempty"`
		Values               []sqltypes.PlanValue            `json:",omitempty"`
		ChangedVindexValues  map[string][]sqltypes.PlanValue `json:",omitempty"`
		Table                string                          `json:",omitempty"`
		OwnedVindexQuery     string                          `json:",omitempty"`
		MultiShardAutocommit bool                            `json:",omitempty"`
		QueryTimeout         int                             `json:",omitempty"`
	}{
		Opcode:               upd.RouteType(),
		Keyspace:             upd.Keyspace,
		Query:                upd.Query,
		Vindex:               vindexName,
		Values:               upd.Values,
		ChangedVindexValues:  upd.ChangedVindexValues,
		Table:                tname,
		OwnedVindexQuery:     upd.OwnedVindexQuery,
		MultiShardAutocommit: upd.MultiShardAutocommit,
		QueryTimeout:         upd.QueryTimeout,
	}
	return jsonutil.MarshalNoEscape(marshalUpdate)
}

var updName = map[DMLOpcode]string{
	Unsharded:     "UpdateUnsharded",
	Equal:         "UpdateEqual",
	Scatter:       "UpdateScatter",
	ByDestination: "UpdateByDestination",
}

// RouteType returns a description of the query routing type used by the primitive
func (upd *Update) RouteType() string {
	return updName[upd.Opcode]
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to.
func (upd *Update) GetKeyspaceName() string {
	return upd.Keyspace.Name
}

// GetTableName specifies the table that this primitive routes to.
func (upd *Update) GetTableName() string {
	if upd.Table != nil {
		return upd.Table.Name.String()
	}
	return ""
}

// Execute performs a non-streaming exec.
func (upd *Update) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	if upd.QueryTimeout != 0 {
		cancel := vcursor.SetContextTimeout(time.Duration(upd.QueryTimeout) * time.Millisecond)
		defer cancel()
	}

	switch upd.Opcode {
	case Unsharded:
		return upd.execUpdateUnsharded(vcursor, bindVars)
	case Equal:
		return upd.execUpdateEqual(vcursor, bindVars)
	case Scatter:
		return upd.execUpdateByDestination(vcursor, bindVars, key.DestinationAllShards{})
	case ByDestination:
		return upd.execUpdateByDestination(vcursor, bindVars, upd.TargetDestination)
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
	rss, _, err := vcursor.ResolveDestinations(upd.Keyspace.Name, nil, []key.Destination{key.DestinationAllShards{}})
	if err != nil {
		return nil, vterrors.Wrap(err, "execUpdateUnsharded")
	}
	if len(rss) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "Keyspace does not have exactly one shard: %v", rss)
	}
	return execShard(vcursor, upd.Query, bindVars, rss[0], true, true /* canAutocommit */)
}

func (upd *Update) execUpdateEqual(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	key, err := upd.Values[0].ResolveValue(bindVars)
	if err != nil {
		return nil, vterrors.Wrap(err, "execUpdateEqual")
	}
	rs, ksid, err := resolveSingleShard(vcursor, upd.Vindex, upd.Keyspace, key)
	if err != nil {
		return nil, vterrors.Wrap(err, "execUpdateEqual")
	}
	if len(ksid) == 0 {
		return &sqltypes.Result{}, nil
	}
	if len(upd.ChangedVindexValues) != 0 {
		if err := upd.updateVindexEntries(vcursor, bindVars, []*srvtopo.ResolvedShard{rs}); err != nil {
			return nil, vterrors.Wrap(err, "execUpdateEqual")
		}
	}
	rewritten := sqlannotation.AddKeyspaceIDs(upd.Query, [][]byte{ksid}, "")
	return execShard(vcursor, rewritten, bindVars, rs, true /* isDML */, true /* canAutocommit */)
}

// updateVindexEntries performs an update when a vindex is being modified
// by the statement.
// Note: the commit order may be different from the DML order because it's possible
// for DMLs to reuse existing transactions.
// Note 2: While changes are being committed, the changing row could be
// unreachable by either the new or old column values.
func (upd *Update) updateVindexEntries(vcursor VCursor, bindVars map[string]*querypb.BindVariable, rss []*srvtopo.ResolvedShard) error {
	queries := make([]*querypb.BoundQuery, len(rss))
	for i := range rss {
		queries[i] = &querypb.BoundQuery{Sql: upd.OwnedVindexQuery, BindVariables: bindVars}
	}
	subQueryResult, errors := vcursor.ExecuteMultiShard(rss, queries, false, false)
	for _, err := range errors {
		if err != nil {
			return vterrors.Wrap(err, "updateVindexEntries")
		}
	}

	if len(subQueryResult.Rows) == 0 {
		return nil
	}

	for _, row := range subQueryResult.Rows {
		colnum := 1 // we start from the first non-vindex col
		ksid := row[0].ToBytes()
		for _, colVindex := range upd.Table.Owned {
			// Fetch the column values. colnum must keep incrementing.
			fromIds := make([]sqltypes.Value, 0, len(colVindex.Columns))
			for range colVindex.Columns {
				fromIds = append(fromIds, row[colnum])
				colnum++
			}

			// Update columns only if they're being changed.
			if colValues, ok := upd.ChangedVindexValues[colVindex.Name]; ok {
				var vindexColumnKeys []sqltypes.Value
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
	}
	return nil
}

func (upd *Update) execUpdateByDestination(vcursor VCursor, bindVars map[string]*querypb.BindVariable, dest key.Destination) (*sqltypes.Result, error) {
	rss, _, err := vcursor.ResolveDestinations(upd.Keyspace.Name, nil, []key.Destination{dest})
	if err != nil {
		return nil, vterrors.Wrap(err, "execUpdateByDestination")
	}

	queries := make([]*querypb.BoundQuery, len(rss))
	sql := sqlannotation.AnnotateIfDML(upd.Query, nil)
	for i := range rss {
		queries[i] = &querypb.BoundQuery{
			Sql:           sql,
			BindVariables: bindVars,
		}
	}

	// update any owned vindexes
	if len(upd.ChangedVindexValues) != 0 {
		if err := upd.updateVindexEntries(vcursor, bindVars, rss); err != nil {
			return nil, vterrors.Wrap(err, "execUpdateByDestination")
		}
	}

	autocommit := (len(rss) == 1 || upd.MultiShardAutocommit) && vcursor.AutocommitApproval()
	result, errs := vcursor.ExecuteMultiShard(rss, queries, true /* isDML */, autocommit)
	return result, vterrors.Aggregate(errs)
}
