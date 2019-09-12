/*
Copyright 2018 The Vitess Authors.

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
	"time"

	"vitess.io/vitess/go/trace"

	"golang.org/x/net/context"

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

var _ Primitive = (*Delete)(nil)

// Delete represents the instructions to perform a delete.
type Delete struct {
	// Opcode is the execution opcode.
	Opcode DeleteOpcode

	// Keyspace specifies the keyspace to send the query to.
	Keyspace *vindexes.Keyspace

	// TargetDestination specifies the destination to send the query to.
	TargetDestination key.Destination

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

	// Option to override the standard behavior and allow a multi-shard delete
	// to use single round trip autocommit.
	MultiShardAutocommit bool

	// QueryTimeout contains the optional timeout (in milliseconds) to apply to this query
	QueryTimeout int
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
		Opcode               DeleteOpcode
		Keyspace             *vindexes.Keyspace   `json:",omitempty"`
		Query                string               `json:",omitempty"`
		Vindex               string               `json:",omitempty"`
		Values               []sqltypes.PlanValue `json:",omitempty"`
		Table                string               `json:",omitempty"`
		OwnedVindexQuery     string               `json:",omitempty"`
		MultiShardAutocommit bool                 `json:",omitempty"`
		QueryTimeout         int                  `json:",omitempty"`
	}{
		Opcode:               del.Opcode,
		Keyspace:             del.Keyspace,
		Query:                del.Query,
		Vindex:               vindexName,
		Values:               del.Values,
		Table:                tname,
		OwnedVindexQuery:     del.OwnedVindexQuery,
		MultiShardAutocommit: del.MultiShardAutocommit,
		QueryTimeout:         del.QueryTimeout,
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
	// DeleteScatter is for routing a scattered
	// delete statement.
	DeleteScatter
	// DeleteByDestination is to route explicitly to a given
	// target destination. Is used when the query explicitly sets a target destination:
	// in the from clause:
	// e.g: DELETE FROM `keyspace[-]`.x1 LIMIT 100
	DeleteByDestination
)

var delName = map[DeleteOpcode]string{
	DeleteUnsharded:     "DeleteUnsharded",
	DeleteEqual:         "DeleteEqual",
	DeleteScatter:       "DeleteScatter",
	DeleteByDestination: "DeleteByDestination",
}

// MarshalJSON serializes the DeleteOpcode as a JSON string.
// It's used for testing and diagnostics.
func (code DeleteOpcode) MarshalJSON() ([]byte, error) {
	return json.Marshal(delName[code])
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
func (del *Delete) Execute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	span, _ := trace.NewSpan(ctx, "Delete.Execute")
	defer span.Finish()
	if del.QueryTimeout != 0 {
		cancel := vcursor.SetContextTimeout(time.Duration(del.QueryTimeout) * time.Millisecond)
		defer cancel()
	}

	switch del.Opcode {
	case DeleteUnsharded:
		return del.execDeleteUnsharded(vcursor, bindVars)
	case DeleteEqual:
		return del.execDeleteEqual(vcursor, bindVars)
	case DeleteScatter:
		return del.execDeleteByDestination(vcursor, bindVars, key.DestinationAllShards{})
	case DeleteByDestination:
		return del.execDeleteByDestination(vcursor, bindVars, del.TargetDestination)
	default:
		// Unreachable.
		return nil, fmt.Errorf("unsupported opcode: %v", del)
	}
}

// StreamExecute performs a streaming exec.
func (del *Delete) StreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	return fmt.Errorf("query %q cannot be used for streaming", del.Query)
}

// GetFields fetches the field info.
func (del *Delete) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
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
		err = del.deleteVindexEntries(vcursor, bindVars, rs, ksid)
		if err != nil {
			return nil, vterrors.Wrap(err, "execDeleteEqual")
		}
	}
	rewritten := sqlannotation.AddKeyspaceIDs(del.Query, [][]byte{ksid}, "")
	return execShard(vcursor, rewritten, bindVars, rs, true /* isDML */, true /* canAutocommit */)
}

func (del *Delete) deleteVindexEntries(vcursor VCursor, bindVars map[string]*querypb.BindVariable, rs *srvtopo.ResolvedShard, ksid []byte) error {
	result, err := execShard(vcursor, del.OwnedVindexQuery, bindVars, rs, false /* isDML */, false /* canAutocommit */)
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

func (del *Delete) execDeleteByDestination(vcursor VCursor, bindVars map[string]*querypb.BindVariable, dest key.Destination) (*sqltypes.Result, error) {
	rss, _, err := vcursor.ResolveDestinations(del.Keyspace.Name, nil, []key.Destination{dest})
	if err != nil {
		return nil, vterrors.Wrap(err, "execDeleteScatter")
	}

	queries := make([]*querypb.BoundQuery, len(rss))
	sql := sqlannotation.AnnotateIfDML(del.Query, nil)
	for i := range rss {
		queries[i] = &querypb.BoundQuery{
			Sql:           sql,
			BindVariables: bindVars,
		}
	}
	autocommit := (len(rss) == 1 || del.MultiShardAutocommit) && vcursor.AutocommitApproval()
	res, errs := vcursor.ExecuteMultiShard(rss, queries, true /* isDML */, autocommit)
	return res, vterrors.Aggregate(errs)
}
