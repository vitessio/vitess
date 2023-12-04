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
	"context"
	"fmt"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*Delete)(nil)

// Delete represents the instructions to perform a delete.
type Delete struct {
	*DML

	// Delete does not take inputs
	noInputs
}

// TryExecute performs a non-streaming exec.
func (del *Delete) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, _ bool) (*sqltypes.Result, error) {
	ctx, cancelFunc := addQueryTimeout(ctx, vcursor, del.QueryTimeout)
	defer cancelFunc()

	rss, _, err := del.findRoute(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	err = allowOnlyPrimary(rss...)
	if err != nil {
		return nil, err
	}

	switch del.Opcode {
	case Unsharded:
		return del.execUnsharded(ctx, del, vcursor, bindVars, rss)
	case Equal, IN, Scatter, ByDestination, SubShard, EqualUnique, MultiEqual:
		return del.execMultiDestination(ctx, del, vcursor, bindVars, rss, del.deleteVindexEntries)
	default:
		// Unreachable.
		return nil, fmt.Errorf("unsupported opcode: %v", del.Opcode)
	}
}

// TryStreamExecute performs a streaming exec.
func (del *Delete) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	res, err := del.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(res)
}

// GetFields fetches the field info.
func (del *Delete) GetFields(context.Context, VCursor, map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, fmt.Errorf("BUG: unreachable code for %q", del.Query)
}

// deleteVindexEntries performs an delete if table owns vindex.
// Note: the commit order may be different from the DML order because it's possible
// for DMLs to reuse existing transactions.
func (del *Delete) deleteVindexEntries(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, rss []*srvtopo.ResolvedShard) error {
	if del.OwnedVindexQuery == "" {
		return nil
	}
	queries := make([]*querypb.BoundQuery, len(rss))
	for i := range rss {
		queries[i] = &querypb.BoundQuery{Sql: del.OwnedVindexQuery, BindVariables: bindVars}
	}
	subQueryResults, errors := vcursor.ExecuteMultiShard(ctx, del, rss, queries, false /* rollbackOnError */, false /* canAutocommit */)
	for _, err := range errors {
		if err != nil {
			return err
		}
	}

	if len(subQueryResults.Rows) == 0 {
		return nil
	}

	for _, row := range subQueryResults.Rows {
		ksid, err := resolveKeyspaceID(ctx, vcursor, del.KsidVindex, row[0:del.KsidLength])
		if err != nil {
			return err
		}
		colnum := del.KsidLength
		for _, colVindex := range del.Vindexes {
			// Fetch the column values. colnum must keep incrementing.
			fromIds := make([]sqltypes.Value, 0, len(colVindex.Columns))
			for range colVindex.Columns {
				fromIds = append(fromIds, row[colnum])
				colnum++
			}
			if err := colVindex.Vindex.(vindexes.Lookup).Delete(ctx, vcursor, [][]sqltypes.Value{fromIds}, ksid); err != nil {
				return err
			}
		}
	}

	return nil
}

func (del *Delete) description() PrimitiveDescription {
	other := map[string]any{
		"Query":                del.Query,
		"Table":                del.GetTableName(),
		"OwnedVindexQuery":     del.OwnedVindexQuery,
		"MultiShardAutocommit": del.MultiShardAutocommit,
		"QueryTimeout":         del.QueryTimeout,
		"NoAutoCommit":         del.PreventAutoCommit,
	}

	addFieldsIfNotEmpty(del.DML, other)

	return PrimitiveDescription{
		OperatorType:     "Delete",
		Keyspace:         del.Keyspace,
		Variant:          del.Opcode.String(),
		TargetTabletType: topodatapb.TabletType_PRIMARY,
		Other:            other,
	}
}

func addFieldsIfNotEmpty(dml *DML, other map[string]any) {
	if dml.Vindex != nil {
		other["Vindex"] = dml.Vindex.String()
	}
	if dml.KsidVindex != nil {
		other["KsidVindex"] = dml.KsidVindex.String()
		other["KsidLength"] = dml.KsidLength
	}
	if len(dml.Values) > 0 {
		s := []string{}
		for _, value := range dml.Values {
			s = append(s, sqlparser.String(value))
		}
		other["Values"] = s
	}
}
