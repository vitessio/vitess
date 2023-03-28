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
	"sort"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*Update)(nil)

// VindexValues contains changed values for a vindex.
type VindexValues struct {
	PvMap  map[string]evalengine.Expr
	Offset int // Offset from ownedVindexQuery to provide input decision for vindex update.
}

// Update represents the instructions to perform an update.
type Update struct {
	*DML

	// ChangedVindexValues contains values for updated Vindexes during an update statement.
	ChangedVindexValues map[string]*VindexValues

	// Update does not take inputs
	noInputs
}

// TryExecute performs a non-streaming exec.
func (upd *Update) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	ctx, cancelFunc := addQueryTimeout(ctx, vcursor, upd.QueryTimeout)
	defer cancelFunc()

	rss, _, err := upd.findRoute(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	err = allowOnlyPrimary(rss...)
	if err != nil {
		return nil, err
	}

	switch upd.Opcode {
	case Unsharded:
		return upd.execUnsharded(ctx, upd, vcursor, bindVars, rss)
	case Equal, EqualUnique, IN, Scatter, ByDestination, SubShard, MultiEqual:
		return upd.execMultiDestination(ctx, upd, vcursor, bindVars, rss, upd.updateVindexEntries)
	default:
		// Unreachable.
		return nil, fmt.Errorf("unsupported opcode: %v", upd.Opcode)
	}
}

// TryStreamExecute performs a streaming exec.
func (upd *Update) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	res, err := upd.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(res)

}

// GetFields fetches the field info.
func (upd *Update) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, fmt.Errorf("BUG: unreachable code for %q", upd.Query)
}

// updateVindexEntries performs an update when a vindex is being modified
// by the statement.
// Note: the commit order may be different from the DML order because it's possible
// for DMLs to reuse existing transactions.
// Note 2: While changes are being committed, the changing row could be
// unreachable by either the new or old column values.
func (upd *Update) updateVindexEntries(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, rss []*srvtopo.ResolvedShard) error {
	if len(upd.ChangedVindexValues) == 0 {
		return nil
	}
	queries := make([]*querypb.BoundQuery, len(rss))
	for i := range rss {
		queries[i] = &querypb.BoundQuery{Sql: upd.OwnedVindexQuery, BindVariables: bindVars}
	}
	subQueryResult, errors := vcursor.ExecuteMultiShard(ctx, upd, rss, queries, false /* rollbackOnError */, false /* canAutocommit */)
	for _, err := range errors {
		if err != nil {
			return err
		}
	}

	if len(subQueryResult.Rows) == 0 {
		return nil
	}

	fieldColNumMap := make(map[string]int)
	for colNum, field := range subQueryResult.Fields {
		fieldColNumMap[field.Name] = colNum
	}
	env := evalengine.NewExpressionEnv(bindVars, vcursor.TimeZone())

	for _, row := range subQueryResult.Rows {
		ksid, err := resolveKeyspaceID(ctx, vcursor, upd.KsidVindex, row[0:upd.KsidLength])
		if err != nil {
			return err
		}

		vindexTable, err := upd.GetSingleTable()
		if err != nil {
			return err
		}
		for _, colVindex := range vindexTable.ColumnVindexes {
			// Skip this vindex if no rows are being changed
			updColValues, ok := upd.ChangedVindexValues[colVindex.Name]
			if !ok {
				continue
			}

			offset := updColValues.Offset
			if !row[offset].IsNull() {
				val, err := evalengine.ToInt64(row[offset])
				if err != nil {
					return err
				}
				if val == int64(1) { // 1 means that the old and new value are same and vindex update is not required.
					continue
				}
			}

			fromIds := make([]sqltypes.Value, 0, len(colVindex.Columns))
			var vindexColumnKeys []sqltypes.Value
			for _, vCol := range colVindex.Columns {
				// Fetch the column values.
				origColValue := row[fieldColNumMap[vCol.String()]]
				fromIds = append(fromIds, origColValue)
				if colValue, exists := updColValues.PvMap[vCol.String()]; exists {
					resolvedVal, err := env.Evaluate(colValue)
					if err != nil {
						return err
					}
					vindexColumnKeys = append(vindexColumnKeys, resolvedVal.Value())
				} else {
					// Set the column value to original as this column in vindex is not updated.
					vindexColumnKeys = append(vindexColumnKeys, origColValue)
				}
			}

			if colVindex.Owned {
				if err := colVindex.Vindex.(vindexes.Lookup).Update(ctx, vcursor, fromIds, ksid, vindexColumnKeys); err != nil {
					return err
				}
			} else {
				allNulls := true
				for _, key := range vindexColumnKeys {
					allNulls = key.IsNull()
					if !allNulls {
						break
					}
				}

				// All columns for this Vindex are set to null, so we can skip verification
				if allNulls {
					continue
				}

				// If values were supplied, we validate against keyspace id.
				verified, err := vindexes.Verify(ctx, colVindex.Vindex, vcursor, [][]sqltypes.Value{vindexColumnKeys}, [][]byte{ksid})
				if err != nil {
					return err
				}

				if !verified[0] {
					return fmt.Errorf("values %v for column %v does not map to keyspace ids", vindexColumnKeys, colVindex.Columns)
				}
			}
		}
	}
	return nil
}

func (upd *Update) description() PrimitiveDescription {
	other := map[string]any{
		"Query":                upd.Query,
		"Table":                upd.GetTableName(),
		"OwnedVindexQuery":     upd.OwnedVindexQuery,
		"MultiShardAutocommit": upd.MultiShardAutocommit,
		"QueryTimeout":         upd.QueryTimeout,
	}

	addFieldsIfNotEmpty(upd.DML, other)

	var changedVindexes []string
	for k, v := range upd.ChangedVindexValues {
		changedVindexes = append(changedVindexes, fmt.Sprintf("%s:%d", k, v.Offset))
	}
	sort.Strings(changedVindexes) // We sort these so random changes in the map order does not affect output
	if len(changedVindexes) > 0 {
		other["ChangedVindexValues"] = changedVindexes
	}

	return PrimitiveDescription{
		OperatorType:     "Update",
		Keyspace:         upd.Keyspace,
		Variant:          upd.Opcode.String(),
		TargetTabletType: topodatapb.TabletType_PRIMARY,
		Other:            other,
	}
}
