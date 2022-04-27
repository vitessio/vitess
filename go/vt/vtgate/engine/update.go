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
	"sort"
	"time"

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

// RouteType returns a description of the query routing type used by the primitive
func (upd *Update) RouteType() string {
	return upd.Opcode.String()
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

// TryExecute performs a non-streaming exec.
func (upd *Update) TryExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	if upd.QueryTimeout != 0 {
		cancel := vcursor.SetContextTimeout(time.Duration(upd.QueryTimeout) * time.Millisecond)
		defer cancel()
	}

	rss, _, err := upd.findRoute(vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	err = allowOnlyPrimary(rss...)
	if err != nil {
		return nil, err
	}

	switch upd.Opcode {
	case Unsharded:
		return upd.execUnsharded(vcursor, bindVars, rss)
	case Equal, EqualUnique, IN, Scatter, ByDestination, SubShard:
		return upd.execMultiDestination(vcursor, bindVars, rss, upd.updateVindexEntries)
	default:
		// Unreachable.
		return nil, fmt.Errorf("unsupported opcode: %v", upd.Opcode)
	}
}

// TryStreamExecute performs a streaming exec.
func (upd *Update) TryStreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	res, err := upd.TryExecute(vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(res)

}

// GetFields fetches the field info.
func (upd *Update) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, fmt.Errorf("BUG: unreachable code for %q", upd.Query)
}

// updateVindexEntries performs an update when a vindex is being modified
// by the statement.
// Note: the commit order may be different from the DML order because it's possible
// for DMLs to reuse existing transactions.
// Note 2: While changes are being committed, the changing row could be
// unreachable by either the new or old column values.
func (upd *Update) updateVindexEntries(vcursor VCursor, bindVars map[string]*querypb.BindVariable, rss []*srvtopo.ResolvedShard) error {
	if len(upd.ChangedVindexValues) == 0 {
		return nil
	}
	queries := make([]*querypb.BoundQuery, len(rss))
	for i := range rss {
		queries[i] = &querypb.BoundQuery{Sql: upd.OwnedVindexQuery, BindVariables: bindVars}
	}
	subQueryResult, errors := vcursor.ExecuteMultiShard(rss, queries, false, false)
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
	env := evalengine.EnvWithBindVars(bindVars, vcursor.ConnCollation())

	for _, row := range subQueryResult.Rows {
		ksid, err := resolveKeyspaceID(vcursor, upd.KsidVindex, row[0:upd.KsidLength])
		if err != nil {
			return err
		}
		for _, colVindex := range upd.Table.Owned {
			// Update columns only if they're being changed.
			if updColValues, ok := upd.ChangedVindexValues[colVindex.Name]; ok {
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

				if err := colVindex.Vindex.(vindexes.Lookup).Update(vcursor, fromIds, ksid, vindexColumnKeys); err != nil {
					return err
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
