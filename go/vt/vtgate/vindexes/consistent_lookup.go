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

package vindexes

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

const (
	consistentLookupParamWriteOnly = "write_only"
)

var (
	_ SingleColumn    = (*ConsistentLookupUnique)(nil)
	_ Lookup          = (*ConsistentLookupUnique)(nil)
	_ WantOwnerInfo   = (*ConsistentLookupUnique)(nil)
	_ LookupPlanable  = (*ConsistentLookupUnique)(nil)
	_ ParamValidating = (*ConsistentLookupUnique)(nil)
	_ SingleColumn    = (*ConsistentLookup)(nil)
	_ Lookup          = (*ConsistentLookup)(nil)
	_ WantOwnerInfo   = (*ConsistentLookup)(nil)
	_ LookupPlanable  = (*ConsistentLookup)(nil)
	_ ParamValidating = (*ConsistentLookup)(nil)

	consistentLookupParams = append(
		append(make([]string, 0), lookupInternalParams...),
		consistentLookupParamWriteOnly,
	)
)

func init() {
	Register("consistent_lookup", newConsistentLookup)
	Register("consistent_lookup_unique", newConsistentLookupUnique)
}

// ConsistentLookup is a non-unique lookup vindex that can stay
// consistent with respect to its owner table.
type ConsistentLookup struct {
	*clCommon
	unknownParams []string
}

// newConsistentLookup creates a ConsistentLookup vindex.
// The supplied map has the following required fields:
//
//	table: name of the backing table. It can be qualified by the keyspace.
//	from: list of columns in the table that have the 'from' values of the lookup vindex.
//	to: The 'to' column name of the table.
func newConsistentLookup(name string, m map[string]string) (Vindex, error) {
	clc, err := newCLCommon(name, m)
	if err != nil {
		return nil, err
	}
	return &ConsistentLookup{
		clCommon:      clc,
		unknownParams: FindUnknownParams(m, consistentLookupParams),
	}, nil
}

// Cost returns the cost of this vindex as 20.
func (lu *ConsistentLookup) Cost() int {
	return 20
}

// IsUnique returns false since the Vindex is non unique.
func (lu *ConsistentLookup) IsUnique() bool {
	return false
}

// NeedsVCursor satisfies the Vindex interface.
func (lu *ConsistentLookup) NeedsVCursor() bool {
	return true
}

// Map can map ids to key.Destination objects.
func (lu *ConsistentLookup) Map(ctx context.Context, vcursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, 0, len(ids))
	if lu.writeOnly {
		for range ids {
			out = append(out, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{}})
		}
		return out, nil
	}

	// if ignore_nulls is set and the query is about single null value, then fallback to all shards
	if len(ids) == 1 && ids[0].IsNull() && lu.lkp.IgnoreNulls {
		for range ids {
			out = append(out, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{}})
		}
		return out, nil
	}

	results, err := lu.lkp.Lookup(ctx, vcursor, ids, vcursor.LookupRowLockShardSession())
	if err != nil {
		return nil, err
	}
	return lu.MapResult(ids, results)
}

// MapResult implements the LookupPlanable interface
func (lu *ConsistentLookup) MapResult(ids []sqltypes.Value, results []*sqltypes.Result) ([]key.Destination, error) {
	out := make([]key.Destination, 0, len(ids))
	if lu.writeOnly {
		for range ids {
			out = append(out, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{}})
		}
		return out, nil
	}
	for _, result := range results {
		if len(result.Rows) == 0 {
			out = append(out, key.DestinationNone{})
			continue
		}
		ksids := make([][]byte, 0, len(result.Rows))
		for _, row := range result.Rows {
			rowBytes, err := row[0].ToBytes()
			if err != nil {
				return nil, err
			}
			ksids = append(ksids, rowBytes)
		}
		out = append(out, key.DestinationKeyspaceIDs(ksids))
	}
	return out, nil
}

// Query implements the LookupPlanable interface
func (lu *ConsistentLookup) Query() (selQuery string, arguments []string) {
	return lu.lkp.query()
}

// AllowBatch implements the LookupPlanable interface
func (lu *ConsistentLookup) AllowBatch() bool {
	return lu.lkp.BatchLookup
}

func (lu *ConsistentLookup) AutoCommitEnabled() bool {
	return lu.lkp.Autocommit
}

// UnknownParams implements the ParamValidating interface.
func (lu *ConsistentLookup) UnknownParams() []string {
	return lu.unknownParams
}

// ====================================================================

// ConsistentLookupUnique defines a vindex that uses a lookup table.
// The table is expected to define the id column as unique. It's
// Unique and a Lookup.
type ConsistentLookupUnique struct {
	*clCommon
	unknownParams []string
}

// newConsistentLookupUnique creates a ConsistentLookupUnique vindex.
// The supplied map has the following required fields:
//
//	table: name of the backing table. It can be qualified by the keyspace.
//	from: list of columns in the table that have the 'from' values of the lookup vindex.
//	to: The 'to' column name of the table.
func newConsistentLookupUnique(name string, m map[string]string) (Vindex, error) {
	clc, err := newCLCommon(name, m)
	if err != nil {
		return nil, err
	}
	return &ConsistentLookupUnique{
		clCommon:      clc,
		unknownParams: FindUnknownParams(m, consistentLookupParams),
	}, nil
}

// Cost returns the cost of this vindex as 10.
func (lu *ConsistentLookupUnique) Cost() int {
	return 10
}

// IsUnique returns true since the Vindex is unique.
func (lu *ConsistentLookupUnique) IsUnique() bool {
	return true
}

// NeedsVCursor satisfies the Vindex interface.
func (lu *ConsistentLookupUnique) NeedsVCursor() bool {
	return true
}

// Map can map ids to key.Destination objects.
func (lu *ConsistentLookupUnique) Map(ctx context.Context, vcursor VCursor, ids []sqltypes.Value) ([]key.Destination, error) {
	out := make([]key.Destination, 0, len(ids))
	if lu.writeOnly {
		for range ids {
			out = append(out, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{}})
		}
		return out, nil
	}

	results, err := lu.lkp.Lookup(ctx, vcursor, ids, vcursor.LookupRowLockShardSession())
	if err != nil {
		return nil, err
	}
	return lu.MapResult(ids, results)
}

// MapResult implements the LookupPlanable interface
func (lu *ConsistentLookupUnique) MapResult(ids []sqltypes.Value, results []*sqltypes.Result) ([]key.Destination, error) {
	out := make([]key.Destination, 0, len(ids))
	if lu.writeOnly {
		for range ids {
			out = append(out, key.DestinationKeyRange{KeyRange: &topodatapb.KeyRange{}})
		}
		return out, nil
	}

	for i, result := range results {
		switch len(result.Rows) {
		case 0:
			out = append(out, key.DestinationNone{})
		case 1:
			rowBytes, err := result.Rows[0][0].ToBytes()
			if err != nil {
				return out, err
			}
			out = append(out, key.DestinationKeyspaceID(rowBytes))
		default:
			return nil, fmt.Errorf("Lookup.Map: unexpected multiple results from vindex %s: %v", lu.lkp.Table, ids[i])
		}
	}
	return out, nil
}

// Query implements the LookupPlanable interface
func (lu *ConsistentLookupUnique) Query() (selQuery string, arguments []string) {
	return lu.lkp.query()
}

// AllowBatch implements the LookupPlanable interface
func (lu *ConsistentLookupUnique) AllowBatch() bool {
	return lu.lkp.BatchLookup
}

func (lu *ConsistentLookupUnique) AutoCommitEnabled() bool {
	return lu.lkp.Autocommit
}

// ====================================================================

// clCommon defines a vindex that uses a lookup table.
// The table is expected to define the id column as unique. It's
// Unique and a Lookup.
type clCommon struct {
	name         string
	writeOnly    bool
	lkp          lookupInternal
	keyspace     string
	ownerTable   string
	ownerColumns []string

	lockLookupQuery   string
	lockOwnerQuery    string
	insertLookupQuery string
	updateLookupQuery string
}

// newCLCommon is commone code for the consistent lookup vindexes.
func newCLCommon(name string, m map[string]string) (*clCommon, error) {
	lu := &clCommon{name: name}
	var err error
	lu.writeOnly, err = boolFromMap(m, consistentLookupParamWriteOnly)
	if err != nil {
		return nil, err
	}

	if err := lu.lkp.Init(m, false /* autocommit */, false /* upsert */, false /* multiShardAutocommit */); err != nil {
		return nil, err
	}
	return lu, nil
}

func (lu *clCommon) SetOwnerInfo(keyspace, table string, cols []sqlparser.IdentifierCI) error {
	lu.keyspace = keyspace
	lu.ownerTable = sqlparser.String(sqlparser.NewIdentifierCS(table))
	if len(cols) != len(lu.lkp.FromColumns) {
		return vterrors.VT03029(lu.name)
	}
	lu.ownerColumns = make([]string, len(cols))
	for i, col := range cols {
		lu.ownerColumns[i] = col.String()
	}
	lu.lockLookupQuery = lu.generateLockLookup()
	lu.lockOwnerQuery = lu.generateLockOwner()
	lu.insertLookupQuery = lu.generateInsertLookup()
	lu.updateLookupQuery = lu.generateUpdateLookup()
	return nil
}

// String returns the name of the vindex.
func (lu *clCommon) String() string {
	return lu.name
}

// Verify returns true if ids maps to ksids.
func (lu *clCommon) Verify(ctx context.Context, vcursor VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	if lu.writeOnly {
		out := make([]bool, len(ids))
		for i := range ids {
			out[i] = true
		}
		return out, nil
	}
	return lu.lkp.VerifyCustom(ctx, vcursor, ids, ksidsToValues(ksids), vtgatepb.CommitOrder_PRE)
}

// Create reserves the id by inserting it into the vindex table.
func (lu *clCommon) Create(ctx context.Context, vcursor VCursor, rowsColValues [][]sqltypes.Value, ksids [][]byte, ignoreMode bool) error {
	origErr := lu.lkp.createCustom(ctx, vcursor, rowsColValues, ksidsToValues(ksids), ignoreMode, vtgatepb.CommitOrder_PRE)
	if origErr == nil {
		return nil
	}
	// Try and convert the error to a MySQL error
	sqlErr, isSQLErr := sqlerror.NewSQLErrorFromError(origErr).(*sqlerror.SQLError)
	// If it is a MySQL error and its code is of duplicate entry, then we would like to continue
	// Otherwise, we return the error
	if !(isSQLErr && sqlErr != nil && sqlErr.Number() == sqlerror.ERDupEntry) {
		return origErr
	}
	for i, row := range rowsColValues {
		if err := lu.handleDup(ctx, vcursor, row, ksids[i], origErr); err != nil {
			return err
		}
	}
	return nil
}

func (lu *clCommon) handleDup(ctx context.Context, vcursor VCursor, values []sqltypes.Value, ksid []byte, dupError error) error {
	bindVars := make(map[string]*querypb.BindVariable, len(values))
	for colnum, val := range values {
		bindVars[lu.lkp.FromColumns[colnum]] = sqltypes.ValueBindVariable(val)
	}
	bindVars[lu.lkp.To] = sqltypes.BytesBindVariable(ksid)

	// Lock the lookup row using pre priority.
	qr, err := vcursor.Execute(ctx, "VindexCreate", lu.lockLookupQuery, bindVars, false /* rollbackOnError */, vtgatepb.CommitOrder_PRE)
	if err != nil {
		return err
	}
	switch len(qr.Rows) {
	case 0:
		if _, err := vcursor.Execute(ctx, "VindexCreate", lu.insertLookupQuery, bindVars, true /* rollbackOnError */, vtgatepb.CommitOrder_PRE); err != nil {
			return err
		}
	case 1:
		existingksid, err := qr.Rows[0][0].ToBytes()
		if err != nil {
			return err
		}
		// Lock the target row using normal transaction priority.
		qr, err = vcursor.ExecuteKeyspaceID(ctx, lu.keyspace, existingksid, lu.lockOwnerQuery, bindVars, false /* rollbackOnError */, false /* autocommit */)
		if err != nil {
			return err
		}
		if len(qr.Rows) >= 1 {
			return dupError
		}
		if bytes.Equal(existingksid, ksid) {
			return nil
		}
		if _, err := vcursor.Execute(ctx, "VindexCreate", lu.updateLookupQuery, bindVars, true /* rollbackOnError */, vtgatepb.CommitOrder_PRE); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unexpected rows: %v from consistent lookup vindex", qr.Rows)
	}
	return nil
}

// Delete deletes the entry from the vindex table.
func (lu *clCommon) Delete(ctx context.Context, vcursor VCursor, rowsColValues [][]sqltypes.Value, ksid []byte) error {
	return lu.lkp.Delete(ctx, vcursor, rowsColValues, sqltypes.MakeTrusted(sqltypes.VarBinary, ksid), vtgatepb.CommitOrder_POST)
}

// Update updates the entry in the vindex table.
func (lu *clCommon) Update(ctx context.Context, vcursor VCursor, oldValues []sqltypes.Value, ksid []byte, newValues []sqltypes.Value) error {
	equal := true
	for i := range oldValues {
		result, err := evalengine.NullsafeCompare(oldValues[i], newValues[i], vcursor.Environment().CollationEnv(), vcursor.ConnCollation())
		// errors from NullsafeCompare can be ignored. if they are real problems, we'll see them in the Create/Update
		if err != nil || result != 0 {
			equal = false
			break
		}
	}
	if equal {
		return nil
	}
	if err := lu.Delete(ctx, vcursor, [][]sqltypes.Value{oldValues}, ksid); err != nil {
		return err
	}
	return lu.Create(ctx, vcursor, [][]sqltypes.Value{newValues}, [][]byte{ksid}, false /* ignoreMode */)
}

// MarshalJSON returns a JSON representation of clCommon.
func (lu *clCommon) MarshalJSON() ([]byte, error) {
	return json.Marshal(lu.lkp)
}

func (lu *clCommon) generateLockLookup() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "select %s from %s", lu.lkp.To, lu.lkp.Table)
	lu.addWhere(&buf, lu.lkp.FromColumns)
	fmt.Fprintf(&buf, " for update")
	return buf.String()
}

func (lu *clCommon) generateLockOwner() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "select %s from %s", lu.ownerColumns[0], lu.ownerTable)
	lu.addWhere(&buf, lu.ownerColumns)
	// We can lock in share mode because we only want to check
	// if the row exists. We still need to lock to make us wait
	// in case a previous transaction is creating it.
	fmt.Fprintf(&buf, " lock in share mode")
	return buf.String()
}

func (lu *clCommon) generateInsertLookup() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "insert into %s(", lu.lkp.Table)
	for _, col := range lu.lkp.FromColumns {
		fmt.Fprintf(&buf, "%s, ", col)
	}
	fmt.Fprintf(&buf, "%s) values(", lu.lkp.To)
	for _, col := range lu.lkp.FromColumns {
		fmt.Fprintf(&buf, ":%s, ", col)
	}
	fmt.Fprintf(&buf, ":%s)", lu.lkp.To)
	return buf.String()
}

func (lu *clCommon) generateUpdateLookup() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "update %s set %s=:%s", lu.lkp.Table, lu.lkp.To, lu.lkp.To)
	lu.addWhere(&buf, lu.lkp.FromColumns)
	return buf.String()
}

func (lu *clCommon) addWhere(buf *strings.Builder, cols []string) {
	buf.WriteString(" where ")
	for colIdx, column := range cols {
		if colIdx != 0 {
			buf.WriteString(" and ")
		}
		buf.WriteString(column + " = :" + lu.lkp.FromColumns[colIdx])
	}
}

// GetCommitOrder implements the LookupPlanable interface
func (lu *clCommon) GetCommitOrder() vtgatepb.CommitOrder {
	return vtgatepb.CommitOrder_PRE
}

// IsBackfilling implements the LookupBackfill interface
func (lu *clCommon) IsBackfilling() bool {
	return lu.writeOnly
}

// UnknownParams implements the ParamValidating interface.
func (lu *ConsistentLookupUnique) UnknownParams() []string {
	return lu.unknownParams
}
