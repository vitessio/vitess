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
	"fmt"
	"sort"
	"strconv"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

const (
	readLockExclusive = "exclusive"
	readLockShared    = "shared"
	readLockNone      = "none"
	readLockDefault   = readLockExclusive

	lookupCommonParamAutocommit           = "autocommit"
	lookupCommonParamMultiShardAutocommit = "multi_shard_autocommit"

	lookupInternalParamTable       = "table"
	lookupInternalParamFrom        = "from"
	lookupInternalParamTo          = "to"
	lookupInternalParamIgnoreNulls = "ignore_nulls"
	lookupInternalParamBatchLookup = "batch_lookup"
	lookupInternalParamReadLock    = "read_lock"
)

var (
	readLockExprs = map[string]string{
		readLockExclusive: "for update",
		readLockShared:    "lock in share mode",
		readLockNone:      "",
	}

	// lookupCommonParams are used only by lookup_* vindexes.
	lookupCommonParams = append(
		append(make([]string, 0), lookupInternalParams...),
		lookupCommonParamAutocommit,
		lookupCommonParamMultiShardAutocommit,
	)

	// lookupInternalParams are used by both lookup_* vindexes and the newer
	// consistent_lookup_* vindexes.
	lookupInternalParams = []string{
		lookupInternalParamTable,
		lookupInternalParamFrom,
		lookupInternalParamTo,
		lookupInternalParamIgnoreNulls,
		lookupInternalParamBatchLookup,
		lookupInternalParamReadLock,
	}
)

// lookupInternal implements the functions for the Lookup vindexes.
type lookupInternal struct {
	Table                   string   `json:"table"`
	FromColumns             []string `json:"from_columns"`
	To                      string   `json:"to"`
	Autocommit              bool     `json:"autocommit,omitempty"`
	MultiShardAutocommit    bool     `json:"multi_shard_autocommit,omitempty"`
	Upsert                  bool     `json:"upsert,omitempty"`
	IgnoreNulls             bool     `json:"ignore_nulls,omitempty"`
	BatchLookup             bool     `json:"batch_lookup,omitempty"`
	ReadLock                string   `json:"read_lock,omitempty"`
	sel, selTxDml, ver, del string   // sel: map query, ver: verify query, del: delete query
}

func (lkp *lookupInternal) Init(lookupQueryParams map[string]string, autocommit, upsert, multiShardAutocommit bool) error {
	lkp.Table = lookupQueryParams[lookupInternalParamTable]
	lkp.To = lookupQueryParams[lookupInternalParamTo]
	var fromColumns []string
	for _, from := range strings.Split(lookupQueryParams[lookupInternalParamFrom], ",") {
		fromColumns = append(fromColumns, strings.TrimSpace(from))
	}
	lkp.FromColumns = fromColumns

	var err error
	lkp.IgnoreNulls, err = boolFromMap(lookupQueryParams, lookupInternalParamIgnoreNulls)
	if err != nil {
		return err
	}
	lkp.BatchLookup, err = boolFromMap(lookupQueryParams, lookupInternalParamBatchLookup)
	if err != nil {
		return err
	}
	if readLock, ok := lookupQueryParams[lookupInternalParamReadLock]; ok {
		if _, valid := readLockExprs[readLock]; !valid {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid %s value: %s", lookupInternalParamReadLock, readLock)
		}
		lkp.ReadLock = readLock
	}

	lkp.Autocommit = autocommit
	lkp.Upsert = upsert
	if multiShardAutocommit {
		lkp.Autocommit = true
		lkp.MultiShardAutocommit = true
	}

	// TODO @rafael: update sel and ver to support multi column vindexes. This will be done
	// as part of face 2 of https://github.com/vitessio/vitess/issues/3481
	// For now multi column behaves as a single column for Map and Verify operations
	lkp.sel = fmt.Sprintf("select %s, %s from %s where %s in ::%s", lkp.FromColumns[0], lkp.To, lkp.Table, lkp.FromColumns[0], lkp.FromColumns[0])
	if lkp.ReadLock != readLockNone {
		lockExpr, ok := readLockExprs[lkp.ReadLock]
		if !ok {
			lockExpr = readLockExprs[readLockDefault]
		}
		lkp.selTxDml = fmt.Sprintf("%s %s", lkp.sel, lockExpr)
	} else {
		lkp.selTxDml = lkp.sel
	}
	lkp.ver = fmt.Sprintf("select %s from %s where %s = :%s and %s = :%s", lkp.FromColumns[0], lkp.Table, lkp.FromColumns[0], lkp.FromColumns[0], lkp.To, lkp.To)
	lkp.del = lkp.initDelStmt()
	return nil
}

// Lookup performs a lookup for the ids.
func (lkp *lookupInternal) Lookup(ctx context.Context, vcursor VCursor, ids []sqltypes.Value, co vtgatepb.CommitOrder) ([]*sqltypes.Result, error) {
	if vcursor == nil {
		return nil, vterrors.VT13001("cannot perform lookup: no vcursor provided")
	}
	results := make([]*sqltypes.Result, 0, len(ids))
	if lkp.Autocommit {
		co = vtgatepb.CommitOrder_AUTOCOMMIT
	}
	var sel string
	if vcursor.InTransactionAndIsDML() {
		sel = lkp.selTxDml
	} else {
		sel = lkp.sel
	}
	if ids[0].IsIntegral() || lkp.BatchLookup {
		// for integral types, batch query all ids and then map them back to the input order
		vars, err := sqltypes.BuildBindVariable(ids)
		if err != nil {
			return nil, err
		}
		bindVars := map[string]*querypb.BindVariable{
			lkp.FromColumns[0]: vars,
		}
		result, err := vcursor.Execute(ctx, "VindexLookup", sel, bindVars, false /* rollbackOnError */, co)
		if err != nil {
			return nil, vterrors.Wrap(err, "lookup.Map")
		}
		resultMap := make(map[string][][]sqltypes.Value)
		for _, row := range result.Rows {
			resultMap[row[0].ToString()] = append(resultMap[row[0].ToString()], []sqltypes.Value{row[1]})
		}

		for _, id := range ids {
			results = append(results, &sqltypes.Result{
				Rows: resultMap[id.ToString()],
			})
		}
	} else {
		// for non integral and binary type, fallback to send query per id
		for _, id := range ids {
			vars, err := sqltypes.BuildBindVariable([]any{id})
			if err != nil {
				return nil, err
			}
			bindVars := map[string]*querypb.BindVariable{
				lkp.FromColumns[0]: vars,
			}
			var result *sqltypes.Result
			result, err = vcursor.Execute(ctx, "VindexLookup", sel, bindVars, false /* rollbackOnError */, co)
			if err != nil {
				return nil, vterrors.Wrap(err, "lookup.Map")
			}
			rows := make([][]sqltypes.Value, 0, len(result.Rows))
			for _, row := range result.Rows {
				rows = append(rows, []sqltypes.Value{row[1]})
			}
			results = append(results, &sqltypes.Result{
				Rows: rows,
			})
		}
	}
	return results, nil
}

// Verify returns true if ids map to values.
func (lkp *lookupInternal) Verify(ctx context.Context, vcursor VCursor, ids, values []sqltypes.Value) ([]bool, error) {
	co := vtgatepb.CommitOrder_NORMAL
	if lkp.Autocommit {
		co = vtgatepb.CommitOrder_AUTOCOMMIT
	}
	return lkp.VerifyCustom(ctx, vcursor, ids, values, co)
}

func (lkp *lookupInternal) VerifyCustom(ctx context.Context, vcursor VCursor, ids, values []sqltypes.Value, co vtgatepb.CommitOrder) ([]bool, error) {
	out := make([]bool, len(ids))
	for i, id := range ids {
		bindVars := map[string]*querypb.BindVariable{
			lkp.FromColumns[0]: sqltypes.ValueBindVariable(id),
			lkp.To:             sqltypes.ValueBindVariable(values[i]),
		}
		result, err := vcursor.Execute(ctx, "VindexVerify", lkp.ver, bindVars, false /* rollbackOnError */, co)
		if err != nil {
			return nil, vterrors.Wrap(err, "lookup.Verify")
		}
		out[i] = (len(result.Rows) != 0)
	}
	return out, nil
}

type sorter struct {
	rowsColValues [][]sqltypes.Value
	toValues      []sqltypes.Value
}

func (v *sorter) Len() int {
	return len(v.toValues)
}

func (v *sorter) Less(i, j int) bool {
	leftRow := v.rowsColValues[i]
	rightRow := v.rowsColValues[j]
	for cell, left := range leftRow {
		right := rightRow[cell]
		lBytes, _ := left.ToBytes()
		rBytes, _ := right.ToBytes()
		compare := bytes.Compare(lBytes, rBytes)
		if compare < 0 {
			return true
		}
		if compare > 0 {
			return false
		}
	}
	iBytes, _ := v.toValues[i].ToBytes()
	jBytes, _ := v.toValues[j].ToBytes()
	return bytes.Compare(iBytes, jBytes) < 0
}

func (v *sorter) Swap(i, j int) {
	v.toValues[i], v.toValues[j] = v.toValues[j], v.toValues[i]
	v.rowsColValues[i], v.rowsColValues[j] = v.rowsColValues[j], v.rowsColValues[i]
}

// Create creates an association between rowsColValues and toValues by inserting rows in the vindex table.
// rowsColValues contains all the rows that are being inserted.
// For each row, we store the value of each column defined in the vindex.
// toValues contains the keyspace_id of each row being inserted.
// Given a vindex with two columns and the following insert:
//
// INSERT INTO table_a (colum_a, column_b, column_c) VALUES (value_a0, value_b0, value_c0), (value_a1, value_b1, value_c1);
// If we assume that the primary vindex is on column_c. The call to create will look like this:
// Create(vcursor, [[value_a0, value_b0,], [value_a1, value_b1]], [binary(value_c0), binary(value_c1)])
// Notice that toValues contains the computed binary value of the keyspace_id.
func (lkp *lookupInternal) Create(ctx context.Context, vcursor VCursor, rowsColValues [][]sqltypes.Value, toValues []sqltypes.Value, ignoreMode bool) error {
	if lkp.Autocommit {
		return lkp.createCustom(ctx, vcursor, rowsColValues, toValues, ignoreMode, vtgatepb.CommitOrder_AUTOCOMMIT)
	}
	return lkp.createCustom(ctx, vcursor, rowsColValues, toValues, ignoreMode, vtgatepb.CommitOrder_NORMAL)
}

func (lkp *lookupInternal) createCustom(ctx context.Context, vcursor VCursor, rowsColValues [][]sqltypes.Value, toValues []sqltypes.Value, ignoreMode bool, co vtgatepb.CommitOrder) error {
	// Trim rows with null values
	trimmedRowsCols := make([][]sqltypes.Value, 0, len(rowsColValues))
	trimmedToValues := make([]sqltypes.Value, 0, len(toValues))
nextRow:
	for i, row := range rowsColValues {
		for j, col := range row {
			if col.IsNull() {
				if !lkp.IgnoreNulls {
					cols := strings.Join(lkp.FromColumns, ",")
					return vterrors.VT03028(cols, i, j)
				}
				continue nextRow
			}
		}
		trimmedRowsCols = append(trimmedRowsCols, row)
		trimmedToValues = append(trimmedToValues, toValues[i])
	}
	if len(trimmedRowsCols) == 0 {
		return nil
	}
	// We only need to check the first row. Number of cols per row
	// is guaranteed by the engine to be uniform.
	if len(trimmedRowsCols[0]) != len(lkp.FromColumns) {
		return vterrors.VT03030(lkp.FromColumns, len(trimmedRowsCols[0]))
	}
	sort.Sort(&sorter{rowsColValues: trimmedRowsCols, toValues: trimmedToValues})

	insStmt := "insert"
	if lkp.MultiShardAutocommit {
		insStmt = "insert /*vt+ MULTI_SHARD_AUTOCOMMIT=1 */"
	}
	var buf strings.Builder
	if ignoreMode {
		fmt.Fprintf(&buf, "%s ignore into %s(", insStmt, lkp.Table)
	} else {
		fmt.Fprintf(&buf, "%s into %s(", insStmt, lkp.Table)
	}
	for _, col := range lkp.FromColumns {
		fmt.Fprintf(&buf, "%s, ", col)
	}
	fmt.Fprintf(&buf, "%s) values(", lkp.To)

	bindVars := make(map[string]*querypb.BindVariable, 2*len(trimmedRowsCols))
	for rowIdx := range trimmedToValues {
		colIds := trimmedRowsCols[rowIdx]
		if rowIdx != 0 {
			buf.WriteString(", (")
		}
		for colIdx, colID := range colIds {
			fromStr := lkp.FromColumns[colIdx] + "_" + strconv.Itoa(rowIdx)
			bindVars[fromStr] = sqltypes.ValueBindVariable(colID)
			buf.WriteString(":" + fromStr + ", ")
		}
		toStr := lkp.To + "_" + strconv.Itoa(rowIdx)
		buf.WriteString(":" + toStr + ")")
		bindVars[toStr] = sqltypes.ValueBindVariable(trimmedToValues[rowIdx])
	}

	if lkp.Upsert {
		fmt.Fprintf(&buf, " on duplicate key update ")
		for _, col := range lkp.FromColumns {
			fmt.Fprintf(&buf, "%s=values(%s), ", col, col)
		}
		fmt.Fprintf(&buf, "%s=values(%s)", lkp.To, lkp.To)
	}

	if _, err := vcursor.Execute(ctx, "VindexCreate", buf.String(), bindVars, true /* rollbackOnError */, co); err != nil {
		return vterrors.Wrap(err, "lookup.Create")
	}
	return nil
}

// Delete deletes the association between ids and value.
// rowsColValues contains all the rows that are being deleted.
// For each row, we store the value of each column defined in the vindex.
// value contains the keyspace_id of the vindex entry being deleted.
//
// Given the following information in a vindex table with two columns:
//
//	+------------------+-----------+--------+
//	| hex(keyspace_id) | a         | b      |
//	+------------------+-----------+--------+
//	| 52CB7B1B31B2222E | valuea    | valueb |
//	+------------------+-----------+--------+
//
// A call to Delete would look like this:
// Delete(vcursor, [[valuea, valueb]], 52CB7B1B31B2222E)
func (lkp *lookupInternal) Delete(ctx context.Context, vcursor VCursor, rowsColValues [][]sqltypes.Value, value sqltypes.Value, co vtgatepb.CommitOrder) error {
	// In autocommit mode, it's not safe to delete. So, it's a no-op.
	if lkp.Autocommit {
		return nil
	}
	if len(rowsColValues) == 0 {
		// This code is unreachable. It's just a failsafe.
		return nil
	}
	// We only need to check the first row. Number of cols per row
	// is guaranteed by the engine to be uniform.
	if len(rowsColValues[0]) != len(lkp.FromColumns) {
		return vterrors.VT03030(lkp.FromColumns, len(rowsColValues[0]))
	}
	for _, column := range rowsColValues {
		bindVars := make(map[string]*querypb.BindVariable, len(rowsColValues))
		for colIdx, columnValue := range column {
			bindVars[lkp.FromColumns[colIdx]] = sqltypes.ValueBindVariable(columnValue)
		}
		bindVars[lkp.To] = sqltypes.ValueBindVariable(value)
		_, err := vcursor.Execute(ctx, "VindexDelete", lkp.del, bindVars, true /* rollbackOnError */, co)
		if err != nil {
			return vterrors.Wrap(err, "lookup.Delete")
		}
	}
	return nil
}

// Update implements the update functionality.
func (lkp *lookupInternal) Update(ctx context.Context, vcursor VCursor, oldValues []sqltypes.Value, ksid []byte, toValue sqltypes.Value, newValues []sqltypes.Value) error {
	if err := lkp.Delete(ctx, vcursor, [][]sqltypes.Value{oldValues}, toValue, vtgatepb.CommitOrder_NORMAL); err != nil {
		return err
	}
	return lkp.Create(ctx, vcursor, [][]sqltypes.Value{newValues}, []sqltypes.Value{toValue}, false /* ignoreMode */)
}

func (lkp *lookupInternal) initDelStmt() string {
	var delBuffer strings.Builder
	fmt.Fprintf(&delBuffer, "delete from %s where ", lkp.Table)
	for colIdx, column := range lkp.FromColumns {
		if colIdx != 0 {
			delBuffer.WriteString(" and ")
		}
		delBuffer.WriteString(column + " = :" + column)
	}
	delBuffer.WriteString(" and " + lkp.To + " = :" + lkp.To)
	return delBuffer.String()
}

func (lkp *lookupInternal) query() (selQuery string, arguments []string) {
	return lkp.sel, lkp.FromColumns
}

type commonConfig struct {
	autocommit           bool
	multiShardAutocommit bool
}

func parseCommonConfig(m map[string]string) (*commonConfig, error) {
	var c commonConfig
	var err error
	if c.autocommit, err = boolFromMap(m, lookupCommonParamAutocommit); err != nil {
		return nil, err
	}
	if c.multiShardAutocommit, err = boolFromMap(m, lookupCommonParamMultiShardAutocommit); err != nil {
		return nil, err
	}
	return &c, nil
}

func boolFromMap(m map[string]string, key string) (bool, error) {
	val, ok := m[key]
	if !ok {
		return false, nil
	}
	switch val {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%s value must be 'true' or 'false': '%s'", key, val)
	}
}
