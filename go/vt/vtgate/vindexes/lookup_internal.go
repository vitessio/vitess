/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vindexes

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

// lookupInternal implements the functions for the Lookup vindexes.
type lookupInternal struct {
	Table         string   `json:"table"`
	FromColumns   []string `json:"from_columns"`
	To            string   `json:"to"`
	Autocommit    bool     `json:"autocommit,omitempty"`
	Upsert        bool     `json:"upsert,omitempty"`
	sel, ver, del string
}

func (lkp *lookupInternal) Init(lookupQueryParams map[string]string, autocommit, upsert bool) error {
	lkp.Table = lookupQueryParams["table"]
	lkp.To = lookupQueryParams["to"]
	var fromColumns []string
	for _, from := range strings.Split(lookupQueryParams["from"], ",") {
		fromColumns = append(fromColumns, strings.TrimSpace(from))
	}
	lkp.FromColumns = fromColumns

	lkp.Autocommit = autocommit
	lkp.Upsert = upsert

	// TODO @rafael: update sel and ver to support multi column vindexes. This will be done
	// as part of face 2 of https://github.com/vitessio/vitess/issues/3481
	// For now multi column behaves as a single column for Map and Verify operations
	lkp.sel = fmt.Sprintf("select %s from %s where %s = :%s", lkp.To, lkp.Table, lkp.FromColumns[0], lkp.FromColumns[0])
	lkp.ver = fmt.Sprintf("select %s from %s where %s = :%s and %s = :%s", lkp.FromColumns[0], lkp.Table, lkp.FromColumns[0], lkp.FromColumns[0], lkp.To, lkp.To)
	lkp.del = lkp.initDelStmt()
	return nil
}

// Lookup performs a lookup for the ids.
func (lkp *lookupInternal) Lookup(vcursor VCursor, ids []sqltypes.Value) ([]*sqltypes.Result, error) {
	results := make([]*sqltypes.Result, 0, len(ids))
	for _, id := range ids {
		bindVars := map[string]*querypb.BindVariable{
			lkp.FromColumns[0]: sqltypes.ValueBindVariable(id),
		}
		var err error
		var result *sqltypes.Result
		co := vtgatepb.CommitOrder_NORMAL
		if lkp.Autocommit {
			co = vtgatepb.CommitOrder_AUTOCOMMIT
		}
		result, err = vcursor.Execute("VindexLookup", lkp.sel, bindVars, false /* isDML */, co)
		if err != nil {
			return nil, fmt.Errorf("lookup.Map: %v", err)
		}
		results = append(results, result)
	}
	return results, nil
}

// Verify returns true if ids map to values.
func (lkp *lookupInternal) Verify(vcursor VCursor, ids, values []sqltypes.Value) ([]bool, error) {
	out := make([]bool, len(ids))
	for i, id := range ids {
		bindVars := map[string]*querypb.BindVariable{
			lkp.FromColumns[0]: sqltypes.ValueBindVariable(id),
			lkp.To:             sqltypes.ValueBindVariable(values[i]),
		}
		var err error
		var result *sqltypes.Result
		co := vtgatepb.CommitOrder_NORMAL
		if lkp.Autocommit {
			co = vtgatepb.CommitOrder_AUTOCOMMIT
		}
		result, err = vcursor.Execute("VindexVerify", lkp.ver, bindVars, false /* isDML */, co)
		if err != nil {
			return nil, fmt.Errorf("lookup.Verify: %v", err)
		}
		out[i] = (len(result.Rows) != 0)
	}
	return out, nil
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
func (lkp *lookupInternal) Create(vcursor VCursor, rowsColValues [][]sqltypes.Value, toValues []sqltypes.Value, ignoreMode bool) error {
	if lkp.Autocommit {
		return lkp.createCustom(vcursor, rowsColValues, toValues, ignoreMode, vtgatepb.CommitOrder_AUTOCOMMIT)
	}
	return lkp.createCustom(vcursor, rowsColValues, toValues, ignoreMode, vtgatepb.CommitOrder_NORMAL)
}

func (lkp *lookupInternal) createCustom(vcursor VCursor, rowsColValues [][]sqltypes.Value, toValues []sqltypes.Value, ignoreMode bool, co vtgatepb.CommitOrder) error {
	if len(rowsColValues) == 0 {
		// This code is unreachable. It's just a failsafe.
		return nil
	}
	// We only need to check the first row. Number of cols per row
	// is guaranteed by the engine to be uniform.
	if len(rowsColValues[0]) != len(lkp.FromColumns) {
		return fmt.Errorf("lookup.Create: column vindex count does not match the columns in the lookup: %d vs %v", len(rowsColValues[0]), lkp.FromColumns)
	}
	buf := new(bytes.Buffer)
	if ignoreMode {
		fmt.Fprintf(buf, "insert ignore into %s(", lkp.Table)
	} else {
		fmt.Fprintf(buf, "insert into %s(", lkp.Table)
	}
	for _, col := range lkp.FromColumns {
		fmt.Fprintf(buf, "%s, ", col)
	}
	fmt.Fprintf(buf, "%s) values(", lkp.To)

	bindVars := make(map[string]*querypb.BindVariable, 2*len(rowsColValues))
	for rowIdx := range toValues {
		colIds := rowsColValues[rowIdx]
		if rowIdx != 0 {
			buf.WriteString(", (")
		}
		for colIdx, colID := range colIds {
			fromStr := lkp.FromColumns[colIdx] + strconv.Itoa(rowIdx)
			bindVars[fromStr] = sqltypes.ValueBindVariable(colID)
			buf.WriteString(":" + fromStr + ", ")
		}
		toStr := lkp.To + strconv.Itoa(rowIdx)
		buf.WriteString(":" + toStr + ")")
		bindVars[toStr] = sqltypes.ValueBindVariable(toValues[rowIdx])
	}

	if lkp.Upsert {
		fmt.Fprintf(buf, " on duplicate key update ")
		for _, col := range lkp.FromColumns {
			fmt.Fprintf(buf, "%s=values(%s), ", col, col)
		}
		fmt.Fprintf(buf, "%s=values(%s)", lkp.To, lkp.To)
	}

	if _, err := vcursor.Execute("VindexCreate", buf.String(), bindVars, true /* isDML */, co); err != nil {
		return fmt.Errorf("lookup.Create: %v", err)
	}
	return nil
}

// Delete deletes the association between ids and value.
// rowsColValues contains all the rows that are being deleted.
// For each row, we store the value of each column defined in the vindex.
// value cointains the keyspace_id of the vindex entry being deleted.
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
func (lkp *lookupInternal) Delete(vcursor VCursor, rowsColValues [][]sqltypes.Value, value sqltypes.Value, co vtgatepb.CommitOrder) error {
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
		return fmt.Errorf("lookup.Delete: column vindex count does not match the columns in the lookup: %d vs %v", len(rowsColValues[0]), lkp.FromColumns)
	}
	for _, column := range rowsColValues {
		bindVars := make(map[string]*querypb.BindVariable, len(rowsColValues))
		for colIdx, columnValue := range column {
			bindVars[lkp.FromColumns[colIdx]] = sqltypes.ValueBindVariable(columnValue)
		}
		bindVars[lkp.To] = sqltypes.ValueBindVariable(value)
		_, err := vcursor.Execute("VindexDelete", lkp.del, bindVars, true /* isDML */, co)
		if err != nil {
			return fmt.Errorf("lookup.Delete: %v", err)
		}
	}
	return nil
}

// Update implements the update functionality.
func (lkp *lookupInternal) Update(vcursor VCursor, oldValues []sqltypes.Value, ksid sqltypes.Value, newValues []sqltypes.Value) error {
	if err := lkp.Delete(vcursor, [][]sqltypes.Value{oldValues}, ksid, vtgatepb.CommitOrder_NORMAL); err != nil {
		return err
	}
	return lkp.Create(vcursor, [][]sqltypes.Value{newValues}, []sqltypes.Value{ksid}, false /* ignoreMode */)
}

func (lkp *lookupInternal) initDelStmt() string {
	var delBuffer bytes.Buffer
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
		return false, fmt.Errorf("%s value must be 'true' or 'false': '%s'", key, val)
	}
}
