/*
Copyright 2017 Google Inc.

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

package tabletserver

import (
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// buildValueList builds the set of PK reference rows used to drive the next query.
// It uses the PK values supplied in the original query and bind variables.
// The generated reference rows are validated for type match against the PK of the table.
func buildValueList(table *schema.Table, pkValues []sqltypes.PlanValue, bindVars map[string]*querypb.BindVariable) ([][]sqltypes.Value, error) {
	rows, err := sqltypes.ResolveRows(pkValues, bindVars)
	if err != nil {
		return nil, err
	}
	// Iterate by columns.
	for j := range pkValues {
		typ := table.GetPKColumn(j).Type
		for i := range rows {
			rows[i][j], err = sqltypes.Cast(rows[i][j], typ)
			if err != nil {
				return nil, err
			}
		}
	}
	return rows, nil
}

// buildSecondaryList is used for handling ON DUPLICATE DMLs, or those that change the PK.
func buildSecondaryList(table *schema.Table, pkList [][]sqltypes.Value, secondaryList []sqltypes.PlanValue, bindVars map[string]*querypb.BindVariable) ([][]sqltypes.Value, error) {
	if secondaryList == nil {
		return nil, nil
	}
	secondaryRows, err := sqltypes.ResolveRows(secondaryList, bindVars)
	if err != nil {
		return nil, err
	}
	rows := make([][]sqltypes.Value, len(pkList))
	for i, row := range pkList {
		// If secondaryRows has only one row, then that
		// row should be duplicated for every row in pkList.
		// Otherwise, we use the individual values.
		var changedValues []sqltypes.Value
		if len(secondaryRows) == 1 {
			changedValues = secondaryRows[0]
		} else {
			changedValues = secondaryRows[i]
		}
		rows[i] = make([]sqltypes.Value, len(row))
		for j, value := range row {
			if changedValues[j].IsNull() {
				rows[i][j] = value
			} else {
				rows[i][j] = changedValues[j]
			}
		}
	}
	return rows, nil
}

// resolveNumber extracts a number from a bind variable or sql value.
func resolveNumber(pv sqltypes.PlanValue, bindVars map[string]*querypb.BindVariable) (int64, error) {
	v, err := pv.ResolveValue(bindVars)
	if err != nil {
		return 0, err
	}
	ret, err := sqltypes.ToInt64(v)
	if err != nil {
		return 0, err
	}
	return ret, nil
}

func validateRow(table *schema.Table, columnNumbers []int, row []sqltypes.Value) error {
	if len(row) != len(columnNumbers) {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "data inconsistency %d vs %d", len(row), len(columnNumbers))
	}
	for j, value := range row {
		if err := validateValue(&table.Columns[columnNumbers[j]], value); err != nil {
			return err
		}
	}

	return nil
}

func validateValue(col *schema.TableColumn, value sqltypes.Value) error {
	if value.IsNull() {
		return nil
	}
	if sqltypes.IsIntegral(col.Type) {
		if !value.IsIntegral() {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "type mismatch, expecting numeric type for %v for column: %v", value, col)
		}
	} else if col.Type == sqltypes.VarBinary {
		if !value.IsQuoted() {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "type mismatch, expecting string type for %v for column: %v", value, col)
		}
	}
	return nil
}

func buildStreamComment(table *schema.Table, pkValueList [][]sqltypes.Value, secondaryList [][]sqltypes.Value) string {
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf(" /* _stream %v (", table.Name)
	// We assume the first index exists, and is the pk
	for _, pkName := range table.Indexes[0].Columns {
		buf.Myprintf("%v ", pkName)
	}
	buf.WriteString(")")
	buildPKValueList(buf, table, pkValueList)
	buildPKValueList(buf, table, secondaryList)
	buf.WriteString("; */")
	return buf.String()
}

func buildPKValueList(buf *sqlparser.TrackedBuffer, table *schema.Table, pkValueList [][]sqltypes.Value) {
	for _, pkValues := range pkValueList {
		buf.WriteString(" (")
		for _, pkValue := range pkValues {
			pkValue.EncodeASCII(buf)
			buf.WriteString(" ")
		}
		buf.WriteString(")")
	}
}

func applyFilterWithPKDefaults(table *schema.Table, columnNumbers []int, input []sqltypes.Value) (output []sqltypes.Value) {
	output = make([]sqltypes.Value, len(columnNumbers))
	for colIndex, colPointer := range columnNumbers {
		if colPointer >= 0 {
			output[colIndex] = input[colPointer]
		} else {
			output[colIndex] = table.GetPKColumn(colIndex).Default
		}
	}
	return output
}

// unicoded returns a valid UTF-8 string that json won't reject
func unicoded(in string) (out string) {
	for i, v := range in {
		if v == 0xFFFD {
			return in[:i]
		}
	}
	return in
}
