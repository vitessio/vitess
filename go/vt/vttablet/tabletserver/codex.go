// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema"
	"github.com/youtube/vitess/go/vt/vterrors"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// buildValueList builds the set of PK reference rows used to drive the next query.
// It uses the PK values supplied in the original query and bind variables.
// The generated reference rows are validated for type match against the PK of the table.
func buildValueList(table *schema.Table, pkValues []interface{}, bindVars map[string]interface{}) ([][]sqltypes.Value, error) {
	resolved, length, err := resolvePKValues(table, pkValues, bindVars)
	if err != nil {
		return nil, err
	}
	valueList := make([][]sqltypes.Value, length)
	for i := 0; i < length; i++ {
		valueList[i] = make([]sqltypes.Value, len(resolved))
		for j, val := range resolved {
			if list, ok := val.([]sqltypes.Value); ok {
				valueList[i][j] = list[i]
			} else {
				valueList[i][j] = val.(sqltypes.Value)
			}
		}
	}
	return valueList, nil
}

func resolvePKValues(table *schema.Table, pkValues []interface{}, bindVars map[string]interface{}) (resolved []interface{}, length int, err error) {
	length = -1
	setLengthFunc := func(list []sqltypes.Value) error {
		if length == -1 {
			length = len(list)
		} else if len(list) != length {
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "mismatched lengths for values %v", pkValues)
		}
		return nil
	}
	resolved = make([]interface{}, len(pkValues))
	for i, val := range pkValues {
		switch val := val.(type) {
		case string:
			if val[1] != ':' {
				resolved[i], err = resolveValue(table.GetPKColumn(i), val, bindVars)
				if err != nil {
					return nil, 0, err
				}
			} else {
				list, err := resolveListArg(table.GetPKColumn(i), val, bindVars)
				if err != nil {
					return nil, 0, err
				}
				if err := setLengthFunc(list); err != nil {
					return nil, 0, err
				}
				resolved[i] = list
			}
		case []interface{}:
			list := make([]sqltypes.Value, len(val))
			for j, listVal := range val {
				list[j], err = resolveValue(table.GetPKColumn(i), listVal, bindVars)
				if err != nil {
					return nil, 0, err
				}
			}
			if err := setLengthFunc(list); err != nil {
				return nil, 0, err
			}
			resolved[i] = list
		default:
			resolved[i], err = resolveValue(table.GetPKColumn(i), val, nil)
			if err != nil {
				return nil, 0, err
			}
		}
	}
	if length == -1 {
		length = 1
	}
	return resolved, length, nil
}

func resolveListArg(col *schema.TableColumn, key string, bindVars map[string]interface{}) ([]sqltypes.Value, error) {
	val, _, err := sqlparser.FetchBindVar(key, bindVars)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
	}

	switch list := val.(type) {
	case []interface{}:
		resolved := make([]sqltypes.Value, len(list))
		for i, v := range list {
			sqlval, err := sqltypes.BuildConverted(col.Type, v)
			if err != nil {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
			}
			if err = validateValue(col, sqlval); err != nil {
				return nil, err
			}
			resolved[i] = sqlval
		}
		return resolved, nil
	case *querypb.BindVariable:
		if list.Type != querypb.Type_TUPLE {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "expecting list for bind var %s: %v", key, list)
		}
		resolved := make([]sqltypes.Value, len(list.Values))
		for i, v := range list.Values {
			// We can use MakeTrusted as BuildConverted will check the value.
			sqlval := sqltypes.MakeTrusted(v.Type, v.Value)
			sqlval, err := sqltypes.BuildConverted(col.Type, sqlval)
			if err != nil {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
			}
			if err = validateValue(col, sqlval); err != nil {
				return nil, err
			}
			resolved[i] = sqlval
		}
		return resolved, nil
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unknown type for bind variable %v", key)
	}
}

// buildSecondaryList is used for handling ON DUPLICATE DMLs, or those that change the PK.
func buildSecondaryList(table *schema.Table, pkList [][]sqltypes.Value, secondaryList []interface{}, bindVars map[string]interface{}) ([][]sqltypes.Value, error) {
	if secondaryList == nil {
		return nil, nil
	}
	valueList := make([][]sqltypes.Value, len(pkList))
	for i, row := range pkList {
		valueList[i] = make([]sqltypes.Value, len(row))
		for j, cell := range row {
			if secondaryList[j] == nil {
				valueList[i][j] = cell
			} else {
				var err error
				if valueList[i][j], err = resolveValue(table.GetPKColumn(j), secondaryList[j], bindVars); err != nil {
					return valueList, err
				}
			}
		}
	}
	return valueList, nil
}

func resolveValue(col *schema.TableColumn, value interface{}, bindVars map[string]interface{}) (result sqltypes.Value, err error) {
	if v, ok := value.(string); ok {
		value, _, err = sqlparser.FetchBindVar(v, bindVars)
		if err != nil {
			return result, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
		}
	}
	result, err = sqltypes.BuildConverted(col.Type, value)
	if err != nil {
		return result, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
	}
	if err = validateValue(col, result); err != nil {
		return result, err
	}
	return result, nil
}

// resolveNumber extracts a number from a bind variable or sql value.
func resolveNumber(value interface{}, bindVars map[string]interface{}) (int64, error) {
	var err error
	if v, ok := value.(string); ok {
		value, _, err = sqlparser.FetchBindVar(v, bindVars)
		if err != nil {
			return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
		}
	}
	v, err := sqltypes.BuildValue(value)
	if err != nil {
		return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
	}
	ret, err := v.ParseInt64()
	if err != nil {
		return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v", err)
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

func buildStreamComment(table *schema.Table, pkValueList [][]sqltypes.Value, secondaryList [][]sqltypes.Value) []byte {
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
	return buf.Bytes()
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
