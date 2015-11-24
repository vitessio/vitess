// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"bytes"
	"fmt"

	"github.com/youtube/vitess/go/sqltypes"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/sqlparser"
)

// buildValueList builds the set of PK reference rows used to drive the next query.
// It uses the PK values supplied in the original query and bind variables.
// The generated reference rows are validated for type match against the PK of the table.
func buildValueList(tableInfo *TableInfo, pkValues []interface{}, bindVars map[string]interface{}) ([][]sqltypes.Value, error) {
	resolved, length, err := resolvePKValues(tableInfo, pkValues, bindVars)
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

func resolvePKValues(tableInfo *TableInfo, pkValues []interface{}, bindVars map[string]interface{}) (resolved []interface{}, length int, err error) {
	length = -1
	setLengthFunc := func(list []sqltypes.Value) error {
		if length == -1 {
			length = len(list)
		} else if len(list) != length {
			return NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "mismatched lengths for values %v", pkValues)
		}
		return nil
	}
	resolved = make([]interface{}, len(pkValues))
	for i, val := range pkValues {
		switch val := val.(type) {
		case string:
			if val[1] != ':' {
				resolved[i], err = resolveValue(tableInfo.GetPKColumn(i), val, bindVars)
				if err != nil {
					return nil, 0, err
				}
			} else {
				list, err := resolveListArg(tableInfo.GetPKColumn(i), val, bindVars)
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
				list[j], err = resolveValue(tableInfo.GetPKColumn(i), listVal, bindVars)
				if err != nil {
					return nil, 0, err
				}
			}
			if err := setLengthFunc(list); err != nil {
				return nil, 0, err
			}
			resolved[i] = list
		default:
			resolved[i], err = resolveValue(tableInfo.GetPKColumn(i), val, nil)
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
		return nil, NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "%v", err)
	}
	list := val.([]interface{})
	resolved := make([]sqltypes.Value, len(list))
	for i, v := range list {
		sqlval, err := sqltypes.BuildValue(v)
		if err != nil {
			return nil, NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "%v", err)
		}
		if err = validateValue(col, sqlval); err != nil {
			return nil, err
		}
		resolved[i] = sqlval
	}
	return resolved, nil
}

// buildSecondaryList is used for handling ON DUPLICATE DMLs, or those that change the PK.
func buildSecondaryList(tableInfo *TableInfo, pkList [][]sqltypes.Value, secondaryList []interface{}, bindVars map[string]interface{}) ([][]sqltypes.Value, error) {
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
				if valueList[i][j], err = resolveValue(tableInfo.GetPKColumn(j), secondaryList[j], bindVars); err != nil {
					return valueList, err
				}
			}
		}
	}
	return valueList, nil
}

func resolveValue(col *schema.TableColumn, value interface{}, bindVars map[string]interface{}) (result sqltypes.Value, err error) {
	switch v := value.(type) {
	case string:
		val, _, err := sqlparser.FetchBindVar(v, bindVars)
		if err != nil {
			return result, NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "%v", err)
		}
		sqlval, err := sqltypes.BuildValue(val)
		if err != nil {
			return result, NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "%v", err)
		}
		result = sqlval
	case sqltypes.Value:
		result = v
	default:
		return result, NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "incompatible value type %v", v)
	}

	if err = validateValue(col, result); err != nil {
		return result, err
	}
	return result, nil
}

func validateRow(tableInfo *TableInfo, columnNumbers []int, row []sqltypes.Value) error {
	if len(row) != len(columnNumbers) {
		return NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "data inconsistency %d vs %d", len(row), len(columnNumbers))
	}
	for j, value := range row {
		if err := validateValue(&tableInfo.Columns[columnNumbers[j]], value); err != nil {
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
			return NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "type mismatch, expecting numeric type for %v for column: %v", value, col)
		}
	} else if col.Type == sqltypes.VarBinary {
		if !value.IsQuoted() {
			return NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "type mismatch, expecting string type for %v for column: %v", value, col)
		}
	}
	return nil
}

// getLimit resolves the rowcount or offset of the limit clause value.
// It returns -1 if it's not set.
func getLimit(limit interface{}, bv map[string]interface{}) (int64, error) {
	switch lim := limit.(type) {
	case string:
		lookup, ok := bv[lim[1:]]
		if !ok {
			return -1, NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "missing bind var %s", lim)
		}
		var newlim int64
		switch l := lookup.(type) {
		case int64:
			newlim = l
		case int32:
			newlim = int64(l)
		case int:
			newlim = int64(l)
		default:
			return -1, NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "want number type for %s, got %T", lim, lookup)
		}
		if newlim < 0 {
			return -1, NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "negative limit %d", newlim)
		}
		return newlim, nil
	case int64:
		return lim, nil
	default:
		return -1, nil
	}
}

func buildKey(row []sqltypes.Value) (key string) {
	buf := &bytes.Buffer{}
	for i, pkValue := range row {
		if pkValue.IsNull() {
			return ""
		}
		pkValue.EncodeASCII(buf)
		if i != len(row)-1 {
			buf.WriteByte('.')
		}
	}
	return buf.String()
}

func buildStreamComment(tableInfo *TableInfo, pkValueList [][]sqltypes.Value, secondaryList [][]sqltypes.Value) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 256))
	fmt.Fprintf(buf, " /* _stream %s (", tableInfo.Name)
	// We assume the first index exists, and is the pk
	for _, pkName := range tableInfo.Indexes[0].Columns {
		buf.WriteString(pkName)
		buf.WriteString(" ")
	}
	buf.WriteString(")")
	buildPKValueList(buf, tableInfo, pkValueList)
	buildPKValueList(buf, tableInfo, secondaryList)
	buf.WriteString("; */")
	return buf.Bytes()
}

func buildPKValueList(buf *bytes.Buffer, tableInfo *TableInfo, pkValueList [][]sqltypes.Value) {
	for _, pkValues := range pkValueList {
		buf.WriteString(" (")
		for _, pkValue := range pkValues {
			pkValue.EncodeASCII(buf)
			buf.WriteString(" ")
		}
		buf.WriteString(")")
	}
}

func applyFilter(columnNumbers []int, input []sqltypes.Value) (output []sqltypes.Value) {
	output = make([]sqltypes.Value, len(columnNumbers))
	for colIndex, colPointer := range columnNumbers {
		if colPointer >= 0 {
			output[colIndex] = input[colPointer]
		}
	}
	return output
}

func applyFilterWithPKDefaults(tableInfo *TableInfo, columnNumbers []int, input []sqltypes.Value) (output []sqltypes.Value) {
	output = make([]sqltypes.Value, len(columnNumbers))
	for colIndex, colPointer := range columnNumbers {
		if colPointer >= 0 {
			output[colIndex] = input[colPointer]
		} else {
			output[colIndex] = tableInfo.GetPKColumn(colIndex).Default
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
