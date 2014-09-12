// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strings"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/schema"
)

// buildValueList builds the set of PK reference rows used to drive the next query.
// It uses the PK values supplied in the original query and bind variables.
// The generated reference rows are validated for type match against the PK of the table.
func buildValueList(tableInfo *TableInfo, pkValues []interface{}, bindVars map[string]interface{}) ([][]sqltypes.Value, error) {
	length := -1
	for _, pkValue := range pkValues {
		if list, ok := pkValue.([]interface{}); ok {
			if length == -1 {
				if length = len(list); length == 0 {
					panic(fmt.Sprintf("empty list for values %v", pkValues))
				}
			} else if length != len(list) {
				panic(fmt.Sprintf("mismatched lengths for values %v", pkValues))
			}
		}
	}
	if length == -1 {
		length = 1
	}
	valueList := make([][]sqltypes.Value, length)
	for i := 0; i < length; i++ {
		valueList[i] = make([]sqltypes.Value, len(pkValues))
		for j, pkValue := range pkValues {
			var value interface{}
			if list, ok := pkValue.([]interface{}); ok {
				value = list[i]
			} else {
				value = pkValue
			}
			var err error
			if valueList[i][j], err = resolveValue(tableInfo.GetPKColumn(j), value, bindVars); err != nil {
				return valueList, err
			}
		}
	}
	return valueList, nil
}

// buildINValueList builds the set of PK reference rows used to drive the next query
// using an IN clause. This works only for tables with no composite PK columns.
// The generated reference rows are validated for type match against the PK of the table.
func buildINValueList(tableInfo *TableInfo, pkValues []interface{}, bindVars map[string]interface{}) ([][]sqltypes.Value, error) {
	if len(tableInfo.PKColumns) != 1 {
		panic(fmt.Sprintf("buildINValueList not allowed on composite PK table: %v", tableInfo.Name))
	}

	valueList := make([][]sqltypes.Value, len(pkValues))
	for i, pkValue := range pkValues {
		valueList[i] = make([]sqltypes.Value, 1)
		var err error
		if valueList[i][0], err = resolveValue(tableInfo.GetPKColumn(0), pkValue, bindVars); err != nil {
			return valueList, err
		}
	}
	return valueList, nil
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
		lookup, ok := bindVars[v[1:]]
		if !ok {
			return result, NewTabletError(FAIL, "Missing bind var %s", v)
		}
		if sqlval, err := sqltypes.BuildValue(lookup); err != nil {
			return result, NewTabletError(FAIL, err.Error())
		} else {
			result = sqlval
		}
	case sqltypes.Value:
		result = v
	case nil:
		// no op
	default:
		panic(fmt.Sprintf("incompatible value type %v", v))
	}

	if err = validateValue(col, result); err != nil {
		return result, err
	}
	return result, nil
}

func validateRow(tableInfo *TableInfo, columnNumbers []int, row []sqltypes.Value) error {
	if len(row) != len(columnNumbers) {
		return NewTabletError(FAIL, "data inconsistency %d vs %d", len(row), len(columnNumbers))
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
	switch col.Category {
	case schema.CAT_NUMBER:
		if !value.IsNumeric() {
			return NewTabletError(FAIL, "type mismatch, expecting numeric type for %v", value)
		}
	case schema.CAT_VARBINARY:
		if !value.IsString() {
			return NewTabletError(FAIL, "type mismatch, expecting string type for %v", value)
		}
	}
	return nil
}

func buildKey(row []sqltypes.Value) (key string) {
	buf := bytes.NewBuffer(make([]byte, 0, 32))
	for i, pkValue := range row {
		if pkValue.IsNull() {
			return ""
		}
		pkValue.EncodeAscii(buf)
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
			pkValue.EncodeAscii(buf)
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

func validateKey(tableInfo *TableInfo, key string) (newKey string) {
	if key == "" {
		// TODO: Verify auto-increment table
		return
	}
	pieces := strings.Split(key, ".")
	if len(pieces) != len(tableInfo.PKColumns) {
		// TODO: Verify auto-increment table
		return ""
	}
	pkValues := make([]sqltypes.Value, len(tableInfo.PKColumns))
	for i, piece := range pieces {
		if piece[0] == '\'' {
			s, err := base64.StdEncoding.DecodeString(piece[1 : len(piece)-1])
			if err != nil {
				log.Warningf("Error decoding key %s for table %s: %v", key, tableInfo.Name, err)
				internalErrors.Add("Mismatch", 1)
				return
			}
			pkValues[i] = sqltypes.MakeString(s)
		} else if piece == "null" {
			// TODO: Verify auto-increment table
			return ""
		} else {
			n, err := sqltypes.BuildNumeric(piece)
			if err != nil {
				log.Warningf("Error decoding key %s for table %s: %v", key, tableInfo.Name, err)
				internalErrors.Add("Mismatch", 1)
				return
			}
			pkValues[i] = n
		}
	}
	if newKey = buildKey(pkValues); newKey != key {
		log.Warningf("Error: Key mismatch, received: %s, computed: %s", key, newKey)
		internalErrors.Add("Mismatch", 1)
	}
	return newKey
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
