// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"bytes"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/vt/schema"
	"code.google.com/p/vitess/go/vt/sqlparser"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
)

func buildValueList(pkValues []interface{}, bindVars map[string]interface{}) [][]interface{} {
	length := -1
	for _, pkValue := range pkValues {
		if list, ok := pkValue.([]interface{}); ok {
			if length == -1 {
				if length = len(list); length == 0 {
					panic(NewTabletError(FAIL, "empty list for values %v", pkValues))
				}
			} else if length != len(list) {
				panic(NewTabletError(FAIL, "mismatched lengths for values %v", pkValues))
			}
		}
	}
	if length == -1 {
		length = 1
	}
	valueList := make([][]interface{}, length)
	for i := 0; i < length; i++ {
		valueList[i] = make([]interface{}, len(pkValues))
		for j, pkValue := range pkValues {
			if list, ok := pkValue.([]interface{}); ok {
				valueList[i][j] = resolveValue(list[i], bindVars)
			} else {
				valueList[i][j] = resolveValue(pkValue, bindVars)
			}
		}
	}
	return valueList
}

func buildSecondaryList(pkList [][]interface{}, secondaryList []interface{}, bindVars map[string]interface{}) [][]interface{} {
	if secondaryList == nil {
		return nil
	}
	valueList := make([][]interface{}, len(pkList))
	for i, row := range pkList {
		valueList[i] = make([]interface{}, len(row))
		for j, cell := range row {
			if secondaryList[j] == nil {
				valueList[i][j] = cell
			} else {
				valueList[i][j] = resolveValue(secondaryList[j], bindVars)
			}
		}
	}
	return valueList
}

func resolveValue(value interface{}, bindVars map[string]interface{}) interface{} {
	if v, ok := value.(string); ok {
		if v[0] == ':' {
			lookup, ok := bindVars[v[1:]]
			if !ok {
				panic(NewTabletError(FAIL, "No bind var found for %s", v))
			}
			return lookup
		}
	}
	return value
}

func copyRows(rows [][]interface{}) (result [][]interface{}) {
	result = make([][]interface{}, len(rows))
	for i, row := range rows {
		result[i] = make([]interface{}, len(row))
		copy(result[i], row)
	}
	return result
}

func normalizePKRows(tableInfo *TableInfo, pkRows [][]interface{}) {
	normalizeRows(tableInfo, tableInfo.PKColumns, pkRows)
}

func normalizeRows(tableInfo *TableInfo, columnNumbers []int, rows [][]interface{}) {
	for _, row := range rows {
		if len(row) != len(columnNumbers) {
			panic(NewTabletError(FAIL, "data inconsistency %d vs %d", len(row), len(columnNumbers)))
		}
		for j, cell := range row {
			if tableInfo.Columns[columnNumbers[j]].Category == schema.CAT_NUMBER {
				switch val := cell.(type) {
				case string:
					row[j] = tonumber(val)
				case []byte:
					row[j] = tonumber(string(val))
				}
			}
		}
	}
}

func fillPKDefaults(tableInfo *TableInfo, pkRows [][]interface{}) {
	for _, row := range pkRows {
		for j, cell := range row {
			if tableInfo.Columns[tableInfo.PKColumns[j]].IsAuto {
				continue
			}
			if cell == nil {
				row[j] = tableInfo.Columns[tableInfo.PKColumns[j]].Default
			}
		}
	}
}

func buildKey(tableInfo *TableInfo, row []interface{}) (key string) {
	buf := bytes.NewBuffer(make([]byte, 0, 32))
	for i, pkValue := range row {
		if pkValue == nil { // Can happen for inserts with auto_increment columns
			return ""
		}
		encodePKValue(buf, pkValue, tableInfo.Columns[tableInfo.PKColumns[i]].Category)
		if i != len(row)-1 {
			buf.WriteByte('.')
		}
	}
	return buf.String()
}

func buildStreamComment(tableInfo *TableInfo, pkValueList [][]interface{}, secondaryList [][]interface{}) []byte {
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

func buildPKValueList(buf *bytes.Buffer, tableInfo *TableInfo, pkValueList [][]interface{}) {
	for _, pkValues := range pkValueList {
		buf.WriteString(" (")
		for j, pkValue := range pkValues {
			if pkValue == nil {
				buf.WriteString("null ")
				continue
			}
			encodePKValue(buf, pkValue, tableInfo.Columns[tableInfo.PKColumns[j]].Category)
			buf.WriteString(" ")
		}
		buf.WriteString(")")
	}
}

func encodePKValue(buf *bytes.Buffer, pkValue interface{}, category int) {
	switch category {
	case schema.CAT_NUMBER:
		switch val := pkValue.(type) {
		case int, int32, int64, uint, uint32, uint64:
			sqlparser.EncodeValue(buf, val)
		case string:
			sqlparser.EncodeValue(buf, tonumber(val))
		case []byte:
			sqlparser.EncodeValue(buf, tonumber(string(val)))
		default:
			panic(NewTabletError(FAIL, "Type %T disallowed for pk columns", val))
		}
	default:
		buf.WriteByte('\'')
		encoder := base64.NewEncoder(base64.StdEncoding, buf)
		switch val := pkValue.(type) {
		case string:
			encoder.Write([]byte(val))
		case []byte:
			encoder.Write(val)
		default:
			panic(NewTabletError(FAIL, "Type %T disallowed for non-number pk columns", val))
		}
		encoder.Close()
		buf.WriteByte('\'')
	}
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
	pkValues := make([]interface{}, len(tableInfo.PKColumns))
	for i, piece := range pieces {
		if piece[0] == '\'' {
			var err error
			pkValues[i], err = base64.StdEncoding.DecodeString(piece[1 : len(piece)-1])
			if err != nil {
				relog.Warning("Error decoding key %s for table %s: %v", key, tableInfo.Name, err)
				return
			}
			//pkValues[i] = piece[1 : len(piece)-1]
		} else if piece == "null" {
			// TODO: Verify auto-increment table
			return ""
		} else {
			pkValues[i] = piece
		}
	}
	if newKey = buildKey(tableInfo, pkValues); newKey != key {
		relog.Warning("Error: Key mismatch, received: %s, computed: %s", key, newKey)
	}
	return buildKey(tableInfo, pkValues)
}

// duplicated in multipe packages
func tonumber(val string) (number interface{}) {
	var err error
	if val[0] == '-' {
		number, err = strconv.ParseInt(val, 0, 64)
	} else {
		number, err = strconv.ParseUint(val, 0, 64)
	}
	if err != nil {
		panic(NewTabletError(FAIL, "%s", err))
	}
	return number
}

func base64Decode(b []byte) string {
	decodedKey := make([]byte, base64.StdEncoding.DecodedLen(len(b)))
	if _, err := base64.StdEncoding.Decode(decodedKey, b); err != nil {
		panic(NewTabletError(FAIL, "%s", err))
	}
	return string(decodedKey)
}
