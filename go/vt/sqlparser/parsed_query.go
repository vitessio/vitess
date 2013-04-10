// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import (
	"bytes"
	"encoding/json"
	"strconv"

	"code.google.com/p/vitess/go/sqltypes"
)

type BindLocation struct {
	Offset, Length int
}

type ParsedQuery struct {
	Query         string
	BindLocations []BindLocation
}

type EncoderFunc func(value interface{}) ([]byte, error)

func (pq *ParsedQuery) GenerateQuery(bindVariables map[string]interface{}, listVariables []sqltypes.Value) ([]byte, error) {
	if len(pq.BindLocations) == 0 {
		return []byte(pq.Query), nil
	}
	buf := bytes.NewBuffer(make([]byte, 0, len(pq.Query)))
	current := 0
	for _, loc := range pq.BindLocations {
		buf.WriteString(pq.Query[current:loc.Offset])
		varName := pq.Query[loc.Offset+1 : loc.Offset+loc.Length]
		var supplied interface{}
		if varName[0] >= '0' && varName[0] <= '9' {
			index, err := strconv.Atoi(varName)
			if err != nil {
				return nil, NewParserError("Unexpected: %v for %s", err, varName)
			}
			if index >= len(listVariables) {
				return nil, NewParserError("Index out of range: %d", index)
			}
			supplied = listVariables[index]
		} else {
			var ok bool
			supplied, ok = bindVariables[varName]
			if !ok {
				return nil, NewParserError("Missing bind var %s", varName)
			}
		}
		if err := EncodeValue(buf, supplied); err != nil {
			return nil, err
		}
		current = loc.Offset + loc.Length
	}
	buf.WriteString(pq.Query[current:])
	return buf.Bytes(), nil
}

func (pq *ParsedQuery) MarshalJSON() ([]byte, error) {
	return json.Marshal(pq.Query)
}

func EncodeValue(buf *bytes.Buffer, value interface{}) error {
	switch bindVal := value.(type) {
	case nil:
		buf.WriteString("null")
	case []sqltypes.Value:
		for i := 0; i < len(bindVal); i++ {
			if i != 0 {
				buf.WriteString(", ")
			}
			if err := EncodeValue(buf, bindVal[i]); err != nil {
				return err
			}
		}
	case [][]sqltypes.Value:
		for i := 0; i < len(bindVal); i++ {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteByte('(')
			if err := EncodeValue(buf, bindVal[i]); err != nil {
				return err
			}
			buf.WriteByte(')')
		}
	default:
		v, err := sqltypes.BuildValue(bindVal)
		if err != nil {
			return err
		}
		v.EncodeSql(buf)
	}
	return nil
}
