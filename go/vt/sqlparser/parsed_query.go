// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/youtube/vitess/go/sqltypes"
)

type bindLocation struct {
	offset, length int
}

type ParsedQuery struct {
	Query         string
	bindLocations []bindLocation
}

type EncoderFunc func(value interface{}) ([]byte, error)

func (pq *ParsedQuery) GenerateQuery(bindVariables map[string]interface{}, listVariables []sqltypes.Value) ([]byte, error) {
	if len(pq.bindLocations) == 0 {
		return []byte(pq.Query), nil
	}
	buf := bytes.NewBuffer(make([]byte, 0, len(pq.Query)))
	current := 0
	for _, loc := range pq.bindLocations {
		buf.WriteString(pq.Query[current:loc.offset])
		varName := pq.Query[loc.offset+1 : loc.offset+loc.length]
		var supplied interface{}
		if varName[0] >= '0' && varName[0] <= '9' {
			index, err := strconv.Atoi(varName)
			if err != nil {
				return nil, fmt.Errorf("unexpected: %v for %s", err, varName)
			}
			if index >= len(listVariables) {
				return nil, fmt.Errorf("index out of range: %d", index)
			}
			supplied = listVariables[index]
		} else if varName[0] == '*' {
			supplied = listVariables
		} else {
			var ok bool
			supplied, ok = bindVariables[varName]
			if !ok {
				return nil, fmt.Errorf("missing bind var %s", varName)
			}
		}
		if err := EncodeValue(buf, supplied); err != nil {
			return nil, err
		}
		current = loc.offset + loc.length
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
	case TupleEqualityList:
		if err := bindVal.Encode(buf); err != nil {
			return err
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

type TupleEqualityList struct {
	Columns []string
	Rows    [][]sqltypes.Value
}

func (tpl *TupleEqualityList) Encode(buf *bytes.Buffer) error {
	if len(tpl.Rows) == 0 {
		return errors.New("cannot encode with 0 rows")
	}
	if len(tpl.Columns) == 1 {
		return tpl.encodeAsIN(buf)
	}
	return tpl.encodeAsEquality(buf)
}

func (tpl *TupleEqualityList) encodeAsIN(buf *bytes.Buffer) error {
	buf.WriteString(tpl.Columns[0])
	buf.WriteString(" in (")
	for i, r := range tpl.Rows {
		if len(r) != 1 {
			return errors.New("values don't match column count")
		}
		if i != 0 {
			buf.WriteString(", ")
		}
		if err := EncodeValue(buf, r); err != nil {
			return err
		}
	}
	buf.WriteByte(')')
	return nil
}

func (tpl *TupleEqualityList) encodeAsEquality(buf *bytes.Buffer) error {
	for i, r := range tpl.Rows {
		if i != 0 {
			buf.WriteString(" or ")
		}
		tpl.encodeLHS(buf)
		buf.WriteString(" = (")
		if err := EncodeValue(buf, r); err != nil {
			return err
		}
		buf.WriteByte(')')
	}
	return nil
}

func (tpl *TupleEqualityList) encodeLHS(buf *bytes.Buffer) {
	buf.WriteByte('(')
	for i, c := range tpl.Columns {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(c)
	}
	buf.WriteByte(')')
}
