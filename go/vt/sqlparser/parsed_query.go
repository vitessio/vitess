// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/youtube/vitess/go/sqltypes"
)

type bindLocation struct {
	offset, length int
}

// ParsedQuery represents a parsed query where
// bind locations are precompued for fast substitutions.
type ParsedQuery struct {
	Query         string
	bindLocations []bindLocation
}

// GenerateQuery generates a query by substituting the specified
// bindVariables.
func (pq *ParsedQuery) GenerateQuery(bindVariables map[string]interface{}) ([]byte, error) {
	if len(pq.bindLocations) == 0 {
		return []byte(pq.Query), nil
	}
	buf := bytes.NewBuffer(make([]byte, 0, len(pq.Query)))
	current := 0
	for _, loc := range pq.bindLocations {
		buf.WriteString(pq.Query[current:loc.offset])
		name := pq.Query[loc.offset : loc.offset+loc.length]
		supplied, _, err := FetchBindVar(name, bindVariables)
		if err != nil {
			return nil, err
		}
		if err := EncodeValue(buf, supplied); err != nil {
			return nil, err
		}
		current = loc.offset + loc.length
	}
	buf.WriteString(pq.Query[current:])
	return buf.Bytes(), nil
}

// MarshalJSON is a custom JSON marshaler for ParsedQuery.
func (pq *ParsedQuery) MarshalJSON() ([]byte, error) {
	return json.Marshal(pq.Query)
}

// EncodeValue encodes one bind variable value into the query.
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
	case []interface{}:
		buf.WriteByte('(')
		for i, v := range bindVal {
			if i != 0 {
				buf.WriteString(", ")
			}
			if err := EncodeValue(buf, v); err != nil {
				return err
			}
		}
		buf.WriteByte(')')
	case TupleEqualityList:
		if err := bindVal.Encode(buf); err != nil {
			return err
		}
	default:
		v, err := sqltypes.BuildValue(bindVal)
		if err != nil {
			return err
		}
		v.EncodeSQL(buf)
	}
	return nil
}

// TupleEqualityList is for generating equality constraints
// for tables that have composite primary keys.
type TupleEqualityList struct {
	Columns []string
	Rows    [][]sqltypes.Value
}

// Encode generates the where clause constraints for the tuple
// equality.
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
		buf.WriteString("(")
		for j, c := range tpl.Columns {
			if j != 0 {
				buf.WriteString(" and ")
			}
			buf.WriteString(c)
			buf.WriteString(" = ")
			if err := EncodeValue(buf, r[j]); err != nil {
				return err
			}
		}
		buf.WriteByte(')')
	}
	return nil
}

// FetchBindVar resolves the bind variable by fetching it from bindVariables.
func FetchBindVar(name string, bindVariables map[string]interface{}) (val interface{}, isList bool, err error) {
	name = name[1:]
	if name[0] == ':' {
		name = name[1:]
		isList = true
	}
	supplied, ok := bindVariables[name]
	if !ok {
		return nil, false, fmt.Errorf("missing bind var %s", name)
	}
	list, gotList := supplied.([]interface{})
	if isList {
		if !gotList {
			return nil, false, fmt.Errorf("unexpected list arg type %T for key %s", supplied, name)
		}
		if len(list) == 0 {
			return nil, false, fmt.Errorf("empty list supplied for %s", name)
		}
		return list, true, nil
	}
	if gotList {
		return nil, false, fmt.Errorf("unexpected arg type %T for key %s", supplied, name)
	}
	return supplied, false, nil
}
