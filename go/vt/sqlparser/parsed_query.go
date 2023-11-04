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

package sqlparser

import (
	"encoding/json"
	"fmt"
	"strings"

	"vitess.io/vitess/go/bytes2"
	vjson "vitess.io/vitess/go/mysql/json"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// ParsedQuery represents a parsed query where
// bind locations are precomputed for fast substitutions.
type ParsedQuery struct {
	Query         string
	bindLocations []bindLocation
}

type bindLocation struct {
	offset, length int
}

// NewParsedQuery returns a ParsedQuery of the ast.
func NewParsedQuery(node SQLNode) *ParsedQuery {
	buf := NewTrackedBuffer(nil)
	buf.Myprintf("%v", node)
	return buf.ParsedQuery()
}

// GenerateQuery generates a query by substituting the specified
// bindVariables. The extras parameter specifies special parameters
// that can perform custom encoding.
func (pq *ParsedQuery) GenerateQuery(bindVariables map[string]*querypb.BindVariable, extras map[string]Encodable) (string, error) {
	if len(pq.bindLocations) == 0 {
		return pq.Query, nil
	}
	var buf strings.Builder
	buf.Grow(len(pq.Query))
	if err := pq.Append(&buf, bindVariables, extras); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// Append appends the generated query to the provided buffer.
func (pq *ParsedQuery) Append(buf *strings.Builder, bindVariables map[string]*querypb.BindVariable, extras map[string]Encodable) error {
	current := 0
	for _, loc := range pq.bindLocations {
		buf.WriteString(pq.Query[current:loc.offset])
		name := pq.Query[loc.offset : loc.offset+loc.length]
		if encodable, ok := extras[name[1:]]; ok {
			encodable.EncodeSQL(buf)
		} else {
			supplied, _, err := FetchBindVar(name, bindVariables)
			if err != nil {
				return err
			}
			EncodeValue(buf, supplied)
		}
		current = loc.offset + loc.length
	}
	buf.WriteString(pq.Query[current:])
	return nil
}

// AppendFromRow behaves like Append but takes a querypb.Row directly, assuming that
// the fields in the row are in the same order as the placeholders in this query. The fields might include generated
// columns which are dropped, by checking against skipFields, before binding the variables
// note: there can be more fields than bind locations since extra columns might be requested from the source if not all
// primary keys columns are present in the target table, for example. Also some values in the row may not correspond for
// values from the database on the source: sum/count for aggregation queries, for example
func (pq *ParsedQuery) AppendFromRow(buf *bytes2.Buffer, fields []*querypb.Field, row *querypb.Row, skipFields map[string]bool) error {
	if len(fields) < len(pq.bindLocations) {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "wrong number of fields: got %d fields for %d bind locations ",
			len(fields), len(pq.bindLocations))
	}

	type colInfo struct {
		typ    querypb.Type
		length int64
		offset int64
	}
	rowInfo := make([]*colInfo, 0)

	offset := int64(0)
	for i, field := range fields { // collect info required for fields to be bound
		length := row.Lengths[i]
		if !skipFields[strings.ToLower(field.Name)] {
			rowInfo = append(rowInfo, &colInfo{
				typ:    field.Type,
				length: length,
				offset: offset,
			})
		}
		if length > 0 {
			offset += row.Lengths[i]
		}
	}

	// bind field values to locations
	var offsetQuery int
	for i, loc := range pq.bindLocations {
		col := rowInfo[i]
		buf.WriteString(pq.Query[offsetQuery:loc.offset])
		typ := col.typ

		switch typ {
		case querypb.Type_TUPLE:
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected Type_TUPLE for value %d", i)
		case querypb.Type_JSON:
			if col.length < 0 { // An SQL NULL and not an actual JSON value
				buf.WriteString(sqltypes.NullStr)
			} else { // A JSON value (which may be a JSON null literal value)
				buf2 := row.Values[col.offset : col.offset+col.length]
				vv, err := vjson.MarshalSQLValue(buf2)
				if err != nil {
					return err
				}
				buf.WriteString(vv.RawStr())
			}
		default:
			if col.length < 0 {
				// -1 means a null variable; serialize it directly
				buf.WriteString(sqltypes.NullStr)
			} else {
				vv := sqltypes.MakeTrusted(typ, row.Values[col.offset:col.offset+col.length])
				vv.EncodeSQLBytes2(buf)
			}
		}
		offsetQuery = loc.offset + loc.length
	}
	buf.WriteString(pq.Query[offsetQuery:])
	return nil
}

// MarshalJSON is a custom JSON marshaler for ParsedQuery.
// Note that any queries longer that 512 bytes will be truncated.
func (pq *ParsedQuery) MarshalJSON() ([]byte, error) {
	return json.Marshal(TruncateForUI(pq.Query))
}

// EncodeValue encodes one bind variable value into the query.
func EncodeValue(buf *strings.Builder, value *querypb.BindVariable) {
	switch value.Type {
	case querypb.Type_TUPLE:
		buf.WriteByte('(')
		for i, bv := range value.Values {
			if i != 0 {
				buf.WriteString(", ")
			}
			sqltypes.ProtoToValue(bv).EncodeSQLStringBuilder(buf)
		}
		buf.WriteByte(')')
	case querypb.Type_JSON:
		v, _ := sqltypes.BindVariableToValue(value)
		buf.Write(v.Raw())
	default:
		v, _ := sqltypes.BindVariableToValue(value)
		v.EncodeSQLStringBuilder(buf)
	}
}

// FetchBindVar resolves the bind variable by fetching it from bindVariables.
func FetchBindVar(name string, bindVariables map[string]*querypb.BindVariable) (val *querypb.BindVariable, isList bool, err error) {
	name = name[1:]
	if name[0] == ':' {
		name = name[1:]
		isList = true
	}
	supplied, ok := bindVariables[name]
	if !ok {
		return nil, false, fmt.Errorf("missing bind var %s", name)
	}

	if isList {
		if supplied.Type != querypb.Type_TUPLE {
			return nil, false, fmt.Errorf("unexpected list arg type (%v) for key %s", supplied.Type, name)
		}
		if len(supplied.Values) == 0 {
			return nil, false, fmt.Errorf("empty list supplied for %s", name)
		}
		return supplied, true, nil
	}

	if supplied.Type == querypb.Type_TUPLE {
		return nil, false, fmt.Errorf("unexpected arg type (TUPLE) for non-list key %s", name)
	}

	return supplied, false, nil
}

// ParseAndBind is a one step sweep that binds variables to an input query, in order of placeholders.
// It is useful when one doesn't have any parser-variables, just bind variables.
// Example:
//
//	query, err := ParseAndBind("select * from tbl where name=%a", sqltypes.StringBindVariable("it's me"))
func ParseAndBind(in string, binds ...*querypb.BindVariable) (query string, err error) {
	vars := make([]any, len(binds))
	for i, bv := range binds {
		switch bv.Type {
		case querypb.Type_TUPLE:
			vars[i] = fmt.Sprintf("::vars%d", i)
		default:
			vars[i] = fmt.Sprintf(":var%d", i)
		}
	}
	parsed := BuildParsedQuery(in, vars...)

	bindVars := map[string]*querypb.BindVariable{}
	for i, bv := range binds {
		switch bv.Type {
		case querypb.Type_TUPLE:
			bindVars[fmt.Sprintf("vars%d", i)] = binds[i]
		default:
			bindVars[fmt.Sprintf("var%d", i)] = binds[i]
		}
	}
	return parsed.GenerateQuery(bindVars, nil)
}
