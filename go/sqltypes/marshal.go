/*
Copyright 2023 The Vitess Authors.

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

package sqltypes

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/vt/vterrors"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/proto/vttime"
)

// ResultMarshaller knows how to marshal itself into a Result.
type ResultMarshaller interface {
	MarshalResult() (*Result, error)
}

// ValueMarshaller knows how to marshal itself into the bytes for a column of
// a particular type.
type ValueMarshaller interface {
	MarshalSQL(typ querypb.Type) ([]byte, error)
}

// ReplaceFields remaps the fields and/or row columns of a given result. This
// is useful when you need to embed a struct to modify its marshalling behavior,
// then cleanup or otherwise transfer the redunant fields.
// For example:
/*
| uuid | tablet | retries | migration_uuid | $$tablet  |
| abc  | ---    | 1       | abc            | zone1-101 |

=> becomes

| migration_uuid | tablet    | retries |
| abc            | zone1-101 | 1       |
*/
func ReplaceFields(result *Result, remap map[string]string) *Result {
	var (
		// orig maps fieldname => original col (field and row)
		orig = make(map[string]int, len(result.Fields))
		// fieldIdx maps final col (field) => fieldname
		fieldIdx = make([]string, len(result.Fields))
		// rowIdx maps final col (row) => fieldname
		rowIdx = make([]string, len(result.Fields))

		// inverseRemap is the inverse of the remapping, so we know also if a
		// field is the target of a rename
		inverseRemap = make(map[string]string, len(remap))
	)

	for i, field := range result.Fields {
		orig[field.Name] = i

		if n, ok := remap[field.Name]; ok {
			inverseRemap[n] = field.Name
		}
	}

	for i, field := range result.Fields {
		if _, ok := inverseRemap[field.Name]; ok {
			continue
		}

		if newName, ok := remap[field.Name]; ok {
			rowIdx[i] = newName
			rowIdx[orig[newName]] = field.Name

			if strings.HasPrefix(field.Name, "$$") {
				// Replace rows only; field stays unchanged.
				fieldIdx[i] = field.Name
				fieldIdx[orig[newName]] = newName
			} else {
				fieldIdx[i] = newName
				fieldIdx[orig[newName]] = field.Name
			}
		} else {
			fieldIdx[i] = field.Name
			rowIdx[i] = field.Name
		}
	}

	var fields []*querypb.Field
	for _, name := range fieldIdx {
		fields = append(fields, result.Fields[orig[name]])
	}

	fields = fields[:len(result.Fields)-len(remap)]

	var rows []Row
	for _, origRow := range result.Rows {
		var row []Value
		for _, name := range rowIdx {
			row = append(row, origRow[orig[name]])
		}

		rows = append(rows, row[:len(fields)])
	}

	return &Result{
		Fields: fields,
		Rows:   rows,
	}
}

// MarshalResult marshals the object into a Result object. It is semi-complete.
func MarshalResult(v any) (*Result, error) {
	if m, ok := v.(ResultMarshaller); ok {
		return m.MarshalResult()
	}

	val := reflect.ValueOf(v)
	if val.Type().Kind() != reflect.Slice {
		vals := reflect.Append(
			reflect.MakeSlice(reflect.SliceOf(val.Type()), 0, 1),
			val,
		)
		return MarshalResult(vals.Interface())
	}

	// Value of the slice element.
	// TODO: handle other cases; We're assuming it's a pointer to a struct
	elem := val.Type().Elem()
	elemType := elem.Elem()

	var (
		exportedStructFields []reflect.StructField
		fields               []*querypb.Field
		rows                 []Row
	)

	for _, field := range reflect.VisibleFields(elemType) {
		if !field.IsExported() {
			continue
		}

		// Anonymous fields are redundant. For example, consider the following:
		//
		//	type T1 struct { Foo string }
		//	type T2 struct { *T1; Bar string }
		//
		// If we did not skip Anonymous fields, marshalling T2 would result in
		// the following "fields":
		// 	| t1 | foo | bar |
		//
		// Skipping Anonymous fields results in the correct set:
		//	| foo | bar |
		//
		// From the VisibleFields documentation:
		//	> The returned fields include fields inside anonymous struct members
		//	> and unexported fields. They follow the same order found in the
		//	> struct, with anonymous fields followed immediately by their
		//	> promoted fields.
		if field.Anonymous {
			continue
		}

		exportedStructFields = append(exportedStructFields, field)
		sqlField, err := structToQueryField(field)
		if err != nil {
			return nil, err
		}
		fields = append(fields, sqlField)
	}

	for i := 0; i < val.Len(); i++ {
		// TODO: handle case where val is a slice of non-pointer objects.
		v := val.Index(i).Elem()
		row, err := marshalRow(v, fields, exportedStructFields)
		if err != nil {
			return nil, err
		}

		rows = append(rows, row)
	}

	return &Result{
		Fields: fields,
		Rows:   rows,
	}, nil
}

func marshalRow(val reflect.Value, sqlFields []*querypb.Field, structFields []reflect.StructField) (Row, error) {
	var row Row
	for i, structField := range structFields {
		var (
			sqlField = sqlFields[i]

			sqlVal Value
			err    error
		)
		if f := val.FieldByName(structField.Name); f.IsValid() {
			sqlVal, err = structToQueryValue(f.Interface(), structField, sqlField.Type)
			if err != nil {
				return nil, err
			}
		} else {
			sqlVal = NULL
		}

		row = append(row, sqlVal)
	}

	return row, nil
}

func structToQueryField(field reflect.StructField) (*querypb.Field, error) {
	name := field.Name
	parts := strings.SplitN(field.Tag.Get("sqltypes"), ",", 3)
	for len(parts) < 3 {
		parts = append(parts, "")
	}

	if parts[0] != "" {
		name = parts[0]
	}

	typ, err := fieldType(field)
	if err != nil {
		return nil, err
	}

	return &querypb.Field{
		Name: snakeCase(name),
		Type: typ,
	}, nil
}

func fieldType(field reflect.StructField) (querypb.Type, error) {
	var err error
	typeName := field.Type.String()
	switch field.Type.Kind() {
	case reflect.Pointer:
		ptr := field.Type.Elem()
		switch ptr.Kind() {
		case reflect.Struct:
			switch ptr.PkgPath() {
			case "vitess.io/vitess/go/vt/proto/vttime":
				switch ptr.Name() {
				case "Time":
					typeName = "timestamp"
				case "Duration":
					typeName = "varchar"
				default:
					// Impossible unless we add a new type to vttime.proto and
					// forget to update this function.
					err = vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unknown vttime proto message %s", ptr.Name())
				}
			case "time":
				switch ptr.Name() {
				case "Time":
					typeName = "timestamp"
				case "Duration":
					typeName = "varchar"
				default:
					err = vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unknown time type %s", ptr.Name())
				}
			}
		default:
			err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported pointer type %v", ptr.Kind())
		}
	case reflect.Struct:
		switch field.Type.PkgPath() {
		case "vitess.io/vitess/go/vt/proto/vttime":
			switch field.Type.Name() {
			case "Time":
				typeName = "timestamp"
			case "Duration":
				typeName = "varchar"
			default:
				// Impossible unless we add a new type to vttime.proto and
				// forget to update this function.
				err = vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unknown vttime proto message %s", field.Type.Name())
			}
		case "time":
			switch field.Type.Name() {
			case "Time":
				typeName = "timestamp"
			case "Duration":
				typeName = "varchar"
			default:
				err = vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unknown time type %s", field.Type.Name())
			}
		}
	case reflect.Int:
		typeName = "int64"
	case reflect.Uint:
		typeName = "uint64"
	case reflect.String:
		typeName = "varchar"
	case reflect.Slice:
		elem := field.Type.Elem()
		switch elem.Kind() {
		case reflect.Uint8:
			typeName = "varbinary"
		default:
			err = vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unsupported field type %v", field.Type.Kind())
		}
	}

	if err != nil {
		return 0, err
	}

	return querypb.Type(querypb.Type_value[strings.ToUpper(typeName)]), nil
}

func structToQueryValue(value any, field reflect.StructField, typ querypb.Type) (Value, error) {
	if v, ok := value.(ValueMarshaller); ok {
		col, err := v.MarshalSQL(typ)
		if err != nil {
			return Value{}, err
		}

		return MakeTrusted(typ, col), nil
	}

	switch typ {
	case querypb.Type_UINT8:
		if v, ok := value.(bool); ok {
			return NewBoolean(v), nil
		} else if v, ok := value.(uint8); ok {
			return NewUint8(v), nil
		} else {
			return Value{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v (%T) is not uint8 or bool", value, value)
		}
	case querypb.Type_UINT16:
		if v, ok := value.(uint16); ok {
			return NewUint16(v), nil
		} else {
			return Value{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v (%T) is not uint16", value, value)
		}
	case querypb.Type_UINT32:
		if v, ok := value.(uint32); ok {
			return NewUint32(v), nil
		} else {
			return Value{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v (%T) is not uint32", value, value)
		}
	case querypb.Type_UINT64:
		switch v := value.(type) {
		case uint64:
			return NewUint64(v), nil
		case uint:
			return NewUint64(uint64(v)), nil
		default:
			return Value{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v (%T) is not uint64", value, value)
		}
	case querypb.Type_INT8:
		if v, ok := value.(int8); ok {
			return NewInt8(v), nil
		} else {
			return Value{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v (%T) is not int8", value, value)
		}
	case querypb.Type_INT16:
		if v, ok := value.(int16); ok {
			return NewInt16(v), nil
		} else {
			return Value{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v (%T) is not int16", value, value)
		}
	case querypb.Type_INT32:
		if v, ok := value.(int32); ok {
			return NewInt32(v), nil
		} else {
			return Value{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v (%T) is not int32", value, value)
		}
	case querypb.Type_INT64:
		switch v := value.(type) {
		case int64:
			return NewInt64(v), nil
		case int:
			return NewInt64(int64(v)), nil
		default:
			return Value{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v (%T) is not int64", value, value)
		}
	case querypb.Type_FLOAT32:
		if v, ok := value.(float32); ok {
			return NewFloat32(v), nil
		} else {
			return Value{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v (%T) is not float32", value, value)
		}
	case querypb.Type_FLOAT64:
		if v, ok := value.(float64); ok {
			return NewFloat64(v), nil
		} else {
			return Value{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v (%T) is not float64", value, value)
		}
	case querypb.Type_VARCHAR, querypb.Type_VARBINARY:
		var s string
		if v, ok := value.(fmt.Stringer); ok {
			s = v.String()
		} else if v, ok := value.(string); ok {
			s = v
		} else {
			return Value{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v (%T) is not string-like", value, value)
		}

		if typ == querypb.Type_VARBINARY {
			return NewVarBinary(s), nil
		}

		return NewVarChar(s), nil
	case querypb.Type_TIMESTAMP:
		var s string
		switch v := value.(type) { // TODO: support overrides for other timestamp formats
		case *time.Time:
			if v == nil {
				return NULL, nil
			}

			s = v.Format(TimestampFormat)
		case time.Time:
			s = v.Format(TimestampFormat)
		case *vttime.Time:
			if v == nil {
				return NULL, nil
			}

			s = protoutil.TimeFromProto(v).Format(TimestampFormat)
		case vttime.Time:
			s = protoutil.TimeFromProto(&v).Format(TimestampFormat)
		case string:
			s = v
		default:
			_s, ok := value.(string)
			if !ok {
				return Value{}, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v (%T) is not time or string-like", value, value)
			}

			s = _s
		}

		return NewTimestamp(s), nil
	case querypb.Type_NULL_TYPE:
		return NewValue(Null, nil)
	}

	return Value{}, vterrors.Errorf(0, "unsupported query field type %s", strings.ToLower(querypb.Type_name[int32(typ)]))
}

func snakeCase(s string) string {
	var (
		buf   strings.Builder
		start = true
		lower = strings.ToLower(s)
	)

	/*
		Foo => foo
		FooBar => foo_bar
	*/
	for i, c := range s {
		// `c` is an uppercase letter
		if byte(c) != lower[i] {
			if !start {
				buf.WriteByte('_')
			}

			start = false
		}

		buf.WriteByte(lower[i])
	}

	return buf.String()
}
