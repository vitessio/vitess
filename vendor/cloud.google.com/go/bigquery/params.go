// Copyright 2016 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bigquery

import (
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/internal/fields"

	bq "google.golang.org/api/bigquery/v2"
)

var (
	// See https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#timestamp-type.
	timestampFormat = "2006-01-02 15:04:05.999999-07:00"

	// See https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#schema.fields.name
	validFieldName = regexp.MustCompile("^[a-zA-Z_][a-zA-Z0-9_]{0,127}$")
)

func bqTagParser(t reflect.StructTag) (name string, keep bool, other interface{}, err error) {
	if s := t.Get("bigquery"); s != "" {
		if s == "-" {
			return "", false, nil, nil
		}
		if !validFieldName.MatchString(s) {
			return "", false, nil, errInvalidFieldName
		}
		return s, true, nil, nil
	}
	return "", true, nil, nil
}

var fieldCache = fields.NewCache(bqTagParser, nil)

var (
	int64ParamType     = &bq.QueryParameterType{Type: "INT64"}
	float64ParamType   = &bq.QueryParameterType{Type: "FLOAT64"}
	boolParamType      = &bq.QueryParameterType{Type: "BOOL"}
	stringParamType    = &bq.QueryParameterType{Type: "STRING"}
	bytesParamType     = &bq.QueryParameterType{Type: "BYTES"}
	dateParamType      = &bq.QueryParameterType{Type: "DATE"}
	timeParamType      = &bq.QueryParameterType{Type: "TIME"}
	dateTimeParamType  = &bq.QueryParameterType{Type: "DATETIME"}
	timestampParamType = &bq.QueryParameterType{Type: "TIMESTAMP"}
)

var (
	typeOfDate     = reflect.TypeOf(civil.Date{})
	typeOfTime     = reflect.TypeOf(civil.Time{})
	typeOfDateTime = reflect.TypeOf(civil.DateTime{})
	typeOfGoTime   = reflect.TypeOf(time.Time{})
)

type QueryParameter struct {
	// Name is used for named parameter mode.
	// It must match the name in the query case-insensitively.
	Name string

	// Value is the value of the parameter.
	// The following Go types are supported, with their corresponding
	// Bigquery types:
	// int, int8, int16, int32, int64, uint8, uint16, uint32: INT64
	//   Note that uint, uint64 and uintptr are not supported, because
	//   they may contain values that cannot fit into a 64-bit signed integer.
	// float32, float64: FLOAT64
	// bool: BOOL
	// string: STRING
	// []byte: BYTES
	// time.Time: TIMESTAMP
	// Arrays and slices of the above.
	// Structs of the above. Only the exported fields are used.
	Value interface{}
}

func (p QueryParameter) toRaw() (*bq.QueryParameter, error) {
	pv, err := paramValue(reflect.ValueOf(p.Value))
	if err != nil {
		return nil, err
	}
	pt, err := paramType(reflect.TypeOf(p.Value))
	if err != nil {
		return nil, err
	}
	return &bq.QueryParameter{
		Name:           p.Name,
		ParameterValue: &pv,
		ParameterType:  pt,
	}, nil
}

func paramType(t reflect.Type) (*bq.QueryParameterType, error) {
	if t == nil {
		return nil, errors.New("bigquery: nil parameter")
	}
	switch t {
	case typeOfDate:
		return dateParamType, nil
	case typeOfTime:
		return timeParamType, nil
	case typeOfDateTime:
		return dateTimeParamType, nil
	case typeOfGoTime:
		return timestampParamType, nil
	}
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return int64ParamType, nil

	case reflect.Float32, reflect.Float64:
		return float64ParamType, nil

	case reflect.Bool:
		return boolParamType, nil

	case reflect.String:
		return stringParamType, nil

	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return bytesParamType, nil
		}
		fallthrough

	case reflect.Array:
		et, err := paramType(t.Elem())
		if err != nil {
			return nil, err
		}
		return &bq.QueryParameterType{Type: "ARRAY", ArrayType: et}, nil

	case reflect.Ptr:
		if t.Elem().Kind() != reflect.Struct {
			break
		}
		t = t.Elem()
		fallthrough

	case reflect.Struct:
		var fts []*bq.QueryParameterTypeStructTypes
		fields, err := fieldCache.Fields(t)
		if err != nil {
			return nil, err
		}
		for _, f := range fields {
			pt, err := paramType(f.Type)
			if err != nil {
				return nil, err
			}
			fts = append(fts, &bq.QueryParameterTypeStructTypes{
				Name: f.Name,
				Type: pt,
			})
		}
		return &bq.QueryParameterType{Type: "STRUCT", StructTypes: fts}, nil
	}
	return nil, fmt.Errorf("bigquery: Go type %s cannot be represented as a parameter type", t)
}

func paramValue(v reflect.Value) (bq.QueryParameterValue, error) {
	var res bq.QueryParameterValue
	if !v.IsValid() {
		return res, errors.New("bigquery: nil parameter")
	}
	t := v.Type()
	switch t {
	case typeOfDate:
		res.Value = v.Interface().(civil.Date).String()
		return res, nil

	case typeOfTime:
		// civil.Time has nanosecond resolution, but BigQuery TIME only microsecond.
		res.Value = civilTimeParamString(v.Interface().(civil.Time))
		return res, nil

	case typeOfDateTime:
		dt := v.Interface().(civil.DateTime)
		res.Value = dt.Date.String() + " " + civilTimeParamString(dt.Time)
		return res, nil

	case typeOfGoTime:
		res.Value = v.Interface().(time.Time).Format(timestampFormat)
		return res, nil
	}
	switch t.Kind() {
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			res.Value = base64.StdEncoding.EncodeToString(v.Interface().([]byte))
			return res, nil
		}
		fallthrough

	case reflect.Array:
		var vals []*bq.QueryParameterValue
		for i := 0; i < v.Len(); i++ {
			val, err := paramValue(v.Index(i))
			if err != nil {
				return bq.QueryParameterValue{}, err
			}
			vals = append(vals, &val)
		}
		return bq.QueryParameterValue{ArrayValues: vals}, nil

	case reflect.Ptr:
		if t.Elem().Kind() != reflect.Struct {
			return res, fmt.Errorf("bigquery: Go type %s cannot be represented as a parameter value", t)
		}
		t = t.Elem()
		v = v.Elem()
		if !v.IsValid() {
			// nil pointer becomes empty value
			return res, nil
		}
		fallthrough

	case reflect.Struct:
		fields, err := fieldCache.Fields(t)
		if err != nil {
			return bq.QueryParameterValue{}, err
		}
		res.StructValues = map[string]bq.QueryParameterValue{}
		for _, f := range fields {
			fv := v.FieldByIndex(f.Index)
			fp, err := paramValue(fv)
			if err != nil {
				return bq.QueryParameterValue{}, err
			}
			res.StructValues[f.Name] = fp
		}
		return res, nil
	}
	// None of the above: assume a scalar type. (If it's not a valid type,
	// paramType will catch the error.)
	res.Value = fmt.Sprint(v.Interface())
	return res, nil
}

func civilTimeParamString(t civil.Time) string {
	if t.Nanosecond == 0 {
		return t.String()
	} else {
		micro := (t.Nanosecond + 500) / 1000 // round to nearest microsecond
		t.Nanosecond = 0
		return t.String() + fmt.Sprintf(".%06d", micro)
	}
}
