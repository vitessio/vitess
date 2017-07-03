/*
Copyright 2017 Google Inc.

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
	"errors"
	"fmt"
	"strconv"

	"github.com/golang/protobuf/proto"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// BuildBindVars builds a map[string]*querypb.BindVariable from a map[string]interface{}.
func BuildBindVars(in map[string]interface{}) (map[string]*querypb.BindVariable, error) {
	if len(in) == 0 {
		return nil, nil
	}

	out := make(map[string]*querypb.BindVariable, len(in))
	for k, v := range in {
		bv, err := BuildBindVar(v)
		if err != nil {
			return nil, fmt.Errorf("%s: %v", k, err)
		}
		out[k] = bv
	}
	return out, nil
}

// BuildBindVar builds a *querypb.BindVariable from a valid input type.
func BuildBindVar(v interface{}) (*querypb.BindVariable, error) {
	switch v := v.(type) {
	case string:
		return &querypb.BindVariable{
			Type:  querypb.Type_VARCHAR,
			Value: []byte(v),
		}, nil
	case []byte:
		return &querypb.BindVariable{
			Type:  querypb.Type_VARBINARY,
			Value: v,
		}, nil
	case int:
		return &querypb.BindVariable{
			Type:  querypb.Type_INT64,
			Value: strconv.AppendInt(nil, int64(v), 10),
		}, nil
	case int64:
		return &querypb.BindVariable{
			Type:  querypb.Type_INT64,
			Value: strconv.AppendInt(nil, v, 10),
		}, nil
	case uint64:
		return &querypb.BindVariable{
			Type:  querypb.Type_UINT64,
			Value: strconv.AppendUint(nil, v, 10),
		}, nil
	case float64:
		return &querypb.BindVariable{
			Type:  querypb.Type_FLOAT64,
			Value: strconv.AppendFloat(nil, v, 'f', -1, 64),
		}, nil
	case nil:
		return &querypb.BindVariable{
			Type: querypb.Type_NULL_TYPE,
		}, nil
	case Value:
		return &querypb.BindVariable{
			Type:  v.Type(),
			Value: v.Raw(),
		}, nil
	case *querypb.BindVariable:
		return v, nil
	case []interface{}:
		bv := &querypb.BindVariable{
			Type:   querypb.Type_TUPLE,
			Values: make([]*querypb.Value, len(v)),
		}
		values := make([]querypb.Value, len(v))
		for i, lv := range v {
			lbv, err := BuildBindVar(lv)
			if err != nil {
				return nil, err
			}
			values[i].Type = lbv.Type
			values[i].Value = lbv.Value
			bv.Values[i] = &values[i]
		}
		return bv, nil
	case []string:
		bv := &querypb.BindVariable{
			Type:   querypb.Type_TUPLE,
			Values: make([]*querypb.Value, len(v)),
		}
		values := make([]querypb.Value, len(v))
		for i, lv := range v {
			values[i].Type = querypb.Type_VARCHAR
			values[i].Value = []byte(lv)
			bv.Values[i] = &values[i]
		}
		return bv, nil
	case [][]byte:
		bv := &querypb.BindVariable{
			Type:   querypb.Type_TUPLE,
			Values: make([]*querypb.Value, len(v)),
		}
		values := make([]querypb.Value, len(v))
		for i, lv := range v {
			values[i].Type = querypb.Type_VARBINARY
			values[i].Value = lv
			bv.Values[i] = &values[i]
		}
		return bv, nil
	case []int:
		bv := &querypb.BindVariable{
			Type:   querypb.Type_TUPLE,
			Values: make([]*querypb.Value, len(v)),
		}
		values := make([]querypb.Value, len(v))
		for i, lv := range v {
			values[i].Type = querypb.Type_INT64
			values[i].Value = strconv.AppendInt(nil, int64(lv), 10)
			bv.Values[i] = &values[i]
		}
		return bv, nil
	case []int64:
		bv := &querypb.BindVariable{
			Type:   querypb.Type_TUPLE,
			Values: make([]*querypb.Value, len(v)),
		}
		values := make([]querypb.Value, len(v))
		for i, lv := range v {
			values[i].Type = querypb.Type_INT64
			values[i].Value = strconv.AppendInt(nil, lv, 10)
			bv.Values[i] = &values[i]
		}
		return bv, nil
	case []uint64:
		bv := &querypb.BindVariable{
			Type:   querypb.Type_TUPLE,
			Values: make([]*querypb.Value, len(v)),
		}
		values := make([]querypb.Value, len(v))
		for i, lv := range v {
			values[i].Type = querypb.Type_UINT64
			values[i].Value = strconv.AppendUint(nil, lv, 10)
			bv.Values[i] = &values[i]
		}
		return bv, nil
	case []float64:
		bv := &querypb.BindVariable{
			Type:   querypb.Type_TUPLE,
			Values: make([]*querypb.Value, len(v)),
		}
		values := make([]querypb.Value, len(v))
		for i, lv := range v {
			values[i].Type = querypb.Type_FLOAT64
			values[i].Value = strconv.AppendFloat(nil, lv, 'g', -1, 64)
			bv.Values[i] = &values[i]
		}
		return bv, nil
	}
	return nil, fmt.Errorf("type %T not supported as bind var: %v", v, v)
}

// ValidateBindVars validates a map[string]*querypb.BindVariable.
func ValidateBindVars(bv map[string]*querypb.BindVariable) error {
	for k, v := range bv {
		if err := ValidateBindVar(v); err != nil {
			return fmt.Errorf("%s: %v", k, err)
		}
	}
	return nil
}

// ValidateBindVar returns an error if the bind variable has inconsistent
// fields.
func ValidateBindVar(bv *querypb.BindVariable) error {
	if bv == nil {
		return errors.New("bind variable is nil")
	}

	if bv.Type == Tuple {
		if len(bv.Values) == 0 {
			return errors.New("empty tuple is not allowed")
		}
		for _, val := range bv.Values {
			if val.Type == Tuple {
				return errors.New("tuple not allowed inside another tuple")
			}
			if err := ValidateBindVar(&querypb.BindVariable{Type: val.Type, Value: val.Value}); err != nil {
				return err
			}
		}
		return nil
	}

	// Only values convertible to mysql types are allowed.
	// Additionally NULL is also not allowed.
	if _, ok := typeToMySQL[bv.Type]; !ok || bv.Type == Null {
		return fmt.Errorf("type: %v is invalid", bv.Type)
	}
	switch {
	case IsSigned(bv.Type):
		if _, err := strconv.ParseInt(string(bv.Value), 0, 64); err != nil {
			return err
		}
	case IsUnsigned(bv.Type):
		if _, err := strconv.ParseUint(string(bv.Value), 0, 64); err != nil {
			return err
		}
	case IsFloat(bv.Type) || bv.Type == Decimal:
		if _, err := strconv.ParseFloat(string(bv.Value), 64); err != nil {
			return err
		}
	}
	return nil
}

// BindVarToValue converts an already validated bind var into a Value.
func BindVarToValue(bv *querypb.BindVariable) Value {
	// This is a failsafe. There are still places in the code
	// that send nil bind vars.
	if bv == nil {
		return NULL
	}
	return MakeTrusted(bv.Type, bv.Value)
}

// ValueToBindVar converts a Value to a *querypb.BindVariable.
func ValueToBindVar(v Value) *querypb.BindVariable {
	return &querypb.BindVariable{
		Type:  v.typ,
		Value: v.val,
	}
}

// BindVariablesEqual compares two maps of bind variables.
// For protobuf messages we have to use "proto.Equal".
func BindVariablesEqual(x, y map[string]*querypb.BindVariable) bool {
	return proto.Equal(&querypb.BoundQuery{BindVariables: x}, &querypb.BoundQuery{BindVariables: y})
}
