/*
Copyright 2024 The Vitess Authors.

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

package streamlog

import (
	"encoding/json"
	"errors"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var ErrUnrecognizedBindVarType = errors.New("unrecognized bind variable type")

// BindVariableValue is used to store querypb.BindVariable values.
type BindVariableValue struct {
	Type  string
	Value []byte
}

// MarshalJSON renders the BindVariableValue as json and optionally redacts the value.
func (bv BindVariableValue) MarshalJSON() ([]byte, error) {
	out := map[string]interface{}{
		"Type":  bv.Type,
		"Value": bv.Value,
	}
	if GetRedactDebugUIQueries() {
		out["Value"] = nil
	}
	return json.Marshal(out)
}

// BindVariable is a wrapper for marshal/unmarshaling querypb.BindVariable as json.
// It ensures that the "Type" field is a string representation of the variable
// type instead of an integer-based code that is less portable and human-readable.
//
// This allows a *querypb.BindVariable that would have marshaled
// to this:
//
//	{"Type":10262,"Value":"FmtAtEq6S9Y="}
//
// to marshal to this:
//
//	{"Type":"VARBINARY","Value":"FmtAtEq6S9Y="}
//
// or if query redaction is enabled, like this:
//
//	{"Type":"VARBINARY","Value":null}
type BindVariable struct {
	Type   string
	Value  []byte
	Values []*BindVariableValue
}

// NewBindVariable returns a wrapped *querypb.BindVariable object.
func NewBindVariable(bv *querypb.BindVariable) BindVariable {
	newBv := BindVariable{
		Type:  bv.Type.String(),
		Value: bv.Value,
	}
	for _, val := range bv.Values {
		newBv.Values = append(newBv.Values, &BindVariableValue{
			Type:  val.Type.String(),
			Value: val.Value,
		})
	}
	return newBv
}

// NewBindVariables returns a string-map of wrapped *querypb.BindVariable objects.
func NewBindVariables(bvs map[string]*querypb.BindVariable) map[string]BindVariable {
	out := make(map[string]BindVariable, len(bvs))
	for key, bindVar := range bvs {
		out[key] = NewBindVariable(bindVar)
	}
	return out
}

// MarshalJSON renders the BindVariable as json and optionally redacts the value.
func (bv BindVariable) MarshalJSON() ([]byte, error) {
	out := map[string]interface{}{
		"Type":  bv.Type,
		"Value": bv.Value,
	}
	if GetRedactDebugUIQueries() {
		out["Value"] = nil
	}
	return json.Marshal(out)
}

// bindVariablesValuesToProto converts a slice of *BindVariableValue to *querypb.Value.
func bindVariablesValuesToProto(vals []*BindVariableValue) ([]*querypb.Value, error) {
	values := make([]*querypb.Value, len(vals))
	for _, val := range vals {
		varType, found := querypb.Type_value[val.Type]
		if !found {
			return nil, ErrUnrecognizedBindVarType
		}
		values = append(values, &querypb.Value{
			Type:  querypb.Type(varType),
			Value: val.Value,
		})
	}
	return values, nil
}

// BindVariablesToProto converts a string-map of BindVariable to a string-map of *querypb.BindVariable.
func BindVariablesToProto(bvs map[string]BindVariable) (map[string]*querypb.BindVariable, error) {
	out := make(map[string]*querypb.BindVariable, len(bvs))
	for key, bindVar := range bvs {
		// convert type string to querypb.Type.
		varType, found := querypb.Type_value[bindVar.Type]
		if !found {
			return nil, ErrUnrecognizedBindVarType
		}
		values, err := bindVariablesValuesToProto(bindVar.Values)
		if err != nil {
			return nil, err
		}
		out[key] = &querypb.BindVariable{
			Type:   querypb.Type(varType),
			Value:  bindVar.Value,
			Values: values,
		}
	}
	return out, nil
}
