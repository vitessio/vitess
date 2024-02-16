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

// BindVariable is a wrapper for marshal/unmarshaling querypb.BindVariable as json.
type BindVariable querypb.BindVariable

// NewBindVariable returns a wrapped *querypb.BindVariable object.
func NewBindVariable(bv *querypb.BindVariable) BindVariable {
	return BindVariable(*bv)
}

// NewBindVariables returns a string-map of wrapped *querypb.BindVariable objects.
func NewBindVariables(bvs map[string]*querypb.BindVariable) map[string]BindVariable {
	out := make(map[string]BindVariable, len(bvs))
	for key, bindVar := range bvs {
		out[key] = NewBindVariable(bindVar)
	}
	return out
}

// UnmarshalJSON unmarshals the custom BindVariable json-format.
// See MarshalJSON for more information.
func (bv *BindVariable) UnmarshalJSON(b []byte) error {
	in := struct {
		Type  string
		Value []byte
	}{}
	if err := json.Unmarshal(b, &in); err != nil {
		return err
	}
	// convert type string to querypb.Type and pass along Value.
	typeVal, found := querypb.Type_value[in.Type]
	if !found {
		return ErrUnrecognizedBindVarType
	}
	bv.Type = querypb.Type(typeVal)
	bv.Value = in.Value
	return nil
}

// MarshalJSON renders the wrapped *querypb.BindVariable as json. It
// ensures that the "Type" field is a string representation of the
// variable type instead of an integer-based code that is less
// portable and human-readable.
//
// This allows a *querypb.BindVariable that would have marshaled
// to this:
//   {"Type":10262,"Value":"FmtAtEq6S9Y="}
// to marshal to this:
//   {"Type":"VARBINARY","Value":"FmtAtEq6S9Y="}
func (bv BindVariable) MarshalJSON() ([]byte, error) {
	// convert querypb.Type integer to string and pass along Value.
	out := map[string]interface{}{
		"Type":  bv.Type.String(),
		"Value": bv.Value,
	}
	if GetRedactDebugUIQueries() {
		out["Value"] = nil
	}
	return json.Marshal(out)
}

// BindVariablesToProto converts a string-map of BindVariable to a string-map of *querypb.BindVariable.
func BindVariablesToProto(bvs map[string]BindVariable) map[string]*querypb.BindVariable {
	out := make(map[string]*querypb.BindVariable, len(bvs))
	for key, bindVar := range bvs {
		bv := querypb.BindVariable(bindVar)
		out[key] = &bv
	}
	return out
}
