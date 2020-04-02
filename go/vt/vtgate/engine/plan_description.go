/*
Copyright 2020 The Vitess Authors.

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

package engine

import (
	"bytes"
	"encoding/json"
	"sort"

	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// PrimitiveDescription is used to create a serializable representation of the Primitive tree
type PrimitiveDescription struct {
	OperatorType string
	Variant      string
	// Keyspace specifies the keyspace to send the query to.
	Keyspace *vindexes.Keyspace
	// TargetDestination specifies an explicit target destination to send the query to.
	TargetDestination key.Destination
	// TargetTabletType specifies an explicit target destination tablet type
	// this is only used in conjunction with TargetDestination
	TargetTabletType topodatapb.TabletType
	Other            map[string]string
	Inputs           []PrimitiveDescription
}

// MarshalJSON serializes the PlanDescription into a JSON representation.
// We do this rather manual thing here so the `other` map looks like
//fields belonging to pd and not a map in a field.
func (pd PrimitiveDescription) MarshalJSON() ([]byte, error) {
	buf := &bytes.Buffer{}
	buf.WriteString("{")

	if err := marshalAdd("", buf, "OperatorType", pd.OperatorType); err != nil {
		return nil, err
	}
	if err := marshalAdd(",", buf, "Variant", pd.Variant); err != nil {
		return nil, err
	}
	if pd.Keyspace != nil {
		if err := marshalAdd(",", buf, "Keyspace", pd.Keyspace); err != nil {
			return nil, err
		}
	}
	if pd.TargetDestination != nil {
		s := pd.TargetDestination.String()
		dest := s[11:] // TODO: All these start with Destination. We should fix that instead if trimming it out here

		if err := marshalAdd(",", buf, "TargetDestination", dest); err != nil {
			return nil, err
		}
	}
	if pd.TargetTabletType != topodatapb.TabletType_UNKNOWN {
		if err := marshalAdd(",", buf, "TargetTabletType", pd.TargetTabletType.String()); err != nil {
			return nil, err
		}
	}
	err := addMap(pd.Other, buf)
	if err != nil {
		return nil, err
	}

	if len(pd.Inputs) > 0 {
		if err := marshalAdd(",", buf, "Inputs", pd.Inputs); err != nil {
			return nil, err
		}
	}

	buf.WriteString("}")

	return buf.Bytes(), nil
}

func addMap(input map[string]string, buf *bytes.Buffer) error {
	mk := make([]string, len(input))
	i := 0
	for k := range input {
		mk[i] = k
		i++
	}
	sort.Strings(mk)
	for _, k := range mk {
		v := input[k]
		if err := marshalAdd(",", buf, k, v); err != nil {
			return err
		}
	}
	return nil
}

func marshalAdd(prepend string, buf *bytes.Buffer, name string, obj interface{}) error {
	buf.WriteString(prepend + `"` + name + `":`)
	b, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	buf.Write(b)
	return nil
}

//PrimitiveToPlanDescription transforms a primitive tree into a corresponding PlanDescription tree
func PrimitiveToPlanDescription(in Primitive) PrimitiveDescription {
	this := in.description()

	for _, input := range in.Inputs() {
		this.Inputs = append(this.Inputs, PrimitiveToPlanDescription(input))
	}

	if len(in.Inputs()) == 0 {
		this.Inputs = []PrimitiveDescription{}
	}

	return this
}
