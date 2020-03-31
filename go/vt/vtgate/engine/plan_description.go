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
	"encoding/json"

	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// PlanDescription is used to create a serializable representation of the Primitive tree
type PlanDescription struct {
	OperatorType string
	Variant      string `json:",omitempty"`
	// Keyspace specifies the keyspace to send the query to.
	Keyspace *vindexes.Keyspace `json:",omitempty"`
	// TargetDestination specifies an explicit target destination to send the query to.
	TargetDestination key.Destination `json:",omitempty"`
	// TargetTabletType specifies an explicit target destination tablet type
	// this is only used in conjunction with TargetDestination
	TargetTabletType topodatapb.TabletType `json:",omitempty"`
	Other            map[string]string     `json:",omitempty"`
	Inputs           []PlanDescription     `json:",omitempty"`
}

// MarshalJSON serializes the PlanDescription into a JSON representation.
func (pd *PlanDescription) MarshalJSON() ([]byte, error) {
	var dest string
	if pd.TargetDestination != nil {
		dest = pd.TargetDestination.String()
	}
	out := struct {
		OperatorType      string
		Variant           string             `json:",omitempty"`
		Keyspace          *vindexes.Keyspace `json:",omitempty"`
		TargetDestination string             `json:",omitempty"`
		TargetTabletType  string             `json:",omitempty"`
		Other             map[string]string  `json:",omitempty"`
		Inputs            []PlanDescription  `json:",omitempty"`
	}{
		OperatorType:      pd.OperatorType,
		Variant:           pd.Variant,
		Keyspace:          pd.Keyspace,
		TargetDestination: dest,
		TargetTabletType:  pd.TargetTabletType.String(),
		Other:             pd.Other,
		Inputs:            pd.Inputs,
	}
	return json.Marshal(out)
}

//PrimitiveToPlanDescription transforms a primitive tree into a corresponding PlanDescription tree
func PrimitiveToPlanDescription(in Primitive) PlanDescription {
	this := in.description()

	for _, input := range in.Inputs() {
		this.Inputs = append(this.Inputs, PrimitiveToPlanDescription(input))
	}

	if len(in.Inputs()) == 0 {
		this.Inputs = []PlanDescription{}
	}

	return this
}
