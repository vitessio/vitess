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
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// PlanDescription is used to create a serializable representation of the Primitive tree
type PlanDescription struct {
	OperatorType string
	OpCode       string
	Keyspace     string
	Destination  string
	TabletType   topodatapb.TabletType
	Other        map[string]string
	Inputs       []PlanDescription
}

//PrimitiveToPlanDescription transforms a primitive tree into a corresponding PlanDescription tree
func PrimitiveToPlanDescription(in Primitive) PlanDescription {
	var this PlanDescription

	switch p := in.(type) {
	case *Route:
		this = PlanDescription{
			OperatorType: "Route",
			OpCode:       p.RouteType(),
			Keyspace:     p.Keyspace.Name,
			Destination:  p.TargetDestination.String(),
			TabletType:   p.TargetTabletType,
		}
	}
	return this
}
