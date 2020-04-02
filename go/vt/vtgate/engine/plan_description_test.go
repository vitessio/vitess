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
	"fmt"
	"testing"

	"vitess.io/vitess/go/sqltypes"

	"github.com/google/go-cmp/cmp"
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestCreateRoutePlanDescription(t *testing.T) {
	route := createRoute()

	planDescription := PrimitiveToPlanDescription(route)

	expected := PrimitiveDescription{
		OperatorType:      "Route",
		Variant:           "SelectScatter",
		Keyspace:          &vindexes.Keyspace{Name: "ks"},
		TargetDestination: key.DestinationAllShards{},
		TargetTabletType:  topodatapb.TabletType_MASTER,
		Other: map[string]string{
			"Query":     route.Query,
			"TableName": route.TableName,
		},
		Inputs: []PrimitiveDescription{},
	}

	if diff := cmp.Diff(planDescription, expected); diff != "" {
		t.Errorf(diff)
		bytes, _ := json.MarshalIndent(expected, "", "  ")
		fmt.Println(string(bytes))
		fmt.Printf("%v\n", expected)
	}

}

func createRoute() *Route {
	return &Route{
		Opcode:            SelectScatter,
		Keyspace:          &vindexes.Keyspace{Name: "ks"},
		TargetDestination: key.DestinationAllShards{},
		TargetTabletType:  topodatapb.TabletType_MASTER,
		Query:             "select all the things",
		TableName:         "tableName",
		FieldQuery:        "more query",
		Vindex:            &vindexes.Null{},
		Values:            []sqltypes.PlanValue{},
		OrderBy:           []OrderbyParams{},
	}
}

func TestPlanDescriptionWithInputs(t *testing.T) {
	route := createRoute()
	routeDescr := getDescriptionFor(route)
	limit := &Limit{
		Count:  int64PlanValue(12),
		Offset: int64PlanValue(4),
		Input:  route,
	}

	planDescription := PrimitiveToPlanDescription(limit)

	expected := PrimitiveDescription{
		OperatorType: "Limit",
		Other: map[string]string{
			"Count":  "12",
			"Offset": "4",
		},
		Inputs: []PrimitiveDescription{routeDescr},
	}

	if diff := cmp.Diff(planDescription, expected); diff != "" {
		t.Errorf(diff)
		bytes, _ := json.MarshalIndent(expected, "", "  ")
		fmt.Println(string(bytes))
		bytes, _ = json.MarshalIndent(planDescription, "", "  ")
		fmt.Println(string(bytes))
	}

}

func getDescriptionFor(route *Route) PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType:      "Route",
		Variant:           routeName[route.Opcode],
		Keyspace:          &vindexes.Keyspace{Name: "ks"},
		TargetDestination: key.DestinationAllShards{},
		TargetTabletType:  topodatapb.TabletType_MASTER,
		Other: map[string]string{
			"Query":     route.Query,
			"TableName": route.TableName,
		},
		Inputs: []PrimitiveDescription{},
	}
}
