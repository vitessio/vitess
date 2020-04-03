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
	"testing"

	"vitess.io/vitess/go/test/utils"

	"vitess.io/vitess/go/sqltypes"

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
		Other: map[string]interface{}{
			"Query":      route.Query,
			"Table":      route.TableName,
			"FieldQuery": route.FieldQuery,
			"Vindex":     route.Vindex.String(),
		},
		Inputs: []PrimitiveDescription{},
	}

	utils.MustMatch(t, expected, planDescription, "descriptions did not match")
}

func createRoute() *Route {
	hash, _ := vindexes.NewHash("vindex name", nil)
	return &Route{
		Opcode:            SelectScatter,
		Keyspace:          &vindexes.Keyspace{Name: "ks"},
		TargetDestination: key.DestinationAllShards{},
		TargetTabletType:  topodatapb.TabletType_MASTER,
		Query:             "select all the things",
		TableName:         "tableName",
		FieldQuery:        "more query",
		Vindex:            hash.(*vindexes.Hash),
		Values:            []sqltypes.PlanValue{},
		OrderBy:           []OrderbyParams{},
	}
}

func TestPlanDescriptionWithInputs(t *testing.T) {
	route := createRoute()
	routeDescr := getDescriptionFor(route)
	count := int64PlanValue(12)
	offset := int64PlanValue(4)
	limit := &Limit{
		Count:  count,
		Offset: offset,
		Input:  route,
	}

	planDescription := PrimitiveToPlanDescription(limit)

	expected := PrimitiveDescription{
		OperatorType: "Limit",
		Other: map[string]interface{}{
			"Count":  count.Value,
			"Offset": offset.Value,
		},
		Inputs: []PrimitiveDescription{routeDescr},
	}

	mustMatch(t, expected, planDescription, "descriptions did not match")
}

var mustMatch = utils.MustMatchFn(
	[]interface{}{ // types with unexported fields
		sqltypes.Value{},
	},
	[]string{}, // ignored fields
)

func getDescriptionFor(route *Route) PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType:      "Route",
		Variant:           routeName[route.Opcode],
		Keyspace:          &vindexes.Keyspace{Name: "ks"},
		TargetDestination: key.DestinationAllShards{},
		TargetTabletType:  topodatapb.TabletType_MASTER,
		Other: map[string]interface{}{
			"Query":      route.Query,
			"Table":      route.TableName,
			"FieldQuery": route.FieldQuery,
			"Vindex":     route.Vindex.String(),
		},
		Inputs: []PrimitiveDescription{},
	}
}
