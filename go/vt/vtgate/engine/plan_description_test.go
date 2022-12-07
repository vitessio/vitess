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

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/test/utils"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestCreateRoutePlanDescription(t *testing.T) {
	route := createRoute()

	planDescription := PrimitiveToPlanDescription(route)

	expected := PrimitiveDescription{
		OperatorType:      "Route",
		Variant:           "Scatter",
		Keyspace:          &vindexes.Keyspace{Name: "ks"},
		TargetDestination: key.DestinationAllShards{},
		Other: map[string]any{
			"Query":      route.Query,
			"Table":      route.GetTableName(),
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
		RoutingParameters: &RoutingParameters{
			Opcode:            Scatter,
			Keyspace:          &vindexes.Keyspace{Name: "ks"},
			TargetDestination: key.DestinationAllShards{},
			Vindex:            hash.(*vindexes.Hash),
		},
		Query:      "select all the things",
		TableName:  "tableName",
		FieldQuery: "more query",
	}
}

func TestPlanDescriptionWithInputs(t *testing.T) {
	route := createRoute()
	routeDescr := getDescriptionFor(route)
	count := evalengine.NewLiteralInt(12)
	offset := evalengine.NewLiteralInt(4)
	limit := &Limit{
		Count:  count,
		Offset: offset,
		Input:  route,
	}

	planDescription := PrimitiveToPlanDescription(limit)

	expected := PrimitiveDescription{
		OperatorType: "Limit",
		Other: map[string]any{
			"Count":  evalengine.FormatExpr(count),
			"Offset": evalengine.FormatExpr(offset),
		},
		Inputs: []PrimitiveDescription{routeDescr},
	}

	utils.MustMatch(t, expected, planDescription, "descriptions did not match")
}

func getDescriptionFor(route *Route) PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType:      "Route",
		Variant:           route.Opcode.String(),
		Keyspace:          &vindexes.Keyspace{Name: "ks"},
		TargetDestination: key.DestinationAllShards{},
		Other: map[string]any{
			"Query":      route.Query,
			"Table":      route.GetTableName(),
			"FieldQuery": route.FieldQuery,
			"Vindex":     route.Vindex.String(),
		},
		Inputs: []PrimitiveDescription{},
	}
}
