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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/test/utils"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestCreateRoutePlanDescription(t *testing.T) {
	route := createRoute()

	planDescription := PrimitiveToPlanDescription(route, nil)

	expected := PrimitiveDescription{
		OperatorType:      "Route",
		Variant:           "Scatter",
		Keyspace:          &vindexes.Keyspace{Name: "ks"},
		TargetDestination: key.DestinationAllShards{},
		Other: map[string]any{
			"Query":      route.Query,
			"FieldQuery": route.FieldQuery,
			"Vindex":     route.Vindex.String(),
		},
		Inputs: []PrimitiveDescription{},
	}

	utils.MustMatch(t, expected, planDescription, "descriptions did not match")
}

func createRoute() *Route {
	hash, _ := vindexes.CreateVindex("hash", "vindex name", nil)
	return &Route{
		RoutingParameters: &RoutingParameters{
			Opcode:            Scatter,
			Keyspace:          &vindexes.Keyspace{Name: "ks"},
			TargetDestination: key.DestinationAllShards{},
			Vindex:            hash.(*vindexes.Hash),
		},
		Query:      "select all the things",
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

	planDescription := PrimitiveToPlanDescription(limit, nil)

	expected := PrimitiveDescription{
		OperatorType: "Limit",
		Other: map[string]any{
			"Count":  sqlparser.String(count),
			"Offset": sqlparser.String(offset),
		},
		Inputs: []PrimitiveDescription{routeDescr},
	}

	utils.MustMatch(t, expected, planDescription, "descriptions did not match")
}

// TestPrimitiveDescriptionFromMapRowsReceived verifies that RowsReceived is
// reconstructed from the lossy JSON form (NoOfCalls + AvgNumberOfRows) without
// truncating the total row count, including the streaming-mode case where the
// average is fractional (e.g. RowsReceived [0, 1] -> AvgNumberOfRows 0.5).
func TestPrimitiveDescriptionFromMapRowsReceived(t *testing.T) {
	tests := []struct {
		name string
		in   map[string]any
		want RowsReceived
	}{
		{
			name: "streaming fractional average rounds to total",
			in: map[string]any{
				"NoOfCalls":       float64(2),
				"AvgNumberOfRows": float64(0.5),
			},
			want: RowsReceived{1},
		},
		{
			name: "single call preserves count",
			in: map[string]any{
				"NoOfCalls":       float64(1),
				"AvgNumberOfRows": float64(5),
			},
			want: RowsReceived{5},
		},
		{
			name: "missing NoOfCalls falls back to single call",
			in: map[string]any{
				"AvgNumberOfRows": float64(7),
			},
			want: RowsReceived{7},
		},
		{
			name: "missing AvgNumberOfRows leaves RowsReceived unset",
			in:   map[string]any{},
			want: nil,
		},
		{
			name: "non-trivial average across multiple calls",
			in: map[string]any{
				"NoOfCalls":       float64(3),
				"AvgNumberOfRows": float64(2.6666666666666665), // (3+2+3)/3
			},
			want: RowsReceived{8},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pd, err := PrimitiveDescriptionFromMap(tc.in)
			require.NoError(t, err)
			assert.Equal(t, tc.want, pd.RowsReceived)
		})
	}
}

func getDescriptionFor(route *Route) PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType:      "Route",
		Variant:           route.Opcode.String(),
		Keyspace:          &vindexes.Keyspace{Name: "ks"},
		TargetDestination: key.DestinationAllShards{},
		Other: map[string]any{
			"Query":      route.Query,
			"FieldQuery": route.FieldQuery,
			"Vindex":     route.Vindex.String(),
		},
		Inputs: []PrimitiveDescription{},
	}
}
