/*
Copyright 2023 The Vitess Authors.

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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// TestDeleteCascade tests that FK_Cascade executes the child and parent primitives for a delete cascade.
func TestDeleteCascade(t *testing.T) {
	fakeRes := sqltypes.MakeTestResult(sqltypes.MakeTestFields("cola|colb", "int64|varchar"), "1|a", "2|b")

	inputP := &Route{
		Query: "select cola, colb from parent where foo = 48",
		RoutingParameters: &RoutingParameters{
			Opcode:   Unsharded,
			Keyspace: &vindexes.Keyspace{Name: "ks"},
		},
	}
	childP := &Delete{
		DML: &DML{
			Query: "delete from child where (ca, cb) in ::__vals",
			RoutingParameters: &RoutingParameters{
				Opcode:   Unsharded,
				Keyspace: &vindexes.Keyspace{Name: "ks"},
			},
		},
	}
	parentP := &Delete{
		DML: &DML{
			Query: "delete from parent where foo = 48",
			RoutingParameters: &RoutingParameters{
				Opcode:   Unsharded,
				Keyspace: &vindexes.Keyspace{Name: "ks"},
			},
		},
	}
	fkc := &FK_Cascade{
		Input:  inputP,
		Child:  []Child{{BVName: "__vals", Cols: []int{0, 1}, P: childP}},
		Parent: parentP,
	}

	vc := newDMLTestVCursor("0")
	vc.results = []*sqltypes.Result{fakeRes}
	_, err := fkc.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: select cola, colb from parent where foo = 48 {} false false`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: delete from child where (ca, cb) in ::__vals {__vals: type:TUPLE values:{type:TUPLE values:{type:INT64 value:"1"} values:{type:VARCHAR value:"a"}} values:{type:TUPLE values:{type:INT64 value:"2"} values:{type:VARCHAR value:"b"}}} true true`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: delete from parent where foo = 48 {} true true`,
	})
}

// TestUpdateCascade tests that FK_Cascade executes the child and parent primitives for an update cascade.
func TestUpdateCascade(t *testing.T) {
	fakeRes := sqltypes.MakeTestResult(sqltypes.MakeTestFields("cola|colb", "int64|varchar"), "1|a", "2|b")

	inputP := &Route{
		Query: "select cola, colb from parent where foo = 48",
		RoutingParameters: &RoutingParameters{
			Opcode:   Unsharded,
			Keyspace: &vindexes.Keyspace{Name: "ks"},
		},
	}
	childP := &Update{
		DML: &DML{
			Query: "update child set ca = :vtg1 where (ca, cb) in ::__vals",
			RoutingParameters: &RoutingParameters{
				Opcode:   Unsharded,
				Keyspace: &vindexes.Keyspace{Name: "ks"},
			},
		},
	}
	parentP := &Update{
		DML: &DML{
			Query: "update parent set cola = 1 where foo = 48",
			RoutingParameters: &RoutingParameters{
				Opcode:   Unsharded,
				Keyspace: &vindexes.Keyspace{Name: "ks"},
			},
		},
	}
	fkc := &FK_Cascade{
		Input:  inputP,
		Child:  []Child{{BVName: "__vals", Cols: []int{0, 1}, P: childP}},
		Parent: parentP,
	}

	vc := newDMLTestVCursor("0")
	vc.results = []*sqltypes.Result{fakeRes}
	_, err := fkc.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: select cola, colb from parent where foo = 48 {} false false`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: update child set ca = :vtg1 where (ca, cb) in ::__vals {__vals: type:TUPLE values:{type:TUPLE values:{type:INT64 value:"1"} values:{type:VARCHAR value:"a"}} values:{type:TUPLE values:{type:INT64 value:"2"} values:{type:VARCHAR value:"b"}}} true true`,
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ks.0: update parent set cola = 1 where foo = 48 {} true true`,
	})
}
