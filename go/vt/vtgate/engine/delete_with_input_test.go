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

func TestDeleteWithInputSingleOffset(t *testing.T) {
	input := &fakePrimitive{results: []*sqltypes.Result{
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1", "2", "3"),
	}}

	del := &DeleteWithInput{
		Input: input,
		Delete: &Delete{
			DML: &DML{
				RoutingParameters: &RoutingParameters{
					Opcode: Scatter,
					Keyspace: &vindexes.Keyspace{
						Name:    "ks",
						Sharded: true,
					},
				},
				Query: "dummy_delete",
			},
		},
		OutputCols: []int{0},
	}

	vc := newDMLTestVCursor("-20", "20-")
	_, err := del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ` +
			`ks.-20: dummy_delete {dm_vals: type:TUPLE values:{type:INT64 value:"1"} values:{type:INT64 value:"2"} values:{type:INT64 value:"3"}} ` +
			`ks.20-: dummy_delete {dm_vals: type:TUPLE values:{type:INT64 value:"1"} values:{type:INT64 value:"2"} values:{type:INT64 value:"3"}} true false`,
	})

	vc.Rewind()
	input.rewind()
	err = del.TryStreamExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false, func(result *sqltypes.Result) error { return nil })
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ` +
			`ks.-20: dummy_delete {dm_vals: type:TUPLE values:{type:INT64 value:"1"} values:{type:INT64 value:"2"} values:{type:INT64 value:"3"}} ` +
			`ks.20-: dummy_delete {dm_vals: type:TUPLE values:{type:INT64 value:"1"} values:{type:INT64 value:"2"} values:{type:INT64 value:"3"}} true false`,
	})
}

func TestDeleteWithInputMultiOffset(t *testing.T) {
	input := &fakePrimitive{results: []*sqltypes.Result{
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|col", "int64|varchar"), "1|a", "2|b", "3|c"),
	}}

	del := &DeleteWithInput{
		Input: input,
		Delete: &Delete{
			DML: &DML{
				RoutingParameters: &RoutingParameters{
					Opcode: Scatter,
					Keyspace: &vindexes.Keyspace{
						Name:    "ks",
						Sharded: true,
					},
				},
				Query: "dummy_delete",
			},
		},
		OutputCols: []int{1, 0},
	}

	vc := newDMLTestVCursor("-20", "20-")
	_, err := del.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ` +
			`ks.-20: dummy_delete {dm_vals: type:TUPLE values:{type:TUPLE value:"\x950\x01a\x89\x02\x011"} values:{type:TUPLE value:"\x950\x01b\x89\x02\x012"} values:{type:TUPLE value:"\x950\x01c\x89\x02\x013"}} ` +
			`ks.20-: dummy_delete {dm_vals: type:TUPLE values:{type:TUPLE value:"\x950\x01a\x89\x02\x011"} values:{type:TUPLE value:"\x950\x01b\x89\x02\x012"} values:{type:TUPLE value:"\x950\x01c\x89\x02\x013"}} true false`,
	})

	vc.Rewind()
	input.rewind()
	err = del.TryStreamExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false, func(result *sqltypes.Result) error { return nil })
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
		`ExecuteMultiShard ` +
			`ks.-20: dummy_delete {dm_vals: type:TUPLE values:{type:TUPLE value:"\x950\x01a\x89\x02\x011"} values:{type:TUPLE value:"\x950\x01b\x89\x02\x012"} values:{type:TUPLE value:"\x950\x01c\x89\x02\x013"}} ` +
			`ks.20-: dummy_delete {dm_vals: type:TUPLE values:{type:TUPLE value:"\x950\x01a\x89\x02\x011"} values:{type:TUPLE value:"\x950\x01b\x89\x02\x012"} values:{type:TUPLE value:"\x950\x01c\x89\x02\x013"}} true false`,
	})
}
