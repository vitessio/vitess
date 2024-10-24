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

package engine

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var (
	vindex, _ = vindexes.CreateVindex("lookup_unique", "", map[string]string{
		"table":      "lkp",
		"from":       "from",
		"to":         "toc",
		"write_only": "true",
	})
	ks = &vindexes.Keyspace{Name: "ks", Sharded: true}
)

func TestVindexLookup(t *testing.T) {
	planableVindex, ok := vindex.(vindexes.LookupPlanable)
	require.True(t, ok, "not a lookup vindex")
	_, args := planableVindex.Query()

	fp := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("id|keyspace_id", "int64|varbinary"),
				"1|\x10"),
		},
	}
	route := NewRoute(ByDestination, ks, "dummy_select", "dummy_select_field")
	vdxLookup := &VindexLookup{
		Opcode:    EqualUnique,
		Keyspace:  ks,
		Vindex:    planableVindex,
		Arguments: args,
		Values:    []evalengine.Expr{evalengine.NewLiteralInt(1)},
		Lookup:    fp,
		SendTo:    route,
	}

	vc := &loggingVCursor{results: []*sqltypes.Result{defaultSelectResult}}

	result, err := vdxLookup.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	fp.ExpectLog(t, []string{`Execute from: type:TUPLE values:{type:INT64 value:"1"} false`})
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationKeyspaceID(10)`,
		`ExecuteMultiShard ks.-20: dummy_select {} false false`,
	})
	expectResult(t, result, defaultSelectResult)

	fp.rewind()
	vc.Rewind()
	result, err = wrapStreamExecute(vdxLookup, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationKeyspaceID(10)`,
		`StreamExecuteMulti dummy_select ks.-20: {} `,
	})
	expectResult(t, result, defaultSelectResult)
}

func TestVindexLookupTruncate(t *testing.T) {
	planableVindex, ok := vindex.(vindexes.LookupPlanable)
	require.True(t, ok, "not a lookup vindex")
	_, args := planableVindex.Query()

	fp := &fakePrimitive{
		results: []*sqltypes.Result{
			sqltypes.MakeTestResult(
				sqltypes.MakeTestFields("id|keyspace_id", "int64|varbinary"),
				"1|\x10"),
		},
	}
	route := NewRoute(ByDestination, ks, "dummy_select", "dummy_select_field")
	route.TruncateColumnCount = 1
	vdxLookup := &VindexLookup{
		Opcode:    EqualUnique,
		Keyspace:  ks,
		Vindex:    planableVindex,
		Arguments: args,
		Values:    []evalengine.Expr{evalengine.NewLiteralInt(1)},
		Lookup:    fp,
		SendTo:    route,
	}

	vc := &loggingVCursor{results: []*sqltypes.Result{
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("name|morecol", "varchar|int64"),
			"foo|1", "bar|2", "baz|3"),
	}}

	wantRes := sqltypes.MakeTestResult(sqltypes.MakeTestFields("name", "varchar"),
		"foo", "bar", "baz")
	result, err := vdxLookup.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	fp.ExpectLog(t, []string{`Execute from: type:TUPLE values:{type:INT64 value:"1"} false`})
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationKeyspaceID(10)`,
		`ExecuteMultiShard ks.-20: dummy_select {} false false`,
	})
	expectResult(t, result, wantRes)

	fp.rewind()
	vc.Rewind()
	result, err = wrapStreamExecute(vdxLookup, vc, map[string]*querypb.BindVariable{}, false)
	require.NoError(t, err)
	vc.ExpectLog(t, []string{
		`ResolveDestinations ks [type:INT64 value:"1"] Destinations:DestinationKeyspaceID(10)`,
		`StreamExecuteMulti dummy_select ks.-20: {} `,
	})
	expectResult(t, result, wantRes)
}
