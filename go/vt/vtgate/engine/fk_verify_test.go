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

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestFKVerifyUpdate(t *testing.T) {
	verifyP := &Route{
		Query: "select distinct cola, colb from parent where (cola, colb) in ::__vals",
		RoutingParameters: &RoutingParameters{
			Opcode:   Unsharded,
			Keyspace: &vindexes.Keyspace{Name: "ks"},
		},
	}
	childP := &Update{
		DML: &DML{
			Query: "update child set cola = 1, colb = 'a' where foo = 48",
			RoutingParameters: &RoutingParameters{
				Opcode:   Unsharded,
				Keyspace: &vindexes.Keyspace{Name: "ks"},
			},
		},
	}
	fkc := &FkVerify{
		Verify: []*FkParent{
			{
				Values: []sqlparser.Exprs{{sqlparser.NewIntLiteral("1"), sqlparser.NewStrLiteral("a")}},
				Cols: []CheckCol{
					{Col: 0, Type: sqltypes.Int64, Collation: collations.CollationBinaryID},
					{Col: 1, Type: sqltypes.VarChar, Collation: collations.CollationUtf8mb4ID},
				},
				BvName: "__vals",
				Exec:   verifyP,
			},
		},
		Exec: childP,
	}

	t.Run("foreign key verification success", func(t *testing.T) {
		fakeRes := sqltypes.MakeTestResult(sqltypes.MakeTestFields("cola|colb", "int64|varchar"), "1|a")
		vc := newDMLTestVCursor("0")
		vc.results = []*sqltypes.Result{fakeRes}
		_, err := fkc.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
		require.NoError(t, err)
		vc.ExpectLog(t, []string{
			`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
			`ExecuteMultiShard ks.0: select distinct cola, colb from parent where (cola, colb) in ::__vals {__vals: type:TUPLE values:{type:TUPLE values:{type:INT64 value:"1"} values:{type:VARCHAR value:"a"}}} false false`,
			`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
			`ExecuteMultiShard ks.0: update child set cola = 1, colb = 'a' where foo = 48 {} true true`,
		})

		vc.Rewind()
		err = fkc.TryStreamExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true, func(result *sqltypes.Result) error { return nil })
		require.NoError(t, err)
		vc.ExpectLog(t, []string{
			`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
			`StreamExecuteMulti select distinct cola, colb from parent where (cola, colb) in ::__vals ks.0: {__vals: type:TUPLE values:{type:TUPLE values:{type:INT64 value:"1"} values:{type:VARCHAR value:"a"}}} `,
			`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
			`ExecuteMultiShard ks.0: update child set cola = 1, colb = 'a' where foo = 48 {} true true`,
		})
	})

	t.Run("foreign key verification failure", func(t *testing.T) {
		// No results from select, should cause the foreign key verification to fail.
		fakeRes := sqltypes.MakeTestResult(sqltypes.MakeTestFields("cola|colb", "int64|varchar"))
		vc := newDMLTestVCursor("0")
		vc.results = []*sqltypes.Result{fakeRes}
		_, err := fkc.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
		require.ErrorContains(t, err, "Cannot add or update a child row: a foreign key constraint fails")
		vc.ExpectLog(t, []string{
			`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
			`ExecuteMultiShard ks.0: select distinct cola, colb from parent where (cola, colb) in ::__vals {__vals: type:TUPLE values:{type:TUPLE values:{type:INT64 value:"1"} values:{type:VARCHAR value:"a"}}} false false`,
		})

		vc.Rewind()
		err = fkc.TryStreamExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true, func(result *sqltypes.Result) error { return nil })
		require.ErrorContains(t, err, "Cannot add or update a child row: a foreign key constraint fails")
		vc.ExpectLog(t, []string{
			`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
			`StreamExecuteMulti select distinct cola, colb from parent where (cola, colb) in ::__vals ks.0: {__vals: type:TUPLE values:{type:TUPLE values:{type:INT64 value:"1"} values:{type:VARCHAR value:"a"}}} `,
		})
	})
}
