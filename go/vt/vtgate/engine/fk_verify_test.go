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

func TestFKVerifyUpdate(t *testing.T) {
	verifyP := &Route{
		Query: "select 1 from child c left join parent p on p.cola = 1 and p.colb = 'a' where p.cola is null and p.colb is null",
		RoutingParameters: &RoutingParameters{
			Opcode:   Unsharded,
			Keyspace: &vindexes.Keyspace{Name: "ks"},
		},
	}
	verifyC := &Route{
		Query: "select 1 from grandchild g join child c on g.cola = c.cola and g.colb = c.colb where c.foo = 48",
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
		Verify: []*Verify{{Exec: verifyP, Typ: ParentVerify}},
		Exec:   childP,
	}

	t.Run("foreign key verification success", func(t *testing.T) {
		fakeRes := sqltypes.MakeTestResult(sqltypes.MakeTestFields("1", "int64"))
		vc := newDMLTestVCursor("0")
		vc.results = []*sqltypes.Result{fakeRes}
		_, err := fkc.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
		require.NoError(t, err)
		vc.ExpectLog(t, []string{
			`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
			`ExecuteMultiShard ks.0: select 1 from child c left join parent p on p.cola = 1 and p.colb = 'a' where p.cola is null and p.colb is null {} false false`,
			`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
			`ExecuteMultiShard ks.0: update child set cola = 1, colb = 'a' where foo = 48 {} true true`,
		})

		vc.Rewind()
		err = fkc.TryStreamExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true, func(result *sqltypes.Result) error { return nil })
		require.NoError(t, err)
		vc.ExpectLog(t, []string{
			`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
			`ExecuteMultiShard ks.0: select 1 from child c left join parent p on p.cola = 1 and p.colb = 'a' where p.cola is null and p.colb is null {} false false`,
			`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
			`ExecuteMultiShard ks.0: update child set cola = 1, colb = 'a' where foo = 48 {} true true`,
		})
	})

	t.Run("parent foreign key verification failure", func(t *testing.T) {
		// No results from select, should cause the foreign key verification to fail.
		fakeRes := sqltypes.MakeTestResult(sqltypes.MakeTestFields("1", "int64"), "1", "1", "1")
		vc := newDMLTestVCursor("0")
		vc.results = []*sqltypes.Result{fakeRes}
		_, err := fkc.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
		require.ErrorContains(t, err, "Cannot add or update a child row: a foreign key constraint fails")
		vc.ExpectLog(t, []string{
			`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
			`ExecuteMultiShard ks.0: select 1 from child c left join parent p on p.cola = 1 and p.colb = 'a' where p.cola is null and p.colb is null {} false false`,
		})

		vc.Rewind()
		err = fkc.TryStreamExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true, func(result *sqltypes.Result) error { return nil })
		require.ErrorContains(t, err, "Cannot add or update a child row: a foreign key constraint fails")
		vc.ExpectLog(t, []string{
			`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
			`ExecuteMultiShard ks.0: select 1 from child c left join parent p on p.cola = 1 and p.colb = 'a' where p.cola is null and p.colb is null {} false false`,
		})
	})

	fkc.Verify[0] = &Verify{Exec: verifyC, Typ: ChildVerify}
	t.Run("child foreign key verification failure", func(t *testing.T) {
		// No results from select, should cause the foreign key verification to fail.
		fakeRes := sqltypes.MakeTestResult(sqltypes.MakeTestFields("1", "int64"), "1", "1", "1")
		vc := newDMLTestVCursor("0")
		vc.results = []*sqltypes.Result{fakeRes}
		_, err := fkc.TryExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true)
		require.ErrorContains(t, err, "Cannot delete or update a parent row: a foreign key constraint fails")
		vc.ExpectLog(t, []string{
			`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
			`ExecuteMultiShard ks.0: select 1 from grandchild g join child c on g.cola = c.cola and g.colb = c.colb where c.foo = 48 {} false false`,
		})

		vc.Rewind()
		err = fkc.TryStreamExecute(context.Background(), vc, map[string]*querypb.BindVariable{}, true, func(result *sqltypes.Result) error { return nil })
		require.ErrorContains(t, err, "Cannot delete or update a parent row: a foreign key constraint fails")
		vc.ExpectLog(t, []string{
			`ResolveDestinations ks [] Destinations:DestinationAllShards()`,
			`ExecuteMultiShard ks.0: select 1 from grandchild g join child c on g.cola = c.cola and g.colb = c.colb where c.foo = 48 {} false false`,
		})
	})
}
