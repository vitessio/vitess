/*
Copyright 2021 The Vitess Authors.

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

package planbuilder

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/test/vschemawrapper"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

// collationInTable allows us to set a collation on a column
type collationInTable struct {
	ks, table, collationName string
	colName                  string
}

type collationTestCase struct {
	query      string
	check      func(t *testing.T, colls []collationInTable, primitive engine.Primitive)
	collations []collationInTable
}

func (tc *collationTestCase) run(t *testing.T) {
	vschemaWrapper := &vschemawrapper.VSchemaWrapper{
		V:             loadSchema(t, "vschemas/schema.json", false),
		SysVarEnabled: true,
		Version:       Gen4,
		Env:           vtenv.NewTestEnv(),
	}

	tc.addCollationsToSchema(vschemaWrapper)
	plan, err := TestBuilder(tc.query, vschemaWrapper, vschemaWrapper.CurrentDb())
	require.NoError(t, err)
	tc.check(t, tc.collations, plan.Instructions)
}

func (tc *collationTestCase) addCollationsToSchema(vschema *vschemawrapper.VSchemaWrapper) {
	for _, collation := range tc.collations {
		tbl := vschema.V.Keyspaces[collation.ks].Tables[collation.table]
		for i, c := range tbl.Columns {
			if c.Name.EqualString(collation.colName) {
				tbl.Columns[i].CollationName = collation.collationName
			}
		}
	}
}

func TestOrderedAggregateCollations(t *testing.T) {
	collid := func(collname string) collations.ID {
		return collations.MySQL8().LookupByName(collname)
	}
	testCases := []collationTestCase{
		{
			collations: []collationInTable{{ks: "user", table: "user", collationName: "utf8mb4_bin", colName: "textcol1"}},
			query:      "select textcol1 from user group by textcol1",
			check: func(t *testing.T, colls []collationInTable, primitive engine.Primitive) {
				oa, isOA := primitive.(*engine.OrderedAggregate)
				require.True(t, isOA, "should be an OrderedAggregate")
				require.Equal(t, collid(colls[0].collationName), oa.GroupByKeys[0].Type.Collation())
			},
		},
		{
			collations: []collationInTable{{ks: "user", table: "user", collationName: "utf8mb4_bin", colName: "textcol1"}},
			query:      "select distinct textcol1 from user",
			check: func(t *testing.T, colls []collationInTable, primitive engine.Primitive) {
				distinct, isDistinct := primitive.(*engine.Distinct)
				require.True(t, isDistinct, "should be a distinct")
				require.Equal(t, collid(colls[0].collationName), distinct.CheckCols[0].Type.Collation())
			},
		},
		{
			collations: []collationInTable{
				{ks: "user", table: "user", collationName: "utf8mb4_bin", colName: "textcol1"},
				{ks: "user", table: "user", collationName: "utf8mb4_bin", colName: "textcol2"},
			},
			query: "select textcol1, textcol2 from user group by textcol1, textcol2",
			check: func(t *testing.T, colls []collationInTable, primitive engine.Primitive) {
				oa, isOA := primitive.(*engine.OrderedAggregate)
				require.True(t, isOA, "should be an OrderedAggregate")
				require.Equal(t, collid(colls[0].collationName), oa.GroupByKeys[0].Type.Collation())
				require.Equal(t, collid(colls[1].collationName), oa.GroupByKeys[1].Type.Collation())
			},
		},
		{
			collations: []collationInTable{
				{ks: "user", table: "user", collationName: "utf8mb4_bin", colName: "textcol2"},
			},
			query: "select count(*), textcol2 from user group by textcol2",
			check: func(t *testing.T, colls []collationInTable, primitive engine.Primitive) {
				oa, isOA := primitive.(*engine.OrderedAggregate)
				require.True(t, isOA, "should be an OrderedAggregate")
				require.Equal(t, collid(colls[0].collationName), oa.GroupByKeys[0].Type.Collation())
			},
		},
		{
			collations: []collationInTable{
				{ks: "user", table: "user", collationName: "utf8mb4_bin", colName: "textcol2"},
			},
			query: "select count(*) as c, textcol2 from user group by textcol2 order by c",
			check: func(t *testing.T, colls []collationInTable, primitive engine.Primitive) {
				memSort, isMemSort := primitive.(*engine.MemorySort)
				require.True(t, isMemSort, "should be a MemorySort")
				oa, isOA := memSort.Input.(*engine.OrderedAggregate)
				require.True(t, isOA, "should be an OrderedAggregate")
				require.Equal(t, collid(colls[0].collationName), oa.GroupByKeys[0].Type.Collation())
			},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d %s", i+1, tc.query), func(t *testing.T) {
			tc.run(t)
		})
	}
}
