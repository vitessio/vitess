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
	"vitess.io/vitess/go/vt/vtgate/engine"
)

// collationInTable allows us to set a collation on a column
type collationInTable struct {
	ks, table, collationName string
	offsetInTable            int
}

type collationTestCase struct {
	query      string
	check      func(t *testing.T, colls []collationInTable, primitive engine.Primitive)
	collations []collationInTable
}

func (tc *collationTestCase) run(t *testing.T) {
	vschemaWrapper := &vschemaWrapper{
		v:             loadSchema(t, "schema_test.json"),
		sysVarEnabled: true,
		version:       Gen4,
	}

	tc.addCollationsToSchema(vschemaWrapper)
	plan, err := TestBuilder(tc.query, vschemaWrapper, vschemaWrapper.currentDb())
	require.NoError(t, err)
	tc.check(t, tc.collations, plan.Instructions)
}

func (tc *collationTestCase) addCollationsToSchema(vschema *vschemaWrapper) {
	for _, collation := range tc.collations {
		vschema.v.Keyspaces[collation.ks].Tables[collation.table].Columns[collation.offsetInTable].CollationName = collation.collationName
	}
}

func TestOrderedAggregateCollations(t *testing.T) {
	testCases := []collationTestCase{
		{
			collations: []collationInTable{{ks: "user", table: "user", collationName: "utf8mb4_bin", offsetInTable: 2}},
			query:      "select textcol1 from user group by textcol1",
			check: func(t *testing.T, colls []collationInTable, primitive engine.Primitive) {
				oa, isOA := primitive.(*engine.OrderedAggregate)
				require.True(t, isOA, "should be an OrderedAggregate")
				require.Equal(t, collations.LookupByName(colls[0].collationName).Id(), oa.GroupByKeys[0].CollationID)
			},
		},
		{
			collations: []collationInTable{{ks: "user", table: "user", collationName: "utf8mb4_bin", offsetInTable: 2}},
			query:      "select distinct textcol1 from user",
			check: func(t *testing.T, colls []collationInTable, primitive engine.Primitive) {
				oa, isOA := primitive.(*engine.OrderedAggregate)
				require.True(t, isOA, "should be an OrderedAggregate")
				require.Equal(t, collations.LookupByName(colls[0].collationName).Id(), oa.GroupByKeys[0].CollationID)
			},
		},
		{
			collations: []collationInTable{
				{ks: "user", table: "user", collationName: "utf8mb4_bin", offsetInTable: 2},
				{ks: "user", table: "user", collationName: "utf8mb4_bin", offsetInTable: 4},
			},
			query: "select textcol1, textcol2 from user group by textcol1, textcol2",
			check: func(t *testing.T, colls []collationInTable, primitive engine.Primitive) {
				oa, isOA := primitive.(*engine.OrderedAggregate)
				require.True(t, isOA, "should be an OrderedAggregate")
				require.Equal(t, collations.LookupByName(colls[0].collationName).Id(), oa.GroupByKeys[0].CollationID)
				require.Equal(t, collations.LookupByName(colls[1].collationName).Id(), oa.GroupByKeys[1].CollationID)
			},
		},
		{
			collations: []collationInTable{
				{ks: "user", table: "user", collationName: "utf8mb4_bin", offsetInTable: 4},
			},
			query: "select count(*), textcol2 from user group by textcol2",
			check: func(t *testing.T, colls []collationInTable, primitive engine.Primitive) {
				oa, isOA := primitive.(*engine.OrderedAggregate)
				require.True(t, isOA, "should be an OrderedAggregate")
				require.Equal(t, collations.LookupByName(colls[0].collationName).Id(), oa.GroupByKeys[0].CollationID)
			},
		},
		{
			collations: []collationInTable{
				{ks: "user", table: "user", collationName: "utf8mb4_bin", offsetInTable: 4},
			},
			query: "select count(*) as c, textcol2 from user group by textcol2 order by c",
			check: func(t *testing.T, colls []collationInTable, primitive engine.Primitive) {
				memSort, isMemSort := primitive.(*engine.MemorySort)
				require.True(t, isMemSort, "should be a MemorySort")
				oa, isOA := memSort.Input.(*engine.OrderedAggregate)
				require.True(t, isOA, "should be an OrderedAggregate")
				require.Equal(t, collations.LookupByName(colls[0].collationName).Id(), oa.GroupByKeys[0].CollationID)
			},
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d %s", i+1, tc.query), func(t *testing.T) {
			tc.run(t)
		})
	}
}
