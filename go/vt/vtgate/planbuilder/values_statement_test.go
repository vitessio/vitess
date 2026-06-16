/*
Copyright 2026 The Vitess Authors.

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
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/vschemawrapper"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestValuesStatementPlanning(t *testing.T) {
	env := vtenv.NewTestEnv()
	vschema := vindexes.BuildVSchema(&vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"main": {Sharded: false},
		},
	}, sqlparser.NewTestParser())
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(t, err)
	vw.Keyspace = vschema.Keyspaces["main"].Keyspace

	tests := []string{
		"values row(1)",
		"values row('top-level VALUES ORDER BY generated name', 'values row(1) order by column_0', 1) order by column_0",
		"values row('top-level VALUES ORDER BY ordinal', 'values row(1) order by 1', 1) order by 1",
		"values row(2, 'b'), row(1, 'a') order by column_0",
		"select 1 union values row(1)",
		"select * from (select 1) a union values row(1)",
		"values row(1) union select 2",
		"values row(1) union values row(2)",
		"select * from (values row(1) union select 2) as dt",
		"select * from (values row(1) union values row(2)) as dt",
	}

	for _, query := range tests {
		t.Run(query, func(t *testing.T) {
			var plan *engine.Plan
			require.NotPanics(t, func() {
				plan, err = TestBuilder(query, vw, vw.CurrentDb())
			})
			require.NoError(t, err)
			require.NotNil(t, plan.Instructions)
			if query == "values row('top-level VALUES ORDER BY generated name', 'values row(1) order by column_0', 1) order by column_0" {
				route, ok := plan.Instructions.(*engine.Route)
				require.True(t, ok)
				require.Equal(t, "values row('top-level VALUES ORDER BY generated name', 'values row(1) order by column_0', 1) limit 0", route.FieldQuery)
			}
		})
	}
}

func TestValuesStatementPlanningRewritesOrderByGeneratedColumns(t *testing.T) {
	env := vtenv.NewTestEnv()
	vschema := vindexes.BuildVSchema(&vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"main": {Sharded: false},
		},
	}, sqlparser.NewTestParser())
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(t, err)
	vw.Keyspace = vschema.Keyspaces["main"].Keyspace

	tests := []struct {
		query         string
		routeQuery    string
		truncateCount int
	}{
		{
			query:      "values row(2, 'b'), row(1, 'a') order by column_0",
			routeQuery: "values row(2, 'b'), row(1, 'a') order by 1 asc",
		},
		{
			query:         "values row('case', 'sql', 1, 10), row('case', 'sql', 2, 20) order by column_2 + column_3 desc",
			routeQuery:    "values row('case', 'sql', 1, 10, 1 + 10), row('case', 'sql', 2, 20, 2 + 20) order by 5 desc",
			truncateCount: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			plan, err := TestBuilder(tt.query, vw, vw.CurrentDb())
			require.NoError(t, err)
			route, ok := plan.Instructions.(*engine.Route)
			require.True(t, ok)
			require.Equal(t, tt.routeQuery, route.Query)
			require.Equal(t, tt.truncateCount, route.TruncateColumnCount)
		})
	}
}

func TestValuesStatementPlanningRejectsSubqueries(t *testing.T) {
	env := vtenv.NewTestEnv()
	vschema := vindexes.BuildVSchema(&vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"main": {Sharded: false},
		},
	}, sqlparser.NewTestParser())
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(t, err)
	vw.Keyspace = vschema.Keyspaces["main"].Keyspace

	tests := []struct {
		query string
		err   string
	}{
		{
			query: "values row((select sku from product limit 1))",
			err:   "VT12001: unsupported: subqueries in VALUES statements",
		},
		{
			query: "select * from (values row((select sku from product limit 1))) as dt",
			err:   "VT12001: unsupported: subqueries in VALUES statements",
		},
		{
			query: "select (values row(1))",
			err:   "VT12001: unsupported: VALUES statements in subqueries",
		},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			_, err := TestBuilder(tt.query, vw, vw.CurrentDb())
			require.ErrorContains(t, err, tt.err)
		})
	}
}

func TestValuesStatementUnionAgainstShardedTable(t *testing.T) {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(t, "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(t, err)
	vw.Keyspace = vschema.Keyspaces["user"].Keyspace

	var plan *engine.Plan
	require.NotPanics(t, func() {
		plan, err = TestBuilder("select id from user union values row(1)", vw, vw.CurrentDb())
	})
	require.NoError(t, err)
	require.NotNil(t, plan.Instructions)
}
