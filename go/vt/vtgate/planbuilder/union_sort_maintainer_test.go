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

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/vschemawrapper"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func TestUnionSortKnownNumericAvoidsMetadata(t *testing.T) {
	env := vtenv.NewTestEnv()
	vw, err := vschemawrapper.NewVschemaWrapper(env, loadSchema(t, "vschemas/schema.json", true), TestBuilder)
	require.NoError(t, err)
	q := "select intcol from user union all select col14 from unsharded_fk_allow.u_tbl1 order by intcol limit 10"
	plan, err := TestBuilder(q, vw, vw.CurrentDb())
	require.NoError(t, err)
	sort := engine.Find(func(p engine.Primitive) bool { _, ok := p.(*engine.MemorySort); return ok }, plan.Instructions).(*engine.MemorySort)
	require.NotEqual(t, sqltypes.Unknown, sort.OrderBy[0].Type.Type())
	require.NotNil(t, sort.UpperLimit, "the global count must bound the UNION heap")
	require.Empty(t, sort.CollationQueries, "known numeric UNIONs must avoid runtime collation probes")
}

func TestUnionSortOriginalStatementEligibility(t *testing.T) {
	env := vtenv.NewTestEnv()
	vw, err := vschemawrapper.NewVschemaWrapper(env, loadSchema(t, "vschemas/schema.json", true), TestBuilder)
	require.NoError(t, err)
	for _, q := range []string{"select name,sum(id) from unsharded where name='a'", "select distinct name,id from unsharded"} {
		t.Run(q, func(t *testing.T) {
			stmt, err := env.Parser().Parse(q)
			require.NoError(t, err)
			route := &engine.Route{QueryStatement: stmt, FieldQuery: sqlparser.NewTrackedBuffer(sqlparser.FormatImpossibleQuery).WriteNode(stmt).ParsedQuery().Query}
			metadata, err := unionSortCollationQuery(&plancontext.PlanningContext{VSchema: vw}, route, []int{0})
			require.NoError(t, err)
			if q == "select name,sum(id) from unsharded where name='a'" {
				require.Nil(t, metadata, "implicit aggregation must be excluded")
			} else {
				require.NotNil(t, metadata, "DISTINCT is preserved inside the original arm")
			}
		})
	}
}

func TestUnionSortKnownSharedColumnsAvoidMetadata(t *testing.T) {
	env := vtenv.NewTestEnv()
	vw, err := vschemawrapper.NewVschemaWrapper(env, loadSchema(t, "vschemas/schema.json", true), TestBuilder)
	require.NoError(t, err)
	plan, err := TestBuilder("select textcol1 as x from user union all select col1 from unsharded_fk_allow.u_tbl1 order by x limit 10", vw, vw.CurrentDb())
	require.NoError(t, err)
	sort := engine.Find(func(p engine.Primitive) bool { _, ok := p.(*engine.MemorySort); return ok }, plan.Instructions).(*engine.MemorySort)
	require.True(t, sort.UnionCollationKnown)
	require.Empty(t, sort.CollationQueries)
}
