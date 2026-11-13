/*
Copyright 2019 The Vitess Authors.

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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/nsf/jsondiff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/test/vschemawrapper"
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sidecardb"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var expectedDir = "testdata/expected"

func getTestExpectationDir() string {
	return filepath.Clean(expectedDir)
}

type planTestSuite struct {
	suite.Suite
	outputDir string
}

func (s *planTestSuite) SetupSuite() {
	dir := getTestExpectationDir()
	err := os.RemoveAll(dir)
	s.Require().NoError(err)
	err = os.Mkdir(dir, 0o755)
	s.Require().NoError(err)
	s.outputDir = dir
}

func TestPlanTestSuite(t *testing.T) {
	suite.Run(t, new(planTestSuite))
}

func (s *planTestSuite) TestPlan() {
	defer utils.EnsureNoLeaks(s.T())

	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)

	s.addPKs(vschema, "user", []string{"user", "music"})
	s.addPKsProvided(vschema, "user", []string{"user_extra"}, []string{"id", "user_id"})
	s.addPKsProvided(vschema, "ordering", []string{"order"}, []string{"oid", "region_id"})
	s.addPKsProvided(vschema, "ordering", []string{"order_event"}, []string{"oid", "ename"})
	s.addPKsProvided(vschema, "main", []string{"source_of_ref"}, []string{"id"})

	// You will notice that some tests expect user.Id instead of user.id.
	// This is because we now pre-create vindex columns in the symbol
	// table, which come from vschema. In the test vschema,
	// the column is named as Id. This is to make sure that
	// column names are case-preserved, but treated as
	// case-insensitive even if they come from the vschema.
	s.testFile("aggr_cases.json", vw, false)
	s.testFile("dml_cases.json", vw, false)
	s.testFile("from_cases.json", vw, false)
	s.testFile("filter_cases.json", vw, false)
	s.testFile("postprocess_cases.json", vw, false)
	s.testFile("select_cases.json", vw, false)
	s.testFile("window_function_cases.json", vw, false)
	s.testFile("symtab_cases.json", vw, false)
	s.testFile("unsupported_cases.json", vw, false)
	s.testFile("unknown_schema_cases.json", vw, false)
	s.testFile("vindex_func_cases.json", vw, false)
	s.testFile("wireup_cases.json", vw, false)
	s.testFile("memory_sort_cases.json", vw, false)
	s.testFile("use_cases.json", vw, false)
	s.testFile("set_cases.json", vw, false)
	s.testFile("union_cases.json", vw, false)
	s.testFile("large_union_cases.json", vw, false)
	s.testFile("transaction_cases.json", vw, false)
	s.testFile("lock_cases.json", vw, false)
	s.testFile("large_cases.json", vw, false)
	s.testFile("ddl_cases_no_default_keyspace.json", vw, false)
	s.testFile("flush_cases_no_default_keyspace.json", vw, false)
	s.testFile("show_cases_no_default_keyspace.json", vw, false)
	s.testFile("stream_cases.json", vw, false)
	s.testFile("info_schema80_cases.json", vw, false)
	s.testFile("reference_cases.json", vw, false)
	s.testFile("vexplain_cases.json", vw, false)
	s.testFile("misc_cases.json", vw, false)
	s.testFile("cte_cases.json", vw, false)
}

func (s *planTestSuite) TestReferenceUnionLookupMissUsesFallbackRoute() {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)
	s.addPKs(vschema, "user", []string{"user", "music"})

	plan, err := TestBuilder("select id from ref union all select id from music where id = 1", vw, vw.CurrentDb())
	s.Require().NoError(err)
	lookup, ok := plan.Instructions.(*engine.VindexLookup)
	s.Require().True(ok)
	s.Require().True(lookup.SendTo.NoRoutesSpecialHandling)
}

func (s *planTestSuite) TestMixedUnionAllAndDistinctLimitWithoutSchemaTracking() {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	for _, tableName := range []string{"user", "music"} {
		vschema.Keyspaces["user"].Tables[tableName].Columns = nil
	}
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)
	s.addPKs(vschema, "user", []string{"user", "music"})

	for _, query := range []string{
		"select id from user union all select id from music union select id from user",
		"select id from user union all select id from music union select id from user limit 5",
		"select id from user union all select id from music union select id from user order by id limit 5",
	} {
		_, err := TestBuilder(query, vw, vw.CurrentDb())
		s.Require().NoError(err, query)
	}

	plan, err := TestBuilder("select cast(id as signed) as id from ref union all select cast(id as signed) from user union select cast(id as signed) from music order by id limit 5", vw, vw.CurrentDb())
	s.Require().NoError(err)
	routes := routePrimitives(plan.Instructions)
	s.Require().NotEmpty(routes)
	for _, route := range routes {
		s.Require().NotContains(route.Query, "dt.c0 as id, id,", "hidden columns must reference the derived UNION output")
		s.Require().NotContains(route.Query, "weight_string(id)", "weight strings must reference the derived UNION output")
	}
}

func routePrimitives(root engine.Primitive) []*engine.Route {
	if root == nil {
		return nil
	}
	if route, ok := root.(*engine.Route); ok {
		return []*engine.Route{route}
	}
	inputs, _ := root.Inputs()
	var routes []*engine.Route
	for _, input := range inputs {
		routes = append(routes, routePrimitives(input)...)
	}
	return routes
}

func hasUnboundCTEReference(root engine.Primitive, insideRecursiveTerm bool) bool {
	if root == nil {
		return false
	}
	if recurse, ok := root.(*engine.RecurseCTE); ok {
		return hasUnboundCTEReference(recurse.Seed, false) || hasUnboundCTEReference(recurse.Term, true)
	}
	if route, ok := root.(*engine.Route); ok && !insideRecursiveTerm {
		for _, value := range route.Values {
			if sqlparser.String(value) == ":c_id" {
				return true
			}
		}
	}
	inputs, _ := root.Inputs()
	for _, input := range inputs {
		if hasUnboundCTEReference(input, insideRecursiveTerm) {
			return true
		}
	}
	return false
}

func (s *planTestSuite) TestDistinctUnionLimitIsPushedIntoLocalRoute() {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	for _, tableName := range []string{"user", "music"} {
		vschema.Keyspaces["user"].Tables[tableName].Columns = nil
	}
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)
	s.addPKs(vschema, "user", []string{"user", "music"})

	for _, query := range []string{
		"select id from user union select id from music limit 10",
		"select id from user union select 3 limit 10",
		"select col from user union select col from unsharded limit 5",
	} {
		plan, err := TestBuilder(query, vw, vw.CurrentDb())
		s.Require().NoError(err, query)
		routes := routePrimitives(plan.Instructions)
		s.Require().NotEmpty(routes, query)
		for _, route := range routes {
			s.Require().Contains(strings.ToLower(route.Query), "limit", query)
		}
	}
}

func (s *planTestSuite) TestOuterDistinctOverUnionAllDoesNotPushLimitIntoArms() {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)
	s.addPKs(vschema, "user", []string{"user"})

	plan, err := TestBuilder("select distinct id from (select id from user union all select id from unsharded) t limit 5", vw, vw.CurrentDb())
	s.Require().NoError(err)
	routes := routePrimitives(plan.Instructions)
	s.Require().NotEmpty(routes)
	for _, route := range routes {
		s.Require().NotContains(strings.ToLower(route.Query), "limit :__upper_limit")
	}

	ordered, err := TestBuilder("select distinct id from (select id from user union all select id from unsharded) t order by id limit 5", vw, vw.CurrentDb())
	s.Require().NoError(err)
	orderedRoutes := routePrimitives(ordered.Instructions)
	s.Require().NotEmpty(orderedRoutes)
	for _, route := range orderedRoutes {
		s.Require().NotContains(strings.ToLower(route.Query), "limit :__upper_limit")
	}
}

func (s *planTestSuite) TestMixedUnionOrderingAcrossKeyspaces() {
	for _, tracked := range []bool{false, true} {
		env := vtenv.NewTestEnv()
		vschema := loadSchema(s.T(), "vschemas/schema.json", true)
		if !tracked {
			for _, name := range []string{"user", "music"} {
				vschema.Keyspaces["user"].Tables[name].Columns = nil
			}
		}
		vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
		s.Require().NoError(err)
		s.addPKs(vschema, "user", []string{"user", "music"})
		for _, query := range []string{
			"select id from ref union all select id from user union select id from unsharded order by id",
			"select id from user union all select id from unsharded union select id from user order by id",
			"select id from user union all select id from unsharded union select id from user order by id limit 5",
			"select col from user union all select col from unsharded union select col from user order by col limit 5",
			"select cast(col as signed) as c from user union all select cast(col as signed) from unsharded union select cast(col as signed) from user order by c limit 5",
			"select cast(col as signed) as c from user union select cast(col as signed) from unsharded order by c limit 5",
			"select col from user union select col from music order by col limit 5",
		} {
			plan, err := TestBuilder(query, vw, vw.CurrentDb())
			s.Require().NoError(err, "tracking=%t query=%s", tracked, query)
			for _, route := range routePrimitives(plan.Instructions) {
				if strings.Contains(strings.ToLower(route.Query), "limit :__upper_limit") {
					s.Require().Contains(strings.ToLower(route.Query), "order by", query)
					s.Require().Contains(strings.ToLower(route.Query), "select distinct", query)
				}
			}
		}
	}
}

func (s *planTestSuite) TestUnknownCollationDistinctUnionKeepsWeightStringForSort() {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	for _, tableName := range []string{"user", "music"} {
		vschema.Keyspaces["user"].Tables[tableName].Columns = nil
	}
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)
	s.addPKs(vschema, "user", []string{"user", "music"})

	plan, err := TestBuilder("select name from user union select name from music order by name", vw, vw.CurrentDb())
	s.Require().NoError(err)
	sort, ok := plan.Instructions.(*engine.MemorySort)
	s.Require().True(ok)
	s.Require().NotEmpty(sort.OrderBy)
	s.Require().GreaterOrEqual(sort.OrderBy[0].WeightStringCol, 0,
		"unknown field collation must still compare using the route's weight string")
}

func (s *planTestSuite) TestReferenceLeftJoinUnionKeepsReferenceInputSeparate() {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)
	s.addPKs(vschema, "user", []string{"user", "music"})

	plan, err := TestBuilder("select id from (select r.id from ref r left join user u on u.id = r.id union all select id from music) as t", vw, vw.CurrentDb())
	s.Require().NoError(err)
	concatenate, ok := plan.Instructions.(*engine.Concatenate)
	s.Require().True(ok, "reference-preserving LEFT JOIN must remain a separate UNION ALL input")
	s.Require().Len(concatenate.Sources, 2)
	_, ok = concatenate.Sources[0].(*engine.Join)
	s.Require().True(ok, "LEFT JOIN input must not be merged into the scatter route")
}

func (s *planTestSuite) TestMergedDualLeftJoinUnionInputStaysSingleShard() {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)
	s.addPKs(vschema, "user", []string{"user", "music"})

	plan, err := TestBuilder("select t.title from (select 'x' as title from dual) as t left join user u on u.id = 1 union all select 'y' from music", vw, vw.CurrentDb())
	s.Require().NoError(err)
	concatenate, ok := plan.Instructions.(*engine.Concatenate)
	s.Require().True(ok, "merged dual LEFT JOIN input must not widen a later UNION ALL to scatter")
	s.Require().Len(concatenate.Sources, 2)
	first, ok := concatenate.Sources[0].(*engine.Route)
	s.Require().True(ok)
	s.Require().Equal(engine.EqualUnique, first.Opcode)
}

func (s *planTestSuite) TestSpecialUnionProvenanceSurvivesEveryMergeSite() {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)
	s.addPKs(vschema, "user", []string{"user", "music"})

	queries := []string{
		"select id from user where id = 1 union all select id from ref union all select id from user where id = null union all select id from user",
		"select id from user where id = 1 union all select id from ref union all select id from user",
		"select id from ref union all select id from user where id = 1 union all select id from user where id = 1 union all select id from user",
		"select (select id from user where id = 1) from dual union all select id from user",
		"select t.id from (select id from user where id = 1 union all select id from ref) t where t.id in (select u2.id from user u2 where u2.id = 1) or t.id = 5 union all select id from user",
	}
	for _, query := range queries {
		s.Run(query, func() {
			plan, err := TestBuilder(query, vw, vw.CurrentDb())
			s.Require().NoError(err)
			s.Require().IsType(&engine.Concatenate{}, plan.Instructions,
				"special UNION ALL inputs must stay outside the later scatter route")
		})
	}
}

func (s *planTestSuite) TestReferenceLeftJoinCanMergeIntoRightRoute() {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)
	s.addPKs(vschema, "user", []string{"user", "music"})

	for _, query := range []string{
		"select r.col from ref r join (select id from user) u on u.id = r.col",
		"with c as (select id from user where id = 1) select r.col from ref r join c on c.id = r.col",
		"select ref.col from ref straight_join user on user.id = ref.col",
		"select r.col from ref r left join (select id from user where id = 1) u on u.id = r.col",
	} {
		s.Run(query, func() {
			plan, err := TestBuilder(query, vw, vw.CurrentDb())
			s.Require().NoError(err)
			s.Require().IsType(&engine.Route{}, plan.Instructions,
				"the reference-left join should route through the right side without a per-reference nested loop")
		})
	}
}

func (s *planTestSuite) TestReferenceOuterJoinDMLKeepsSingleRoutePlans() {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)
	s.addPKs(vschema, "user", []string{"user", "music"})

	for _, query := range []string{
		"update ref r left join user_metadata m on m.user_id = r.id set m.email = 'x'",
		"update ref_with_source r left join user u on u.id = r.id set r.col = 1",
	} {
		s.Run(query, func() {
			plan, err := TestBuilder(query, vw, vw.CurrentDb())
			s.Require().NoError(err)
			update, ok := plan.Instructions.(*engine.Update)
			s.Require().True(ok)
			s.Require().Equal(engine.Scatter, update.Opcode)
		})
	}
}

func (s *planTestSuite) TestReferenceOuterJoinHonorsCrossKeyspaceReadPolicy() {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vschema.Keyspaces["main"].PreventCrossKeyspaceReads = true
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)
	s.addPKs(vschema, "user", []string{"user"})

	plan, err := TestBuilder("select r.id from source_of_ref r left join user u on u.id = r.id", vw, vw.CurrentDb())
	s.Require().NoError(err)
	routes := routePrimitives(plan.Instructions)
	s.Require().NotEmpty(routes)
	for _, route := range routes {
		s.Require().Equal("user", route.Keyspace.Name)
	}
}

func (s *planTestSuite) TestReferenceUnionProvenanceSurvivesMergedCTE() {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)
	s.addPKs(vschema, "user", []string{"user", "music"})

	plan, err := TestBuilder("with recursive cte as (select ue.id, ue.foo from user u join user_extra ue on u.id = ue.user_id union all select sr.id, sr.foo from cte join main.source_of_ref sr on sr.foo = cte.foo join main.rerouted_ref rr on rr.bar = sr.bar) select id from cte union all select id from music", vw, vw.CurrentDb())
	s.Require().NoError(err)
	_, ok := plan.Instructions.(*engine.Concatenate)
	s.Require().True(ok, "merged recursive CTE with reference provenance must remain separate from a later scatter UNION ALL")
}

func (s *planTestSuite) TestRecursiveReferenceUnionDoesNotLeaveUnboundCTEBind() {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)
	s.addPKs(vschema, "user", []string{"user"})

	plan, err := TestBuilder("with recursive c as (select id from ref union all select u.id from user u join c on u.id = c.id) select id from c union all select id from user", vw, vw.CurrentDb())
	s.Require().NoError(err)
	s.Require().False(hasUnboundCTEReference(plan.Instructions, false), "recursive CTE bind must be owned by the recursive term")
	s.Require().NotNil(engine.Find(func(primitive engine.Primitive) bool {
		_, ok := primitive.(*engine.RecurseCTE)
		return ok
	}, plan.Instructions), "replicated seed must be evaluated once by VTGate")
}

// TestForeignKeyPlanning tests the planning of foreign keys in a managed mode by Vitess.
func (s *planTestSuite) TestForeignKeyPlanning() {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)

	s.setFks(vschema)
	s.testFile("foreignkey_cases.json", vw, false)
}

// TestForeignKeyChecksOn tests the planning when the session variable for foreign_key_checks is set to ON.
func (s *planTestSuite) TestForeignKeyChecksOn() {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)

	fkChecksState := true
	vw.ForeignKeyChecksState = &fkChecksState

	s.setFks(vschema)
	s.testFile("foreignkey_checks_on_cases.json", vw, false)
}

// TestForeignKeyChecksOff tests the planning when the session variable for foreign_key_checks is set to OFF.
func (s *planTestSuite) TestForeignKeyChecksOff() {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)

	fkChecksState := false
	vw.ForeignKeyChecksState = &fkChecksState

	s.setFks(vschema)
	s.testFile("foreignkey_checks_off_cases.json", vw, false)
}

func (s *planTestSuite) setFks(vschema *vindexes.VSchema) {
	if vschema.Keyspaces["sharded_fk_allow"] != nil {
		// FK from multicol_tbl2 referencing multicol_tbl1 that is shard scoped.
		_ = vschema.AddForeignKey("sharded_fk_allow", "multicol_tbl2", createFkDefinition([]string{"colb", "cola", "x", "colc", "y"}, "multicol_tbl1", []string{"colb", "cola", "y", "colc", "x"}, sqlparser.Cascade, sqlparser.Cascade))

		// FK from tbl2 referencing tbl1 that is shard scoped.
		_ = vschema.AddForeignKey("sharded_fk_allow", "tbl2", createFkDefinition([]string{"col2"}, "tbl1", []string{"col1"}, sqlparser.Restrict, sqlparser.Restrict))
		_ = vschema.AddForeignKey("sharded_fk_allow", "tbl2", createFkDefinition([]string{"col2", "col"}, "tbl1", []string{"col1", "col"}, sqlparser.Restrict, sqlparser.Restrict))
		// FK from tbl3 referencing tbl1 that is not shard scoped.
		_ = vschema.AddForeignKey("sharded_fk_allow", "tbl3", createFkDefinition([]string{"coly"}, "tbl1", []string{"t1col1"}, sqlparser.DefaultAction, sqlparser.DefaultAction))
		// FK from tbl10 referencing tbl2 that is shard scoped.
		_ = vschema.AddForeignKey("sharded_fk_allow", "tbl10", createFkDefinition([]string{"sk", "col"}, "tbl2", []string{"col2", "col"}, sqlparser.Restrict, sqlparser.Restrict))
		// FK from tbl10 referencing tbl3 that is not shard scoped.
		_ = vschema.AddForeignKey("sharded_fk_allow", "tbl10", createFkDefinition([]string{"col"}, "tbl3", []string{"col"}, sqlparser.Restrict, sqlparser.Restrict))

		// FK from tbl4 referencing tbl5 that is shard scoped.
		_ = vschema.AddForeignKey("sharded_fk_allow", "tbl4", createFkDefinition([]string{"col4"}, "tbl5", []string{"col5"}, sqlparser.SetNull, sqlparser.Cascade))
		_ = vschema.AddForeignKey("sharded_fk_allow", "tbl4", createFkDefinition([]string{"t4col4"}, "tbl5", []string{"t5col5"}, sqlparser.SetNull, sqlparser.Cascade))

		// FK from tbl5 referencing tbl8 that is shard scoped of SET-NULL types.
		_ = vschema.AddForeignKey("sharded_fk_allow", "tbl5", createFkDefinition([]string{"col5"}, "tbl8", []string{"col8"}, sqlparser.SetNull, sqlparser.SetNull))

		// FK from tbl4 referencing tbl9 that is not shard scoped of SET-NULL types.
		_ = vschema.AddForeignKey("sharded_fk_allow", "tbl4", createFkDefinition([]string{"col_ref"}, "tbl9", []string{"col9"}, sqlparser.SetNull, sqlparser.SetNull))

		// FK from tbl6 referencing tbl7 that is shard scoped.
		_ = vschema.AddForeignKey("sharded_fk_allow", "tbl6", createFkDefinition([]string{"col6"}, "tbl7", []string{"col7"}, sqlparser.NoAction, sqlparser.NoAction))
		_ = vschema.AddForeignKey("sharded_fk_allow", "tbl6", createFkDefinition([]string{"t6col6"}, "tbl7", []string{"t7col7"}, sqlparser.NoAction, sqlparser.NoAction))
		_ = vschema.AddForeignKey("sharded_fk_allow", "tbl6", createFkDefinition([]string{"t6col62"}, "tbl7", []string{"t7col72"}, sqlparser.NoAction, sqlparser.NoAction))

		// FK from tblrefDef referencing tbl20 that is shard scoped of SET-Default types.
		_ = vschema.AddForeignKey("sharded_fk_allow", "tblrefDef", createFkDefinition([]string{"ref"}, "tbl20", []string{"col2"}, sqlparser.SetDefault, sqlparser.SetDefault))

		// FK from tbl_auth referencing tbl20 that is shard scoped of CASCADE types.
		_ = vschema.AddForeignKey("sharded_fk_allow", "tbl_auth", createFkDefinition([]string{"id"}, "tbl20", []string{"col2"}, sqlparser.Cascade, sqlparser.Cascade))
		s.addPKs(vschema, "sharded_fk_allow", []string{
			"tbl1", "tbl2", "tbl3", "tbl4", "tbl5", "tbl6", "tbl7", "tbl9", "tbl10",
			"multicol_tbl1", "multicol_tbl2", "tbl_auth", "tblrefDef", "tbl20",
		})
	}
	if vschema.Keyspaces["unsharded_fk_allow"] != nil {
		// u_tbl2(col2)  				-> u_tbl1(col1)  				Cascade.
		// u_tbl4(col41) 				-> u_tbl1(col14) 				Restrict.
		// u_tbl9(col9)  				-> u_tbl1(col1)  				Cascade Null.
		// u_tbl3(col2)  				-> u_tbl2(col2)  				Cascade Null.
		// u_tbl4(col4)  				-> u_tbl3(col3)  				Restrict.
		// u_tbl6(col6)  				-> u_tbl5(col5)  				Restrict.
		// u_tbl8(col8)  				-> u_tbl9(col9)  				Null Null.
		// u_tbl8(col8)  				-> u_tbl6(col6)  				Cascade Null.
		// u_tbl4(col4)  				-> u_tbl7(col7)  				Cascade Cascade.
		// u_tbl9(col9)  				-> u_tbl4(col4)  				Restrict Restrict.
		// u_tbl12(parent_id) 			-> u_tbl12(id)  				Restrict Restrict.
		// u_multicol_tbl2(cola, colb)  -> u_multicol_tbl1(cola, colb)  Null Null.
		// u_multicol_tbl3(cola, colb)  -> u_multicol_tbl2(cola, colb)  Cascade Cascade.

		_ = vschema.AddForeignKey("unsharded_fk_allow", "u_tbl2", createFkDefinition([]string{"col2"}, "u_tbl1", []string{"col1"}, sqlparser.Cascade, sqlparser.Cascade))
		_ = vschema.AddForeignKey("unsharded_fk_allow", "u_tbl9", createFkDefinition([]string{"col9"}, "u_tbl1", []string{"col1"}, sqlparser.SetNull, sqlparser.NoAction))
		_ = vschema.AddForeignKey("unsharded_fk_allow", "u_tbl4", createFkDefinition([]string{"col41"}, "u_tbl1", []string{"col14"}, sqlparser.NoAction, sqlparser.NoAction))
		_ = vschema.AddForeignKey("unsharded_fk_allow", "u_tbl3", createFkDefinition([]string{"col3"}, "u_tbl2", []string{"col2"}, sqlparser.SetNull, sqlparser.SetNull))
		_ = vschema.AddForeignKey("unsharded_fk_allow", "u_tbl4", createFkDefinition([]string{"col4"}, "u_tbl3", []string{"col3"}, sqlparser.Restrict, sqlparser.Restrict))
		_ = vschema.AddForeignKey("unsharded_fk_allow", "u_tbl6", createFkDefinition([]string{"col6"}, "u_tbl5", []string{"col5"}, sqlparser.DefaultAction, sqlparser.DefaultAction))
		_ = vschema.AddForeignKey("unsharded_fk_allow", "u_tbl8", createFkDefinition([]string{"col8"}, "u_tbl9", []string{"col9"}, sqlparser.SetNull, sqlparser.SetNull))
		_ = vschema.AddForeignKey("unsharded_fk_allow", "u_tbl8", createFkDefinition([]string{"col8"}, "u_tbl6", []string{"col6"}, sqlparser.Cascade, sqlparser.Cascade))
		_ = vschema.AddForeignKey("unsharded_fk_allow", "u_tbl4", createFkDefinition([]string{"col4"}, "u_tbl7", []string{"col7"}, sqlparser.Cascade, sqlparser.Cascade))
		_ = vschema.AddForeignKey("unsharded_fk_allow", "u_tbl9", createFkDefinition([]string{"col9"}, "u_tbl4", []string{"col4"}, sqlparser.Restrict, sqlparser.Restrict))
		_ = vschema.AddForeignKey("unsharded_fk_allow", "u_tbl11", createFkDefinition([]string{"col"}, "u_tbl10", []string{"col"}, sqlparser.Cascade, sqlparser.Cascade))
		_ = vschema.AddForeignKey("unsharded_fk_allow", "u_tbl", createFkDefinition([]string{"col"}, "sharded_fk_allow.s_tbl", []string{"col"}, sqlparser.Restrict, sqlparser.Restrict))

		_ = vschema.AddForeignKey("unsharded_fk_allow", "u_multicol_tbl2", createFkDefinition([]string{"cola", "colb"}, "u_multicol_tbl1", []string{"cola", "colb"}, sqlparser.SetNull, sqlparser.SetNull))
		_ = vschema.AddForeignKey("unsharded_fk_allow", "u_multicol_tbl3", createFkDefinition([]string{"cola", "colb"}, "u_multicol_tbl2", []string{"cola", "colb"}, sqlparser.Cascade, sqlparser.Cascade))

		_ = vschema.AddUniqueKey("unsharded_fk_allow", "u_tbl9", []sqlparser.Expr{sqlparser.NewColName("col9")})
		_ = vschema.AddUniqueKey("unsharded_fk_allow", "u_tbl9", []sqlparser.Expr{&sqlparser.BinaryExpr{Operator: sqlparser.MultOp, Left: sqlparser.NewColName("col9"), Right: sqlparser.NewColName("foo")}})
		_ = vschema.AddUniqueKey("unsharded_fk_allow", "u_tbl9", []sqlparser.Expr{sqlparser.NewColName("col9"), sqlparser.NewColName("foo")})
		_ = vschema.AddUniqueKey("unsharded_fk_allow", "u_tbl9", []sqlparser.Expr{sqlparser.NewColName("foo"), sqlparser.NewColName("bar")})
		_ = vschema.AddUniqueKey("unsharded_fk_allow", "u_tbl9", []sqlparser.Expr{sqlparser.NewColName("bar"), sqlparser.NewColName("col9")})
		_ = vschema.AddUniqueKey("unsharded_fk_allow", "u_tbl8", []sqlparser.Expr{sqlparser.NewColName("col8")})

		// FK from u_tbl12 that is self-referential.
		_ = vschema.AddForeignKey("unsharded_fk_allow", "u_tbl12", createFkDefinition([]string{"parent_id"}, "u_tbl12", []string{"id"}, sqlparser.Restrict, sqlparser.Restrict))

		s.addPKs(vschema, "unsharded_fk_allow", []string{
			"u_tbl1", "u_tbl2", "u_tbl3", "u_tbl4", "u_tbl5", "u_tbl6", "u_tbl7", "u_tbl8", "u_tbl9", "u_tbl10", "u_tbl11", "u_tbl12",
			"u_multicol_tbl1", "u_multicol_tbl2", "u_multicol_tbl3",
		})
	}
}

func (s *planTestSuite) addPKs(vschema *vindexes.VSchema, ks string, tbls []string) {
	for _, tbl := range tbls {
		s.Require().NoError(vschema.AddPrimaryKey(ks, tbl, []string{"id"}))
	}
}

func (s *planTestSuite) addPKsProvided(vschema *vindexes.VSchema, ks string, tbls []string, pks []string) {
	for _, tbl := range tbls {
		s.Require().NoError(vschema.AddPrimaryKey(ks, tbl, pks))
	}
}

func (s *planTestSuite) TestSystemTables57() {
	// first we move everything to use 5.7 logic
	env, err := vtenv.New(vtenv.Options{
		MySQLServerVersion: "5.7.9",
	})
	s.Require().NoError(err)
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)

	s.testFile("info_schema57_cases.json", vw, false)
}

func (s *planTestSuite) TestSysVarSetDisabled() {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)

	vw.SysVarEnabled = false

	s.testFile("set_sysvar_disabled_cases.json", vw, false)
}

func (s *planTestSuite) TestViews() {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)

	vw.EnableViews = true

	s.testFile("view_cases.json", vw, false)
}

func (s *planTestSuite) TestOne() {
	reset := operators.EnableDebugPrinting()
	defer reset()

	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)

	s.setFks(vschema)
	s.addPKs(vschema, "user", []string{"user", "music"})
	s.addPKs(vschema, "main", []string{"unsharded"})
	s.addPKsProvided(vschema, "user", []string{"user_extra"}, []string{"id", "user_id"})
	s.addPKsProvided(vschema, "ordering", []string{"order"}, []string{"oid", "region_id"})
	s.addPKsProvided(vschema, "ordering", []string{"order_event"}, []string{"oid", "ename"})
	s.addPKsProvided(vschema, "main", []string{"source_of_ref"}, []string{"id"})

	s.testFile("onecase.json", vw, false)
}

func (s *planTestSuite) TestOneTPCC() {
	reset := operators.EnableDebugPrinting()
	defer reset()

	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/tpcc_schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)

	s.testFile("onecase.json", vw, false)
}

func (s *planTestSuite) TestOneWithMainAsDefault() {
	reset := operators.EnableDebugPrinting()
	defer reset()

	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)

	vw.Vcursor.SetTarget("main")
	vw.Keyspace = &vindexes.Keyspace{Name: "main"}

	s.testFile("onecase.json", vw, false)
}

func (s *planTestSuite) TestOneWithSecondUserAsDefault() {
	reset := operators.EnableDebugPrinting()
	defer reset()

	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)

	vw.Vcursor.SetTarget("second_user")
	vw.Keyspace = &vindexes.Keyspace{
		Name:    "second_user",
		Sharded: true,
	}

	s.testFile("onecase.json", vw, false)
}

func (s *planTestSuite) TestOneWithUserAsDefault() {
	reset := operators.EnableDebugPrinting()
	defer reset()

	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)

	vw.Vcursor.SetTarget("user")
	vw.Keyspace = &vindexes.Keyspace{
		Name:    "user",
		Sharded: true,
	}

	s.testFile("onecase.json", vw, false)
}

func (s *planTestSuite) TestOneWithTPCHVSchema() {
	reset := operators.EnableDebugPrinting()
	defer reset()

	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/tpch_schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)

	s.testFile("onecase.json", vw, false)
}

func (s *planTestSuite) TestOneWith57Version() {
	reset := operators.EnableDebugPrinting()
	defer reset()
	// first we move everything to use 5.7 logic
	env, err := vtenv.New(vtenv.Options{
		MySQLServerVersion: "5.7.9",
	})
	s.Require().NoError(err)
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)

	s.testFile("onecase.json", vw, false)
}

func (s *planTestSuite) TestRubyOnRailsQueries() {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/rails_schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)

	s.testFile("rails_cases.json", vw, false)
}

func (s *planTestSuite) TestOLTP() {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/oltp_schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)

	s.testFile("oltp_cases.json", vw, false)
}

func (s *planTestSuite) TestTPCC() {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/tpcc_schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)

	s.testFile("tpcc_cases.json", vw, false)
}

func (s *planTestSuite) TestTPCH() {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/tpch_schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)

	s.testFile("tpch_cases.json", vw, false)
}

func BenchmarkOLTP(b *testing.B) {
	benchmarkWorkload(b, "oltp")
}

func BenchmarkTPCC(b *testing.B) {
	benchmarkWorkload(b, "tpcc")
}

func BenchmarkTPCH(b *testing.B) {
	benchmarkWorkload(b, "tpch")
}

func benchmarkWorkload(b *testing.B, name string) {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(b, "vschemas/"+name+"_schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(b, err)

	testCases := readJSONTests(name + "_cases.json")
	b.ResetTimer()
	benchmarkPlanner(b, Gen4, testCases, vw)
}

func (s *planTestSuite) TestBypassPlanningShardTargetFromFile() {
	vschema := &vschemawrapper.VSchemaWrapper{
		V: loadSchema(s.T(), "vschemas/schema.json", true),
		Keyspace: &vindexes.Keyspace{
			Name:    "main",
			Sharded: false,
		},
		TabletType_: topodatapb.TabletType_PRIMARY,
		Dest:        key.DestinationShard("-80"),
		Env:         vtenv.NewTestEnv(),
	}

	s.testFile("bypass_shard_cases.json", vschema, false)
}

func (s *planTestSuite) TestBypassPlanningKeyrangeTargetFromFile() {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)

	keyRange, _ := key.ParseShardingSpec("-")
	vw.Dest = key.DestinationExactKeyRange{KeyRange: keyRange[0]}

	vw.Vcursor.SetTarget("main")
	vw.Keyspace = &vindexes.Keyspace{Name: "main"}

	s.testFile("bypass_keyrange_cases.json", vw, false)
}

func (s *planTestSuite) TestWithDefaultKeyspaceFromFile() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// We are testing this separately so we can set a default keyspace
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)

	vw.Vcursor.SetTarget("main")
	vw.Keyspace = &vindexes.Keyspace{Name: "main"}

	ts := memorytopo.NewServer(ctx, "cell1")
	ts.CreateKeyspace(ctx, "main", &topodatapb.Keyspace{})
	ts.CreateKeyspace(ctx, "user", &topodatapb.Keyspace{})
	// Create a cache to use for lookups of the sidecar database identifier
	// in use by each keyspace.
	_, created := sidecardb.NewIdentifierCache(func(ctx context.Context, keyspace string) (string, error) {
		ki, err := ts.GetKeyspace(ctx, keyspace)
		if err != nil {
			return "", err
		}
		return ki.SidecarDbName, nil
	})
	s.Require().True(created)

	s.testFile("alterVschema_cases.json", vw, false)
	s.testFile("ddl_cases.json", vw, false)
	s.testFile("migration_cases.json", vw, false)
	s.testFile("flush_cases.json", vw, false)
	s.testFile("show_cases.json", vw, false)
	s.testFile("call_cases.json", vw, false)
}

func (s *planTestSuite) TestSingleUnshardedKeyspace() {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/unsharded_schema.json", false)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)

	s.testFile("unsharded_cases.json", vw, false)
}

func (s *planTestSuite) TestWithDefaultKeyspaceFromFileSharded() {
	// We are testing this separately so we can set a default keyspace
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)

	vw.Vcursor.SetTarget("second_user")
	vw.Keyspace = &vindexes.Keyspace{
		Name:    "second_user",
		Sharded: true,
	}

	s.testFile("select_cases_with_default.json", vw, false)
}

func (s *planTestSuite) TestWithUserDefaultKeyspaceFromFileSharded() {
	// We are testing this separately so we can set a default keyspace
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)

	vw.Vcursor.SetTarget("user")
	vw.Keyspace = &vindexes.Keyspace{
		Name:    "user",
		Sharded: true,
	}

	s.testFile("select_cases_with_user_as_default.json", vw, false)
	s.testFile("dml_cases_with_user_as_default.json", vw, false)
}

func (s *planTestSuite) TestWithSystemSchemaAsDefaultKeyspace() {
	// We are testing this separately so we can set a default keyspace
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)

	vw.Keyspace = &vindexes.Keyspace{Name: "information_schema"}

	s.testFile("sysschema_default.json", vw, false)
}

func (s *planTestSuite) TestOtherPlanningFromFile() {
	// We are testing this separately so we can set a default keyspace
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)

	s.testFile("other_read_cases.json", vw, false)

	vw.Vcursor.SetTarget("main")
	vw.Keyspace = &vindexes.Keyspace{Name: "main"}
	s.testFile("other_admin_cases.json", vw, false)
}

func (s *planTestSuite) TestMirrorPlanning() {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/mirror_schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)

	s.testFile("mirror_cases.json", vw, false)
}

func (s *planTestSuite) TestOneMirror() {
	reset := operators.EnableDebugPrinting()
	defer reset()

	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)

	s.testFile("onecase.json", vw, false)
}

func loadSchema(t testing.TB, filename string, setCollation bool) *vindexes.VSchema {
	formal, err := vindexes.LoadFormal(locateFile(filename))
	require.NoError(t, err)
	vschema := vindexes.BuildVSchema(formal, sqlparser.NewTestParser())
	require.NoError(t, err)
	for _, ks := range vschema.Keyspaces {
		require.NoError(t, ks.Error)

		// adding view in user keyspace
		if ks.Keyspace.Name == "user" {
			err = vschema.AddView(ks.Keyspace.Name, "user_details_view", "select user.id, user_extra.col from user join user_extra on user.id = user_extra.user_id", sqlparser.NewTestParser())
			require.NoError(t, err)
			err = vschema.AddUDF(ks.Keyspace.Name, "udf_aggr")
			require.NoError(t, err)
		}

		// setting a default value to all the text columns in the tables of this keyspace
		// so that we can "simulate" a real case scenario where the vschema is aware of
		// columns' collations.
		if setCollation {
			for _, table := range ks.Tables {
				for i, col := range table.Columns {
					if sqltypes.IsText(col.Type) && col.CollationName == "" {
						table.Columns[i].CollationName = "latin1_swedish_ci"
					}
				}
			}
		}
	}
	return vschema
}

// createFkDefinition is a helper function to create a Foreign key definition struct from the columns used in it provided as list of strings.
func createFkDefinition(childCols []string, parentTableName string, parentCols []string, onUpdate, onDelete sqlparser.ReferenceAction) *sqlparser.ForeignKeyDefinition {
	pKs, pTbl, _ := sqlparser.NewTestParser().ParseTable(parentTableName)
	return &sqlparser.ForeignKeyDefinition{
		Source: sqlparser.MakeColumns(childCols...),
		ReferenceDefinition: &sqlparser.ReferenceDefinition{
			ReferencedTable:   sqlparser.NewTableNameWithQualifier(pTbl, pKs),
			ReferencedColumns: sqlparser.MakeColumns(parentCols...),
			OnUpdate:          onUpdate,
			OnDelete:          onDelete,
		},
	}
}

func (s *planTestSuite) testFile(filename string, vschema *vschemawrapper.VSchemaWrapper, render bool) {
	opts := jsondiff.DefaultConsoleOptions()

	s.Run(filename, func() {
		t := s.T()
		failed := false
		var expected []PlanTest
		for _, tcase := range readJSONTests(filename) {
			testName := tcase.Comment
			if testName == "" {
				testName = tcase.Query
			}
			if tcase.Query == "" {
				continue
			}
			current := PlanTest{
				Comment: tcase.Comment,
				Query:   tcase.Query,
				SkipE2E: tcase.SkipE2E,
			}
			vschema.Version = Gen4
			out := getPlanOutput(tcase, vschema, render)

			// our expectation for the planner on the query is one of three
			// - produces same plan as expected
			// - produces a different plan than expected
			// - fails to produce a plan
			t.Run(testName, func(t *testing.T) {
				defer func() {
					if t.Failed() {
						failed = true
					}
				}()
				compare, s := jsondiff.Compare(tcase.Plan, []byte(out), &opts)
				if compare != jsondiff.FullMatch {
					message := fmt.Sprintf("%s\nDiff:\n%s\n[%s] \n[%s]", filename, s, tcase.Plan, out)
					if tcase.Skip {
						t.Skip(message)
					} else {
						assert.Fail(t, message)
					}
				} else if tcase.Skip {
					assert.Failf(t, "skipped query is correct", "query is correct even though it is skipped:\n %s", tcase.Query)
				}
				current.Plan = []byte(out)
			})
			expected = append(expected, current)
		}
		if s.outputDir != "" && failed {
			name := strings.TrimSuffix(filename, filepath.Ext(filename))
			name = filepath.Join(s.outputDir, name+".json")
			file, err := os.Create(name)
			require.NoError(t, err)
			enc := json.NewEncoder(file)
			enc.SetEscapeHTML(false)
			enc.SetIndent("", "  ")
			err = enc.Encode(expected)
			if err != nil {
				require.NoError(t, err)
			}
		}
	})
}

func readJSONTests(filename string) []PlanTest {
	var output []PlanTest
	file, err := os.Open(locateFile(filename))
	if err != nil {
		panic(err)
	}
	dec := json.NewDecoder(file)
	dec.DisallowUnknownFields()
	err = dec.Decode(&output)
	if err != nil {
		panic(err)
	}
	return output
}

func getPlanOutput(tcase PlanTest, vschema *vschemawrapper.VSchemaWrapper, render bool) (out string) {
	defer func() {
		if r := recover(); r != nil {
			out = fmt.Sprintf("panicked: %v\n%s", r, string(debug.Stack()))
		}
	}()
	plan, err := TestBuilder(tcase.Query, vschema, vschema.CurrentDb())
	if render && plan != nil {
		viz, err := engine.GraphViz(plan.Instructions)
		if err == nil {
			_ = viz.Render()
		}
	}
	return getPlanOrErrorOutput(err, plan)
}

func getPlanOrErrorOutput(err error, plan *engine.Plan) string {
	if err != nil {
		return "\"" + err.Error() + "\""
	}
	b := new(bytes.Buffer)
	enc := json.NewEncoder(b)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	err = enc.Encode(plan)
	if err != nil {
		panic(err)
	}
	return b.String()
}

func locateFile(name string) string {
	return "testdata/" + name
}

var benchMarkFiles = []string{"from_cases.json", "filter_cases.json", "large_cases.json", "aggr_cases.json", "select_cases.json", "union_cases.json"}

func BenchmarkPlanner(b *testing.B) {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(b, "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(b, err)

	for _, filename := range benchMarkFiles {
		testCases := readJSONTests(filename)
		b.Run(filename+"-gen4", func(b *testing.B) {
			benchmarkPlanner(b, Gen4, testCases, vw)
		})
	}
}

func BenchmarkSemAnalysis(b *testing.B) {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(b, "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(b, err)

	for b.Loop() {
		for _, filename := range benchMarkFiles {
			for _, tc := range readJSONTests(filename) {
				exerciseAnalyzer(tc.Query, vw.CurrentDb(), vw)
			}
		}
	}
}

func exerciseAnalyzer(query, database string, s semantics.SchemaInformation) {
	defer func() {
		// if analysis panics, let's just continue. this is just a benchmark
		recover()
	}()

	ast, err := sqlparser.NewTestParser().Parse(query)
	if err != nil {
		return
	}
	sel, ok := ast.(sqlparser.SelectStatement)
	if !ok {
		return
	}

	_, _ = semantics.Analyze(sel, database, s)
}

func BenchmarkSelectVsDML(b *testing.B) {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(b, "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(b, err)

	dmlCases := readJSONTests("dml_cases.json")
	selectCases := readJSONTests("select_cases.json")

	rand.Shuffle(len(dmlCases), func(i, j int) {
		dmlCases[i], dmlCases[j] = dmlCases[j], dmlCases[i]
	})

	rand.Shuffle(len(selectCases), func(i, j int) {
		selectCases[i], selectCases[j] = selectCases[j], selectCases[i]
	})

	b.Run("DML (random sample, N=32)", func(b *testing.B) {
		benchmarkPlanner(b, Gen4, dmlCases[:32], vw)
	})

	b.Run("Select (random sample, N=32)", func(b *testing.B) {
		benchmarkPlanner(b, Gen4, selectCases[:32], vw)
	})
}

func BenchmarkBaselineVsMirrored(b *testing.B) {
	env := vtenv.NewTestEnv()
	baseline := loadSchema(b, "vschemas/mirror_schema.json", true)
	baseline.MirrorRules = map[string]*vindexes.MirrorRule{}
	bvw, err := vschemawrapper.NewVschemaWrapper(env, baseline, TestBuilder)
	require.NoError(b, err)

	mirroredSchema := loadSchema(b, "vschemas/mirror_schema.json", true)
	mvw, err := vschemawrapper.NewVschemaWrapper(env, mirroredSchema, TestBuilder)
	require.NoError(b, err)

	cases := readJSONTests("mirror_cases.json")

	b.Run("Baseline", func(b *testing.B) {
		benchmarkPlanner(b, Gen4, cases, bvw)
	})

	b.Run("Mirrored", func(b *testing.B) {
		benchmarkPlanner(b, Gen4, cases, mvw)
	})
}

func benchmarkPlanner(b *testing.B, version plancontext.PlannerVersion, testCases []PlanTest, vschema *vschemawrapper.VSchemaWrapper) {
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		for _, tcase := range testCases {
			if len(tcase.Plan) > 0 {
				vschema.Version = version
				_, _ = TestBuilder(tcase.Query, vschema, vschema.CurrentDb())
			}
		}
	}
}

// DML joins can merge, but read-only subqueries inside the same statement must
// still preserve reference and dual rows on the left of an outer join.
func (s *planTestSuite) TestDMLSubqueryOuterJoinsPreserveRows() {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)
	s.addPKs(vschema, "user", []string{"user", "music"})
	for _, query := range []string{
		"update user set col = (select count(*) from (select 1 as x from dual) d left join music m on m.col = 5) where id = 1",
		"update user set col = (select count(*) from ref r left join music m on m.col = r.col) where id = 1",
		"update user set col = (select count(*) from ref r left join music m on m.id = r.col) where id = 1",
	} {
		plan, err := TestBuilder(query, vw, vw.CurrentDb())
		s.Require().NoError(err, query)
		s.Require().NotNil(engine.Find(func(p engine.Primitive) bool {
			join, ok := p.(*engine.Join)
			return ok && join.Opcode == engine.LeftJoin
		}, plan.Instructions), query)
	}
}

// A refused merge of a replicated seed cannot fall back to VTGate DISTINCT
// recursion, which does not deduplicate across levels and shards.
func (s *planTestSuite) TestRecursiveDistinctRequiresSingleShard() {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)
	s.addPKs(vschema, "user", []string{"user"})
	for _, seed := range []string{"select id from ref", "select 1 as id from dual"} {
		query := "with recursive c as (" + seed + " union select u.id from user u join c on u.id = c.id) select id from c"
		_, err := TestBuilder(query, vw, vw.CurrentDb())
		s.Require().ErrorContains(err, "VT12001", query)
		s.Require().ErrorContains(err, "recursive UNION DISTINCT", query)
	}
	for _, seed := range []string{"select id from ref", "select 1 as id from dual"} {
		query := "with recursive c as (" + seed + " union select u.id from user u join c on u.id = 5) select id from c"
		plan, err := TestBuilder(query, vw, vw.CurrentDb())
		s.Require().NoError(err, query)
		routes := routePrimitives(plan.Instructions)
		s.Require().Len(routes, 1, query)
		s.Require().Equal(engine.EqualUnique, routes[0].Opcode, query)
		widened, err := TestBuilder(query+" union all select id from user", vw, vw.CurrentDb())
		s.Require().NoError(err)
		s.Require().IsType(&engine.Concatenate{}, widened.Instructions, "a merged reference-seeded DISTINCT CTE must not widen under a later UNION ALL")
	}
}

// Rejecting unsafe replicated-seed merges must not reject existing recursive
// DISTINCT plans over sharded tables or an unsharded seed in another keyspace.
func (s *planTestSuite) TestRecursiveDistinctPreservesExistingPlans() {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)
	s.addPKs(vschema, "user", []string{"user"})
	for _, query := range []string{
		"with recursive c as (select id, manager_id from user where id = 5 union select u.id, u.manager_id from user u join c on u.id = c.manager_id) select * from c",
		"with recursive cte as (select name, id from user where manager_id is null union select e.name, e.id from user e inner join cte on e.manager_id = cte.id) select name from cte",
		"with recursive c as (select id from unsharded union select u.id from user u join c on u.id = c.id) select id from c",
	} {
		plan, err := TestBuilder(query, vw, vw.CurrentDb())
		s.Require().NoError(err, query)
		s.Require().NotNil(engine.Find(func(p engine.Primitive) bool {
			_, ok := p.(*engine.RecurseCTE)
			return ok
		}, plan.Instructions), query)
	}
}

// Choose reference copies before seed projection planning and enforce the
// policy on actual remaining keyspaces if recursion stays in VTGate.
func (s *planTestSuite) TestRecursiveCTEReferenceCopiesAndReadPolicy() {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(s.T(), "vschemas/schema.json", true)
	vschema.Keyspaces["main"].PreventCrossKeyspaceReads = true
	vschema.Keyspaces["user"].PreventCrossKeyspaceReads = true
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	s.Require().NoError(err)
	s.addPKs(vschema, "user", []string{"user"})
	for _, query := range []string{
		"with recursive c as (select id from source_of_ref union all select u.id from user u join c on u.col = c.id) select id from c",
		"with recursive c as (select r.id as x from source_of_ref r union all select u.id from user u join c on u.col = c.x) select x from c",
	} {
		plan, err := TestBuilder(query, vw, vw.CurrentDb())
		s.Require().NoError(err, query)
		routes := routePrimitives(plan.Instructions)
		s.Require().NotEmpty(routes, query)
		for _, route := range routes {
			s.Require().Equal("user", route.Keyspace.Name, query)
		}
		s.Require().Contains(routes[0].Query, "ref_with_source", query)
	}
	// An already-projected seed cannot be replaced by a bare alternate. Keep
	// the projection and require an override for the actual cross-keyspace read.
	derived := "with recursive c as (select x from (select id as x from source_of_ref where id = 42) t union all select u.id from user u join c on u.col = c.x) select x from c"
	_, err = TestBuilder(derived, vw, vw.CurrentDb())
	s.Require().ErrorContains(err, "cross-keyspace", derived)
	derivedOverride := strings.Replace(derived, "select x from c", "select /*vt+ ALLOW_CROSS_KEYSPACE_READS */ x from c", 1)
	plan, err := TestBuilder(derivedOverride, vw, vw.CurrentDb())
	s.Require().NoError(err, derivedOverride)
	routes := routePrimitives(plan.Instructions)
	s.Require().NotEmpty(routes)
	s.Require().Contains(routes[0].Query, "= 42")
	s.Require().Contains(routes[0].Query, " as x")
	query := "with recursive c as (select id from unsharded union all select u.id from user u join c on u.col = c.id) select id from c"
	_, err = TestBuilder(query, vw, vw.CurrentDb())
	s.Require().ErrorContains(err, "cross-keyspace", query)
	override := strings.Replace(query, "select id from c", "select /*vt+ ALLOW_CROSS_KEYSPACE_READS */ id from c", 1)
	_, err = TestBuilder(override, vw, vw.CurrentDb())
	s.Require().NoError(err, override)
}
