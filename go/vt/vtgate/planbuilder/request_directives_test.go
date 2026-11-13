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
	"slices"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/vschemawrapper"
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// TestRequestDirectivesMarker checks, over the planner's test cases, that
// planning a statement whose request-only comment directive blocks are
// replaced with marker blocks, and then restoring the blocks, gives the plan
// that planning the statement with the blocks gives. VTGate relies on this
// to share one plan between statements that differ only in those directives.
func (s *planTestSuite) TestRequestDirectivesMarker() {
	// The second block also has a directive that affects the plan, which its
	// marker block keeps.
	blocks := []string{
		"/*vt+ WORKLOAD_NAME=w1 PRIORITY=10 */",
		"/*vt+ CONSOLIDATOR=enabled SCATTER_ERRORS_AS_WARNINGS IGNORE_MAX_MEMORY_ROWS=1 IGNORE_MAX_PAYLOAD_SIZE=1 */",
	}

	env := vtenv.NewTestEnv()
	newVSchema := func(file string, setCollation bool) (*vindexes.VSchema, *vschemawrapper.VSchemaWrapper) {
		vschema := loadSchema(s.T(), file, setCollation)
		vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
		s.Require().NoError(err)
		return vschema, vw
	}

	type config struct {
		files []string
		vw    *vschemawrapper.VSchemaWrapper
	}
	var configs []config

	vschema, vw := newVSchema("vschemas/schema.json", true)
	s.addPKs(vschema, "user", []string{"user", "music"})
	s.addPKsProvided(vschema, "user", []string{"user_extra"}, []string{"id", "user_id"})
	s.addPKsProvided(vschema, "ordering", []string{"order"}, []string{"oid", "region_id"})
	s.addPKsProvided(vschema, "ordering", []string{"order_event"}, []string{"oid", "ename"})
	s.addPKsProvided(vschema, "main", []string{"source_of_ref"}, []string{"id"})
	configs = append(configs, config{files: []string{
		"aggr_cases.json", "dml_cases.json", "from_cases.json", "filter_cases.json", "postprocess_cases.json",
		"select_cases.json", "window_function_cases.json", "symtab_cases.json", "unsupported_cases.json",
		"unknown_schema_cases.json", "vindex_func_cases.json", "wireup_cases.json", "memory_sort_cases.json",
		"union_cases.json", "large_union_cases.json", "lock_cases.json", "large_cases.json",
		"info_schema80_cases.json", "reference_cases.json", "cte_cases.json",
	}, vw: vw})

	for _, fk := range []struct {
		file   string
		checks *bool
	}{
		{file: "foreignkey_cases.json"},
		{file: "foreignkey_checks_on_cases.json", checks: new(true)},
		{file: "foreignkey_checks_off_cases.json", checks: new(false)},
	} {
		vschema, vw := newVSchema("vschemas/schema.json", true)
		vw.ForeignKeyChecksState = fk.checks
		s.setFks(vschema)
		configs = append(configs, config{files: []string{fk.file}, vw: vw})
	}

	_, vw = newVSchema("vschemas/schema.json", true)
	vw.Vcursor.SetTarget("second_user")
	vw.Keyspace = &vindexes.Keyspace{Name: "second_user", Sharded: true}
	configs = append(configs, config{files: []string{"select_cases_with_default.json"}, vw: vw})

	_, vw = newVSchema("vschemas/schema.json", true)
	vw.Vcursor.SetTarget("user")
	vw.Keyspace = &vindexes.Keyspace{Name: "user", Sharded: true}
	configs = append(configs, config{files: []string{"select_cases_with_user_as_default.json", "dml_cases_with_user_as_default.json"}, vw: vw})

	configs = append(configs, config{files: []string{"bypass_shard_cases.json"}, vw: &vschemawrapper.VSchemaWrapper{
		V:           loadSchema(s.T(), "vschemas/schema.json", true),
		Keyspace:    &vindexes.Keyspace{Name: "main"},
		TabletType_: topodatapb.TabletType_PRIMARY,
		Dest:        key.DestinationShard("-80"),
		Env:         env,
	}})

	_, vw = newVSchema("vschemas/schema.json", true)
	keyRange, err := key.ParseShardingSpec("-")
	s.Require().NoError(err)
	vw.Dest = key.DestinationExactKeyRange{KeyRange: keyRange[0]}
	vw.Vcursor.SetTarget("main")
	vw.Keyspace = &vindexes.Keyspace{Name: "main"}
	configs = append(configs, config{files: []string{"bypass_keyrange_cases.json"}, vw: vw})

	_, vw = newVSchema("vschemas/mirror_schema.json", true)
	configs = append(configs, config{files: []string{"mirror_cases.json"}, vw: vw})

	for _, workload := range []string{"rails", "oltp", "tpcc", "tpch"} {
		_, vw := newVSchema("vschemas/"+workload+"_schema.json", true)
		configs = append(configs, config{files: []string{workload + "_cases.json"}, vw: vw})
	}

	for _, cfg := range configs {
		cfg.vw.Version = Gen4
		for _, file := range cfg.files {
			s.Run(file, func() {
				t := s.T()
				compared := 0
				for _, tcase := range readJSONTests(file) {
					stmt, err := cfg.vw.Environment().Parser().Parse(tcase.Query)
					if err != nil || !sqlparser.CachePlan(stmt) {
						continue
					}
					commented, ok := stmt.(sqlparser.Commented)
					if !ok {
						continue
					}
					comments := commented.GetParsedComments().GetComments()
					commented.SetComments(append(slices.Clone(comments), blocks...))
					withBlocks := sqlparser.String(stmt)

					// The planner reads the text, and a few statements format
					// to text that doesn't parse.
					stmt, err = cfg.vw.Environment().Parser().Parse(withBlocks)
					if err != nil {
						continue
					}
					rd := sqlparser.ReplaceRequestDirectives(stmt)
					require.NotNil(t, rd, withBlocks)
					withMarkers := sqlparser.String(stmt)

					want := getPlanOutput(PlanTest{Query: withBlocks}, cfg.vw, false)
					got := rd.Restore(getPlanOutput(PlanTest{Query: withMarkers}, cfg.vw, false))
					assert.Equal(t, want, got, "query: %s", withBlocks)
					compared++
				}
				t.Logf("compared %d plans", compared)
			})
		}
	}
}
