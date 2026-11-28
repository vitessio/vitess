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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/vschemawrapper"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func TestUnionSortMetadataStatementHints(t *testing.T) {
	env := vtenv.NewTestEnv()
	vw, err := vschemawrapper.NewVschemaWrapper(env, loadSchema(t, "vschemas/schema.json", true), TestBuilder)
	require.NoError(t, err)
	query := "select /*+ MAX_EXECUTION_TIME(5) SET_VAR(sort_buffer_size=32768) RESOURCE_GROUP(rg) NO_ICP(t) */ name from review3_alias t"
	stmt, err := env.Parser().Parse(query)
	require.NoError(t, err)
	route := &engine.Route{QueryStatement: stmt, FieldQuery: sqlparser.NewTrackedBuffer(sqlparser.FormatImpossibleQuery).WriteNode(stmt).ParsedQuery().Query}
	metadata, err := unionSortCollationQuery(&plancontext.PlanningContext{VSchema: vw}, route, []int{0})
	require.NoError(t, err)
	outer := metadata.(*engine.Route).QueryStatement.(*sqlparser.Select)
	t.Logf("metadata wrapper: %s", sqlparser.String(outer))
	assert.Equal(t, sqlparser.Comments{"/*+ MAX_EXECUTION_TIME(5) SET_VAR(sort_buffer_size=32768) RESOURCE_GROUP(rg) */"}, outer.Comments.GetComments())
	assert.NotContains(t, sqlparser.String(outer.Comments), "NO_ICP")
}
