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

package wrangler

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// TestVDiffFindPKsLogsExpression checks that a select expression findPKs
// can't use is logged as SQL under expression, with its Go type, and that
// its literal stays out of msg.
func TestVDiffFindPKsLogsExpression(t *testing.T) {
	table := &tabletmanagerdatapb.TableDefinition{
		Name:              "t1",
		Columns:           []string{"c2", "c1"},
		PrimaryKeyColumns: []string{"c1"},
		Fields:            sqltypes.MakeTestFields("c2|c1", "varchar|int64"),
		Schema:            "create table t1(c2 varchar(10), c1 bigint, primary key(c1))",
	}
	targetSelect := &sqlparser.Select{
		SelectExprs: &sqlparser.SelectExprs{
			Exprs: []sqlparser.SelectExpr{
				&sqlparser.AliasedExpr{Expr: sqlparser.NewStrLiteral("leaked-value"), As: sqlparser.NewIdentifierCI("c2")},
				&sqlparser.AliasedExpr{Expr: sqlparser.NewColName("c1")},
			},
		},
	}
	td := &tableDiffer{
		compareCols: []compareColInfo{{0, collations.Unknown, nil, false}, {1, collations.Unknown, nil, false}},
		comparePKs:  []compareColInfo{},
		pkCols:      []int{},
	}
	logs := logtest.Capture(t, slog.LevelInfo)

	_, err := findPKs(vtenv.NewTestEnv(), table, targetSelect, td)
	require.NoError(t, err)

	record := logs.Record(t, "Not considering column for PK, type not handled")
	assert.Equal(t, "WARN", record["level"])
	assert.Equal(t, "t1", record["table"])
	assert.Equal(t, "'leaked-value' as c2", record["expression"])
	assert.Equal(t, "*sqlparser.Literal", record["type"])
	for _, record := range logs.Records(t) {
		assert.NotContains(t, record["msg"], "leaked-value")
	}
}

// TestAtomicCopyLogsTableNames checks that an AtomicCopy workflow logs the
// names of the tables it creates, not their CREATE TABLE text, whose
// defaults can hold values from user schemas.
func TestAtomicCopyLogsTableNames(t *testing.T) {
	ms := &vtctldatapb.MaterializeSettings{
		Workflow:       "workflow",
		SourceKeyspace: "sourceks",
		TargetKeyspace: "targetks",
		AtomicCopy:     true,
		TableSettings: []*vtctldatapb.TableMaterializeSettings{{
			TargetTable:      "t1",
			SourceExpression: "select * from t1",
			CreateDdl:        "create table t1 (id int primary key, c1 varchar(20) default 'leaked-default')",
		}},
	}
	env, ctx := newTestMaterializerEnv(t, ms, []string{"0"}, []string{"0"})
	delete(env.tmc.schema, "targetks.t1")
	env.tmc.expectVRQuery(200, mzSelectFrozenQuery, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, "/CREATE TABLE `t1`", &sqltypes.Result{})
	env.tmc.expectVRQuery(200, insertPrefix, &sqltypes.Result{})
	env.tmc.expectVRQuery(200, mzUpdateQuery, &sqltypes.Result{})
	logs := logtest.Capture(t, slog.LevelInfo)

	require.NoError(t, env.wr.Materialize(ctx, ms))
	env.tmc.verifyQueries(t)

	record := logs.Record(t, "AtomicCopy schema")
	assert.Equal(t, "INFO", record["level"])
	assert.Equal(t, []any{"t1"}, record["tables"])
	assert.NotContains(t, logs.String(), "leaked-default")
}
