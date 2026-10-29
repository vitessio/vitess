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
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log/logtest"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/workflow"
	"vitess.io/vitess/go/vt/vtenv"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// TestVDiffLogs checks the VDiff lines that name a tablet, which print the
// alias as zone1-0000000100 rather than as a proto, that the progress line
// logs the row count as a number, and the key of the tablet types setting.
func TestVDiffLogs(t *testing.T) {
	ctx := t.Context()
	env := newTestVDiffEnv(t, ctx, []string{"0"}, []string{"0"}, "", nil)
	t.Cleanup(env.close)
	env.tmc.schema = &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "t1",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
			Schema:            "create table t1(c1 bigint, c2 bigint, primary key(c1))",
		}},
	}
	rows := sqltypes.MakeTestStreamingResults(sqltypes.MakeTestFields("c1|c2", "int64|int64"), "1|3")
	env.tablets[101].setResults("select c1, c2 from t1 order by c1 asc", vdiffSourceGtid, rows)
	env.tablets[201].setResults("select c1, c2 from t1 order by c1 asc", vdiffTargetPrimaryPosition, rows)
	logs := logtest.Capture(t, slog.LevelInfo)

	_, err := env.wr.VDiff(ctx, "target", env.workflow, env.cell, env.cell, "replica", 30*time.Second, "", 100, "", false, false, 100)
	require.NoError(t, err)

	var waits, restarts int
	for _, record := range logs.Records(t) {
		switch record["msg"] {
		case "WaitForPosition: tablet should reach position":
			waits++
			assert.Equal(t, "INFO", record["level"])
			assert.Equal(t, env.workflow, record["workflow"])
			assert.Regexp(t, `^cell-\d{10}$`, record["tablet"])
			assert.NotEmpty(t, record["position"])
		case "Restarting the VReplication workflow":
			restarts++
			assert.Equal(t, "INFO", record["level"])
			assert.Equal(t, env.workflow, record["workflow"])
			assert.Equal(t, "cell-0000000200", record["tablet"])
			assert.Contains(t, record["query"], "update _vt.vreplication set state='Running'")
		}
	}
	assert.Positive(t, waits)
	assert.Equal(t, 1, restarts)

	// The tablet types setting is logged as given, under its own key, since
	// tablet_types is a list elsewhere.
	record := logs.Record(t, "Starting VDiff")
	assert.Equal(t, "replica", record["tablet_types_setting"])
	assert.NotContains(t, record, "tablet_types")

	record = logs.Record(t, "VDiff progress")
	assert.Equal(t, "INFO", record["level"])
	assert.Equal(t, "t1", record["table"])
	assert.EqualValues(t, 0, record["processed_rows"])
}

// TestVDiffPlanIncludeLogsTableNames checks that a table list that doesn't
// match the workflow logs the matched table names, not the differs' pointers.
func TestVDiffPlanIncludeLogsTableNames(t *testing.T) {
	td := func(name string) *tabletmanagerdatapb.TableDefinition {
		return &tabletmanagerdatapb.TableDefinition{
			Name:              name,
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
			Schema:            "create table " + name + "(c1 bigint, c2 bigint, primary key(c1))",
		}
	}
	schm := &tabletmanagerdatapb.SchemaDefinition{TableDefinitions: []*tabletmanagerdatapb.TableDefinition{td("t3"), td("t1"), td("t4"), td("t2")}}
	df := &vdiff{env: vtenv.NewTestEnv(), workflow: "wf"}
	filter := &binlogdatapb.Filter{Rules: []*binlogdatapb.Rule{{Match: "/.*"}}}
	logs := logtest.Capture(t, slog.LevelInfo)

	require.Error(t, df.buildVDiffPlan(filter, schm, []string{"t3", "t1", "t4", "t2", "t5"}))

	record := logs.Record(t, "one or more tables provided are not present in the workflow")
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, "wf", record["workflow"])
	assert.Equal(t, []any{"t3", "t1", "t4", "t2", "t5"}, record["tables"])
	assert.Equal(t, []any{"t1", "t2", "t3", "t4"}, record["matched_tables"])
	assert.NotContains(t, logs.String(), "0x")

	// When no table matches, the matched tables are an empty list, not null.
	logs.Reset()
	require.Error(t, df.buildVDiffPlan(filter, schm, []string{"t5"}))

	record = logs.Record(t, "one or more tables provided are not present in the workflow")
	assert.Equal(t, []any{"t5"}, record["tables"])
	assert.Equal(t, []any{}, record["matched_tables"])
}

// TestSwitchReadsLogsLists checks that switching reads logs the tablet types
// and cells as lists, and that no cells, which means all cells, logs an
// empty list rather than null.
func TestSwitchReadsLogsLists(t *testing.T) {
	ctx := t.Context()
	tme := newTestTableMigrater(ctx, t)
	t.Cleanup(func() { tme.close(t) })
	tme.expectNoPreviousJournals()
	logs := logtest.Capture(t, slog.LevelInfo)

	_, err := tme.wr.SwitchReads(ctx, tme.targetKeyspace, "test", []topodatapb.TabletType{topodatapb.TabletType_REPLICA}, nil, workflow.DirectionForward, false)
	require.NoError(t, err)

	record := logs.Record(t, "Switching reads")
	assert.Equal(t, "INFO", record["level"])
	assert.Equal(t, []any{"replica"}, record["tablet_types"])
	assert.Equal(t, []any{}, record["cells"])
	// With no rdonly tablets in the cells, switching replica reads also
	// switches rdonly reads.
	record = logs.Record(t, "switchTableReads")
	assert.Equal(t, []any{"replica", "rdonly"}, record["tablet_types"])
}

// TestGetShardSubsetLogsSelection checks that selecting a subset of shards
// logs the counts under selected and total, as the vtctld line does, and the
// shards as a list.
func TestGetShardSubsetLogsSelection(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "zone1")
	require.NoError(t, ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{}))
	for _, shard := range []string{"-40", "40-80", "80-"} {
		require.NoError(t, ts.CreateShard(ctx, "ks", shard))
	}
	wr := New(vtenv.NewTestEnv(), logutil.NewMemoryLogger(), ts, nil)
	logs := logtest.Capture(t, slog.LevelInfo)

	_, err := wr.getShardSubset(ctx, "ks", []string{"80-", "-40"})
	require.NoError(t, err)

	record := logs.Record(t, "Selecting subset of shards in keyspace")
	assert.Equal(t, "INFO", record["level"])
	assert.Equal(t, "ks", record["keyspace"])
	assert.EqualValues(t, 2, record["selected"])
	assert.EqualValues(t, 3, record["total"])
	assert.Equal(t, []any{"80-", "-40"}, record["shards"])
}

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

// TestTabletExternallyReparentedLogsAlias checks that the alias of a tablet
// that can't be read is logged as zone1-0000000100, not as a proto.
func TestTabletExternallyReparentedLogsAlias(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "zone1")
	wr := New(vtenv.NewTestEnv(), logutil.NewMemoryLogger(), ts, nil)
	logs := logtest.Capture(t, slog.LevelInfo)

	err := wr.TabletExternallyReparented(ctx, &topodatapb.TabletAlias{Cell: "zone1", Uid: 100})
	require.Error(t, err)

	record := logs.Record(t, "TabletExternallyReparented: failed to read tablet record")
	assert.Equal(t, "WARN", record["level"])
	assert.Equal(t, "zone1-0000000100", record["tablet"])
	assert.Equal(t, err.Error(), record["error"])
}

var factoryCount atomic.Int64

// TestMoveTablesExternalTopoLogsClusterName checks that opening an external
// cluster's topo logs the cluster's name rather than the topo.Server struct,
// which printed only pointers.
func TestMoveTablesExternalTopoLogsClusterName(t *testing.T) {
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "zone1")
	_, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
	// Factory names can't be registered twice, so each run uses its own.
	factoryName := fmt.Sprintf("wrangler-logging-test-%d", factoryCount.Add(1))
	topo.RegisterFactory(factoryName, factory)
	require.NoError(t, ts.CreateExternalVitessCluster(ctx, "ext1", &topodatapb.ExternalVitessCluster{
		TopoConfig: &topodatapb.TopoConfig{TopoType: factoryName, Server: "ext1-server", Root: "/vitess/global"},
	}))
	wr := New(vtenv.NewTestEnv(), logutil.NewMemoryLogger(), ts, nil)
	logs := logtest.Capture(t, slog.LevelInfo)

	// The external cluster has no source keyspace, so MoveTables fails after
	// opening its topo.
	err := wr.MoveTables(ctx, "wf", "source", "target", "t1", "", "", false, "", false, false,
		"ext1", false, false, "", "", nil, false, false)
	require.Error(t, err)

	record := logs.Record(t, "Successfully opened external topo")
	assert.Equal(t, "INFO", record["level"])
	assert.Equal(t, "ext1", record["external_cluster"])
	assert.NotContains(t, logs.String(), "globalCell")
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
