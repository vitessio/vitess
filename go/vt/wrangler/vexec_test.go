/*
Copyright 2020 The Vitess Authors.

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
	"context"
	_ "embed"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/vt/logutil"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtenv"
)

var (
	//go:embed testdata/show-all-shards.json
	want_show_all_shards string
	//go:embed testdata/show-dash80.json
	want_show_dash_80 string
	//go:embed testdata/show-80dash.json
	want_show_80_dash string
)

func TestVExec(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	workflow := "wrWorkflow"
	keyspace := "target"
	query := "update _vt.vreplication set state = 'Running'"
	env := newWranglerTestEnv(t, ctx, []string{"0"}, []string{"-80", "80-"}, nil, time.Now().Unix())
	defer env.close()
	var logger = logutil.NewMemoryLogger()
	wr := New(vtenv.NewTestEnv(), logger, env.topoServ, env.tmc)

	vx := newVExec(ctx, workflow, keyspace, query, wr)
	err := vx.getPrimaries(nil)
	require.Nil(t, err)
	primaries := vx.primaries
	require.NotNil(t, primaries)
	require.Equal(t, len(primaries), 2)
	var shards []string
	for _, primary := range primaries {
		shards = append(shards, primary.Shard)
	}
	sort.Strings(shards)
	require.Equal(t, fmt.Sprintf("%v", shards), "[-80 80-]")

	plan, err := vx.parseAndPlan(ctx)
	require.NoError(t, err)
	require.NotNil(t, plan)

	addWheres := func(query string) string {
		if strings.Contains(query, " where ") {
			query += " and "
		} else {
			query += " where "
		}
		query += fmt.Sprintf("db_name = %s and workflow = %s", encodeString("vt_"+keyspace), encodeString(workflow))
		return query
	}
	want := addWheres(query)
	require.Equal(t, want, plan.parsedQuery.Query)

	vx.plannedQuery = plan.parsedQuery.Query
	vx.exec()

	res, err := wr.getStreams(ctx, workflow, keyspace, nil)
	require.NoError(t, err)
	require.Less(t, res.MaxVReplicationLag, int64(3 /*seconds*/)) // lag should be very small

	type TestCase struct {
		name        string
		query       string
		result      *sqltypes.Result
		errorString string
	}

	var result *sqltypes.Result
	var testCases []*TestCase
	result = sqltypes.MakeTestResult(sqltypes.MakeTestFields(
		"id|source|message|cell|tablet_types|workflow_type|workflow_sub_type|defer_secondary_keys",
		"int64|varchar|varchar|varchar|varchar|int64|int64|int64"),
		"1|keyspace:\"source\" shard:\"0\" filter:{rules:{match:\"t1\"} rules:{match:\"t2\"}}||||0|0|0",
	)
	testCases = append(testCases, &TestCase{
		name:   "select",
		query:  "select id, source, message, cell, tablet_types, workflow_type, workflow_sub_type, defer_secondary_keys from _vt.vreplication",
		result: result,
	})
	result = &sqltypes.Result{
		RowsAffected: 1,
		Rows:         [][]sqltypes.Value{},
	}
	testCases = append(testCases, &TestCase{
		name:   "delete",
		query:  "delete from _vt.vreplication where message != ''",
		result: result,
	})
	result = &sqltypes.Result{
		RowsAffected: 1,
		Rows:         [][]sqltypes.Value{},
	}
	testCases = append(testCases, &TestCase{
		name:   "update",
		query:  "update _vt.vreplication set state='Stopped', message='for wrangler test'",
		result: result,
	})

	errorString := "query not supported by vexec"
	testCases = append(testCases, &TestCase{
		name:        "insert",
		query:       "insert into _vt.vreplication(state, workflow, db_name) values ('Running', 'wk1', 'ks1'), ('Stopped', 'wk1', 'ks1')",
		errorString: errorString,
	})

	errorString = "table not supported by vexec"
	testCases = append(testCases, &TestCase{
		name:        "delete invalid-other-table",
		query:       "delete from _vt.copy_state",
		errorString: errorString,
	})

	for _, testCase := range testCases {
		t.Run(testCase.query, func(t *testing.T) {
			results, err := wr.VExec(ctx, workflow, keyspace, testCase.query, false)
			if testCase.errorString == "" {
				require.NoError(t, err)
				for _, result := range results {
					if !testCase.result.Equal(result) {
						t.Errorf("mismatched result:\nwant: %v\ngot:  %v", testCase.result, result)
					}
				}
			} else {
				require.Error(t, err)
				if !strings.Contains(err.Error(), testCase.errorString) {
					t.Fatalf("Wrong error, want %s, got %s", testCase.errorString, err.Error())
				}
			}
		})
	}

	query = "delete from _vt.vreplication"
	_, err = wr.VExec(ctx, workflow, keyspace, query, true)
	require.NoError(t, err)
	dryRunResults := []string{
		"Query: delete from _vt.vreplication where db_name = 'vt_target' and workflow = 'wrWorkflow'",
		"will be run on the following streams in keyspace target for workflow wrWorkflow:\n\n",
		`+----------------------+----+--------------------------------+---------+-----------+------------------------------------------+
|        TABLET        | ID |          BINLOGSOURCE          |  STATE  |  DBNAME   |               CURRENT GTID               |
+----------------------+----+--------------------------------+---------+-----------+------------------------------------------+
| -80/zone1-0000000200 |  1 | keyspace:"source" shard:"0"    | Copying | vt_target | 14b68925-696a-11ea-aee7-fec597a91f5e:1-3 |
|                      |    | filter:{rules:{match:"t1"}     |         |           |                                          |
|                      |    | rules:{match:"t2"}}            |         |           |                                          |
+----------------------+----+--------------------------------+---------+-----------+------------------------------------------+
| 80-/zone1-0000000210 |  1 | keyspace:"source" shard:"0"    | Copying | vt_target | 14b68925-696a-11ea-aee7-fec597a91f5e:1-3 |
|                      |    | filter:{rules:{match:"t1"}     |         |           |                                          |
|                      |    | rules:{match:"t2"}}            |         |           |                                          |
+----------------------+----+--------------------------------+---------+-----------+------------------------------------------+`,
	}
	require.Equal(t, strings.Join(dryRunResults, "\n")+"\n\n\n\n\n", logger.String())
	logger.Clear()
}

func TestWorkflowStatusUpdate(t *testing.T) {
	require.Equal(t, binlogdatapb.VReplicationWorkflowState_Running.String(), updateState("for vdiff", binlogdatapb.VReplicationWorkflowState_Running, nil, int64(time.Now().Second())))
	require.Equal(t, binlogdatapb.VReplicationWorkflowState_Running.String(), updateState("", binlogdatapb.VReplicationWorkflowState_Running, nil, int64(time.Now().Second())))
	require.Equal(t, binlogdatapb.VReplicationWorkflowState_Lagging.String(), updateState("", binlogdatapb.VReplicationWorkflowState_Running, nil, int64(time.Now().Second())-100))
	require.Equal(t, binlogdatapb.VReplicationWorkflowState_Copying.String(), updateState("", binlogdatapb.VReplicationWorkflowState_Running, []copyState{{Table: "t1", LastPK: "[[INT64(10)]]"}}, int64(time.Now().Second())))
	require.Equal(t, binlogdatapb.VReplicationWorkflowState_Error.String(), updateState("error: primary tablet not contactable", binlogdatapb.VReplicationWorkflowState_Running, nil, 0))
}

func TestWorkflowListStreams(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	workflow := "wrWorkflow"
	keyspace := "target"
	env := newWranglerTestEnv(t, ctx, []string{"0"}, []string{"-80", "80-"}, nil, 1234)
	defer env.close()
	logger := logutil.NewMemoryLogger()
	wr := New(vtenv.NewTestEnv(), logger, env.topoServ, env.tmc)

	_, err := wr.WorkflowAction(ctx, workflow, keyspace, "listall", false, nil, nil)
	require.NoError(t, err)

	_, err = wr.WorkflowAction(ctx, workflow, "badks", "show", false, nil, nil)
	require.Errorf(t, err, "node doesn't exist: keyspaces/badks/shards")

	_, err = wr.WorkflowAction(ctx, "badwf", keyspace, "show", false, nil, nil)
	require.Errorf(t, err, "no streams found for workflow badwf in keyspace target")
	logger.Clear()
	var testCases = []struct {
		shards []string
		want   string
	}{
		{[]string{"-80", "80-"}, want_show_all_shards},
		{[]string{"-80"}, want_show_dash_80},
		{[]string{"80-"}, want_show_80_dash},
	}
	scrub := func(s string) string {
		s = strings.ReplaceAll(s, "\t", "")
		s = strings.ReplaceAll(s, "\n", "")
		s = strings.ReplaceAll(s, " ", "")
		return s
	}
	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("%v", testCase.shards), func(t *testing.T) {
			want := scrub(testCase.want)
			_, err = wr.WorkflowAction(ctx, workflow, keyspace, "show", false, nil, testCase.shards)
			require.NoError(t, err)
			got := scrub(logger.String())
			// MaxVReplicationLag needs to be reset. This can't be determinable in this kind of a test because
			// time.Now() is constantly shifting.
			re := regexp.MustCompile(`"MaxVReplicationLag":\d+`)
			got = re.ReplaceAllLiteralString(got, `"MaxVReplicationLag":0`)
			re = regexp.MustCompile(`"MaxVReplicationTransactionLag":\d+`)
			got = re.ReplaceAllLiteralString(got, `"MaxVReplicationTransactionLag":0`)
			require.Equal(t, want, got)
			logger.Clear()
		})
	}

	results, err := wr.execWorkflowAction(ctx, workflow, keyspace, "stop", false, nil, nil)
	require.Nil(t, err)

	// convert map to list and sort it for comparison
	var gotResults []string
	for key, result := range results {
		gotResults = append(gotResults, fmt.Sprintf("%s:%v", key.String(), result))
	}
	sort.Strings(gotResults)
	wantResults := []string{"Tablet{zone1-0000000200}:rows_affected:1", "Tablet{zone1-0000000210}:rows_affected:1"}
	sort.Strings(wantResults)
	require.ElementsMatch(t, wantResults, gotResults)

	logger.Clear()
	results, err = wr.execWorkflowAction(ctx, workflow, keyspace, "stop", true, nil, nil)
	require.Nil(t, err)
	require.Equal(t, "map[]", fmt.Sprintf("%v", results))
	dryRunResult := `Query: update _vt.vreplication set state = 'Stopped' where db_name = 'vt_target' and workflow = 'wrWorkflow'
will be run on the following streams in keyspace target for workflow wrWorkflow:


+----------------------+----+--------------------------------+---------+-----------+------------------------------------------+
|        TABLET        | ID |          BINLOGSOURCE          |  STATE  |  DBNAME   |               CURRENT GTID               |
+----------------------+----+--------------------------------+---------+-----------+------------------------------------------+
| -80/zone1-0000000200 |  1 | keyspace:"source" shard:"0"    | Copying | vt_target | 14b68925-696a-11ea-aee7-fec597a91f5e:1-3 |
|                      |    | filter:{rules:{match:"t1"}     |         |           |                                          |
|                      |    | rules:{match:"t2"}}            |         |           |                                          |
+----------------------+----+--------------------------------+---------+-----------+------------------------------------------+
| 80-/zone1-0000000210 |  1 | keyspace:"source" shard:"0"    | Copying | vt_target | 14b68925-696a-11ea-aee7-fec597a91f5e:1-3 |
|                      |    | filter:{rules:{match:"t1"}     |         |           |                                          |
|                      |    | rules:{match:"t2"}}            |         |           |                                          |
+----------------------+----+--------------------------------+---------+-----------+------------------------------------------+




`
	require.Equal(t, dryRunResult, logger.String())
}

func TestWorkflowListAll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	keyspace := "target"
	workflow := "wrWorkflow"
	env := newWranglerTestEnv(t, ctx, []string{"0"}, []string{"-80", "80-"}, nil, 0)
	defer env.close()
	logger := logutil.NewMemoryLogger()
	wr := New(vtenv.NewTestEnv(), logger, env.topoServ, env.tmc)

	workflows, err := wr.ListAllWorkflows(ctx, keyspace, true)
	require.Nil(t, err)
	require.Equal(t, []string{workflow}, workflows)

	workflows, err = wr.ListAllWorkflows(ctx, keyspace, false)
	require.Nil(t, err)
	require.Equal(t, []string{workflow, "wrWorkflow2"}, workflows)
	logger.Clear()
}

func TestVExecValidations(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	workflow := "wf"
	keyspace := "ks"
	query := ""
	env := newWranglerTestEnv(t, ctx, []string{"0"}, []string{"-80", "80-"}, nil, 0)
	defer env.close()

	wr := New(vtenv.NewTestEnv(), logutil.NewConsoleLogger(), env.topoServ, env.tmc)

	vx := newVExec(ctx, workflow, keyspace, query, wr)

	type badQuery struct {
		name        string
		query       string
		errorString string
	}
	badQueries := []badQuery{
		{
			name:        "invalid",
			query:       "bad query",
			errorString: "syntax error at position 4 near 'bad'",
		},
		{
			name:        "incorrect table",
			query:       "select * from _vt.vreplication2",
			errorString: "table not supported by vexec: _vt.vreplication2",
		},
		{
			name:        "unsupported query",
			query:       "describe _vt.vreplication",
			errorString: "query not supported by vexec: explain _vt.vreplication",
		},
	}
	for _, bq := range badQueries {
		t.Run(bq.name, func(t *testing.T) {
			vx.query = bq.query
			plan, err := vx.parseAndPlan(ctx)
			require.EqualError(t, err, bq.errorString)
			require.Nil(t, plan)
		})
	}

	type action struct {
		name          string
		want          string
		expectedError error
	}
	updateSQL := "update _vt.vreplication set state = %s"
	actions := []action{
		{
			name:          "start",
			want:          fmt.Sprintf(updateSQL, encodeString(binlogdatapb.VReplicationWorkflowState_Running.String())),
			expectedError: nil,
		},
		{
			name:          "stop",
			want:          fmt.Sprintf(updateSQL, encodeString(binlogdatapb.VReplicationWorkflowState_Stopped.String())),
			expectedError: nil,
		},
		{
			name:          "delete",
			want:          "delete from _vt.vreplication",
			expectedError: nil,
		},
		{
			name:          "other",
			want:          "",
			expectedError: fmt.Errorf("invalid action found: other"),
		}}

	for _, a := range actions {
		t.Run(a.name, func(t *testing.T) {
			sql, err := wr.getWorkflowActionQuery(a.name)
			require.Equal(t, a.expectedError, err)
			require.Equal(t, a.want, sql)
		})
	}
}

// TestWorkflowUpdate tests the vtctl client
// Workflow command with the update action.
// It only tests the dry-run output because
// the actual execution happens in the
// tabletmanager and the behavior is tested
// there.
func TestWorkflowUpdate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	workflow := "wrWorkflow"
	keyspace := "target"
	env := newWranglerTestEnv(t, ctx, []string{"0"}, []string{"-80", "80-"}, nil, 1234)
	defer env.close()
	logger := logutil.NewMemoryLogger()
	wr := New(vtenv.NewTestEnv(), logger, env.topoServ, env.tmc)
	nullSlice := textutil.SimulatedNullStringSlice                   // Used to represent a non-provided value
	nullOnDDL := binlogdatapb.OnDDLAction(textutil.SimulatedNullInt) // Used to represent a non-provided value
	tests := []struct {
		name        string
		cells       []string
		tabletTypes []topodatapb.TabletType
		onDDL       binlogdatapb.OnDDLAction
		output      string
		wantErr     string
	}{
		{
			name:        "no flags",
			cells:       nullSlice,
			tabletTypes: []topodatapb.TabletType{topodatapb.TabletType(textutil.SimulatedNullInt)},
			onDDL:       nullOnDDL,
			wantErr:     "no updates were provided; use --cells, --tablet-types, or --on-ddl to specify new values",
		},
		{
			name:        "only cells",
			cells:       []string{"zone1"},
			tabletTypes: []topodatapb.TabletType{topodatapb.TabletType(textutil.SimulatedNullInt)},
			onDDL:       nullOnDDL,
			output:      "The following workflow fields will be updated:\n  cells=\"zone1\"\nOn the following tablets in the target keyspace for workflow wrWorkflow:\n  zone1-0000000200 (target/-80)\n  zone1-0000000210 (target/80-)\n",
		},
		{
			name:        "only tablet types",
			cells:       nullSlice,
			tabletTypes: []topodatapb.TabletType{topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA},
			onDDL:       nullOnDDL,
			output:      "The following workflow fields will be updated:\n  tablet_types=\"primary,replica\"\nOn the following tablets in the target keyspace for workflow wrWorkflow:\n  zone1-0000000200 (target/-80)\n  zone1-0000000210 (target/80-)\n",
		},
		{
			name:        "only on-ddl",
			cells:       nullSlice,
			tabletTypes: []topodatapb.TabletType{topodatapb.TabletType(textutil.SimulatedNullInt)},
			onDDL:       binlogdatapb.OnDDLAction_EXEC_IGNORE,
			output:      "The following workflow fields will be updated:\n  on_ddl=\"EXEC_IGNORE\"\nOn the following tablets in the target keyspace for workflow wrWorkflow:\n  zone1-0000000200 (target/-80)\n  zone1-0000000210 (target/80-)\n",
		},
		{
			name:        "all flags",
			cells:       []string{"zone1", "zone2"},
			tabletTypes: []topodatapb.TabletType{topodatapb.TabletType_RDONLY, topodatapb.TabletType_SPARE},
			onDDL:       binlogdatapb.OnDDLAction_EXEC,
			output:      "The following workflow fields will be updated:\n  cells=\"zone1,zone2\"\n  tablet_types=\"rdonly,spare\"\n  on_ddl=\"EXEC\"\nOn the following tablets in the target keyspace for workflow wrWorkflow:\n  zone1-0000000200 (target/-80)\n  zone1-0000000210 (target/80-)\n",
		},
	}

	for _, tcase := range tests {
		t.Run(tcase.name, func(t *testing.T) {
			rpcReq := &tabletmanagerdatapb.UpdateVReplicationWorkflowRequest{
				Cells:       tcase.cells,
				TabletTypes: tcase.tabletTypes,
				OnDdl:       tcase.onDDL,
			}

			_, err := wr.WorkflowAction(ctx, workflow, keyspace, "update", true, rpcReq, nil)
			if tcase.wantErr != "" {
				require.Error(t, err)
				require.Equal(t, err.Error(), tcase.wantErr)
			} else {
				// Logger.String() adds additional newlines to each log line.
				output := strings.ReplaceAll(logger.String(), "\n\n", "\n")
				require.NoError(t, err)
				require.Equal(t, tcase.output, output)
			}
			logger.Clear()
		})
	}
}
