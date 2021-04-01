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
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/logutil"
)

func TestVExec(t *testing.T) {
	ctx := context.Background()
	workflow := "wrWorkflow"
	keyspace := "target"
	query := "update _vt.vreplication set state = 'Running'"
	env := newWranglerTestEnv([]string{"0"}, []string{"-80", "80-"}, "", nil, time.Now().Unix())
	defer env.close()
	var logger = logutil.NewMemoryLogger()
	wr := New(logger, env.topoServ, env.tmc)

	vx := newVExec(ctx, workflow, keyspace, query, wr)
	err := vx.getMasters()
	require.Nil(t, err)
	masters := vx.masters
	require.NotNil(t, masters)
	require.Equal(t, len(masters), 2)
	var shards []string
	for _, master := range masters {
		shards = append(shards, master.Shard)
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

	res, err := wr.getStreams(ctx, workflow, keyspace)
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
		"id|source|message|cell|tablet_types",
		"int64|varchar|varchar|varchar|varchar"),
		"1|keyspace:\"source\" shard:\"0\" filter:<rules:<match:\"t1\" > >|||",
	)
	testCases = append(testCases, &TestCase{
		name:   "select",
		query:  "select id, source, message, cell, tablet_types from _vt.vreplication",
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
					utils.MustMatch(t, testCase.result, result, "Incorrect result")
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
		"+----------------------+----+--------------------------------+---------+-----------+--------------+",
		"|        TABLET        | ID |          BINLOGSOURCE          |  STATE  |  DBNAME   | CURRENT GTID |",
		"+----------------------+----+--------------------------------+---------+-----------+--------------+",
		"| -80/zone1-0000000200 |  1 | keyspace:\"source\" shard:\"0\"    | Copying | vt_target | pos          |",
		"|                      |    | filter:<rules:<match:\"t1\" > >  |         |           |              |",
		"+----------------------+----+--------------------------------+---------+-----------+--------------+",
		"| 80-/zone1-0000000210 |  1 | keyspace:\"source\" shard:\"0\"    | Copying | vt_target | pos          |",
		"|                      |    | filter:<rules:<match:\"t1\" > >  |         |           |              |",
		"+----------------------+----+--------------------------------+---------+-----------+--------------+",
	}
	require.Equal(t, strings.Join(dryRunResults, "\n")+"\n\n\n\n\n", logger.String())
}

func TestWorkflowStatusUpdate(t *testing.T) {
	require.Equal(t, "Running", updateState("for vdiff", "Running", nil, int64(time.Now().Second())))
	require.Equal(t, "Running", updateState("", "Running", nil, int64(time.Now().Second())))
	require.Equal(t, "Lagging", updateState("", "Running", nil, int64(time.Now().Second())-100))
	require.Equal(t, "Copying", updateState("", "Running", []copyState{{Table: "t1", LastPK: "[[INT64(10)]]"}}, int64(time.Now().Second())))
	require.Equal(t, "Error", updateState("error: master tablet not contactable", "Running", nil, 0))
}

func TestWorkflowListStreams(t *testing.T) {
	ctx := context.Background()
	workflow := "wrWorkflow"
	keyspace := "target"
	env := newWranglerTestEnv([]string{"0"}, []string{"-80", "80-"}, "", nil, 1234)
	defer env.close()
	logger := logutil.NewMemoryLogger()
	wr := New(logger, env.topoServ, env.tmc)

	_, err := wr.WorkflowAction(ctx, workflow, keyspace, "listall", false)
	require.NoError(t, err)

	_, err = wr.WorkflowAction(ctx, workflow, "badks", "show", false)
	require.Errorf(t, err, "node doesn't exist: keyspaces/badks/shards")

	_, err = wr.WorkflowAction(ctx, "badwf", keyspace, "show", false)
	require.Errorf(t, err, "no streams found for workflow badwf in keyspace target")
	logger.Clear()
	_, err = wr.WorkflowAction(ctx, workflow, keyspace, "show", false)
	require.NoError(t, err)
	want := `{
	"Workflow": "wrWorkflow",
	"SourceLocation": {
		"Keyspace": "source",
		"Shards": [
			"0"
		]
	},
	"TargetLocation": {
		"Keyspace": "target",
		"Shards": [
			"-80",
			"80-"
		]
	},
	"MaxVReplicationLag": 0,
	"ShardStatuses": {
		"-80/zone1-0000000200": {
			"MasterReplicationStatuses": [
				{
					"Shard": "-80",
					"Tablet": "zone1-0000000200",
					"ID": 1,
					"Bls": {
						"keyspace": "source",
						"shard": "0",
						"filter": {
							"rules": [
								{
									"match": "t1"
								}
							]
						}
					},
					"Pos": "pos",
					"StopPos": "",
					"State": "Copying",
					"DBName": "vt_target",
					"TransactionTimestamp": 0,
					"TimeUpdated": 1234,
					"Message": "",
					"CopyState": [
						{
							"Table": "t1",
							"LastPK": "pk1"
						}
					]
				}
			],
			"TabletControls": null,
			"MasterIsServing": true
		},
		"80-/zone1-0000000210": {
			"MasterReplicationStatuses": [
				{
					"Shard": "80-",
					"Tablet": "zone1-0000000210",
					"ID": 1,
					"Bls": {
						"keyspace": "source",
						"shard": "0",
						"filter": {
							"rules": [
								{
									"match": "t1"
								}
							]
						}
					},
					"Pos": "pos",
					"StopPos": "",
					"State": "Copying",
					"DBName": "vt_target",
					"TransactionTimestamp": 0,
					"TimeUpdated": 1234,
					"Message": "",
					"CopyState": [
						{
							"Table": "t1",
							"LastPK": "pk1"
						}
					]
				}
			],
			"TabletControls": null,
			"MasterIsServing": true
		}
	}
}

`
	got := logger.String()
	// MaxVReplicationLag needs to be reset. This can't be determinable in this kind of a test because time.Now() is constantly shifting.
	re := regexp.MustCompile(`"MaxVReplicationLag": \d+`)
	got = re.ReplaceAllLiteralString(got, `"MaxVReplicationLag": 0`)
	require.Equal(t, want, got)

	results, err := wr.execWorkflowAction(ctx, workflow, keyspace, "stop", false)
	require.Nil(t, err)

	// convert map to list and sort it for comparison
	var gotResults []string
	for key, result := range results {
		gotResults = append(gotResults, fmt.Sprintf("%s:%v", key.String(), result))
	}
	sort.Strings(gotResults)
	wantResults := []string{"Tablet{zone1-0000000200}:rows_affected:1 ", "Tablet{zone1-0000000210}:rows_affected:1 "}
	sort.Strings(wantResults)
	require.ElementsMatch(t, wantResults, gotResults)

	logger.Clear()
	results, err = wr.execWorkflowAction(ctx, workflow, keyspace, "stop", true)
	require.Nil(t, err)
	require.Equal(t, "map[]", fmt.Sprintf("%v", results))
	dryRunResult := `Query: update _vt.vreplication set state = 'Stopped' where db_name = 'vt_target' and workflow = 'wrWorkflow'
will be run on the following streams in keyspace target for workflow wrWorkflow:


+----------------------+----+--------------------------------+---------+-----------+--------------+
|        TABLET        | ID |          BINLOGSOURCE          |  STATE  |  DBNAME   | CURRENT GTID |
+----------------------+----+--------------------------------+---------+-----------+--------------+
| -80/zone1-0000000200 |  1 | keyspace:"source" shard:"0"    | Copying | vt_target | pos          |
|                      |    | filter:<rules:<match:"t1" > >  |         |           |              |
+----------------------+----+--------------------------------+---------+-----------+--------------+
| 80-/zone1-0000000210 |  1 | keyspace:"source" shard:"0"    | Copying | vt_target | pos          |
|                      |    | filter:<rules:<match:"t1" > >  |         |           |              |
+----------------------+----+--------------------------------+---------+-----------+--------------+




`
	require.Equal(t, dryRunResult, logger.String())
}

func TestWorkflowListAll(t *testing.T) {
	ctx := context.Background()
	keyspace := "target"
	workflow := "wrWorkflow"
	env := newWranglerTestEnv([]string{"0"}, []string{"-80", "80-"}, "", nil, 0)
	defer env.close()
	logger := logutil.NewMemoryLogger()
	wr := New(logger, env.topoServ, env.tmc)

	workflows, err := wr.ListAllWorkflows(ctx, keyspace, true)
	require.Nil(t, err)
	require.Equal(t, []string{workflow}, workflows)

	workflows, err = wr.ListAllWorkflows(ctx, keyspace, false)
	require.Nil(t, err)
	require.Equal(t, []string{workflow, "wrWorkflow2"}, workflows)
}

func TestVExecValidations(t *testing.T) {
	ctx := context.Background()
	workflow := "wf"
	keyspace := "ks"
	query := ""
	env := newWranglerTestEnv([]string{"0"}, []string{"-80", "80-"}, "", nil, 0)
	defer env.close()

	wr := New(logutil.NewConsoleLogger(), env.topoServ, env.tmc)

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
			want:          fmt.Sprintf(updateSQL, encodeString("Running")),
			expectedError: nil,
		},
		{
			name:          "stop",
			want:          fmt.Sprintf(updateSQL, encodeString("Stopped")),
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
