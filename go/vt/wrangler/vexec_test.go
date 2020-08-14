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
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/logutil"
)

func TestListStreams(t *testing.T) {
	ctx := context.Background()
	workflow := "wrWorkflow"
	keyspace := "target"
	env := newWranglerTestEnv([]string{"0"}, []string{"-80", "80-"}, "", nil)
	defer env.close()
	var logger = logutil.NewMemoryLogger()
	wr := New(logger, env.topoServ, env.tmc)
	//query := "select id, source, pos, stop_pos, max_replication_lag, state, db_name, time_updated, message from _vt.vreplication where workflow = 'wrWorkflow' and db_name = 'vt_target'"

	wr.WorkflowAction(ctx, workflow, keyspace, "ListStreams", false)

}

func TestVExec(t *testing.T) {
	ctx := context.Background()
	workflow := "wrWorkflow"
	keyspace := "target"
	query := "update _vt.vreplication set state = 'Running'"
	env := newWranglerTestEnv([]string{"0"}, []string{"-80", "80-"}, "", nil)
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
	plan, err := vx.buildVExecPlan()
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

	query = plan.parsedQuery.Query
	vx.exec(query)

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

	errorString = "invalid table name"
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
		"+----------------------+----+--------------------------------+---------+-----------+--------------+-------------------+",
		"|        TABLET        | ID |          BINLOGSOURCE          |  STATE  |  DBNAME   | CURRENT GTID | MAXREPLICATIONLAG |",
		"+----------------------+----+--------------------------------+---------+-----------+--------------+-------------------+",
		"| -80/zone1-0000000200 |  1 | keyspace:\"source\" shard:\"0\"    | Copying | vt_target | pos          |                 0 |",
		"|                      |    | filter:<rules:<match:\"t1\" > >  |         |           |              |                   |",
		"+----------------------+----+--------------------------------+---------+-----------+--------------+-------------------+",
		"| 80-/zone1-0000000210 |  1 | keyspace:\"source\" shard:\"0\"    | Copying | vt_target | pos          |                 0 |",
		"|                      |    | filter:<rules:<match:\"t1\" > >  |         |           |              |                   |",
		"+----------------------+----+--------------------------------+---------+-----------+--------------+-------------------+",
	}
	require.Equal(t, strings.Join(dryRunResults, "\n")+"\n\n\n\n\n", logger.String())
}

func TestWorkflowStatusUpdate(t *testing.T) {
	require.Equal(t, "Error", updateState("master tablet not contactable", "Running", nil, 0))
	require.Equal(t, "Lagging", updateState("", "Running", nil, int64(time.Now().Second())-100))
	require.Equal(t, "Copying", updateState("", "Running", []copyState{{Table: "t1", LastPK: "[[INT64(10)]]"}}, int64(time.Now().Second())))
}

func TestWorkflowListStreams(t *testing.T) {
	ctx := context.Background()
	workflow := "wrWorkflow"
	keyspace := "target"
	env := newWranglerTestEnv([]string{"0"}, []string{"-80", "80-"}, "", nil)
	defer env.close()
	logger := logutil.NewMemoryLogger()
	wr := New(logger, env.topoServ, env.tmc)

	_, err := wr.ShowWorkflow(ctx, workflow, keyspace)
	require.Nil(t, err)
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
					"MaxReplicationLag": 0,
					"DBName": "vt_target",
					"TimeUpdated": 1234,
					"Message": "",
					"CopyState": [
						{
							"Table": "t",
							"LastPK": "1"
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
					"MaxReplicationLag": 0,
					"DBName": "vt_target",
					"TimeUpdated": 1234,
					"Message": "",
					"CopyState": [
						{
							"Table": "t",
							"LastPK": "1"
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
	require.Equal(t, want, logger.String())

	results, err := wr.execWorkflowAction(ctx, workflow, keyspace, "stop", false)
	require.Nil(t, err)
	require.Equal(t, "map[Tablet{zone1-0000000200}:rows_affected:1  Tablet{zone1-0000000210}:rows_affected:1 ]", fmt.Sprintf("%v", results))

	logger.Clear()
	results, err = wr.execWorkflowAction(ctx, workflow, keyspace, "stop", true)
	require.Nil(t, err)
	require.Equal(t, "map[]", fmt.Sprintf("%v", results))
	dryRunResult := `Query: update _vt.vreplication set state = 'Stopped' where db_name = 'vt_target' and workflow = 'wrWorkflow'
will be run on the following streams in keyspace target for workflow wrWorkflow:


+----------------------+----+--------------------------------+---------+-----------+--------------+-------------------+
|        TABLET        | ID |          BINLOGSOURCE          |  STATE  |  DBNAME   | CURRENT GTID | MAXREPLICATIONLAG |
+----------------------+----+--------------------------------+---------+-----------+--------------+-------------------+
| -80/zone1-0000000200 |  1 | keyspace:"source" shard:"0"    | Copying | vt_target | pos          |                 0 |
|                      |    | filter:<rules:<match:"t1" > >  |         |           |              |                   |
+----------------------+----+--------------------------------+---------+-----------+--------------+-------------------+
| 80-/zone1-0000000210 |  1 | keyspace:"source" shard:"0"    | Copying | vt_target | pos          |                 0 |
|                      |    | filter:<rules:<match:"t1" > >  |         |           |              |                   |
+----------------------+----+--------------------------------+---------+-----------+--------------+-------------------+




`
	require.Equal(t, dryRunResult, logger.String())
}

func TestWorkflowListAll(t *testing.T) {
	ctx := context.Background()
	keyspace := "target"
	workflow := "wrWorkflow"
	env := newWranglerTestEnv([]string{"0"}, []string{"-80", "80-"}, "", nil)
	defer env.close()
	logger := logutil.NewMemoryLogger()
	wr := New(logger, env.topoServ, env.tmc)

	workflows, err := wr.ListAllWorkflows(ctx, keyspace)
	require.Nil(t, err)
	require.Equal(t, []string{workflow}, workflows)
}

func TestVExecValidations(t *testing.T) {
	ctx := context.Background()
	workflow := "wf"
	keyspace := "ks"
	query := ""
	env := newWranglerTestEnv([]string{"0"}, []string{"-80", "80-"}, "", nil)
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
			errorString: "invalid table name: _vt.vreplication2",
		},
		{
			name:        "unsupported query",
			query:       "describe _vt.vreplication",
			errorString: "query not supported by vexec: otherread",
		},
	}
	for _, bq := range badQueries {
		t.Run(bq.name, func(t *testing.T) {
			vx.query = bq.query
			plan, err := vx.buildVExecPlan()
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
