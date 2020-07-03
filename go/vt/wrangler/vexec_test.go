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
		"id|source|message",
		"int64|varchar|varchar"),
		"1|keyspace:\"source\" shard:\"0\" filter:<rules:<match:\"t1\" > >|",
	)
	testCases = append(testCases, &TestCase{
		name:   "select",
		query:  "select id, source, message from _vt.vreplication",
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
	result = &sqltypes.Result{
		RowsAffected: 2,
		Rows:         [][]sqltypes.Value{},
	}
	testCases = append(testCases, &TestCase{
		name:   "insert",
		query:  "insert into _vt.vreplication(state, workflow, db_name) values ('Running', 'wk1', 'ks1'), ('Stopped', 'wk1', 'ks1')",
		result: result,
	})

	errorString := "id should not have a value"
	testCases = append(testCases, &TestCase{
		name:        "insert invalid-id",
		query:       "insert into _vt.vreplication(id, state) values (1, 'Running'), (2, 'Stopped')",
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
		"will be run on the following streams in keyspace target for workflow wrWorkflow:",
		"+----------------------+----+------------------+---------+-----------+",
		"|        TABLET        | ID |   BINLOGSOURCE   |  STATE  |  DBNAME   |",
		"+----------------------+----+------------------+---------+-----------+",
		"| -80/zone1-0000000200 |  1 | zone1-0000000200 | Running | vt_target |",
		"+----------------------+----+------------------+---------+-----------+",
		"| 80-/zone1-0000000210 |  1 | zone1-0000000210 | Running | vt_target |",
		"+----------------------+----+------------------+---------+-----------+",
	}
	require.Equal(t, strings.Join(dryRunResults, "\n")+"\n\n\n\n\n", logger.String())
}

/*
func TextVExecValidations(t *testing.T) {
	ctx := context.Background()
	workflow := ""
	keyspace := ""
	query := ""
	env := newWranglerTestEnv([]string{"0"}, []string{"-80","80-"}, "", nil)
	defer env.close()

	wr := New(logutil.NewConsoleLogger(), env.topoServ, env.tmc)

	//validations: empty workflow/ks/query
	//	valid workflow: no masters found? valid keyspace
	// invalid query
	// insert query

}

*/
