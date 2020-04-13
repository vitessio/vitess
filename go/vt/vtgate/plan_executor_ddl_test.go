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

package vtgate

import (
	"context"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

func TestPlanExecutorDDL(t *testing.T) {
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	executor, sbc1, sbc2, sbclookup := createExecutorEnvUsing(planAllTheThings)

	type cnts struct {
		Sbc1Cnt      int64
		Sbc2Cnt      int64
		SbcLookupCnt int64
	}

	tcs := []struct {
		targetStr string

		hasNoKeyspaceErr bool
		shardQueryCnt    int
		wantCnts         cnts
	}{
		{
			targetStr:        "",
			hasNoKeyspaceErr: true,
		},
		{
			targetStr:     KsTestUnsharded,
			shardQueryCnt: 1,
			wantCnts: cnts{
				Sbc1Cnt:      0,
				Sbc2Cnt:      0,
				SbcLookupCnt: 1,
			},
		},
		{
			targetStr:     "TestExecutor",
			shardQueryCnt: 8,
			wantCnts: cnts{
				Sbc1Cnt:      1,
				Sbc2Cnt:      1,
				SbcLookupCnt: 0,
			},
		},
		{
			targetStr:     "TestExecutor/-20",
			shardQueryCnt: 1,
			wantCnts: cnts{
				Sbc1Cnt:      1,
				Sbc2Cnt:      0,
				SbcLookupCnt: 0,
			},
		},
	}

	stmts := []string{
		"create table t1 (id bigint not null, primary key (id))",
		"alter table t1 add primary key id",
		"rename table t1 to t2",
		"truncate table t2",
		"drop table t2",
	}

	for _, stmt := range stmts {
		for _, tc := range tcs {
			t.Run(tc.targetStr+"_"+stmt, func(t *testing.T) {
				sbc1.ExecCount.Set(0)
				sbc2.ExecCount.Set(0)
				sbclookup.ExecCount.Set(0)

				_, err := executor.Execute(context.Background(), "TestExecute", NewSafeSession(&vtgatepb.Session{TargetString: tc.targetStr}), stmt, nil)
				if tc.hasNoKeyspaceErr {
					assert.Error(t, err, errNoKeyspace)
				} else {
					assert.NoError(t, err)
				}

				diff := cmp.Diff(tc.wantCnts, cnts{
					Sbc1Cnt:      sbc1.ExecCount.Get(),
					Sbc2Cnt:      sbc2.ExecCount.Get(),
					SbcLookupCnt: sbclookup.ExecCount.Get(),
				})
				if diff != "" {
					t.Errorf("stmt: %s\ntc: %+v\n-want,+got:\n%s", stmt, tc, diff)
				}
				testQueryLog(t, logChan, "TestExecute", "DDL", stmt, tc.shardQueryCnt)
			})
		}
	}
}

func TestPlanPassthroughDDL(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnvUsing(planAllTheThings)
	masterSession.TargetString = "TestExecutor"

	_, err := executorExec(executor, "/* leading */ create table passthrough_ddl (\n\tcol bigint default 123\n) /* trailing */", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "/* leading */ create table passthrough_ddl (\n\tcol bigint default 123\n) /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc1.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc2.Queries, wantQueries)
	}
	sbc1.Queries = nil
	sbc2.Queries = nil

	// Force the query to go to only one shard. Normalization doesn't make any difference.
	masterSession.TargetString = "TestExecutor/40-60"
	executor.normalize = true

	_, err = executorExec(executor, "/* leading */ create table passthrough_ddl (\n\tcol bigint default 123\n) /* trailing */", nil)
	require.NoError(t, err)
	require.Nil(t, sbc1.Queries)
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc2.Queries, wantQueries)
	}
	sbc2.Queries = nil
	masterSession.TargetString = ""

	// Use range query
	masterSession.TargetString = "TestExecutor[-]"
	executor.normalize = true

	_, err = executorExec(executor, "/* leading */ create table passthrough_ddl (\n\tcol bigint default 123\n) /* trailing */", nil)
	require.NoError(t, err)
	if !reflect.DeepEqual(sbc1.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc1.Queries, wantQueries)
	}
	if !reflect.DeepEqual(sbc2.Queries, wantQueries) {
		t.Errorf("sbc2.Queries: %+v, want %+v\n", sbc2.Queries, wantQueries)
	}
	sbc2.Queries = nil
	masterSession.TargetString = ""
}
