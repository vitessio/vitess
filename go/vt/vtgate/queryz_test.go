/*
Copyright 2017 Google Inc.

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
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/engine"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestQueryzHandler(t *testing.T) {
	resp := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/schemaz", nil)

	executor, _, _, _ := createExecutorEnv()

	// single shard query
	sql := "select id from user where id = 1"
	_, err := executorExec(executor, sql, nil)
	if err != nil {
		t.Error(err)
	}
	result, ok := executor.plans.Get(sql)
	if !ok {
		t.Fatalf("couldn't get plan from cache")
	}
	plan1 := result.(*engine.Plan)
	plan1.ExecTime = time.Duration(1 * time.Millisecond)

	// scatter
	sql = "select id from user"
	_, err = executorExec(executor, sql, nil)
	if err != nil {
		t.Error(err)
	}
	result, ok = executor.plans.Get(sql)
	if !ok {
		t.Fatalf("couldn't get plan from cache")
	}
	plan2 := result.(*engine.Plan)
	plan2.ExecTime = time.Duration(1 * time.Second)

	sql = "insert into user (id, name) values (:id, :name)"
	_, err = executorExec(executor, sql, map[string]*querypb.BindVariable{
		"id":   sqltypes.Uint64BindVariable(1),
		"name": sqltypes.BytesBindVariable([]byte("myname")),
	})
	if err != nil {
		t.Error(err)
	}
	result, ok = executor.plans.Get(sql)
	if !ok {
		t.Fatalf("couldn't get plan from cache")
	}
	plan3 := result.(*engine.Plan)

	// vindex insert from above execution
	result, ok = executor.plans.Get("insert into name_user_map(name, user_id) values(:name0, :user_id0)")
	if !ok {
		t.Fatalf("couldn't get plan from cache")
	}
	plan4 := result.(*engine.Plan)

	// same query again should add query counts to existing plans
	sql = "insert into user (id, name) values (:id, :name)"
	_, err = executorExec(executor, sql, map[string]*querypb.BindVariable{
		"id":   sqltypes.Uint64BindVariable(1),
		"name": sqltypes.BytesBindVariable([]byte("myname")),
	})

	if err != nil {
		t.Error(err)
	}

	plan3.ExecTime = time.Duration(100 * time.Millisecond)
	plan4.ExecTime = time.Duration(200 * time.Millisecond)

	queryzHandler(executor, resp, req)
	body, _ := ioutil.ReadAll(resp.Body)
	planPattern1 := []string{
		`<tr class="low">`,
		`<td>select id from user where id = 1</td>`,
		`<td>1</td>`,
		`<td>0.001000</td>`,
		`<td>1</td>`,
		`<td>1</td>`,
		`<td>0</td>`,
		`<td>0.001000</td>`,
		`<td>1.000000</td>`,
		`<td>1.000000</td>`,
		`<td>0.000000</td>`,
		`</tr>`,
	}
	checkQueryzHasPlan(t, planPattern1, plan1, body)
	planPattern2 := []string{
		`<tr class="high">`,
		`<td>select id from user</td>`,
		`<td>1</td>`,
		`<td>1.000000</td>`,
		`<td>8</td>`,
		`<td>8</td>`,
		`<td>0</td>`,
		`<td>1.000000</td>`,
		`<td>8.000000</td>`,
		`<td>8.000000</td>`,
		`<td>0.000000</td>`,
		`</tr>`,
	}
	checkQueryzHasPlan(t, planPattern2, plan2, body)
	planPattern3 := []string{
		`<tr class="medium">`,
		`<td>insert into user.*</td>`,
		`<td>2</td>`,
		`<td>0.100000</td>`,
		`<td>2</td>`,
		`<td>2</td>`,
		`<td>0</td>`,
		`<td>0.050000</td>`,
		`<td>1.000000</td>`,
		`<td>1.000000</td>`,
		`<td>0.000000</td>`,
		`</tr>`,
	}
	checkQueryzHasPlan(t, planPattern3, plan3, body)
	planPattern4 := []string{
		`<tr class="high">`,
		`<td>insert into name_user_map.*</td>`,
		`<td>2</td>`,
		`<td>0.200000</td>`,
		`<td>2</td>`,
		`<td>2</td>`,
		`<td>0</td>`,
		`<td>0.100000</td>`,
		`<td>1.000000</td>`,
		`<td>1.000000</td>`,
		`<td>0.000000</td>`,
		`</tr>`,
	}
	checkQueryzHasPlan(t, planPattern4, plan4, body)
}

func checkQueryzHasPlan(t *testing.T, planPattern []string, plan *engine.Plan, page []byte) {
	t.Helper()
	matcher := regexp.MustCompile(strings.Join(planPattern, `\s*`))
	if !matcher.Match(page) {
		t.Fatalf("queryz page does not contain\nplan:\n%v\npattern:\n%v\npage:\n%s", plan, strings.Join(planPattern, `\s*`), string(page))
	}
}
