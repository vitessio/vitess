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

package tabletserver

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
)

func TestQueryzHandler(t *testing.T) {
	resp := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/schemaz", nil)
	qe := newTestQueryEngine(10*time.Second, true, &dbconfigs.DBConfigs{})

	const query1 = "select name from test_table"
	plan1 := &TabletPlan{
		Original: query1,
		Plan: &planbuilder.Plan{
			Table:  &schema.Table{Name: sqlparser.NewIdentifierCS("test_table")},
			PlanID: planbuilder.PlanSelect,
		},
	}
	plan1.AddStats(10, 2*time.Second, 1*time.Second, 0, 2, 0)
	qe.plans.Set(query1, plan1, 0, 0)

	const query2 = "insert into test_table values 1"
	plan2 := &TabletPlan{
		Original: query2,
		Plan: &planbuilder.Plan{
			Table:  &schema.Table{Name: sqlparser.NewIdentifierCS("test_table")},
			PlanID: planbuilder.PlanDDL,
		},
	}
	plan2.AddStats(1, 2*time.Millisecond, 1*time.Millisecond, 1, 0, 0)
	qe.plans.Set(query2, plan2, 0, 0)

	const query3 = "show tables"
	plan3 := &TabletPlan{
		Original: query3,
		Plan: &planbuilder.Plan{
			Table:  &schema.Table{Name: sqlparser.NewIdentifierCS("")},
			PlanID: planbuilder.PlanOtherRead,
		},
	}
	plan3.AddStats(1, 75*time.Millisecond, 50*time.Millisecond, 0, 1, 0)
	qe.plans.Set(query3, plan3, 0, 0)
	qe.plans.Set("", (*TabletPlan)(nil), 0, 0)

	hugeInsert := "insert into test_table values 0"
	for i := 1; i < 1000; i++ {
		hugeInsert = hugeInsert + fmt.Sprintf(", %d", i)
	}
	plan4 := &TabletPlan{
		Original: hugeInsert,
		Plan: &planbuilder.Plan{
			Table:  &schema.Table{Name: sqlparser.NewIdentifierCS("")},
			PlanID: planbuilder.PlanOtherRead,
		},
	}
	plan4.AddStats(1, 1*time.Millisecond, 1*time.Millisecond, 1, 0, 0)
	qe.plans.Set(PlanCacheKey(hugeInsert), plan4, 0, 0)
	qe.plans.Set("", (*TabletPlan)(nil), 0, 0)

	// Wait for cache to settle
	time.Sleep(100 * time.Millisecond)

	queryzHandler(qe, resp, req)
	body, _ := io.ReadAll(resp.Body)
	planPattern1 := []string{
		`<tr class="high">`,
		`<td>select name from test_table</td>`,
		`<td>test_table</td>`,
		`<td>Select</td>`,
		`<td>10</td>`,
		`<td>2.000000</td>`,
		`<td>1.000000</td>`,
		`<td>0</td>`,
		`<td>2</td>`,
		`<td>0</td>`,
		`<td>0.200000</td>`,
		`<td>0.100000</td>`,
		`<td>0.000000</td>`,
		`<td>0.200000</td>`,
		`<td>0.000000</td>`,
	}
	checkQueryzHasPlan(t, planPattern1, plan1, body)
	planPattern2 := []string{
		`<tr class="low">`,
		`<td>insert into test_table values 1</td>`,
		`<td>test_table</td>`,
		`<td>DDL</td>`,
		`<td>1</td>`,
		`<td>0.002000</td>`,
		`<td>0.001000</td>`,
		`<td>1</td>`,
		`<td>0</td>`,
		`<td>0</td>`,
		`<td>0.002000</td>`,
		`<td>0.001000</td>`,
		`<td>1.000000</td>`,
		`<td>0.000000</td>`,
		`<td>0.000000</td>`,
	}
	checkQueryzHasPlan(t, planPattern2, plan2, body)
	planPattern3 := []string{
		`<tr class="medium">`,
		`<td>show tables</td>`,
		`<td></td>`,
		`<td>OtherRead</td>`,
		`<td>1</td>`,
		`<td>0.075000</td>`,
		`<td>0.050000</td>`,
		`<td>0</td>`,
		`<td>1</td>`,
		`<td>0</td>`,
		`<td>0.075000</td>`,
		`<td>0.050000</td>`,
		`<td>0.000000</td>`,
		`<td>1.000000</td>`,
		`<td>0.000000</td>`,
	}
	checkQueryzHasPlan(t, planPattern3, plan3, body)
	planPattern4 := []string{
		`<tr class="low">`,
		`<td>insert into test_table values .* \[TRUNCATED\][^<]*</td>`,
		`<td></td>`,
		`<td>OtherRead</td>`,
		`<td>1</td>`,
		`<td>0.001000</td>`,
		`<td>0.001000</td>`,
		`<td>1</td>`,
		`<td>0</td>`,
		`<td>0</td>`,
		`<td>0.001000</td>`,
		`<td>0.001000</td>`,
		`<td>1.000000</td>`,
		`<td>0.000000</td>`,
		`<td>0.000000</td>`,
	}
	checkQueryzHasPlan(t, planPattern4, plan4, body)
}

func checkQueryzHasPlan(t *testing.T, planPattern []string, plan *TabletPlan, page []byte) {
	matcher := regexp.MustCompile(strings.Join(planPattern, `\s*`))
	if !matcher.Match(page) {
		t.Fatalf("queryz page does not contain\nplan:\n%#v\npattern:\n%v\npage:\n%s", plan, strings.Join(planPattern, `\s*`), string(page))
	}
}
