// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/planbuilder"
)

func TestQueryzHandler(t *testing.T) {
	resp := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/schemaz", nil)
	qe := newTestQueryEngine(100, 10*time.Second, true)

	plan1 := &TabletPlan{
		Plan: &planbuilder.Plan{
			Table:  &schema.Table{Name: sqlparser.NewTableIdent("test_table")},
			PlanID: planbuilder.PlanPassSelect,
			Reason: planbuilder.ReasonTable,
		},
	}
	plan1.AddStats(10, 2*time.Second, 1*time.Second, 2, 0)
	qe.queries.Set("select name from test_table", plan1)

	plan2 := &TabletPlan{
		Plan: &planbuilder.Plan{
			Table:  &schema.Table{Name: sqlparser.NewTableIdent("test_table")},
			PlanID: planbuilder.PlanDDL,
			Reason: planbuilder.ReasonDefault,
		},
	}
	plan2.AddStats(1, 2*time.Millisecond, 1*time.Millisecond, 1, 0)
	qe.queries.Set("insert into test_table values 1", plan2)

	plan3 := &TabletPlan{
		Plan: &planbuilder.Plan{
			Table:  &schema.Table{Name: sqlparser.NewTableIdent("")},
			PlanID: planbuilder.PlanOther,
			Reason: planbuilder.ReasonDefault,
		},
	}
	plan3.AddStats(1, 75*time.Millisecond, 50*time.Millisecond, 1, 0)
	qe.queries.Set("show tables", plan3)
	qe.queries.Set("", (*TabletPlan)(nil))

	plan4 := &TabletPlan{
		Plan: &planbuilder.Plan{
			Table:  &schema.Table{Name: sqlparser.NewTableIdent("")},
			PlanID: planbuilder.PlanOther,
			Reason: planbuilder.ReasonDefault,
		},
	}
	plan4.AddStats(1, 1*time.Millisecond, 1*time.Millisecond, 1, 0)
	hugeInsert := "insert into test_table values 0";
	for i := 1; i < 1000; i++ {
		hugeInsert = hugeInsert + fmt.Sprintf(", %d", i);
	}
	qe.queries.Set(hugeInsert, plan4)
	qe.queries.Set("", (*TabletPlan)(nil))

	queryzHandler(qe, resp, req)
	body, _ := ioutil.ReadAll(resp.Body)
	planPattern1 := []string{
		`<tr class="high">`,
		`<td>select name from test_table</td>`,
		`<td>test_table</td>`,
		`<td>PASS_SELECT</td>`,
		`<td>TABLE</td>`,
		`<td>10</td>`,
		`<td>2.000000</td>`,
		`<td>1.000000</td>`,
		`<td>2</td>`,
		`<td>0</td>`,
		`<td>0.200000</td>`,
		`<td>0.100000</td>`,
		`<td>0.200000</td>`,
		`<td>0.000000</td>`,
	}
	checkQueryzHasPlan(t, planPattern1, plan1, body)
	planPattern2 := []string{
		`<tr class="low">`,
		`<td>insert into test_table values 1</td>`,
		`<td>test_table</td>`,
		`<td>DDL</td>`,
		`<td>DEFAULT</td>`,
		`<td>1</td>`,
		`<td>0.002000</td>`,
		`<td>0.001000</td>`,
		`<td>1</td>`,
		`<td>0</td>`,
		`<td>0.002000</td>`,
		`<td>0.001000</td>`,
		`<td>1.000000</td>`,
		`<td>0.000000</td>`,
	}
	checkQueryzHasPlan(t, planPattern2, plan2, body)
	planPattern3 := []string{
		`<tr class="medium">`,
		`<td>show tables</td>`,
		`<td></td>`,
		`<td>OTHER</td>`,
		`<td>DEFAULT</td>`,
		`<td>1</td>`,
		`<td>0.075000</td>`,
		`<td>0.050000</td>`,
		`<td>1</td>`,
		`<td>0</td>`,
		`<td>0.075000</td>`,
		`<td>0.050000</td>`,
		`<td>1.000000</td>`,
		`<td>0.000000</td>`,
	}
	checkQueryzHasPlan(t, planPattern3, plan3, body)
	planPattern4 := []string{
		`<tr class="low">`,
		`<td>insert into test_table values .* \[TRUNCATED\][^<]*</td>`,
		`<td></td>`,
		`<td>OTHER</td>`,
		`<td>DEFAULT</td>`,
		`<td>1</td>`,
		`<td>0.001000</td>`,
		`<td>0.001000</td>`,
		`<td>1</td>`,
		`<td>0</td>`,
		`<td>0.001000</td>`,
		`<td>0.001000</td>`,
		`<td>1.000000</td>`,
		`<td>0.000000</td>`,
	}
	checkQueryzHasPlan(t, planPattern4, plan4, body)
}

func checkQueryzHasPlan(t *testing.T, planPattern []string, plan *TabletPlan, page []byte) {
	matcher := regexp.MustCompile(strings.Join(planPattern, `\s*`))
	if !matcher.Match(page) {
		t.Fatalf("queryz page does not contain\nplan:\n%v\npattern:\n%v\npage:\n%s", plan, strings.Join(planPattern, `\s*`), string(page))
	}
}
