// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"golang.org/x/net/context"
)

func TestQuerylogzHandlerInvalidLogStats(t *testing.T) {
	req, _ := http.NewRequest("GET", "/querylogz?timeout=10&limit=1", nil)
	response := httptest.NewRecorder()
	ch := make(chan interface{}, 1)
	ch <- "test msg"
	querylogzHandler(ch, response, req)
	close(ch)
	if !strings.Contains(response.Body.String(), "error") {
		t.Fatalf("should show an error page for an non LogStats")
	}
}

func TestQuerylogzHandler(t *testing.T) {
	req, _ := http.NewRequest("GET", "/querylogz?timeout=10&limit=1", nil)
	logStats := tabletenv.NewLogStats(context.Background(), "Execute")
	logStats.PlanType = planbuilder.PlanPassSelect.String()
	logStats.OriginalSQL = "select name from test_table limit 1000"
	logStats.RowsAffected = 1000
	logStats.NumberOfQueries = 1
	logStats.StartTime, _ = time.Parse("Jan 2 15:04:05", "Nov 29 13:33:09")
	logStats.MysqlResponseTime = 1 * time.Millisecond
	logStats.WaitingForConnection = 10 * time.Nanosecond
	logStats.TransactionID = 131
	logStats.Ctx = callerid.NewContext(
		context.Background(),
		callerid.NewEffectiveCallerID("effective-caller", "component", "subcomponent"),
		callerid.NewImmediateCallerID("immediate-caller"),
	)

	// fast query
	fastQueryPattern := []string{
		`<tr class="low">`,
		`<td>Execute</td>`,
		`<td></td>`,
		`<td>effective-caller</td>`,
		`<td>immediate-caller</td>`,
		`<td>Nov 29 13:33:09.000000</td>`,
		`<td>Nov 29 13:33:09.001000</td>`,
		`<td>0.001</td>`,
		`<td>0.001</td>`,
		`<td>1e-08</td>`,
		`<td>PASS_SELECT</td>`,
		`<td>select name from test_table limit 1000</td>`,
		`<td>1</td>`,
		`<td>none</td>`,
		`<td>1000</td>`,
		`<td>0</td>`,
		`<td>131</td>`,
		`<td></td>`,
	}
	logStats.EndTime = logStats.StartTime.Add(1 * time.Millisecond)
	response := httptest.NewRecorder()
	ch := make(chan interface{}, 1)
	ch <- logStats
	querylogzHandler(ch, response, req)
	close(ch)
	body, _ := ioutil.ReadAll(response.Body)
	checkQuerylogzHasStats(t, fastQueryPattern, logStats, body)

	// medium query
	mediumQueryPattern := []string{
		`<tr class="medium">`,
		`<td>Execute</td>`,
		`<td></td>`,
		`<td>effective-caller</td>`,
		`<td>immediate-caller</td>`,
		`<td>Nov 29 13:33:09.000000</td>`,
		`<td>Nov 29 13:33:09.020000</td>`,
		`<td>0.02</td>`,
		`<td>0.001</td>`,
		`<td>1e-08</td>`,
		`<td>PASS_SELECT</td>`,
		`<td>select name from test_table limit 1000</td>`,
		`<td>1</td>`,
		`<td>none</td>`,
		`<td>1000</td>`,
		`<td>0</td>`,
		`<td>131</td>`,
		`<td></td>`,
	}
	logStats.EndTime = logStats.StartTime.Add(20 * time.Millisecond)
	response = httptest.NewRecorder()
	ch = make(chan interface{}, 1)
	ch <- logStats
	querylogzHandler(ch, response, req)
	close(ch)
	body, _ = ioutil.ReadAll(response.Body)
	checkQuerylogzHasStats(t, mediumQueryPattern, logStats, body)

	// slow query
	slowQueryPattern := []string{
		`<tr class="high">`,
		`<td>Execute</td>`,
		`<td></td>`,
		`<td>effective-caller</td>`,
		`<td>immediate-caller</td>`,
		`<td>Nov 29 13:33:09.000000</td>`,
		`<td>Nov 29 13:33:09.500000</td>`,
		`<td>0.5</td>`,
		`<td>0.001</td>`,
		`<td>1e-08</td>`,
		`<td>PASS_SELECT</td>`,
		`<td>select name from test_table limit 1000</td>`,
		`<td>1</td>`,
		`<td>none</td>`,
		`<td>1000</td>`,
		`<td>0</td>`,
		`<td>131</td>`,
		`<td></td>`,
	}
	logStats.EndTime = logStats.StartTime.Add(500 * time.Millisecond)
	ch = make(chan interface{}, 1)
	ch <- logStats
	querylogzHandler(ch, response, req)
	close(ch)
	body, _ = ioutil.ReadAll(response.Body)
	checkQuerylogzHasStats(t, slowQueryPattern, logStats, body)
}

func checkQuerylogzHasStats(t *testing.T, pattern []string, logStats *tabletenv.LogStats, page []byte) {
	matcher := regexp.MustCompile(strings.Join(pattern, `\s*`))
	if !matcher.Match(page) {
		t.Fatalf("querylogz page does not contain stats: %v, pattern: %v, page: %s", logStats, pattern, string(page))
	}
}
