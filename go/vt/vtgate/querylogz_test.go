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

	"github.com/youtube/vitess/go/vt/callerid"
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

func TestQuerylogzHandlerFormatting(t *testing.T) {
	req, _ := http.NewRequest("GET", "/querylogz?timeout=10&limit=1", nil)
	logStats := NewLogStats(context.Background(), "Execute", "select name from test_table limit 1000", nil)
	logStats.StmtType = "select"
	logStats.RowsAffected = 1000
	logStats.ShardQueries = 1
	logStats.StartTime, _ = time.Parse("Jan 2 15:04:05", "Nov 29 13:33:09")
	logStats.PlanTime = 1 * time.Millisecond
	logStats.ExecuteTime = 2 * time.Millisecond
	logStats.CommitTime = 3 * time.Millisecond
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
		`<td>0.002</td>`,
		`<td>0.003</td>`,
		`<td>select</td>`,
		`<td>select name from test_table limit 1000</td>`,
		`<td>1</td>`,
		`<td>1000</td>`,
		`<td></td>`,
		`</tr>`,
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
		`<td>0.002</td>`,
		`<td>0.003</td>`,
		`<td>select</td>`,
		`<td>select name from test_table limit 1000</td>`,
		`<td>1</td>`,
		`<td>1000</td>`,
		`<td></td>`,
		`</tr>`,
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
		`<td>0.002</td>`,
		`<td>0.003</td>`,
		`<td>select</td>`,
		`<td>select name from test_table limit 1000</td>`,
		`<td>1</td>`,
		`<td>1000</td>`,
		`<td></td>`,
		`</tr>`,
	}
	logStats.EndTime = logStats.StartTime.Add(500 * time.Millisecond)
	ch = make(chan interface{}, 1)
	ch <- logStats
	querylogzHandler(ch, response, req)
	close(ch)
	body, _ = ioutil.ReadAll(response.Body)
	checkQuerylogzHasStats(t, slowQueryPattern, logStats, body)
}

func checkQuerylogzHasStats(t *testing.T, pattern []string, logStats *LogStats, page []byte) {
	t.Helper()
	matcher := regexp.MustCompile(strings.Join(pattern, `\s*`))
	if !matcher.Match(page) {
		t.Fatalf("querylogz page does not contain stats: %v, pattern: %v, page: %s", logStats, pattern, string(page))
	}
}
