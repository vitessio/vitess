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
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"text/template"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/vt/logz"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

var (
	querylogzHeader = []byte(`
		<thead>
			<tr>
				<th>Method</th>
				<th>Context</th>
				<th>Effective Caller</th>
				<th>Immediate Caller</th>
				<th>Start</th>
				<th>End</th>
				<th>Duration</th>
				<th>Plan Time</th>
				<th>Execute Time</th>
				<th>Commit Time</th>
				<th>Stmt Type</th>
				<th>SQL</th>
				<th>ShardQueries</th>
				<th>RowsAffected</th>
				<th>Error</th>
			</tr>
		</thead>
	`)
	querylogzFuncMap = template.FuncMap{
		"stampMicro":    func(t time.Time) string { return t.Format(time.StampMicro) },
		"cssWrappable":  logz.Wrappable,
		"truncateQuery": sqlparser.TruncateForUI,
		"unquote":       func(s string) string { return strings.Trim(s, "\"") },
	}
	querylogzTmpl = template.Must(template.New("example").Funcs(querylogzFuncMap).Parse(`
		<tr class="{{.ColorLevel}}">
			<td>{{.Method}}</td>
			<td>{{.ContextHTML}}</td>
			<td>{{.EffectiveCaller}}</td>
			<td>{{.ImmediateCaller}}</td>
			<td>{{.StartTime | stampMicro}}</td>
			<td>{{.EndTime | stampMicro}}</td>
			<td>{{.TotalTime.Seconds}}</td>
			<td>{{.PlanTime.Seconds}}</td>
			<td>{{.ExecuteTime.Seconds}}</td>
			<td>{{.CommitTime.Seconds}}</td>
			<td>{{.StmtType}}</td>
			<td>{{.SQL | truncateQuery | unquote | cssWrappable}}</td>
			<td>{{.ShardQueries}}</td>
			<td>{{.RowsAffected}}</td>
			<td>{{.ErrorStr}}</td>
		</tr>
	`))
)

// querylogzHandler serves a human readable snapshot of the
// current query log.
func querylogzHandler(ch chan interface{}, w http.ResponseWriter, r *http.Request) {
	if err := acl.CheckAccessHTTP(r, acl.DEBUGGING); err != nil {
		acl.SendError(w, err)
		return
	}
	timeout, limit := parseTimeoutLimitParams(r)
	logz.StartHTMLTable(w)
	defer logz.EndHTMLTable(w)
	w.Write(querylogzHeader)

	tmr := time.NewTimer(timeout)
	defer tmr.Stop()
	for i := 0; i < limit; i++ {
		select {
		case out := <-ch:
			select {
			case <-tmr.C:
				return
			default:
			}
			stats, ok := out.(*LogStats)
			if !ok {
				err := fmt.Errorf("Unexpected value in %s: %#v (expecting value of type %T)", tabletenv.TxLogger.Name(), out, &LogStats{})
				io.WriteString(w, `<tr class="error">`)
				io.WriteString(w, err.Error())
				io.WriteString(w, "</tr>")
				log.Error(err)
				continue
			}
			var level string
			if stats.TotalTime().Seconds() < 0.01 {
				level = "low"
			} else if stats.TotalTime().Seconds() < 0.1 {
				level = "medium"
			} else {
				level = "high"
			}
			tmplData := struct {
				*LogStats
				ColorLevel string
			}{stats, level}
			if err := querylogzTmpl.Execute(w, tmplData); err != nil {
				log.Errorf("querylogz: couldn't execute template: %v", err)
			}
		case <-tmr.C:
			return
		}
	}
}

func parseTimeoutLimitParams(req *http.Request) (time.Duration, int) {
	timeout := 10
	limit := 300
	if ts, ok := req.URL.Query()["timeout"]; ok {
		if t, err := strconv.Atoi(ts[0]); err == nil {
			timeout = adjustValue(t, 0, 60)
		}
	}
	if l, ok := req.URL.Query()["limit"]; ok {
		if lim, err := strconv.Atoi(l[0]); err == nil {
			limit = adjustValue(lim, 1, 200000)
		}
	}
	return time.Duration(timeout) * time.Second, limit
}

func adjustValue(val int, lower int, upper int) int {
	if val < lower {
		return lower
	} else if val > upper {
		return upper
	}
	return val
}
