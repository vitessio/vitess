// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"text/template"
	"time"

	log "github.com/golang/glog"
)

var (
	querylogzHeader = []byte(`
		<tr>
			<th>Method</th>
			<th>Client</th>
			<th>User</th>
			<th>Start</th>
			<th>End</th>
			<th>Duration</th>
			<th>MySQL time</th>
			<th>Conn wait</th>
			<th>Plan</th>
			<th>SQL</th>
			<th>Queries</th>
			<th>Sources</th>
			<th>Response Size (Rows)</th>
			<th>Cache Hits</th>
			<th>Cache Misses</th>
			<th>Cache Absent</th>
			<th>Cache Invalidations</th>
		</tr>
	`)
	querylogzFuncMap = template.FuncMap{
		"stampMicro":   func(t time.Time) string { return t.Format(time.StampMicro) },
		"cssWrappable": wrappable,
		"unquote":      func(s string) string { return strings.Trim(s, "\"") },
	}
	querylogzTmpl = template.Must(template.New("example").Funcs(querylogzFuncMap).Parse(`
		<tr class=".ColorLevel">
			<td>{{.Method}}</td>
			<td>{{.RemoteAddr}}</td>
			<td>{{.Username}}</td>
			<td>{{.StartTime | stampMicro}}</td>
			<td>{{.EndTime | stampMicro}}</td>
			<td>{{.TotalTime.Seconds}}</td>
			<td>{{.MysqlResponseTime.Seconds}}</td>
			<td>{{.WaitingForConnection.Seconds}}</td>
			<td>{{.PlanType}}</td>
			<td>{{.OriginalSql | unquote | cssWrappable}}</td>
			<td>{{.NumberOfQueries}}</td>
			<td>{{.FmtQuerySources}}</td>
			<td>{{.SizeOfResponse}}</td>
			<td>{{.CacheHits}}</td>
			<td>{{.CacheMisses}}</td>
			<td>{{.CacheAbsent}}</td>
			<td>{{.CacheInvalidations}}</td>
		</tr>
	`))
)

func init() {
	http.HandleFunc("/querylogz", querylogzHandler)
}

// querylogzHandler serves a human readable snapshot of the
// current query log.
func querylogzHandler(w http.ResponseWriter, r *http.Request) {
	ch := SqlQueryLogger.Subscribe()
	defer SqlQueryLogger.Unsubscribe(ch)
	startHTMLTable(w)
	defer endHTMLTable(w)
	w.Write(querylogzHeader)

	deadline := time.After(10 * time.Second)
	for i := 0; i < 300; i++ {
		select {
		case out := <-ch:
			stats, ok := out.(*sqlQueryStats)
			if !ok {
				err := fmt.Errorf("Unexpected value in %s: %#v (expecting value of type %T)", TxLogger.Name, out, &sqlQueryStats{})
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
				*sqlQueryStats
				ColorLevel string
			}{stats, level}
			querylogzTmpl.Execute(w, tmplData)
		case <-deadline:
			return
		}
	}
}
