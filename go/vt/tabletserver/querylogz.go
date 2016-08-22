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
	"github.com/youtube/vitess/go/acl"
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
				<th>MySQL time</th>
				<th>Conn wait</th>
				<th>Plan</th>
				<th>SQL</th>
				<th>Queries</th>
				<th>Sources</th>
				<th>RowsAffected</th>
				<th>Response Size</th>
				<th>Transaction ID</th>
				<th>Error</th>
			</tr>
		</thead>
	`)
	querylogzFuncMap = template.FuncMap{
		"stampMicro":   func(t time.Time) string { return t.Format(time.StampMicro) },
		"cssWrappable": wrappable,
		"unquote":      func(s string) string { return strings.Trim(s, "\"") },
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
			<td>{{.MysqlResponseTime.Seconds}}</td>
			<td>{{.WaitingForConnection.Seconds}}</td>
			<td>{{.PlanType}}</td>
			<td>{{.OriginalSQL | unquote | cssWrappable}}</td>
			<td>{{.NumberOfQueries}}</td>
			<td>{{.FmtQuerySources}}</td>
			<td>{{.RowsAffected}}</td>
			<td>{{.SizeOfResponse}}</td>
			<td>{{.TransactionID}}</td>
			<td>{{.ErrorStr}}</td>
		</tr>
	`))
)

func init() {
	http.HandleFunc("/querylogz", func(w http.ResponseWriter, r *http.Request) {
		ch := StatsLogger.Subscribe("querylogz")
		defer StatsLogger.Unsubscribe(ch)
		querylogzHandler(ch, w, r)
	})
}

// querylogzHandler serves a human readable snapshot of the
// current query log.
func querylogzHandler(ch chan interface{}, w http.ResponseWriter, r *http.Request) {
	if err := acl.CheckAccessHTTP(r, acl.DEBUGGING); err != nil {
		acl.SendError(w, err)
		return
	}
	timeout, limit := parseTimeoutLimitParams(r)
	startHTMLTable(w)
	defer endHTMLTable(w)
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
				err := fmt.Errorf("Unexpected value in %s: %#v (expecting value of type %T)", TxLogger.Name(), out, &LogStats{})
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
