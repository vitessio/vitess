// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"text/template"
	"time"
)

var (
	queryLogzTmpl = template.Must(template.New("example").Parse(`
		<tr class="{{.Color}}">
			<td>{{.Method}}</td>
			<td>{{.RemoteAddr}}</td>
			<td>{{.Username}}</td>
			<td>{{.Start}}</td>
			<td>{{.End}}</td>
			<td>{{.Duration}}</td>
			<td>{{.MySQL}}</td>
			<td>{{.Conn}}</td>
			<td>{{.PlanType}}</td>
			<td>{{.Sql}}</td>
			<td>{{.Queries}}</td>
			<td>{{.Sources}}</td>
			<td>{{.Rows}}</td>
			<td>{{.Hits}}</td>
			<td>{{.Misses}}</td>
			<td>{{.Absent}}</td>
			<td>{{.Invalidations}}</td>
		</tr>
	`))
)

func init() {
	http.HandleFunc("/querylogz", queryLogzHandler)
}

// queryLogzHandler serves a human readable snapshot of the
// current query log.
func queryLogzHandler(w http.ResponseWriter, r *http.Request) {
	ch := SqlQueryLogger.Subscribe(nil)
	defer SqlQueryLogger.Unsubscribe(ch)
	startHTMLTable(w)
	defer endHTMLTable(w)
	w.Write([]byte(`
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
			<th>Rows</th>
			<th>Hits</th>
			<th>Misses</th>
			<th>Absent</th>
			<th>Invalidations</th>
		</tr>
	`))

	deadline := time.After(10 * time.Second)
	for i := 0; i < 300; i++ {
		select {
		case out := <-ch:
			strs := strings.Split(strings.Trim(out, "\n"), "\t")
			Value := &struct {
				Method        string
				RemoteAddr    string
				Username      string
				Start         string
				End           string
				Duration      string
				MySQL         string
				Conn          string
				PlanType      string
				Sql           string
				Queries       string
				Sources       string
				Rows          string
				Hits          string
				Misses        string
				Absent        string
				Invalidations string
				Color         string
			}{}
			if len(strs) < 19 {
				Value.Method = fmt.Sprintf("Short: %d", len(strs))
			} else {
				Value.Method = strs[0]
				Value.RemoteAddr = strs[1]
				Value.Username = strs[2]
				Value.Start = strs[3]
				Value.End = strs[4]
				Value.Duration = strs[5]
				duration, _ := strconv.ParseFloat(Value.Duration, 64)
				if duration < 0.01 {
					Value.Color = "low"
				} else if duration < 0.1 {
					Value.Color = "medium"
				} else {
					Value.Color = "high"
				}
				Value.MySQL = strs[12]
				Value.Conn = strs[13]
				Value.PlanType = strs[6]
				Value.Sql = strs[7]
				Value.Queries = strs[9]
				Value.Sources = strs[11]
				Value.Rows = strs[14]
				Value.Hits = strs[15]
				Value.Misses = strs[16]
				Value.Absent = strs[17]
				Value.Invalidations = strs[18]
			}
			queryLogzTmpl.Execute(w, Value)
		case <-deadline:
			return
		}
	}
}
