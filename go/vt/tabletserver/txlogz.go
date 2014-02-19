// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var (
	txlogzTmpl = template.Must(template.New("example").Parse(`
		<tr class="{{.Color}}">
			<td>{{.Tid}}</td>
			<td>{{.Start}}</td>
			<td>{{.End}}</td>
			<td>{{.Duration}}</td>
			<td>{{.Decision}}</td>
			<td>
				{{ range .Statements }}
					{{.}}<br>
				{{ end}}
			</td>
		</tr>
	`))
)

func init() {
	http.HandleFunc("/txlogz", txLogzHandler)
}

// txLogzHandler serves a human readable snapshot of the
// current transaction log.
func txLogzHandler(w http.ResponseWriter, r *http.Request) {
	ch := TxLogger.Subscribe(nil)
	defer TxLogger.Unsubscribe(ch)
	startHTMLTable(w)
	defer endHTMLTable(w)
	w.Write([]byte(`
		<tr>
			<th>Transaction id</th>
			<th>Start</th>
			<th>End</th>
			<th>Duration</th>
			<th>Decision</th>
			<th>Statements</th>
		</tr>
	`))

	deadline := time.After(10 * time.Second)
	for i := 0; i < 300; i++ {
		select {
		case out := <-ch:
			strs := strings.Split(strings.Trim(out, "\n"), "\t")
			Value := &struct {
				Tid        string
				Start, End string
				Duration   string
				Decision   string
				Statements []string
				Color      string
			}{}
			if len(strs) < 6 {
				Value.Tid = fmt.Sprintf("Short: %d", len(strs))
			} else {
				Value.Tid = strs[0]
				Value.Start = strs[1]
				Value.End = strs[2]
				Value.Duration = strs[3]
				Value.Decision = strs[4]
				Value.Statements = strings.Split(strs[5], ";")
				duration, _ := strconv.ParseFloat(Value.Duration, 64)
				if duration < 0.1 {
					Value.Color = "low"
				} else if duration < 1.0 {
					Value.Color = "medium"
				} else {
					Value.Color = "high"
				}
			}
			txlogzTmpl.Execute(w, Value)
		case <-deadline:
			return
		}
	}
}
