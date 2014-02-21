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
	txlogzHeader = []byte(`
		<tr>
			<th>Transaction id</th>
			<th>Start</th>
			<th>End</th>
			<th>Duration</th>
			<th>Decision</th>
			<th>Statements</th>
		</tr>
	`)
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

type txlogzRow struct {
	Tid        string
	Start, End string
	Duration   string
	Decision   string
	Statements []string
	Color      string
}

func init() {
	http.HandleFunc("/txlogz", txlogzHandler)
}

// txlogzHandler serves a human readable snapshot of the
// current transaction log.
func txlogzHandler(w http.ResponseWriter, r *http.Request) {
	ch := TxLogger.Subscribe(nil)
	defer TxLogger.Unsubscribe(ch)
	startHTMLTable(w)
	defer endHTMLTable(w)
	w.Write(txlogzHeader)

	deadline := time.After(10 * time.Second)
	for i := 0; i < 300; i++ {
		select {
		case out := <-ch:
			strs := strings.Split(strings.Trim(out, "\n"), "\t")
			if len(strs) < 6 {
				txlogzTmpl.Execute(w, &txlogzRow{Tid: fmt.Sprintf("Short: %d", len(strs))})
				continue
			}
			Value := &txlogzRow{
				Tid:        strs[0],
				Start:      strs[1],
				End:        strs[2],
				Duration:   strs[3],
				Decision:   strs[4],
				Statements: strings.Split(strs[5], ";"),
			}
			duration, _ := strconv.ParseFloat(Value.Duration, 64)
			if duration < 0.1 {
				Value.Color = "low"
			} else if duration < 1.0 {
				Value.Color = "medium"
			} else {
				Value.Color = "high"
			}
			txlogzTmpl.Execute(w, Value)
		case <-deadline:
			return
		}
	}
}
