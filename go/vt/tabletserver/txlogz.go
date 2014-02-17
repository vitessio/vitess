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

func init() {
	http.HandleFunc("/txlogz", txDisplay)
}

func txDisplay(w http.ResponseWriter, r *http.Request) {

	ch := TxLogger.Subscribe(nil)
	defer TxLogger.Unsubscribe(ch)

	w.Write([]byte(`
		<!DOCTYPE html>
		<html>
		<head>
		<style type="text/css">
		table.gridtable {
			font-family: verdana,arial,sans-serif;
			font-size:11px;
			border-width: 1px;
			border-collapse: collapse;
		}
		table.gridtable th {
			border-width: 1px;
			padding: 8px;
			border-style: solid;
			background-color: #dedede;
		}
		table.gridtable tr.ok {
			background-color: #f0f0f0;
		}
		table.gridtable tr.warn {
			background-color: #ffcc00;
		}
		table.gridtable tr.flag {
			background-color: #ff3300;
		}
		table.gridtable td {
			border-width: 1px;
			padding: 4px;
			border-style: solid;
		}
		</style>
		</head>
		<body>
		<table class = "gridtable">
		<tr>
			<th>Transaction id</th>
			<th>Start</th>
			<th>End</th>
			<th>Duration</th>
			<th>Decision</th>
			<th>Statements</th>
		</th>
	`))
	defer w.Write([]byte(`
		</table>
		</body>
		</html>
	`))
	tmpl := template.Must(template.New("example").Parse(`
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
	deadline := time.After(10 * time.Second)
	for i := 0; i < 300; i++ {
		select {
		case out := <-ch:
			strs := strings.Split(out, "\t")
			Value := &struct {
				Tid, Start, End, Duration, Decision string
				Statements                          []string
				Color                               string
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
					Value.Color = "ok"
				} else if duration < 1.0 {
					Value.Color = "warn"
				} else {
					Value.Color = "flag"
				}
			}
			tmpl.Execute(w, Value)
		case <-deadline:
			return
		}
	}
}
