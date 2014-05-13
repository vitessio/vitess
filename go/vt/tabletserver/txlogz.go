// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"html/template"
	"net/http"
	"time"
)

var (
	txlogzHeader = []byte(`<thead>

		<tr>
			<th>Transaction id</th>
			<th>Start</th>
			<th>End</th>
			<th>Duration</th>
			<th>Decision</th>
			<th>Statements</th>
		</tr>
</thead>
	`)
	txlogzFuncMap = template.FuncMap{
		"stampMicro": func(t time.Time) string { return t.Format(time.StampMicro) },
	}
	txlogzTmpl = template.Must(template.New("example").Funcs(txlogzFuncMap).Parse(`
		<tr class="{{.ColorLevel}}">
			<td>{{.TransactionID}}</td>
			<td>{{.StartTime | stampMicro}}</td>
			<td>{{.EndTime | stampMicro}}</td>
			<td>{{.Duration}}</td>
			<td>{{.Conclusion}}</td>
			<td>
				{{ range .Queries }}
					{{.}}<br>
				{{ end}}
			</td>
		</tr>`))
)

func init() {
	http.HandleFunc("/txlogz", txlogzHandler)
}

// txlogzHandler serves a human readable snapshot of the
// current transaction log.
func txlogzHandler(w http.ResponseWriter, r *http.Request) {
	ch := TxLogger.Subscribe()
	defer TxLogger.Unsubscribe(ch)
	startHTMLTable(w)
	defer endHTMLTable(w)
	w.Write(txlogzHeader)

	deadline := time.After(10 * time.Second)
	for i := 0; i < 300; i++ {
		select {
		case out := <-ch:
			txc, ok := out.(*TxConnection)
			if !ok {
				panic("TODO")
				// txlogzTmpl.Execute(w, &txlogzRow{Tid: fmt.Sprintf("Short: %d", len(strs))})
				// continue
			}
			duration := txc.EndTime.Sub(txc.StartTime).Seconds()
			var level string
			if duration < 0.1 {
				level = "low"
			} else if duration < 1.0 {
				level = "medium"
			} else {
				level = "high"
			}
			tmplData := struct {
				*TxConnection
				Duration   float64
				ColorLevel string
			}{txc, duration, level}
			txlogzTmpl.Execute(w, tmplData)
		case <-deadline:
			return
		}
	}
}
