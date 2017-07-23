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

package tabletserver

import (
	"fmt"
	"html/template"
	"io"
	"net/http"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/logz"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

var (
	txlogzHeader = []byte(`
		<thead>
			<tr>
				<th>Transaction id</th>
				<th>Effective caller</th>
				<th>Immediate caller</th>
				<th>Start</th>
				<th>End</th>
				<th>Duration</th>
				<th>Decision</th>
				<th>Statements</th>
			</tr>
		</thead>
	`)
	txlogzFuncMap = template.FuncMap{
		"stampMicro":         func(t time.Time) string { return t.Format(time.StampMicro) },
		"getEffectiveCaller": func(e *vtrpcpb.CallerID) string { return callerid.GetPrincipal(e) },
		"getImmediateCaller": func(i *querypb.VTGateCallerID) string { return callerid.GetUsername(i) },
	}
	txlogzTmpl = template.Must(template.New("example").Funcs(txlogzFuncMap).Parse(`
		<tr class="{{.ColorLevel}}">
			<td>{{.TransactionID}}</td>
			<td>{{.EffectiveCallerID | getEffectiveCaller}}</td>
			<td>{{.ImmediateCallerID | getImmediateCaller}}</td>
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
// Endpoint: /txlogz?timeout=%d&limit=%d
// timeout: the txlogz will keep dumping transactions until timeout
// limit: txlogz will keep dumping transcations until it hits the limit
func txlogzHandler(w http.ResponseWriter, req *http.Request) {
	if err := acl.CheckAccessHTTP(req, acl.DEBUGGING); err != nil {
		acl.SendError(w, err)
		return
	}

	timeout, limit := parseTimeoutLimitParams(req)
	ch := tabletenv.TxLogger.Subscribe("txlogz")
	defer tabletenv.TxLogger.Unsubscribe(ch)
	logz.StartHTMLTable(w)
	defer logz.EndHTMLTable(w)
	w.Write(txlogzHeader)

	tmr := time.NewTimer(timeout)
	defer tmr.Stop()
	for i := 0; i < limit; i++ {
		select {
		case out := <-ch:
			txc, ok := out.(*TxConnection)
			if !ok {
				err := fmt.Errorf("Unexpected value in %s: %#v (expecting value of type %T)", tabletenv.TxLogger.Name(), out, &TxConnection{})
				io.WriteString(w, `<tr class="error">`)
				io.WriteString(w, err.Error())
				io.WriteString(w, "</tr>")
				log.Error(err)
				continue
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
			if err := txlogzTmpl.Execute(w, tmplData); err != nil {
				log.Errorf("txlogz: couldn't execute template: %v", err)
			}
		case <-tmr.C:
			return
		}
	}
}
