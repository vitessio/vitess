/*
Copyright 2019 The Vitess Authors.

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
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"text/template"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logz"
)

var (
	livequeryzHeader = []byte(`<thead>
		<tr>
			<th>Type</th>
			<th>Query</th>
			<th>Context</th>
			<th>Duration</th>
			<th>Start</th>
			<th>ConnectionID</th>
			<th>Terminate</th>
		</tr>
        </thead>
	`)
	livequeryzTmpl = template.Must(template.New("example").Parse(`
		<tr>
			<td>{{.Type}}</td>
			<td>{{.Query}}</td>
			<td>{{.ContextHTML}}</td>
			<td>{{.Duration}}</td>
			<td>{{.Start}}</td>
			<td>{{.ConnID}}</td>
			<td><a href='/livequeryz/terminate?connID={{.ConnID}}'>Terminate</a></td>
		</tr>
	`))
)

func livequeryzHandler(queryLists []*QueryList, w http.ResponseWriter, r *http.Request) {
	if err := acl.CheckAccessHTTP(r, acl.DEBUGGING); err != nil {
		acl.SendError(w, err)
		return
	}
	var rows []QueryDetailzRow
	for _, ql := range queryLists {
		rows = ql.AppendQueryzRows(rows)
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, fmt.Sprintf("cannot parse form: %s", err), http.StatusInternalServerError)
		return
	}
	format := r.FormValue("format")
	if format == "json" {
		js, err := json.Marshal(rows)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		return
	}
	logz.StartHTMLTable(w)
	defer logz.EndHTMLTable(w)
	w.Write(livequeryzHeader)
	for i := range rows {
		if err := livequeryzTmpl.Execute(w, rows[i]); err != nil {
			log.Errorf("livequeryz: couldn't execute template: %v", err)
		}
	}
}

func livequeryzTerminateHandler(queryLists []*QueryList, w http.ResponseWriter, r *http.Request) {
	if err := acl.CheckAccessHTTP(r, acl.ADMIN); err != nil {
		acl.SendError(w, err)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, fmt.Sprintf("cannot parse form: %s", err), http.StatusInternalServerError)
		return
	}
	connID := r.FormValue("connID")
	c, err := strconv.Atoi(connID)
	if err != nil {
		http.Error(w, "invalid connID", http.StatusInternalServerError)
		return
	}
	for _, ql := range queryLists {
		if ql.Terminate(int64(c)) {
			break
		}
	}
	livequeryzHandler(queryLists, w, r)
}
