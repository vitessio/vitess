// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"text/template"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/acl"
	"github.com/youtube/vitess/go/vt/logz"
)

var (
	streamqueryzHeader = []byte(`<thead>
		<tr>
			<th>Query</th>
			<th>Context</th>
			<th>Duration</th>
			<th>Start</th>
			<th>ConnectionID</th>
			<th>Terminate</th>
		</tr>
        </thead>
	`)
	streamqueryzTmpl = template.Must(template.New("example").Parse(`
		<tr>
			<td>{{.Query}}</td>
			<td>{{.ContextHTML}}</td>
			<td>{{.Duration}}</td>
			<td>{{.Start}}</td>
			<td>{{.ConnID}}</td>
			<td><a href='/streamqueryz/terminate?connID={{.ConnID}}'>Terminate</a></td>
		</tr>
	`))
)

func streamQueryzHandler(queryList *QueryList, w http.ResponseWriter, r *http.Request) {
	if err := acl.CheckAccessHTTP(r, acl.DEBUGGING); err != nil {
		acl.SendError(w, err)
		return
	}
	rows := queryList.GetQueryzRows()
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
	w.Write(streamqueryzHeader)
	for i := range rows {
		if err := streamqueryzTmpl.Execute(w, rows[i]); err != nil {
			log.Errorf("streamlogz: couldn't execute template: %v", err)
		}
	}
}

func streamQueryzTerminateHandler(queryList *QueryList, w http.ResponseWriter, r *http.Request) {
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
	if err = queryList.Terminate(int64(c)); err != nil {
		http.Error(w, fmt.Sprintf("error: %v", err), http.StatusInternalServerError)
		return
	}
	streamQueryzHandler(queryList, w, r)
}
