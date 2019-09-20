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
	"html/template"
	"net/http"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/log"
)

var (
	gridTable = []byte(`<!DOCTYPE html>
	<style type="text/css">
			table.gridtable {
				font-family: verdana,arial,sans-serif;
				font-size: 11px;
				border-width: 1px;
				border-collapse: collapse; table-layout:fixed; overflow: hidden;
			}
			table.gridtable th {
				border-width: 1px;
				padding: 8px;
				border-style: solid;
				background-color: #dedede;
				white-space: nowrap;
			}
			table.gridtable td {
				border-width: 1px;
				padding: 5px;
				border-style: solid;
			}
			table.gridtable th {
				padding-left: 2em;
				padding-right: 2em;
			}
	</style>
	`)
	startTable = []byte(`
	<table class="gridtable">
	`)
	endTable = []byte(`
	</table>
	`)

	failedzHeader = []byte(`
	<h3>Failed Transactions</h3>
	<thead><tr>
		<th>DTID</th>
		<th>Queries</th>
		<th>Time</th>
		<th>Action</th>
	</tr></thead>
	`)
	failedzRow = template.Must(template.New("failedz").Parse(`
	<tr>
		<td>{{.Dtid}}</td>
		<td>{{range .Queries}}{{.}}<br>{{end}}</td>
		<td>{{.Time}}</td>
		<td><form>
			<input type="hidden" name="dtid" value="{{.Dtid}}"></input>
			<input type="submit" name="Action" value="Discard"></input>
		</form></td>
	</tr>
	`))

	preparedzHeader = []byte(`
	<h3>Prepared Transactions</h3>
	<thead><tr>
		<th>DTID</th>
		<th>Queries</th>
		<th>Time</th>
		<th>Action</th>
	</tr></thead>
	`)
	preparedzRow = template.Must(template.New("preparedz").Parse(`
	<tr>
		<td>{{.Dtid}}</td>
		<td>{{range .Queries}}{{.}}<br>{{end}}</td>
		<td>{{.Time}}</td>
		<td><form>
			<input type="hidden" name="dtid" value="{{.Dtid}}"></input>
			<input type="submit" name="Action" value="Rollback"></input>
			<input type="submit" name="Action" value="Commit"></input>
		</form></td>
	</tr>
	`))

	distributedzHeader = []byte(`
	<h3>Distributed Transactions</h3>
	<thead><tr>
		<th>DTID</th>
		<th>State</th>
		<th>Time</th>
		<th>Participants</th>
		<th>Action</th>
	</tr></thead>
	`)
	distributedzRow = template.Must(template.New("distributedz").Parse(`
	<tr>
		<td>{{.Dtid}}</td>
		<td>{{.State}}</td>
		<td>{{.Created}}</td>
		<td>{{range .Participants}}{{.Keyspace}}:{{.Shard}}<br>{{end}}</td>
		<td><form>
			<input type="hidden" name="dtid" value="{{.Dtid}}"></input>
			<input type="submit" name="Action" value="Conclude"></input>
		</form></td>
	</tr>
	`))
)

func twopczHandler(txe *TxExecutor, w http.ResponseWriter, r *http.Request) {
	if err := acl.CheckAccessHTTP(r, acl.DEBUGGING); err != nil {
		acl.SendError(w, err)
		return
	}
	var err error
	dtid := r.FormValue("dtid")
	action := r.FormValue("Action")
	switch action {
	case "Discard", "Rollback":
		err = txe.RollbackPrepared(dtid, 0)
	case "Commit":
		err = txe.CommitPrepared(dtid)
	case "Conclude":
		err = txe.ConcludeTransaction(dtid)
	}
	var msg string
	if action != "" {
		if err != nil {
			msg = fmt.Sprintf("%s(%s): %v", r.FormValue("Action"), dtid, err)
		} else {
			msg = fmt.Sprintf("%s(%s): completed.", r.FormValue("Action"), dtid)
		}
	}
	distributed, prepared, failed, err := txe.ReadTwopcInflight()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	format := r.FormValue("format")
	if format == "json" {
		w.Header().Set("Content-Type", "application/json")
		js, err := json.Marshal(struct {
			Distributed      []*DistributedTx
			Prepared, Failed []*PreparedTx
		}{
			Distributed: distributed,
			Prepared:    prepared,
			Failed:      failed,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(js)
		return
	}

	w.Write(gridTable)
	w.Write([]byte("<h2>WARNING: Actions on this page can jeopardize data integrity.</h2>\n"))
	if msg != "" {
		w.Write([]byte(fmt.Sprintf("%s\n", msg)))
	}

	w.Write(startTable)
	w.Write(failedzHeader)
	for _, row := range failed {
		if err := failedzRow.Execute(w, row); err != nil {
			log.Errorf("queryz: couldn't execute template: %v", err)
		}
	}
	w.Write(endTable)

	w.Write(startTable)
	w.Write(preparedzHeader)
	for _, row := range prepared {
		if err := preparedzRow.Execute(w, row); err != nil {
			log.Errorf("queryz: couldn't execute template: %v", err)
		}
	}
	w.Write(endTable)

	w.Write(startTable)
	w.Write(distributedzHeader)
	for _, row := range distributed {
		if err := distributedzRow.Execute(w, row); err != nil {
			log.Errorf("queryz: couldn't execute template: %v", err)
		}
	}
	w.Write(endTable)
}
