/*
Copyright 2020 The Vitess Authors.

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
	"html"
	"net/http"
	"strconv"
	"text/template"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/log"
)

var (
	debugEnvHeader = []byte(`
	<thead><tr>
		<th>Variable Name</th>
		<th>Value</th>
		<th>Action</th>
	</tr></thead>
	`)
	debugEnvRow = template.Must(template.New("debugenv").Parse(`
	<tr><form method="POST">
		<td>{{.VarName}}</td>
		<td>
			<input type="hidden" name="varname" value="{{.VarName}}"></input>
			<input type="text" name="value" value="{{.Value}}"></input>
		</td>
		<td><input type="submit" name="Action" value="Modify"></input></td>
	</form></tr>
	`))
)

type envValue struct {
	VarName string
	Value   string
}

func debugEnvHandler(tsv *TabletServer, w http.ResponseWriter, r *http.Request) {
	if err := acl.CheckAccessHTTP(r, acl.ADMIN); err != nil {
		acl.SendError(w, err)
		return
	}

	var msg string
	if r.Method == "POST" {
		varname := r.FormValue("varname")
		value := r.FormValue("value")
		setIntVal := func(f func(int)) {
			ival, err := strconv.Atoi(value)
			if err != nil {
				msg = fmt.Sprintf("Failed setting value for %v: %v", varname, err)
				return
			}
			f(ival)
			msg = fmt.Sprintf("Setting %v to: %v", varname, value)
		}
		switch varname {
		case "PoolSize":
			setIntVal(tsv.SetPoolSize)
		case "StreamPoolSize":
			setIntVal(tsv.SetStreamPoolSize)
		case "TxPoolSize":
			setIntVal(tsv.SetTxPoolSize)
		case "QueryCacheCapacity":
			setIntVal(tsv.SetQueryPlanCacheCap)
		case "MaxResultSize":
			setIntVal(tsv.SetMaxResultSize)
		case "WarnResultSize":
			setIntVal(tsv.SetWarnResultSize)
		case "Consolidator":
			tsv.SetConsolidatorMode(value)
			msg = fmt.Sprintf("Setting %v to: %v", varname, value)
		}
	}

	var vars []envValue
	addIntVar := func(varname string, f func() int) {
		vars = append(vars, envValue{
			VarName: varname,
			Value:   fmt.Sprintf("%v", f()),
		})
	}
	addIntVar("PoolSize", tsv.PoolSize)
	addIntVar("StreamPoolSize", tsv.StreamPoolSize)
	addIntVar("TxPoolSize", tsv.TxPoolSize)
	addIntVar("QueryCacheCapacity", tsv.QueryPlanCacheCap)
	addIntVar("MaxResultSize", tsv.MaxResultSize)
	addIntVar("WarnResultSize", tsv.WarnResultSize)
	vars = append(vars, envValue{
		VarName: "Consolidator",
		Value:   tsv.ConsolidatorMode(),
	})

	format := r.FormValue("format")
	if format == "json" {
		mvars := make(map[string]string)
		for _, v := range vars {
			mvars[v.VarName] = v.Value
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(mvars)
		return
	}

	// gridTable is reused from twopcz.go.
	w.Write(gridTable)
	w.Write([]byte("<h3>Internal Variables</h3>\n"))
	if msg != "" {
		w.Write([]byte(fmt.Sprintf("<b>%s</b><br /><br />\n", html.EscapeString(msg))))
	}
	w.Write(startTable)
	w.Write(debugEnvHeader)
	for _, v := range vars {
		if err := debugEnvRow.Execute(w, v); err != nil {
			log.Errorf("queryz: couldn't execute template: %v", err)
		}
	}
	w.Write(endTable)
}
