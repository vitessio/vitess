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
	"context"
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"strconv"
	"time"

	"github.com/google/safehtml/template"

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
		<td>{{.Name}}</td>
		<td>
			<input type="hidden" name="varname" value="{{.Name}}"></input>
			<input type="text" name="value" value="{{.Value}}"></input>
		</td>
		<td><input type="submit" name="Action" value="Modify"></input></td>
	</form></tr>
	`))
)

type envValue struct {
	Name  string
	Value string
}

// this cannot be an anonymous function within debugEnvHandler because those kinds
// of functions cannot (currently) have type params.
func addVar[T any](vars []envValue, name string, f func() T) []envValue {
	return append(vars, envValue{
		Name:  name,
		Value: fmt.Sprintf("%v", f()),
	})
}

func debugEnvHandler(tsv *TabletServer, w http.ResponseWriter, r *http.Request) {
	if err := acl.CheckAccessHTTP(r, acl.ADMIN); err != nil {
		acl.SendError(w, err)
		return
	}

	switch r.Method {
	case http.MethodPost:
		handlePost(tsv, w, r)
	case http.MethodGet:
		handleGet(tsv, w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func handlePost(tsv *TabletServer, w http.ResponseWriter, r *http.Request) {
	varname := r.FormValue("varname")
	value := r.FormValue("value")

	var msg string
	if varname == "" || value == "" {
		http.Error(w, "Missing varname or value", http.StatusBadRequest)
		return
	}

	setIntVal := func(f func(int)) error {
		ival, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid int value for %v: %v", varname, err)
		}
		f(ival)
		msg = fmt.Sprintf("Setting %v to: %v", varname, value)
		return nil
	}

	setIntValCtx := func(f func(context.Context, int) error) error {
		ival, err := strconv.Atoi(value)
		if err == nil {
			err = f(r.Context(), ival)
		}
		if err != nil {
			return fmt.Errorf("failed setting value for %v: %v", varname, err)
		}
		msg = fmt.Sprintf("Setting %v to: %v", varname, value)
		return nil
	}

	setInt64Val := func(f func(int64)) error {
		ival, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid int64 value for %v: %v", varname, err)
		}
		f(ival)
		msg = fmt.Sprintf("Setting %v to: %v", varname, value)
		return nil
	}

	setDurationVal := func(f func(time.Duration)) error {
		durationVal, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("invalid duration value for %v: %v", varname, err)
		}
		f(durationVal)
		msg = fmt.Sprintf("Setting %v to: %v", varname, value)
		return nil
	}

	setFloat64Val := func(f func(float64)) error {
		fval, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return fmt.Errorf("invalid float64 value for %v: %v", varname, err)
		}
		f(fval)
		msg = fmt.Sprintf("Setting %v to: %v", varname, value)
		return nil
	}

	var err error
	switch varname {
	case "ReadPoolSize":
		err = setIntValCtx(tsv.SetPoolSize)
	case "StreamPoolSize":
		err = setIntValCtx(tsv.SetStreamPoolSize)
	case "TransactionPoolSize":
		err = setIntValCtx(tsv.SetTxPoolSize)
	case "MaxResultSize":
		err = setIntVal(tsv.SetMaxResultSize)
	case "WarnResultSize":
		err = setIntVal(tsv.SetWarnResultSize)
	case "RowStreamerMaxInnoDBTrxHistLen":
		err = setInt64Val(func(val int64) { tsv.Config().RowStreamer.MaxInnoDBTrxHistLen = val })
	case "RowStreamerMaxMySQLReplLagSecs":
		err = setInt64Val(func(val int64) { tsv.Config().RowStreamer.MaxMySQLReplLagSecs = val })
	case "UnhealthyThreshold":
		err = setDurationVal(func(d time.Duration) { tsv.Config().Healthcheck.UnhealthyThreshold = d })
	case "ThrottleMetricThreshold":
		err = setFloat64Val(tsv.SetThrottleMetricThreshold)
	case "Consolidator":
		tsv.SetConsolidatorMode(value)
		msg = fmt.Sprintf("Setting %v to: %v", varname, value)
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	vars := getVars(tsv)
	sendResponse(r, w, vars, msg)
}

func handleGet(tsv *TabletServer, w http.ResponseWriter, r *http.Request) {
	vars := getVars(tsv)
	sendResponse(r, w, vars, "")
}

func sendResponse(r *http.Request, w http.ResponseWriter, vars []envValue, msg string) {
	format := r.FormValue("format")
	if format == "json" {
		respondWithJSON(w, vars, msg)
		return
	}
	respondWithHTML(w, vars, msg)
}

func getVars(tsv *TabletServer) []envValue {
	var vars []envValue
	vars = addVar(vars, "ReadPoolSize", tsv.PoolSize)
	vars = addVar(vars, "StreamPoolSize", tsv.StreamPoolSize)
	vars = addVar(vars, "TransactionPoolSize", tsv.TxPoolSize)
	vars = addVar(vars, "MaxResultSize", tsv.MaxResultSize)
	vars = addVar(vars, "WarnResultSize", tsv.WarnResultSize)
	vars = addVar(vars, "RowStreamerMaxInnoDBTrxHistLen", func() int64 { return tsv.Config().RowStreamer.MaxInnoDBTrxHistLen })
	vars = addVar(vars, "RowStreamerMaxMySQLReplLagSecs", func() int64 { return tsv.Config().RowStreamer.MaxMySQLReplLagSecs })
	vars = addVar(vars, "UnhealthyThreshold", func() time.Duration { return tsv.Config().Healthcheck.UnhealthyThreshold })
	vars = addVar(vars, "ThrottleMetricThreshold", tsv.ThrottleMetricThreshold)
	vars = append(vars, envValue{
		Name:  "Consolidator",
		Value: tsv.ConsolidatorMode(),
	})

	return vars
}

func respondWithJSON(w http.ResponseWriter, vars []envValue, msg string) {
	mvars := make(map[string]string)
	for _, v := range vars {
		mvars[v.Name] = v.Value
	}
	if msg != "" {
		mvars["ResponseMessage"] = msg
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(mvars)
}

func respondWithHTML(w http.ResponseWriter, vars []envValue, msg string) {
	w.Write(gridTable)
	w.Write([]byte("<h3>Internal Variables</h3>\n"))
	if msg != "" {
		fmt.Fprintf(w, "<b>%s</b><br /><br />\n", html.EscapeString(msg))
	}
	w.Write(startTable)
	w.Write(debugEnvHeader)
	for _, v := range vars {
		if err := debugEnvRow.Execute(w, v); err != nil {
			log.Errorf("debugenv: couldn't execute template: %v", err)
		}
	}
	w.Write(endTable)
}
