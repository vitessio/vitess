// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/worker"
	"github.com/youtube/vitess/go/vt/wrangler"
)

const verticalSplitDiffHTML = `
<!DOCTYPE html>
<head>
  <title>Vertical Split Diff Action</title>
</head>
<body>
  <h1>Vertical Split Diff Action</h1>

    {{if .Error}}
      <b>Error:</b> {{.Error}}</br>
    {{else}}
      {{range $i, $si := .Shards}}
        <li><a href="/Diffs/VerticalSplitDiff?keyspace={{$si.Keyspace}}&shard={{$si.Shard}}">{{$si.Keyspace}}/{{$si.Shard}}</a></li>
      {{end}}
    {{end}}
</body>
`
const verticalSplitDiffHTML2 = `
<!DOCTYPE html>
<head>
  <title>Vertical Split Diff Action</title>
</head>
<body>
  <p>Shard involved: {{.Keyspace}}/{{.Shard}}</p>
  <h1>Vertical Split Diff Action</h1>
    <form action="/Diffs/VerticalSplitDiff" method="post">
      <LABEL for="excludeTables">Exclude Tables: </LABEL>
        <INPUT type="text" id="excludeTables" name="excludeTables" value=""></BR>
      <INPUT type="hidden" name="keyspace" value="{{.Keyspace}}"/>
      <INPUT type="hidden" name="shard" value="{{.Shard}}"/>
      <INPUT type="submit" name="submit" value="Vertical Split Diff"/>
    </form>
  </body>
`

var verticalSplitDiffTemplate = mustParseTemplate("verticalSplitDiff", verticalSplitDiffHTML)
var verticalSplitDiffTemplate2 = mustParseTemplate("verticalSplitDiff2", verticalSplitDiffHTML2)

func commandVerticalSplitDiff(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (worker.Worker, error) {
	excludeTables := subFlags.String("exclude_tables", "", "comma separated list of tables to exclude")
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		return nil, fmt.Errorf("command VerticalSplitDiff requires <keyspace/shard>")
	}
	keyspace, shard, err := topo.ParseKeyspaceShardString(subFlags.Arg(0))
	if err != nil {
		return nil, err
	}
	var excludeTableArray []string
	if *excludeTables != "" {
		excludeTableArray = strings.Split(*excludeTables, ",")
	}
	return worker.NewVerticalSplitDiffWorker(wr, *cell, keyspace, shard, excludeTableArray), nil
}

// shardsWithTablesSources returns all the shards that have SourceShards set
// to one value, with an array of Tables.
func shardsWithTablesSources(wr *wrangler.Wrangler) ([]map[string]string, error) {
	keyspaces, err := wr.TopoServer().GetKeyspaces()
	if err != nil {
		return nil, err
	}

	wg := sync.WaitGroup{}
	mu := sync.Mutex{} // protects result
	result := make([]map[string]string, 0, len(keyspaces))
	rec := concurrency.AllErrorRecorder{}
	for _, keyspace := range keyspaces {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()
			shards, err := wr.TopoServer().GetShardNames(keyspace)
			if err != nil {
				rec.RecordError(err)
				return
			}
			for _, shard := range shards {
				wg.Add(1)
				go func(keyspace, shard string) {
					defer wg.Done()
					si, err := wr.TopoServer().GetShard(keyspace, shard)
					if err != nil {
						rec.RecordError(err)
						return
					}

					if len(si.SourceShards) == 1 && len(si.SourceShards[0].Tables) > 0 {
						mu.Lock()
						result = append(result, map[string]string{
							"Keyspace": keyspace,
							"Shard":    shard,
						})
						mu.Unlock()
					}
				}(keyspace, shard)
			}
		}(keyspace)
	}
	wg.Wait()

	if rec.HasErrors() {
		return nil, rec.Error()
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("There are no shards with SourceShards")
	}
	return result, nil
}

func interactiveVerticalSplitDiff(wr *wrangler.Wrangler, w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		httpError(w, "cannot parse form: %s", err)
		return
	}
	keyspace := r.FormValue("keyspace")
	shard := r.FormValue("shard")

	if keyspace == "" || shard == "" {
		// display the list of possible shards to chose from
		result := make(map[string]interface{})
		shards, err := shardsWithTablesSources(wr)
		if err != nil {
			result["Error"] = err.Error()
		} else {
			result["Shards"] = shards
		}

		executeTemplate(w, verticalSplitDiffTemplate, result)
		return
	}

	submitButtonValue := r.FormValue("submit")
	if submitButtonValue == "" {
		// display the input form
		result := make(map[string]interface{})
		result["Keyspace"] = keyspace
		result["Shard"] = shard
		executeTemplate(w, verticalSplitDiffTemplate2, result)
		return
	}

	// Process input form.
	excludeTables := r.FormValue("excludeTables")
	excludeTableArray := strings.Split(excludeTables, ",")

	// start the diff job
	wrk := worker.NewVerticalSplitDiffWorker(wr, *cell, keyspace, shard, excludeTableArray)
	if _, err := setAndStartWorker(wrk); err != nil {
		httpError(w, "cannot set worker: %s", err)
		return
	}

	http.Redirect(w, r, servenv.StatusURLPath(), http.StatusTemporaryRedirect)
}

func init() {
	addCommand("Diffs", command{"VerticalSplitDiff",
		commandVerticalSplitDiff, interactiveVerticalSplitDiff,
		"[--exclude_tables=''] <keyspace/shard>",
		"Diffs a rdonly destination keyspace against its SourceShard for a vertical split"})
}
