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

const splitDiffHTML = `
<!DOCTYPE html>
<head>
  <title>Split Diff Action</title>
</head>
<body>
  <h1>Split Diff Action</h1>

    {{if .Error}}
      <b>Error:</b> {{.Error}}</br>
    {{else}}
      {{range $i, $si := .Shards}}
        <li><a href="/Diffs/SplitDiff?keyspace={{$si.Keyspace}}&shard={{$si.Shard}}">{{$si.Keyspace}}/{{$si.Shard}}</a></li>
      {{end}}
    {{end}}
</body>
`

const splitDiffHTML2 = `
<!DOCTYPE html>
<head>
  <title>Split Diff Action</title>
</head>
<body>
  <p>Shard involved: {{.Keyspace}}/{{.Shard}}</p>
  <h1>Split Diff Action</h1>
    <form action="/Diffs/SplitDiff" method="post">
      <LABEL for="excludeTables">Exclude Tables: </LABEL>
        <INPUT type="text" id="excludeTables" name="excludeTables" value=""></BR>
      <INPUT type="hidden" name="keyspace" value="{{.Keyspace}}"/>
      <INPUT type="hidden" name="shard" value="{{.Shard}}"/>
      <INPUT type="submit" name="submit" value="Split Diff"/>
    </form>
  </body>
`

var splitDiffTemplate = mustParseTemplate("splitDiff", splitDiffHTML)
var splitDiffTemplate2 = mustParseTemplate("splitDiff2", splitDiffHTML2)

func commandSplitDiff(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (worker.Worker, error) {
	excludeTables := subFlags.String("exclude_tables", "", "comma separated list of tables to exclude")
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		return nil, fmt.Errorf("command SplitDiff requires <keyspace/shard>")
	}
	keyspace, shard, err := topo.ParseKeyspaceShardString(subFlags.Arg(0))
	if err != nil {
		return nil, err
	}
	var excludeTableArray []string
	if *excludeTables != "" {
		excludeTableArray = strings.Split(*excludeTables, ",")
	}
	return worker.NewSplitDiffWorker(wr, *cell, keyspace, shard, excludeTableArray), nil
}

// shardsWithSources returns all the shards that have SourceShards set
// with no Tables list.
func shardsWithSources(wr *wrangler.Wrangler) ([]map[string]string, error) {
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

					if len(si.SourceShards) > 0 && len(si.SourceShards[0].Tables) == 0 {
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

func interactiveSplitDiff(wr *wrangler.Wrangler, w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		httpError(w, "cannot parse form: %s", err)
		return
	}
	keyspace := r.FormValue("keyspace")
	shard := r.FormValue("shard")

	if keyspace == "" || shard == "" {
		// display the list of possible shards to chose from
		result := make(map[string]interface{})
		shards, err := shardsWithSources(wr)
		if err != nil {
			result["Error"] = err.Error()
		} else {
			result["Shards"] = shards
		}

		executeTemplate(w, splitDiffTemplate, result)
		return
	}

	submitButtonValue := r.FormValue("submit")
	if submitButtonValue == "" {
		// display the input form
		result := make(map[string]interface{})
		result["Keyspace"] = keyspace
		result["Shard"] = shard
		executeTemplate(w, splitDiffTemplate2, result)
		return
	}

	// Process input form.
	excludeTables := r.FormValue("excludeTables")
	excludeTableArray := strings.Split(excludeTables, ",")

	// start the diff job
	wrk := worker.NewSplitDiffWorker(wr, *cell, keyspace, shard, excludeTableArray)
	if _, err := setAndStartWorker(wrk); err != nil {
		httpError(w, "cannot set worker: %s", err)
		return
	}

	http.Redirect(w, r, servenv.StatusURLPath(), http.StatusTemporaryRedirect)
}

func init() {
	addCommand("Diffs", command{"SplitDiff",
		commandSplitDiff, interactiveSplitDiff,
		"[--exclude_tables=''] <keyspace/shard>",
		"Diffs a rdonly destination shard against its SourceShards"})
}
