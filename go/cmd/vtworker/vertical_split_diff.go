// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"net/http"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/servenv"
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

var verticalSplitDiffTemplate = loadTemplate("verticalSplitDiff", verticalSplitDiffHTML)

func commandVerticalSplitDiff(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) worker.Worker {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		log.Fatalf("command VerticalSplitDiff requires <keyspace/shard|zk shard path>")
	}
	keyspace, shard := shardParamToKeyspaceShard(subFlags.Arg(0))
	return worker.NewVerticalSplitDiffWorker(wr, *cell, keyspace, shard)
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

	// start the diff job
	wrk := worker.NewVerticalSplitDiffWorker(wr, *cell, keyspace, shard)
	if _, err := setAndStartWorker(wrk, nil); err != nil {
		httpError(w, "cannot set worker: %s", err)
		return
	}

	http.Redirect(w, r, servenv.StatusURLPath(), http.StatusTemporaryRedirect)
}

func init() {
	addCommand("Diffs", command{"VerticalSplitDiff",
		commandVerticalSplitDiff, interactiveVerticalSplitDiff,
		"<keyspace/shard|zk shard path>",
		"Diffs a rdonly destination keyspace against its SourceShard for a vertical split"})
}
