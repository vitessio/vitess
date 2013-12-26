// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"net/http"

	log "github.com/golang/glog"
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

var splitDiffTemplate = loadTemplate("splitdiff", splitDiffHTML)

func commandSplitDiff(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) worker.Worker {
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		log.Fatalf("command SplitDiff requires <keyspace/shard|zk shard path>")
	}
	keyspace, shard := shardParamToKeyspaceShard(subFlags.Arg(0))
	return worker.NewSplitDiffWorker(wr, *cell, keyspace, shard)
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
	} else {
		// start the diff job
		wrk := worker.NewSplitDiffWorker(wr, *cell, keyspace, shard)
		if _, err := setAndStartWorker(wrk); err != nil {
			httpError(w, "cannot set worker: %s", err)
			return
		}

		http.Redirect(w, r, "/status", http.StatusTemporaryRedirect)
	}
}

func init() {
	addCommand("Diffs", command{"SplitDiff",
		commandSplitDiff, interactiveSplitDiff,
		"<keyspace/shard|zk shard path>",
		"Diffs a rdonly destination shard against its SourceShards"})
}
