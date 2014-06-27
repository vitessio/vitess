// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"net/http"
	"strings"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/worker"
	"github.com/youtube/vitess/go/vt/wrangler"
)

const verticalSplitCloneHTML = `
<!DOCTYPE html>
<head>
  <title>Vertical Split Clone Action</title>
</head>
<body>
  <h1>Vertical Split Clone Action</h1>

    {{if .Error}}
      <b>Error:</b> {{.Error}}</br>
    {{else}}
      <p>Choose the destination keyspace for this action.</p>
      <ul>
      {{range $i, $si := .Keyspaces}}
        <li><a href="/Clones/VerticalSplitClone?keyspace={{$si}}">{{$si}}</a></li>
      {{end}}
      </ul>
    {{end}}
</body>
`

const verticalSplitCloneHTML2 = `
<!DOCTYPE html>
<head>
  <title>Vertical Split Clone Action</title>
</head>
<body>
  <p>Destination keyspace: {{.Keyspace}}</p>
  <h1>Vertical Split Clone Action</h1>
    <form action="/Clones/VerticalSplitClone" method="post">
      <LABEL for="tables">Tables: </LABEL>
        <INPUT type="text" id="tables" name="tables" value="moving.*"></BR>
      <LABEL for="strategy">Strategy: </LABEL>
        <INPUT type="text" id="strategy" name="strategy" value="populateBlpCheckpoint"></BR>
      <INPUT type="hidden" name="keyspace" value="{{.Keyspace}}"/>
      <INPUT type="submit" value="Clone"/>
    </form>
  </body>
`

var verticalSplitCloneTemplate = loadTemplate("verticalSplitClone", verticalSplitCloneHTML)
var verticalSplitCloneTemplate2 = loadTemplate("verticalSplitClone2", verticalSplitCloneHTML2)

func commandVerticalSplitClone(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) worker.Worker {
	tables := subFlags.String("tables", "", "comma separated list of tables to replicate (used for vertical split)")
	strategy := subFlags.String("strategy", "", "which strategy to use for restore, use 'mysqlctl multirestore -help' for more info")
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		log.Fatalf("command VerticalSplitClone requires <destination keyspace/shard|zk shard path>")
	}

	keyspace, shard := shardParamToKeyspaceShard(subFlags.Arg(0))
	var tableArray []string
	if *tables != "" {
		tableArray = strings.Split(*tables, ",")
	}
	return worker.NewVerticalSplitCloneWorker(wr, *cell, keyspace, shard, tableArray, *strategy)
}

// keyspacesWithServedFrom returns all the keyspaces that have ServedFrom set
// to one value.
func keyspacesWithServedFrom(wr *wrangler.Wrangler) ([]string, error) {
	keyspaces, err := wr.TopoServer().GetKeyspaces()
	if err != nil {
		return nil, err
	}

	wg := sync.WaitGroup{}
	mu := sync.Mutex{} // protects result
	result := make([]string, 0, len(keyspaces))
	rec := concurrency.AllErrorRecorder{}
	for _, keyspace := range keyspaces {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()
			ki, err := wr.TopoServer().GetKeyspace(keyspace)
			if err != nil {
				rec.RecordError(err)
				return
			}
			if len(ki.ServedFrom) > 0 {
				mu.Lock()
				result = append(result, keyspace)
				mu.Unlock()
			}
		}(keyspace)
	}
	wg.Wait()

	if rec.HasErrors() {
		return nil, rec.Error()
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("There are no keyspaces with ServedFrom")
	}
	return result, nil
}

func interactiveVerticalSplitClone(wr *wrangler.Wrangler, w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		httpError(w, "cannot parse form: %s", err)
		return
	}

	keyspace := r.FormValue("keyspace")
	if keyspace == "" {
		// display the list of possible keyspaces to choose from
		result := make(map[string]interface{})
		keyspaces, err := keyspacesWithServedFrom(wr)
		if err != nil {
			result["Error"] = err.Error()
		} else {
			result["Keyspaces"] = keyspaces
		}

		executeTemplate(w, verticalSplitCloneTemplate, result)
		return
	}

	tables := r.FormValue("tables")
	strategy := r.FormValue("strategy")
	if tables == "" {
		// display the input form
		result := make(map[string]interface{})
		result["Keyspace"] = keyspace
		executeTemplate(w, verticalSplitCloneTemplate2, result)
		return
	}
	tableArray := strings.Split(tables, ",")

	// start the clone job
	wrk := worker.NewVerticalSplitCloneWorker(wr, *cell, keyspace, "0", tableArray, strategy)
	if _, err := setAndStartWorker(wrk); err != nil {
		httpError(w, "cannot set worker: %s", err)
		return
	}

	http.Redirect(w, r, servenv.StatusURLPath(), http.StatusTemporaryRedirect)
}

func init() {
	addCommand("Clones", command{"VerticalSplitClone",
		commandVerticalSplitClone, interactiveVerticalSplitClone,
		"[--tables=''] [--strategy=''] <destination keyspace/shard|zk shard path>",
		"Replicates the data and creates configuration for a vertical split."})
}
