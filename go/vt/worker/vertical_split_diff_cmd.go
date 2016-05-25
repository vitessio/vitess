// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"flag"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"sync"

	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/wrangler"
	"golang.org/x/net/context"
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
      <LABEL for="minHealthyRdonlyTablets">Minimum Number of required healthy RDONLY tablets: </LABEL>
        <INPUT type="text" id="minHealthyRdonlyTablets" name="minHealthyRdonlyTablets" value="{{.DefaultMinHealthyRdonlyTablets}}"></BR>      <INPUT type="hidden" name="keyspace" value="{{.Keyspace}}"/>
      <INPUT type="hidden" name="shard" value="{{.Shard}}"/>
      <INPUT type="submit" name="submit" value="Vertical Split Diff"/>
    </form>
  </body>
`

var verticalSplitDiffTemplate = mustParseTemplate("verticalSplitDiff", verticalSplitDiffHTML)
var verticalSplitDiffTemplate2 = mustParseTemplate("verticalSplitDiff2", verticalSplitDiffHTML2)

func commandVerticalSplitDiff(wi *Instance, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (Worker, error) {
	minHealthyRdonlyTablets := subFlags.Int("min_healthy_rdonly_tablets", defaultMinHealthyRdonlyTablets, "minimum number of healthy RDONLY tablets before taking out one")
	if err := subFlags.Parse(args); err != nil {
		return nil, err
	}
	if subFlags.NArg() != 1 {
		subFlags.Usage()
		return nil, fmt.Errorf("command VerticalSplitDiff requires <keyspace/shard>")
	}
	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return nil, err
	}
	return NewVerticalSplitDiffWorker(wr, wi.cell, keyspace, shard, *minHealthyRdonlyTablets), nil
}

// shardsWithTablesSources returns all the shards that have SourceShards set
// to one value, with an array of Tables.
func shardsWithTablesSources(ctx context.Context, wr *wrangler.Wrangler) ([]map[string]string, error) {
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	keyspaces, err := wr.TopoServer().GetKeyspaces(shortCtx)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("failed to get list of keyspaces: %v", err)
	}

	wg := sync.WaitGroup{}
	mu := sync.Mutex{} // protects result
	result := make([]map[string]string, 0, len(keyspaces))
	rec := concurrency.AllErrorRecorder{}
	for _, keyspace := range keyspaces {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()
			shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
			shards, err := wr.TopoServer().GetShardNames(shortCtx, keyspace)
			cancel()
			if err != nil {
				rec.RecordError(fmt.Errorf("failed to get list of shards for keyspace '%v': %v", keyspace, err))
				return
			}
			for _, shard := range shards {
				wg.Add(1)
				go func(keyspace, shard string) {
					defer wg.Done()
					shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
					si, err := wr.TopoServer().GetShard(shortCtx, keyspace, shard)
					cancel()
					if err != nil {
						rec.RecordError(fmt.Errorf("failed to get details for shard '%v': %v", topoproto.KeyspaceShardString(keyspace, shard), err))
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
		return nil, fmt.Errorf("there are no shards with SourceShards")
	}
	return result, nil
}

func interactiveVerticalSplitDiff(ctx context.Context, wi *Instance, wr *wrangler.Wrangler, w http.ResponseWriter, r *http.Request) (Worker, *template.Template, map[string]interface{}, error) {
	if err := r.ParseForm(); err != nil {
		return nil, nil, nil, fmt.Errorf("cannot parse form: %s", err)
	}
	keyspace := r.FormValue("keyspace")
	shard := r.FormValue("shard")

	if keyspace == "" || shard == "" {
		// display the list of possible shards to chose from
		result := make(map[string]interface{})
		shards, err := shardsWithTablesSources(ctx, wr)
		if err != nil {
			result["Error"] = err.Error()
		} else {
			result["Shards"] = shards
		}
		return nil, verticalSplitDiffTemplate, result, nil
	}

	submitButtonValue := r.FormValue("submit")
	if submitButtonValue == "" {
		// display the input form
		result := make(map[string]interface{})
		result["Keyspace"] = keyspace
		result["Shard"] = shard
		result["DefaultMinHealthyRdonlyTablets"] = fmt.Sprintf("%v", defaultMinHealthyRdonlyTablets)
		return nil, verticalSplitDiffTemplate2, result, nil
	}

	// get other parameters
	minHealthyRdonlyTabletsStr := r.FormValue("minHealthyRdonlyTablets")
	minHealthyRdonlyTablets, err := strconv.ParseInt(minHealthyRdonlyTabletsStr, 0, 64)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cannot parse minHealthyRdonlyTablets: %s", err)
	}

	// start the diff job
	wrk := NewVerticalSplitDiffWorker(wr, wi.cell, keyspace, shard, int(minHealthyRdonlyTablets))
	return wrk, nil, nil, nil
}

func init() {
	AddCommand("Diffs", Command{"VerticalSplitDiff",
		commandVerticalSplitDiff, interactiveVerticalSplitDiff,
		"<keyspace/shard>",
		"Diffs an rdonly tablet from the (destination) keyspace/shard against an rdonly tablet from the respective source keyspace/shard." +
			" Only compares the tables which were set by a previous VerticalSplitClone command."})
}
