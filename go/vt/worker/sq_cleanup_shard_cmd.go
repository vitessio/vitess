/*
Copyright 2017 Google Inc.

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

package worker

import (
	"flag"
	"fmt"
	"html/template"
	"net/http"
	"strings"
	"sync"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/wrangler"
)

const cleanUpShardHTML = `
<!DOCTYPE html>
<head>
  <title>Clean Up Shard Action</title>
</head>
<body>
  <h1>Clean Up Shard Action</h1>

    {{if .Error}}
      <b>Error:</b> {{.Error}}</br>
    {{else}}
      {{range $i, $si := .Shards}}
        <li><a href="/Square/CleanUpShard?keyspace={{$si.Keyspace}}&shard={{$si.Shard}}">{{$si.Keyspace}}/{{$si.Shard}}</a></li>
      {{end}}
    {{end}}
</body>
`

const cleanUpShardHTML2 = `
<!DOCTYPE html>
<head>
  <title>Clean Up Shard Action</title>
</head>
<body>
  <p>Shard involved: {{.Keyspace}}/{{.Shard}}</p>
  <h1>Clean Up Shard Action</h1>
    <form action="/Square/CleanUpShard" method="post">
      <LABEL for="secondPhase">Second Phase: (first phase renames table, second phase actually deletes them)</LABEL>
        <INPUT type="checkbox" id="secondPhase" name="secondPhase" value="true"></BR>
      <LABEL for="excludeTables">Exclude Tables: </LABEL>
        <INPUT type="text" id="excludeTables" name="excludeTables" value=""></BR>
      <INPUT type="hidden" name="keyspace" value="{{.Keyspace}}"/>
      <INPUT type="hidden" name="shard" value="{{.Shard}}"/>
      <INPUT type="submit" name="submit" value="Clean Up Shard"/>
    </form>
  </body>
`

var cleanUpShardTemplate = mustParseTemplate("cleanUpShard", cleanUpShardHTML)
var cleanUpShardTemplate2 = mustParseTemplate("cleanUpShard2", cleanUpShardHTML2)

func commandCleanUpShard(wi *Instance, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (Worker, error) {
	excludeTables := subFlags.String("exclude_tables", "", "comma separated list of tables to exclude")
	secondPhase := subFlags.Bool("second_phase", false, "first phase renames tables, second phase deletes them")
	if err := subFlags.Parse(args); err != nil {
		return nil, err
	}
	if subFlags.NArg() != 1 {
		subFlags.Usage()
		return nil, fmt.Errorf("command CleanUpShard requires <keyspace/shard>")
	}
	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return nil, err
	}
	var excludeTableArray []string
	if *excludeTables != "" {
		excludeTableArray = strings.Split(*excludeTables, ",")
	}
	return NewCleanUpShardWorker(wr, wi.cell, keyspace, shard, *secondPhase, excludeTableArray), nil
}

// shardSources returns all the shards that are SourceShards of at least one other shard.
func nonServingShards(ctx context.Context, wr *wrangler.Wrangler) ([]map[string]string, error) {
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	keyspaces, err := wr.TopoServer().GetKeyspaces(shortCtx)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("failed to get list of keyspaces: %v", err)
	}

	shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
	cells, err := wr.TopoServer().GetKnownCells(shortCtx)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("failed to get list of cells: %v", err)
	}

	wg := sync.WaitGroup{}
	// Use a map to dedupe source shards
	nonServingShards := sync.Map{}
	rec := concurrency.AllErrorRecorder{}
	for _, keyspace := range keyspaces {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()
			shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
			allShards, err := wr.TopoServer().GetShardNames(shortCtx, keyspace)
			cancel()
			if err != nil {
				rec.RecordError(fmt.Errorf("failed to get list of shards for keyspace '%v': %v", keyspace, err))
				return
			}

			// set of serving shards
			servingShards := make(map[string]bool)
			for _, cell := range cells {
				shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
				srvKeyspace, err := wr.TopoServer().GetSrvKeyspace(shortCtx, cell, keyspace)
				cancel()
				if err != nil {
					rec.RecordError(fmt.Errorf("failed to get SrvKeyspace for keyspace '%v' in cell '%v': %v", keyspace, cell, err))
					return
				}
				for _, partition := range srvKeyspace.Partitions {
					for _, shardRef := range partition.ShardReferences {
						servingShards[shardRef.Name] = true
					}
				}
			}

			for _, shard := range allShards {
				_, found := servingShards[shard]
				if !found {
					nonServingShards.Store(fmt.Sprintf("%v/%v", keyspace, shard), map[string]string{
						"Keyspace": keyspace,
						"Shard":    shard,
					})
				}
			}
		}(keyspace)
	}
	wg.Wait()

	if rec.HasErrors() {
		return nil, rec.Error()
	}
	result := make([]map[string]string, 0)
	nonServingShards.Range(func(key, shard interface{}) bool {
		result = append(result, shard.(map[string]string))
		return true
	})
	if len(result) == 0 {
		return nil, fmt.Errorf("there are no non-serving shards")
	}
	return result, nil
}

func interactiveCleanUpShard(ctx context.Context, wi *Instance, wr *wrangler.Wrangler, w http.ResponseWriter, r *http.Request) (Worker, *template.Template, map[string]interface{}, error) {
	if err := r.ParseForm(); err != nil {
		return nil, nil, nil, fmt.Errorf("cannot parse form: %s", err)
	}
	keyspace := r.FormValue("keyspace")
	shard := r.FormValue("shard")
	secondPhaseStr := r.FormValue("secondPhase")
	secondPhase := secondPhaseStr == "true"

	if keyspace == "" || shard == "" {
		// display the list of possible shards to chose from
		result := make(map[string]interface{})
		shards, err := nonServingShards(ctx, wr)
		if err != nil {
			result["Error"] = err.Error()
		} else {
			result["Shards"] = shards
		}
		return nil, cleanUpShardTemplate, result, nil
	}

	submitButtonValue := r.FormValue("submit")
	if submitButtonValue == "" {
		// display the input form
		result := make(map[string]interface{})
		result["Keyspace"] = keyspace
		result["Shard"] = shard
		result["DefaultParallelDiffsCount"] = fmt.Sprintf("%v", defaultParallelDiffsCount)
		return nil, cleanUpShardTemplate2, result, nil
	}

	// Process input form.
	excludeTables := r.FormValue("excludeTables")
	var excludeTableArray []string
	if excludeTables != "" {
		excludeTableArray = strings.Split(excludeTables, ",")
	}

	// start the job
	wrk := NewCleanUpShardWorker(wr, wi.cell, keyspace, shard, secondPhase, excludeTableArray)
	return wrk, nil, nil, nil
}

func init() {
	AddCommand("Square", Command{"CleanUpShard",
		commandCleanUpShard, interactiveCleanUpShard,
		"[--exclude_tables=''] <keyspace/shard>",
		"Drop all tables in a shard"})
}
