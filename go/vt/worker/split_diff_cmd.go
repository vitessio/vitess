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
	"strconv"
	"strings"
	"sync"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/wrangler"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
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
      <LABEL for="sourceUID">Source shard UID: </LABEL>
        <INPUT type="text" id="sourceUID" name="sourceUID" value="{{.DefaultSourceUID}}"></BR>
      <LABEL for="excludeTables">Exclude Tables: </LABEL>
        <INPUT type="text" id="excludeTables" name="excludeTables" value=""></BR>
      <LABEL for="minHealthyRdonlyTablets">Minimum Number of required healthy RDONLY tablets: </LABEL>
        <INPUT type="text" id="minHealthyRdonlyTablets" name="minHealthyRdonlyTablets" value="{{.DefaultMinHealthyRdonlyTablets}}"></BR>
      <LABEL for="parallelDiffsCount">Number of tables to diff in parallel: </LABEL>
        <INPUT type="text" id="parallelDiffsCount" name="parallelDiffsCount" value="{{.DefaultParallelDiffsCount}}"></BR>
      <INPUT type="hidden" name="keyspace" value="{{.Keyspace}}"/>
      <INPUT type="hidden" name="shard" value="{{.Shard}}"/>
      <INPUT type="submit" name="submit" value="Split Diff"/>
    </form>
  </body>
`

var splitDiffTemplate = mustParseTemplate("splitDiff", splitDiffHTML)
var splitDiffTemplate2 = mustParseTemplate("splitDiff2", splitDiffHTML2)

func commandSplitDiff(wi *Instance, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (Worker, error) {
	sourceUID := subFlags.Int("source_uid", 0, "uid of the source shard to run the diff against")
	excludeTables := subFlags.String("exclude_tables", "", "comma separated list of tables to exclude")
	minHealthyRdonlyTablets := subFlags.Int("min_healthy_rdonly_tablets", defaultMinHealthyTablets, "minimum number of healthy RDONLY tablets before taking out one")
	destTabletTypeStr := subFlags.String("dest_tablet_type", defaultDestTabletType, "destination tablet type (RDONLY or REPLICA) that will be used to compare the shards")
	parallelDiffsCount := subFlags.Int("parallel_diffs_count", defaultParallelDiffsCount, "number of tables to diff in parallel")
	if err := subFlags.Parse(args); err != nil {
		return nil, err
	}
	if subFlags.NArg() != 1 {
		subFlags.Usage()
		return nil, vterrors.New(vtrpc.Code_INVALID_ARGUMENT, "command SplitDiff requires <keyspace/shard>")
	}
	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return nil, err
	}
	var excludeTableArray []string
	if *excludeTables != "" {
		excludeTableArray = strings.Split(*excludeTables, ",")
	}

	destTabletType, ok := topodatapb.TabletType_value[*destTabletTypeStr]
	if !ok {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "command SplitDiff invalid dest_tablet_type: %v", destTabletType)
	}

	return NewSplitDiffWorker(wr, wi.cell, keyspace, shard, uint32(*sourceUID), excludeTableArray, *minHealthyRdonlyTablets, *parallelDiffsCount, topodatapb.TabletType(destTabletType)), nil
}

// shardsWithSources returns all the shards that have SourceShards set
// with no Tables list.
func shardsWithSources(ctx context.Context, wr *wrangler.Wrangler) ([]map[string]string, error) {
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	keyspaces, err := wr.TopoServer().GetKeyspaces(shortCtx)
	cancel()
	if err != nil {
		return nil, vterrors.Wrap(err, "failed to get list of keyspaces")
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
				rec.RecordError(vterrors.Wrapf(err, "failed to get list of shards for keyspace '%v'", keyspace))
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
						rec.RecordError(vterrors.Wrapf(err, "failed to get details for shard '%v'", topoproto.KeyspaceShardString(keyspace, shard)))
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
		return nil, vterrors.New(vtrpc.Code_FAILED_PRECONDITION, "there are no shards with SourceShards")
	}
	return result, nil
}

func interactiveSplitDiff(ctx context.Context, wi *Instance, wr *wrangler.Wrangler, w http.ResponseWriter, r *http.Request) (Worker, *template.Template, map[string]interface{}, error) {
	if err := r.ParseForm(); err != nil {
		return nil, nil, nil, vterrors.Wrap(err, "cannot parse form")
	}
	keyspace := r.FormValue("keyspace")
	shard := r.FormValue("shard")

	if keyspace == "" || shard == "" {
		// display the list of possible shards to chose from
		result := make(map[string]interface{})
		shards, err := shardsWithSources(ctx, wr)
		if err != nil {
			result["Error"] = err.Error()
		} else {
			result["Shards"] = shards
		}
		return nil, splitDiffTemplate, result, nil
	}

	submitButtonValue := r.FormValue("submit")
	if submitButtonValue == "" {
		// display the input form
		result := make(map[string]interface{})
		result["Keyspace"] = keyspace
		result["Shard"] = shard
		result["DefaultSourceUID"] = "0"
		result["DefaultMinHealthyRdonlyTablets"] = fmt.Sprintf("%v", defaultMinHealthyTablets)
		result["DefaultParallelDiffsCount"] = fmt.Sprintf("%v", defaultParallelDiffsCount)
		return nil, splitDiffTemplate2, result, nil
	}

	// Process input form.
	sourceUIDStr := r.FormValue("sourceUID")
	sourceUID, err := strconv.ParseInt(sourceUIDStr, 0, 64)
	if err != nil {
		return nil, nil, nil, vterrors.Wrap(err, "cannot parse sourceUID")
	}
	excludeTables := r.FormValue("excludeTables")
	var excludeTableArray []string
	if excludeTables != "" {
		excludeTableArray = strings.Split(excludeTables, ",")
	}
	minHealthyRdonlyTabletsStr := r.FormValue("minHealthyRdonlyTablets")
	parallelDiffsCountStr := r.FormValue("parallelDiffsCount")
	minHealthyRdonlyTablets, err := strconv.ParseInt(minHealthyRdonlyTabletsStr, 0, 64)
	if err != nil {
		return nil, nil, nil, vterrors.Wrap(err, "cannot parse minHealthyRdonlyTablets")
	}
	parallelDiffsCount, err := strconv.ParseInt(parallelDiffsCountStr, 0, 64)
	if err != nil {
		return nil, nil, nil, vterrors.Wrap(err, "cannot parse parallelDiffsCount")
	}

	// start the diff job
	// TODO: @rafael - Add option to set destination tablet type in UI form.
	wrk := NewSplitDiffWorker(wr, wi.cell, keyspace, shard, uint32(sourceUID), excludeTableArray, int(minHealthyRdonlyTablets), int(parallelDiffsCount), topodatapb.TabletType_RDONLY)
	return wrk, nil, nil, nil
}

func init() {
	AddCommand("Diffs", Command{"SplitDiff",
		commandSplitDiff, interactiveSplitDiff,
		"[--exclude_tables=''] <keyspace/shard>",
		"Diffs a rdonly destination shard against its SourceShards"})
}
