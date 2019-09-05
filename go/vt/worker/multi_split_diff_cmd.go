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

	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/wrangler"
)

const multiSplitDiffHTML = `
<!DOCTYPE html>
<head>
  <title>Multi Split Diff Action</title>
</head>
<body>
  <h1>Multi Split Diff Action</h1>

    {{if .Error}}
      <b>Error:</b> {{.Error}}</br>
    {{else}}
      {{range $i, $si := .Shards}}
        <li><a href="/Diffs/MultiSplitDiff?keyspace={{$si.Keyspace}}&shard={{$si.Shard}}">{{$si.Keyspace}}/{{$si.Shard}}</a></li>
      {{end}}
    {{end}}
</body>
`

const multiSplitDiffHTML2 = `
<!DOCTYPE html>
<head>
  <title>Multi Split Diff Action</title>
</head>
<body>
  <p>Shard involved: {{.Keyspace}}/{{.Shard}}</p>
  <h1>Multi Split Diff Action</h1>
    <form action="/Diffs/MultiSplitDiff" method="post">
      <LABEL for="sourceUID">Source shard UID: </LABEL>
        <INPUT type="text" id="sourceUID" name="sourceUID" value="{{.DefaultSourceUID}}"></BR>
      <LABEL for="tabletType">Tablet Type:</LABEL>
			<SELECT id="tabletType" name="tabletType">
  			<OPTION selected value="RDONLY">RDONLY</OPTION>
  			<OPTION value="REPLICA">REPLICA</OPTION>
			</SELECT>
			<BR>
      <LABEL for="excludeTables">Exclude Tables: </LABEL>
        <INPUT type="text" id="excludeTables" name="excludeTables" value=""></BR>
      <LABEL for="minHealthyTablets">Minimum Number of required healthy tablets: </LABEL>
        <INPUT type="text" id="minHealthyTablets" name="minHealthyTablets" value="{{.DefaultMinHealthyTablets}}"></BR>
      <LABEL for="parallelDiffsCount">Number of tables to diff in parallel: </LABEL>
        <INPUT type="text" id="parallelDiffsCount" name="parallelDiffsCount" value="{{.DefaultParallelDiffsCount}}"></BR>
      <LABEL for="waitForFixedTimeRatherThanGtidSet">Wait for fixed time rather than GTID set: (wait for 1m when syncing up the destination RDONLY tablet rather than using the GTID set. Use this when the GTID set on the RDONLY is broken. Make sure the RDONLY is not behind in replication when using this flag.)</LABEL>
        <INPUT type="checkbox" id="waitForFixedTimeRatherThanGtidSet" name="waitForFixedTimeRatherThanGtidSet" value="true"></BR>
      <LABEL for="useConsistentSnapshot">Use consistent snapshot</LABEL>
        <INPUT type="checkbox" id="useConsistentSnapshot" name="useConsistentSnapshot" value="true"><a href="https://dev.mysql.com/doc/refman/5.7/en/glossary.html#glos_consistent_read" target="_blank">?</a></BR>
      <INPUT type="hidden" name="keyspace" value="{{.Keyspace}}"/>
      <INPUT type="hidden" name="shard" value="{{.Shard}}"/>
      <INPUT type="submit" name="submit" value="Split Diff"/>
    </form>
  </body>
`

var multiSplitDiffTemplate = mustParseTemplate("multiSplitDiff", multiSplitDiffHTML)
var multiSplitDiffTemplate2 = mustParseTemplate("multiSplitDiff2", multiSplitDiffHTML2)

func commandMultiSplitDiff(wi *Instance, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (Worker, error) {
	tabletTypeStr := subFlags.String("tablet_type", "RDONLY", "type of tablet used")
	excludeTables := subFlags.String("exclude_tables", "", "comma separated list of tables to exclude")
	minHealthyTablets := subFlags.Int("min_healthy_tablets", defaultMinHealthyTablets, "minimum number of healthy tablets before taking out one")
	parallelDiffsCount := subFlags.Int("parallel_diffs_count", defaultParallelDiffsCount, "number of tables to diff in parallel")
	waitForFixedTimeRatherThanGtidSet := subFlags.Bool("wait_for_fixed_time_rather_than_gtid_set", false, "wait for 1m when syncing up the destination RDONLY tablet rather than using the GTID set. Use this when the GTID set on the RDONLY is broken. Make sure the RDONLY is not behind in replication when using this flag.")
	useConsistentSnapshot := subFlags.Bool("use_consistent_snapshot", defaultUseConsistentSnapshot, "Instead of pausing replication on the source, uses transactions with consistent snapshot to have a stable view of the data.")
	if err := subFlags.Parse(args); err != nil {
		return nil, err
	}
	if subFlags.NArg() != 1 {
		subFlags.Usage()
		return nil, vterrors.New(vtrpc.Code_INVALID_ARGUMENT, "command MultiSplitDiff requires <keyspace/shard>")
	}
	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return nil, err
	}
	var excludeTableArray []string
	if *excludeTables != "" {
		excludeTableArray = strings.Split(*excludeTables, ",")
	}

	tabletType, ok := topodata.TabletType_value[*tabletTypeStr]
	if !ok {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "failed to find this tablet type %v", tabletTypeStr)
	}

	return NewMultiSplitDiffWorker(wr, wi.cell, keyspace, shard, excludeTableArray, *minHealthyTablets, *parallelDiffsCount, *waitForFixedTimeRatherThanGtidSet, *useConsistentSnapshot, topodata.TabletType(tabletType)), nil
}

// shardSources returns all the shards that are SourceShards of at least one other shard.
func shardSources(ctx context.Context, wr *wrangler.Wrangler) ([]map[string]string, error) {
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	keyspaces, err := wr.TopoServer().GetKeyspaces(shortCtx)
	cancel()
	if err != nil {
		return nil, vterrors.Wrap(err, "failed to get list of keyspaces")
	}

	wg := sync.WaitGroup{}
	mu := sync.Mutex{} // protects sourceShards
	// Use a map to dedupe source shards
	sourceShards := make(map[string]map[string]string)
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
						for _, sourceShard := range si.SourceShards {
							sourceShards[fmt.Sprintf("%v/%v", sourceShard.Keyspace, sourceShard.Shard)] =
								map[string]string{
									"Keyspace": sourceShard.Keyspace,
									"Shard":    sourceShard.Shard,
								}
						}
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
	result := make([]map[string]string, 0, len(sourceShards))
	for _, shard := range sourceShards {
		result = append(result, shard)
	}
	if len(result) == 0 {
		return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "there are no shards with SourceShards")
	}
	return result, nil
}

func interactiveMultiSplitDiff(ctx context.Context, wi *Instance, wr *wrangler.Wrangler, _ http.ResponseWriter, r *http.Request) (Worker, *template.Template, map[string]interface{}, error) {
	if err := r.ParseForm(); err != nil {
		return nil, nil, nil, vterrors.Wrap(err, "cannot parse form")
	}
	keyspace := r.FormValue("keyspace")
	shard := r.FormValue("shard")

	if keyspace == "" || shard == "" {
		// display the list of possible shards to chose from
		result := make(map[string]interface{})
		shards, err := shardSources(ctx, wr)
		if err != nil {
			result["Error"] = err.Error()
		} else {
			result["Shards"] = shards
		}
		return nil, multiSplitDiffTemplate, result, nil
	}

	submitButtonValue := r.FormValue("submit")
	if submitButtonValue == "" {
		// display the input form
		result := make(map[string]interface{})
		result["Keyspace"] = keyspace
		result["Shard"] = shard
		result["DefaultSourceUID"] = "0"
		result["DefaultMinHealthyTablets"] = fmt.Sprintf("%v", defaultMinHealthyTablets)
		result["DefaultParallelDiffsCount"] = fmt.Sprintf("%v", defaultParallelDiffsCount)
		return nil, multiSplitDiffTemplate2, result, nil
	}

	// Process input form.
	excludeTables := r.FormValue("excludeTables")
	var excludeTableArray []string
	if excludeTables != "" {
		excludeTableArray = strings.Split(excludeTables, ",")
	}
	minHealthyTabletsStr := r.FormValue("minHealthyTablets")
	parallelDiffsCountStr := r.FormValue("parallelDiffsCount")
	minHealthyTablets, err := strconv.ParseInt(minHealthyTabletsStr, 0, 64)
	if err != nil {
		return nil, nil, nil, vterrors.Wrap(err, "cannot parse minHealthyTablets")
	}
	parallelDiffsCount, err := strconv.ParseInt(parallelDiffsCountStr, 0, 64)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cannot parse parallelDiffsCount: %s", err)
	}
	waitForFixedTimeRatherThanGtidSetStr := r.FormValue("waitForFixedTimeRatherThanGtidSet")
	waitForFixedTimeRatherThanGtidSet := waitForFixedTimeRatherThanGtidSetStr == "true"
	useConsistentSnapshotStr := r.FormValue("useConsistentSnapshot")
	useConsistentSnapshot := useConsistentSnapshotStr == "true"

	tabletTypeStr := r.FormValue("tabletType")
	tabletType, ok := topodata.TabletType_value[tabletTypeStr]
	if !ok {
		return nil, nil, nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "cannot parse tabletType: %s", tabletTypeStr)
	}

	// start the diff job
	wrk := NewMultiSplitDiffWorker(wr, wi.cell, keyspace, shard, excludeTableArray, int(minHealthyTablets), int(parallelDiffsCount), waitForFixedTimeRatherThanGtidSet, useConsistentSnapshot, topodata.TabletType(tabletType))
	return wrk, nil, nil, nil
}

func init() {
	AddCommand("Diffs", Command{"MultiSplitDiff",
		commandMultiSplitDiff, interactiveMultiSplitDiff,
		"[--exclude_tables=''] <keyspace/shard>",
		"Diffs a rdonly destination shard against its SourceShards"})
}
