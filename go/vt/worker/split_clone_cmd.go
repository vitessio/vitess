/*
Copyright 2019 The Vitess Authors.

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

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/wrangler"
)

const splitCloneHTML = `
<!DOCTYPE html>
<head>
  <title>Split Clone Action</title>
</head>
<body>
  <h1>Split Clone Action</h1>

    {{if .Error}}
      <b>Error:</b> {{.Error}}</br>
    {{else}}
      <p>Choose the destination keyspace for this action.</p>
      <ul>
      {{range $i, $si := .Choices}}
        <li><a href="/Clones/SplitClone?keyspace={{$si.Keyspace}}&shard={{$si.Shard}}">{{$si.Keyspace}}/{{$si.Shard}}</a></li>
      {{end}}
      </ul>
    {{end}}
</body>
`

const splitCloneHTML2 = `
<!DOCTYPE html>
<head>
  <title>Split Clone Action</title>
</head>
<body>
  <p>Shard involved: {{.Keyspace}}/{{.Shard}}</p>
  <h1>Split Clone Action</h1>
    <form action="/Clones/SplitClone" method="post">
      <LABEL for="online">Do Online Copy: (optional approximate copy, source and destination tablets will not be put out of serving, minimizes downtime during offline copy)</LABEL>
        <INPUT type="checkbox" id="online" name="online" value="true"{{if .DefaultOnline}} checked{{end}}></BR>
      <LABEL for="offline">Do Offline Copy: (exact copy at a specific GTID, required before shard migration, source and destination tablets will be put out of serving during copy)</LABEL>
        <INPUT type="checkbox" id="offline" name="offline" value="true"{{if .DefaultOnline}} checked{{end}}></BR>
      <LABEL for="excludeTables">Exclude Tables: </LABEL>
        <INPUT type="text" id="excludeTables" name="excludeTables" value="/ignored/"></BR>
      <LABEL for="chunkCount">Chunk Count: </LABEL>
        <INPUT type="text" id="chunkCount" name="chunkCount" value="{{.DefaultChunkCount}}"></BR>
      <LABEL for="minRowsPerChunk">Minimum Number of Rows per Chunk (may reduce the Chunk Count): </LABEL>
        <INPUT type="text" id="minRowsPerChunk" name="minRowsPerChunk" value="{{.DefaultMinRowsPerChunk}}"></BR>
      <LABEL for="sourceReaderCount">Source Reader Count: </LABEL>
        <INPUT type="text" id="sourceReaderCount" name="sourceReaderCount" value="{{.DefaultSourceReaderCount}}"></BR>
      <LABEL for="writeQueryMaxRows">Maximum Number of Rows per Write Query: </LABEL>
        <INPUT type="text" id="writeQueryMaxRows" name="writeQueryMaxRows" value="{{.DefaultWriteQueryMaxRows}}"></BR>
      <LABEL for="writeQueryMaxSize">Maximum Size (in bytes) per Write Query: </LABEL>
        <INPUT type="text" id="writeQueryMaxSize" name="writeQueryMaxSize" value="{{.DefaultWriteQueryMaxSize}}"></BR>
      <LABEL for="destinationWriterCount">Destination Writer Count: </LABEL>
        <INPUT type="text" id="destinationWriterCount" name="destinationWriterCount" value="{{.DefaultDestinationWriterCount}}"></BR>
      <LABEL for="tabletType">Tablet Type:</LABEL>
			<SELECT id="tabletType" name="tabletType">
  			<OPTION selected value="RDONLY">RDONLY</OPTION>
  			<OPTION value="REPLICA">REPLICA</OPTION>
			</SELECT>
			</BR>
      <LABEL for="minHealthyTablets">Minimum Number of required healthy RDONLY tablets in the source and destination shard at start: </LABEL>
        <INPUT type="text" id="minHealthyTablets" name="minHealthyTablets" value="{{.DefaultMinHealthyTablets}}"></BR>
      <LABEL for="maxTPS">Maximum Write Transactions/second (If non-zero, writes on the destination will be throttled. Unlimited by default.): </LABEL>
        <INPUT type="text" id="maxTPS" name="maxTPS" value="{{.DefaultMaxTPS}}"></BR>
      <LABEL for="maxReplicationLag">Maximum Replication Lag Seconds (enables the adapative throttler. Disabled by default.): </LABEL>
        <INPUT type="text" id="maxReplicationLag" name="maxReplicationLag" value="{{.DefaultMaxReplicationLag}}"></BR>
      <INPUT type="hidden" name="keyspace" value="{{.Keyspace}}"/>
      <INPUT type="hidden" name="shard" value="{{.Shard}}"/>
      <LABEL for="useConsistentSnapshot">Use consistent snapshot during the offline cloning:</LABEL>
        <INPUT type="checkbox" id="useConsistentSnapshot" name="useConsistentSnapshot" value="true"><a href="https://dev.mysql.com/doc/refman/5.7/en/glossary.html#glos_consistent_read" target="_blank">?</a></BR>
      <INPUT type="submit" value="Clone"/>
    </form>
  </body>
`

var splitCloneTemplate = mustParseTemplate("splitClone", splitCloneHTML)
var splitCloneTemplate2 = mustParseTemplate("splitClone2", splitCloneHTML2)

func commandSplitClone(wi *Instance, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (Worker, error) {
	online := subFlags.Bool("online", defaultOnline, "do online copy (optional approximate copy, source and destination tablets will not be put out of serving, minimizes downtime during offline copy)")
	offline := subFlags.Bool("offline", defaultOffline, "do offline copy (exact copy at a specific GTID, required before shard migration, source and destination tablets will be put out of serving during copy)")
	excludeTables := subFlags.String("exclude_tables", "", "comma separated list of tables to exclude. Each is either an exact match, or a regular expression of the form /regexp/")
	chunkCount := subFlags.Int("chunk_count", defaultChunkCount, "number of chunks per table")
	minRowsPerChunk := subFlags.Int("min_rows_per_chunk", defaultMinRowsPerChunk, "minimum number of rows per chunk (may reduce --chunk_count)")
	sourceReaderCount := subFlags.Int("source_reader_count", defaultSourceReaderCount, "number of concurrent streaming queries to use on the source")
	writeQueryMaxRows := subFlags.Int("write_query_max_rows", defaultWriteQueryMaxRows, "maximum number of rows per write query")
	writeQueryMaxSize := subFlags.Int("write_query_max_size", defaultWriteQueryMaxSize, "maximum size (in bytes) per write query")
	destinationWriterCount := subFlags.Int("destination_writer_count", defaultDestinationWriterCount, "number of concurrent RPCs to execute on the destination")
	tabletTypeStr := subFlags.String("tablet_type", "RDONLY", "tablet type to use (RDONLY or REPLICA)")
	minHealthyTablets := subFlags.Int("min_healthy_rdonly_tablets", defaultMinHealthyTablets, "minimum number of healthy tablets in the source and destination shard at start")
	useConsistentSnapshot := subFlags.Bool("use_consistent_snapshot", defaultUseConsistentSnapshot, "Instead of pausing replication on the source, uses transactions with consistent snapshot to have a stable view of the data.")
	maxTPS := subFlags.Int64("max_tps", defaultMaxTPS, "rate limit of maximum number of (write) transactions/second on the destination (unlimited by default)")
	maxReplicationLag := subFlags.Int64("max_replication_lag", defaultMaxReplicationLag, "if set, the adapative throttler will be enabled and automatically adjust the write rate to keep the lag below the set value in seconds (disabled by default)")
	if err := subFlags.Parse(args); err != nil {
		return nil, err
	}
	if subFlags.NArg() != 1 {
		subFlags.Usage()
		return nil, vterrors.New(vtrpc.Code_INVALID_ARGUMENT, "command SplitClone requires <keyspace/shard>")
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
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "command SplitClone invalid tablet_type: %v", tabletType)
	}
	worker, err := newSplitCloneWorker(wr, wi.cell, keyspace, shard, *online, *offline, excludeTableArray, *chunkCount, *minRowsPerChunk, *sourceReaderCount, *writeQueryMaxRows, *writeQueryMaxSize, *destinationWriterCount, *minHealthyTablets, topodata.TabletType(tabletType), *maxTPS, *maxReplicationLag, *useConsistentSnapshot)
	if err != nil {
		return nil, vterrors.Wrap(err, "cannot create split clone worker")
	}
	return worker, nil
}

func keyspacesWithOverlappingShards(ctx context.Context, wr *wrangler.Wrangler) ([]map[string]string, error) {
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
			shortCtx, cancel = context.WithTimeout(ctx, *remoteActionsTimeout)
			osList, err := topotools.FindOverlappingShards(shortCtx, wr.TopoServer(), keyspace)
			cancel()
			if err != nil {
				rec.RecordError(err)
				return
			}
			mu.Lock()
			for _, os := range osList {
				result = append(result, map[string]string{
					"Keyspace": os.Left[0].Keyspace(),
					"Shard":    os.Left[0].ShardName(),
				})
			}
			mu.Unlock()
		}(keyspace)
	}
	wg.Wait()

	if rec.HasErrors() {
		return nil, rec.Error()
	}
	if len(result) == 0 {
		return nil, vterrors.New(vtrpc.Code_FAILED_PRECONDITION, "there are no keyspaces with overlapping shards")
	}
	return result, nil
}

func interactiveSplitClone(ctx context.Context, wi *Instance, wr *wrangler.Wrangler, w http.ResponseWriter, r *http.Request) (Worker, *template.Template, map[string]interface{}, error) {
	if err := r.ParseForm(); err != nil {
		return nil, nil, nil, vterrors.Wrap(err, "cannot parse form")
	}

	keyspace := r.FormValue("keyspace")
	shard := r.FormValue("shard")
	if keyspace == "" || shard == "" {
		// display the list of possible splits to choose from
		// (just find all the overlapping guys)
		result := make(map[string]interface{})
		choices, err := keyspacesWithOverlappingShards(ctx, wr)
		if err != nil {
			result["Error"] = err.Error()
		} else {
			result["Choices"] = choices
		}
		return nil, splitCloneTemplate, result, nil
	}

	sourceReaderCountStr := r.FormValue("sourceReaderCount")
	if sourceReaderCountStr == "" {
		// display the input form
		result := make(map[string]interface{})
		result["Keyspace"] = keyspace
		result["Shard"] = shard
		result["DefaultOnline"] = defaultOnline
		result["DefaultOffline"] = defaultOffline
		result["DefaultChunkCount"] = fmt.Sprintf("%v", defaultChunkCount)
		result["DefaultMinRowsPerChunk"] = fmt.Sprintf("%v", defaultMinRowsPerChunk)
		result["DefaultSourceReaderCount"] = fmt.Sprintf("%v", defaultSourceReaderCount)
		result["DefaultWriteQueryMaxRows"] = fmt.Sprintf("%v", defaultWriteQueryMaxRows)
		result["DefaultWriteQueryMaxSize"] = fmt.Sprintf("%v", defaultWriteQueryMaxSize)
		result["DefaultDestinationWriterCount"] = fmt.Sprintf("%v", defaultDestinationWriterCount)
		result["DefaultMinHealthyTablets"] = fmt.Sprintf("%v", defaultMinHealthyTablets)
		result["DefaultMaxTPS"] = fmt.Sprintf("%v", defaultMaxTPS)
		result["DefaultMaxReplicationLag"] = fmt.Sprintf("%v", defaultMaxReplicationLag)
		result["DefaultUseConsistentSnapshot"] = fmt.Sprintf("%v", defaultUseConsistentSnapshot)
		return nil, splitCloneTemplate2, result, nil
	}

	// get other parameters
	onlineStr := r.FormValue("online")
	online := onlineStr == "true"
	offlineStr := r.FormValue("offline")
	offline := offlineStr == "true"
	excludeTables := r.FormValue("excludeTables")
	var excludeTableArray []string
	if excludeTables != "" {
		excludeTableArray = strings.Split(excludeTables, ",")
	}
	chunkCountStr := r.FormValue("chunkCount")
	chunkCount, err := strconv.ParseInt(chunkCountStr, 0, 64)
	if err != nil {
		return nil, nil, nil, vterrors.Wrap(err, "cannot parse chunkCount")
	}
	minRowsPerChunkStr := r.FormValue("minRowsPerChunk")
	minRowsPerChunk, err := strconv.ParseInt(minRowsPerChunkStr, 0, 64)
	if err != nil {
		return nil, nil, nil, vterrors.Wrap(err, "cannot parse minRowsPerChunk")
	}
	sourceReaderCount, err := strconv.ParseInt(sourceReaderCountStr, 0, 64)
	if err != nil {
		return nil, nil, nil, vterrors.Wrap(err, "cannot parse sourceReaderCount")
	}
	writeQueryMaxRowsStr := r.FormValue("writeQueryMaxRows")
	writeQueryMaxRows, err := strconv.ParseInt(writeQueryMaxRowsStr, 0, 64)
	if err != nil {
		return nil, nil, nil, vterrors.Wrap(err, "cannot parse writeQueryMaxRows")
	}
	writeQueryMaxSizeStr := r.FormValue("writeQueryMaxSize")
	writeQueryMaxSize, err := strconv.ParseInt(writeQueryMaxSizeStr, 0, 64)
	if err != nil {
		return nil, nil, nil, vterrors.Wrap(err, "cannot parse writeQueryMaxSize")
	}
	destinationWriterCountStr := r.FormValue("destinationWriterCount")
	destinationWriterCount, err := strconv.ParseInt(destinationWriterCountStr, 0, 64)
	if err != nil {
		return nil, nil, nil, vterrors.Wrap(err, "cannot parse destinationWriterCount")
	}
	minHealthyTabletsStr := r.FormValue("minHealthyTablets")
	minHealthyTablets, err := strconv.ParseInt(minHealthyTabletsStr, 0, 64)
	if err != nil {
		return nil, nil, nil, vterrors.Wrap(err, "cannot parse minHealthyTablets")
	}
	tabletTypeStr := r.FormValue("tabletType")
	tabletType, ok := topodata.TabletType_value[tabletTypeStr]
	if !ok {
		return nil, nil, nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "command SplitClone invalid tablet_type: %v", tabletType)
	}
	maxTPSStr := r.FormValue("maxTPS")
	maxTPS, err := strconv.ParseInt(maxTPSStr, 0, 64)
	if err != nil {
		return nil, nil, nil, vterrors.Wrap(err, "cannot parse maxTPS")
	}
	maxReplicationLagStr := r.FormValue("maxReplicationLag")
	maxReplicationLag, err := strconv.ParseInt(maxReplicationLagStr, 0, 64)
	if err != nil {
		return nil, nil, nil, vterrors.Wrap(err, "cannot parse maxReplicationLag")
	}

	useConsistentSnapshotStr := r.FormValue("useConsistentSnapshot")
	useConsistentSnapshot := useConsistentSnapshotStr == "true"

	// start the clone job
	wrk, err := newSplitCloneWorker(wr, wi.cell, keyspace, shard, online, offline, excludeTableArray, int(chunkCount), int(minRowsPerChunk), int(sourceReaderCount), int(writeQueryMaxRows), int(writeQueryMaxSize), int(destinationWriterCount), int(minHealthyTablets), topodata.TabletType(tabletType), maxTPS, maxReplicationLag, useConsistentSnapshot)
	if err != nil {
		return nil, nil, nil, vterrors.Wrap(err, "cannot create worker")
	}
	return wrk, nil, nil, nil
}

func init() {
	AddCommand("Clones", Command{"SplitClone",
		commandSplitClone, interactiveSplitClone,
		"[--online=false] [--offline=false] [--exclude_tables=''] <keyspace/shard>",
		"Replicates the data and creates configuration for a horizontal split."})
}
