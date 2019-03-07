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
	"vitess.io/vitess/go/vt/vterrors"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/wrangler"
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
        <INPUT type="text" id="tables" name="tables" value="/moving/"></BR>
      <LABEL for="online">Do Online Copy: (optional approximate copy, source and destination tablets will not be put out of serving, minimizes downtime during offline copy)</LABEL>
        <INPUT type="checkbox" id="online" name="online" value="true"{{if .DefaultOnline}} checked{{end}}></BR>
      <LABEL for="offline">Do Offline Copy: (exact copy at a specific GTID, required before shard migration, source and destination tablets will be put out of serving during copy)</LABEL>
        <INPUT type="checkbox" id="offline" name="offline" value="true"{{if .DefaultOnline}} checked{{end}}></BR>
      <LABEL for="chunkCount">Chunk Count: </LABEL>
        <INPUT type="text" id="chunkCount" name="chunkCount" value="{{.DefaultChunkCount}}"></BR>
      <LABEL for="minRowsPerChunk">Minimun Number of Rows per Chunk (may reduce the Chunk Count): </LABEL>
        <INPUT type="text" id="minRowsPerChunk" name="minRowsPerChunk" value="{{.DefaultMinRowsPerChunk}}"></BR>
      <LABEL for="sourceReaderCount">Source Reader Count: </LABEL>
        <INPUT type="text" id="sourceReaderCount" name="sourceReaderCount" value="{{.DefaultSourceReaderCount}}"></BR>
      <LABEL for="writeQueryMaxRows">Maximum Number of Rows per Write Query: </LABEL>
        <INPUT type="text" id="writeQueryMaxRows" name="writeQueryMaxRows" value="{{.DefaultWriteQueryMaxRows}}"></BR>
      <LABEL for="writeQueryMaxSize">Maximum Size (in bytes) per Write Query: </LABEL>
        <INPUT type="text" id="writeQueryMaxSize" name="writeQueryMaxSize" value="{{.DefaultWriteQueryMaxSize}}"></BR>
      <LABEL for="destinationWriterCount">Destination Writer Count: </LABEL>
        <INPUT type="text" id="destinationWriterCount" name="destinationWriterCount" value="{{.DefaultDestinationWriterCount}}"></BR>
      <LABEL for="minHealthyRdonlyTablets">Minimum Number of required healthy RDONLY tablets: </LABEL>
        <INPUT type="text" id="minHealthyRdonlyTablets" name="minHealthyRdonlyTablets" value="{{.DefaultMinHealthyRdonlyTablets}}"></BR>
      <LABEL for="maxTPS">Maximum Write Transactions/second (If non-zero, writes on the destination will be throttled. Unlimited by default.): </LABEL>
        <INPUT type="text" id="maxTPS" name="maxTPS" value="{{.DefaultMaxTPS}}"></BR>
      <LABEL for="maxReplicationLag">Maximum Replication Lag Seconds (enables the adapative throttler. Disabled by default.): </LABEL>
        <INPUT type="text" id="maxReplicationLag" name="maxReplicationLag" value="{{.DefaultMaxReplicationLag}}"></BR>
      <INPUT type="hidden" name="keyspace" value="{{.Keyspace}}"/>
      <LABEL for="useConsistentSnapshot">Use consistent snapshot during the offline cloning:</LABEL>
        <INPUT type="checkbox" id="useConsistentSnapshot" name="useConsistentSnapshot" value="true"><a href="https://dev.mysql.com/doc/refman/5.7/en/glossary.html#glos_consistent_read" target="_blank">?</a></BR>
      <INPUT type="submit" value="Clone"/>
    </form>
  </body>
`

var verticalSplitCloneTemplate = mustParseTemplate("verticalSplitClone", verticalSplitCloneHTML)
var verticalSplitCloneTemplate2 = mustParseTemplate("verticalSplitClone2", verticalSplitCloneHTML2)

func commandVerticalSplitClone(wi *Instance, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (Worker, error) {
	online := subFlags.Bool("online", defaultOnline, "do online copy (optional approximate copy, source and destination tablets will not be put out of serving, minimizes downtime during offline copy)")
	offline := subFlags.Bool("offline", defaultOffline, "do offline copy (exact copy at a specific GTID, required before shard migration, source and destination tablets will be put out of serving during copy)")
	tables := subFlags.String("tables", "", "comma separated list of tables to replicate (used for vertical split). Each is either an exact match, or a regular expression of the form /regexp/")
	chunkCount := subFlags.Int("chunk_count", defaultChunkCount, "number of chunks per table")
	minRowsPerChunk := subFlags.Int("min_rows_per_chunk", defaultMinRowsPerChunk, "minimum number of rows per chunk (may reduce --chunk_count)")
	sourceReaderCount := subFlags.Int("source_reader_count", defaultSourceReaderCount, "number of concurrent streaming queries to use on the source")
	writeQueryMaxRows := subFlags.Int("write_query_max_rows", defaultWriteQueryMaxRows, "maximum number of rows per write query")
	writeQueryMaxSize := subFlags.Int("write_query_max_size", defaultWriteQueryMaxSize, "maximum size (in bytes) per write query")
	destinationWriterCount := subFlags.Int("destination_writer_count", defaultDestinationWriterCount, "number of concurrent RPCs to execute on the destination")
	minHealthyTablets := subFlags.Int("min_healthy_rdonly_tablets", defaultMinHealthyTablets, "minimum number of healthy RDONLY tablets before taking out one")
	useConsistentSnapshot := subFlags.Bool("use_consistent_snapshot", defaultUseConsistentSnapshot, "Instead of pausing replication on the source, uses transactions with consistent snapshot to have a stable view of the data.")
	tabletTypeStr := subFlags.String("tablet_type", "RDONLY", "tablet type to use (RDONLY or REPLICA)")
	maxTPS := subFlags.Int64("max_tps", defaultMaxTPS, "if non-zero, limit copy to maximum number of (write) transactions/second on the destination (unlimited by default)")
	maxReplicationLag := subFlags.Int64("max_replication_lag", defaultMaxReplicationLag, "if set, the adapative throttler will be enabled and automatically adjust the write rate to keep the lag below the set value in seconds (disabled by default)")
	if err := subFlags.Parse(args); err != nil {
		return nil, err
	}
	if subFlags.NArg() != 1 {
		subFlags.Usage()
		return nil, fmt.Errorf("command VerticalSplitClone requires <destination keyspace/shard>")
	}

	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return nil, err
	}
	// The tableArray has zero elements when table flag is empty, which is an
	// error case captured in function newVerticalSplitCloneWorker.
	tableArray := []string{}
	if *tables != "" {
		tableArray = strings.Split(*tables, ",")
	}
	tabletType, ok := topodata.TabletType_value[*tabletTypeStr]
	if !ok {
		return nil, fmt.Errorf("command SplitClone invalid tablet_type: %v", tabletType)
	}

	worker, err := newVerticalSplitCloneWorker(wr, wi.cell, keyspace, shard, *online, *offline, tableArray, *chunkCount, *minRowsPerChunk, *sourceReaderCount, *writeQueryMaxRows, *writeQueryMaxSize, *destinationWriterCount, *minHealthyTablets, topodata.TabletType(tabletType), *maxTPS, *maxReplicationLag, *useConsistentSnapshot)
	if err != nil {
		return nil, vterrors.Wrap(err, "cannot create worker")
	}
	return worker, nil
}

// keyspacesWithServedFrom returns all the keyspaces that have ServedFrom set
// to one value.
func keyspacesWithServedFrom(ctx context.Context, wr *wrangler.Wrangler) ([]string, error) {
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	keyspaces, err := wr.TopoServer().GetKeyspaces(shortCtx)
	cancel()
	if err != nil {
		return nil, vterrors.Wrap(err, "failed to get list of keyspaces")
	}

	wg := sync.WaitGroup{}
	mu := sync.Mutex{} // protects result
	result := make([]string, 0, len(keyspaces))
	rec := concurrency.AllErrorRecorder{}
	for _, keyspace := range keyspaces {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()
			shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
			ki, err := wr.TopoServer().GetKeyspace(shortCtx, keyspace)
			cancel()
			if err != nil {
				rec.RecordError(vterrors.Wrapf(err, "failed to get details for keyspace '%v'", keyspace))
				return
			}
			if len(ki.ServedFroms) > 0 {
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
		return nil, fmt.Errorf("there are no keyspaces with ServedFrom")
	}
	return result, nil
}

func interactiveVerticalSplitClone(ctx context.Context, wi *Instance, wr *wrangler.Wrangler, w http.ResponseWriter, r *http.Request) (Worker, *template.Template, map[string]interface{}, error) {
	if err := r.ParseForm(); err != nil {
		return nil, nil, nil, vterrors.Wrap(err, "cannot parse form")
	}

	keyspace := r.FormValue("keyspace")
	if keyspace == "" {
		// display the list of possible keyspaces to choose from
		result := make(map[string]interface{})
		keyspaces, err := keyspacesWithServedFrom(ctx, wr)
		if err != nil {
			result["Error"] = err.Error()
		} else {
			result["Keyspaces"] = keyspaces
		}
		return nil, verticalSplitCloneTemplate, result, nil
	}

	tables := r.FormValue("tables")
	if tables == "" {
		// display the input form
		result := make(map[string]interface{})
		result["Keyspace"] = keyspace
		result["DefaultOnline"] = defaultOnline
		result["DefaultOffline"] = defaultOffline
		result["DefaultChunkCount"] = fmt.Sprintf("%v", defaultChunkCount)
		result["DefaultMinRowsPerChunk"] = fmt.Sprintf("%v", defaultMinRowsPerChunk)
		result["DefaultSourceReaderCount"] = fmt.Sprintf("%v", defaultSourceReaderCount)
		result["DefaultWriteQueryMaxRows"] = fmt.Sprintf("%v", defaultWriteQueryMaxRows)
		result["DefaultWriteQueryMaxSize"] = fmt.Sprintf("%v", defaultWriteQueryMaxSize)
		result["DefaultDestinationWriterCount"] = fmt.Sprintf("%v", defaultDestinationWriterCount)
		result["DefaultMinHealthyRdonlyTablets"] = fmt.Sprintf("%v", defaultMinHealthyTablets)
		result["DefaultMaxTPS"] = fmt.Sprintf("%v", defaultMaxTPS)
		result["DefaultMaxReplicationLag"] = fmt.Sprintf("%v", defaultMaxReplicationLag)
		result["DefaultUseConsistentSnapshot"] = fmt.Sprintf("%v", defaultUseConsistentSnapshot)
		return nil, verticalSplitCloneTemplate2, result, nil
	}
	tableArray := strings.Split(tables, ",")

	// get other parameters
	onlineStr := r.FormValue("online")
	online := onlineStr == "true"
	offlineStr := r.FormValue("offline")
	offline := offlineStr == "true"
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
	sourceReaderCountStr := r.FormValue("sourceReaderCount")
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
	minHealthyRdonlyTabletsStr := r.FormValue("minHealthyRdonlyTablets")
	minHealthyRdonlyTablets, err := strconv.ParseInt(minHealthyRdonlyTabletsStr, 0, 64)
	if err != nil {
		return nil, nil, nil, vterrors.Wrap(err, "cannot parse minHealthyRdonlyTablets")
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
	tabletTypeStr := r.FormValue("tabletType")
	tabletType, ok := topodata.TabletType_value[tabletTypeStr]
	if !ok {
		return nil, nil, nil, fmt.Errorf("cannot parse tabletType: %v", tabletType)
	}

	// Figure out the shard
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	shardMap, err := wr.TopoServer().FindAllShardsInKeyspace(shortCtx, keyspace)
	cancel()
	if err != nil {
		return nil, nil, nil, vterrors.Wrapf(err, "cannot find shard name for keyspace %s", keyspace)
	}
	if len(shardMap) != 1 {
		return nil, nil, nil, fmt.Errorf("found the wrong number of shards, there should be only one, %v", shardMap)
	}
	var shard string
	for s := range shardMap {
		shard = s
	}

	useConsistentSnapshotStr := r.FormValue("useConsistentSnapshot")
	useConsistentSnapshot := useConsistentSnapshotStr == "true"

	// start the clone job
	wrk, err := newVerticalSplitCloneWorker(wr, wi.cell, keyspace, shard, online, offline, tableArray, int(chunkCount),
		int(minRowsPerChunk), int(sourceReaderCount), int(writeQueryMaxRows), int(writeQueryMaxSize),
		int(destinationWriterCount), int(minHealthyRdonlyTablets), topodata.TabletType(tabletType), maxTPS,
		maxReplicationLag, useConsistentSnapshot)
	if err != nil {
		return nil, nil, nil, vterrors.Wrap(err, "cannot create worker")
	}
	return wrk, nil, nil, nil
}

func init() {
	AddCommand("Clones", Command{"VerticalSplitClone",
		commandVerticalSplitClone, interactiveVerticalSplitClone,
		"[--tables=''] <destination keyspace/shard>",
		"Replicates the data and creates configuration for a vertical split."})
}
