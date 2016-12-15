// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"flag"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/topotools"
	"github.com/youtube/vitess/go/vt/wrangler"
	"golang.org/x/net/context"
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
      <LABEL for="strategy">Strategy: </LABEL>
        <INPUT type="text" id="strategy" name="strategy" value=""></BR>
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
      <LABEL for="minHealthyRdonlyTablets">Minimum Number of required healthy RDONLY tablets in the source and destination shard at start: </LABEL>
        <INPUT type="text" id="minHealthyRdonlyTablets" name="minHealthyRdonlyTablets" value="{{.DefaultMinHealthyRdonlyTablets}}"></BR>
      <LABEL for="maxTPS">Maximum Write Transactions/second (If non-zero, writes on the destination will be throttled. Unlimited by default.): </LABEL>
        <INPUT type="text" id="maxTPS" name="maxTPS" value="{{.DefaultMaxTPS}}"></BR>
      <LABEL for="maxReplicationLag">Maximum Replication Lag (enables the adapative throttler. Disabled by default.): </LABEL>
        <INPUT type="text" id="maxReplicationLag" name="maxReplicationLag" value="{{.DefaultMaxReplicationLag}}"></BR>
      <INPUT type="hidden" name="keyspace" value="{{.Keyspace}}"/>
      <INPUT type="hidden" name="shard" value="{{.Shard}}"/>
      <INPUT type="submit" value="Clone"/>
    </form>

  <h1>Help</h1>
    <p>Strategy can have the following values, comma separated:</p>
    <ul>
      <li><b>skipPopulateBlpCheckpoint</b>: skips creating (if necessary) and populating the blp_checkpoint table in the destination. Not skipped by default because it's required for filtered replication to start.</li>
      <li><b>dontStartBinlogPlayer</b>: (requires skipPopulateBlpCheckpoint to be false) will setup, but not start binlog replication on the destination. The flag has to be manually cleared from the _vt.blp_checkpoint table.</li>
      <li><b>skipSetSourceShards</b>: we won't set SourceShards on the destination shards, disabling filtered replication. Useful for worker tests.</li>
    </ul>
  </body>
`

var splitCloneTemplate = mustParseTemplate("splitClone", splitCloneHTML)
var splitCloneTemplate2 = mustParseTemplate("splitClone2", splitCloneHTML2)

func commandSplitClone(wi *Instance, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (Worker, error) {
	online := subFlags.Bool("online", defaultOnline, "do online copy (optional approximate copy, source and destination tablets will not be put out of serving, minimizes downtime during offline copy)")
	offline := subFlags.Bool("offline", defaultOffline, "do offline copy (exact copy at a specific GTID, required before shard migration, source and destination tablets will be put out of serving during copy)")
	excludeTables := subFlags.String("exclude_tables", "", "comma separated list of tables to exclude. Each is either an exact match, or a regular expression of the form /regexp/")
	strategy := subFlags.String("strategy", "", "which strategy to use for restore, use 'vtworker SplitClone --strategy=-help k/s' for more info")
	chunkCount := subFlags.Int("chunk_count", defaultChunkCount, "number of chunks per table")
	minRowsPerChunk := subFlags.Int("min_rows_per_chunk", defaultMinRowsPerChunk, "minimum number of rows per chunk (may reduce --chunk_count)")
	sourceReaderCount := subFlags.Int("source_reader_count", defaultSourceReaderCount, "number of concurrent streaming queries to use on the source")
	writeQueryMaxRows := subFlags.Int("write_query_max_rows", defaultWriteQueryMaxRows, "maximum number of rows per write query")
	writeQueryMaxSize := subFlags.Int("write_query_max_size", defaultWriteQueryMaxSize, "maximum size (in bytes) per write query")
	destinationWriterCount := subFlags.Int("destination_writer_count", defaultDestinationWriterCount, "number of concurrent RPCs to execute on the destination")
	minHealthyRdonlyTablets := subFlags.Int("min_healthy_rdonly_tablets", defaultMinHealthyRdonlyTablets, "minimum number of healthy RDONLY tablets in the source and destination shard at start")
	maxTPS := subFlags.Int64("max_tps", defaultMaxTPS, "rate limit of maximum number of (write) transactions/second on the destination (unlimited by default)")
	maxReplicationLag := subFlags.Int64("max_replication_lag", defaultMaxReplicationLag, "if set, the adapative throttler will be enabled and automatically adjust the write rate to keep the lag below the set value (disabled by default)")
	if err := subFlags.Parse(args); err != nil {
		return nil, err
	}
	if subFlags.NArg() != 1 {
		subFlags.Usage()
		return nil, fmt.Errorf("command SplitClone requires <keyspace/shard>")
	}

	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return nil, err
	}
	var excludeTableArray []string
	if *excludeTables != "" {
		excludeTableArray = strings.Split(*excludeTables, ",")
	}
	worker, err := newSplitCloneWorker(wr, wi.cell, keyspace, shard, *online, *offline, excludeTableArray, *strategy, *chunkCount, *minRowsPerChunk, *sourceReaderCount, *writeQueryMaxRows, *writeQueryMaxSize, *destinationWriterCount, *minHealthyRdonlyTablets, *maxTPS, *maxReplicationLag)
	if err != nil {
		return nil, fmt.Errorf("cannot create split clone worker: %v", err)
	}
	return worker, nil
}

func keyspacesWithOverlappingShards(ctx context.Context, wr *wrangler.Wrangler) ([]map[string]string, error) {
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
		return nil, fmt.Errorf("There are no keyspaces with overlapping shards")
	}
	return result, nil
}

func interactiveSplitClone(ctx context.Context, wi *Instance, wr *wrangler.Wrangler, w http.ResponseWriter, r *http.Request) (Worker, *template.Template, map[string]interface{}, error) {
	if err := r.ParseForm(); err != nil {
		return nil, nil, nil, fmt.Errorf("cannot parse form: %s", err)
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
		result["DefaultMinHealthyRdonlyTablets"] = fmt.Sprintf("%v", defaultMinHealthyRdonlyTablets)
		result["DefaultMaxTPS"] = fmt.Sprintf("%v", defaultMaxTPS)
		result["DefaultMaxReplicationLag"] = fmt.Sprintf("%v", defaultMaxReplicationLag)
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
	strategy := r.FormValue("strategy")
	chunkCountStr := r.FormValue("chunkCount")
	chunkCount, err := strconv.ParseInt(chunkCountStr, 0, 64)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cannot parse chunkCount: %s", err)
	}
	minRowsPerChunkStr := r.FormValue("minRowsPerChunk")
	minRowsPerChunk, err := strconv.ParseInt(minRowsPerChunkStr, 0, 64)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cannot parse minRowsPerChunk: %s", err)
	}
	sourceReaderCount, err := strconv.ParseInt(sourceReaderCountStr, 0, 64)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cannot parse sourceReaderCount: %s", err)
	}
	writeQueryMaxRowsStr := r.FormValue("writeQueryMaxRows")
	writeQueryMaxRows, err := strconv.ParseInt(writeQueryMaxRowsStr, 0, 64)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cannot parse writeQueryMaxRows: %s", err)
	}
	writeQueryMaxSizeStr := r.FormValue("writeQueryMaxSize")
	writeQueryMaxSize, err := strconv.ParseInt(writeQueryMaxSizeStr, 0, 64)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cannot parse writeQueryMaxSize: %s", err)
	}
	destinationWriterCountStr := r.FormValue("destinationWriterCount")
	destinationWriterCount, err := strconv.ParseInt(destinationWriterCountStr, 0, 64)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cannot parse destinationWriterCount: %s", err)
	}
	minHealthyRdonlyTabletsStr := r.FormValue("minHealthyRdonlyTablets")
	minHealthyRdonlyTablets, err := strconv.ParseInt(minHealthyRdonlyTabletsStr, 0, 64)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cannot parse minHealthyRdonlyTablets: %s", err)
	}
	maxTPSStr := r.FormValue("maxTPS")
	maxTPS, err := strconv.ParseInt(maxTPSStr, 0, 64)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cannot parse maxTPS: %s", err)
	}
	maxReplicationLagStr := r.FormValue("maxReplicationLag")
	maxReplicationLag, err := strconv.ParseInt(maxReplicationLagStr, 0, 64)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cannot parse maxReplicationLag: %s", err)
	}

	// start the clone job
	wrk, err := newSplitCloneWorker(wr, wi.cell, keyspace, shard, online, offline, excludeTableArray, strategy, int(chunkCount), int(minRowsPerChunk), int(sourceReaderCount), int(writeQueryMaxRows), int(writeQueryMaxSize), int(destinationWriterCount), int(minHealthyRdonlyTablets), maxTPS, maxReplicationLag)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cannot create worker: %v", err)
	}
	return wrk, nil, nil, nil
}

func init() {
	AddCommand("Clones", Command{"SplitClone",
		commandSplitClone, interactiveSplitClone,
		"[--online=false] [--offline=false] [--exclude_tables=''] [--strategy=''] <keyspace/shard>",
		"Replicates the data and creates configuration for a horizontal split."})
}
