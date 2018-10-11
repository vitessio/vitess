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
	"sync"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/concurrency"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/wrangler"
)

const backfillLookupHTML = `
<!DOCTYPE html>
<head>
  <title>Backfill Lookup Action</title>
</head>
<body>
  <h1>Backfill Lookup Action</h1>

    {{if .Error}}
      <b>Error:</b> {{.Error}}</br>
    {{else}}
      <p>Choose the destination keyspace for this action.</p>
      <ul>
      {{range $i, $si := .Choices}}
        <li><a href="/Backfills/BackfillLookup?keyspace={{$si.Keyspace}}&vindex={{$si.Vindex}}">{{$si.Keyspace}}.{{$si.Vindex}}</a></li>
      {{end}}
      </ul>
    {{end}}
</body>
`

const backfillLookupHTML2 = `
<!DOCTYPE html>
<head>
  <title>Backfill Lookup Action</title>
</head>
<body>
  <p>Vindex to backfill: {{.Keyspace}}.{{.Vindex}}</p>
  <h1>Backfill Lookup Action</h1>
    <form action="/Backfills/BackfillLookup" method="post">
      <LABEL for="tabletType">Tablet Type:</LABEL>
			<SELECT id="tabletType" name="tabletType">
			  <OPTION value="MASTER">MASTER</OPTION>
  			<OPTION selected value="RDONLY">RDONLY</OPTION>
  			<OPTION value="REPLICA">REPLICA</OPTION>
			</SELECT>
			</BR>
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
      <LABEL for="maxTPS">Maximum Write Transactions/second (If non-zero, writes on the destination will be throttled. Unlimited by default.): </LABEL>
        <INPUT type="text" id="maxTPS" name="maxTPS" value="{{.DefaultMaxTPS}}"></BR>
      <LABEL for="maxReplicationLag">Maximum Replication Lag Seconds (enables the adapative throttler. Disabled by default.): </LABEL>
        <INPUT type="text" id="maxReplicationLag" name="maxReplicationLag" value="{{.DefaultMaxReplicationLag}}"></BR>
      <LABEL for="skipNullRows">Skip rows where NULL is the sharding key.</LABEL>
        <INPUT type="checkbox" id="skipNullRows" name="skipNullRows" value="true"></BR>
      <INPUT type="hidden" name="keyspace" value="{{.Keyspace}}"/>
      <INPUT type="hidden" name="vindex" value="{{.Vindex}}"/>
      <INPUT type="submit" value="Backfill"/>
    </form>
  </body>`

var backfillLookupTemplate = mustParseTemplate("backfillLookup", backfillLookupHTML)
var backfillLookupTemplate2 = mustParseTemplate("backfillLookup2", backfillLookupHTML2)

func commandBackfillLookup(wi *Instance, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (Worker, error) {
	tabletTypeStr := subFlags.String("tablet_type", "RDONLY", "type of tablet used")
	chunkCount := subFlags.Int("chunk_count", defaultChunkCount, "number of chunks per table")
	minRowsPerChunk := subFlags.Int("min_rows_per_chunk", defaultMinRowsPerChunk, "minimum number of rows per chunk (may reduce --chunk_count)")
	sourceReaderCount := subFlags.Int("source_reader_count", defaultSourceReaderCount, "number of concurrent streaming queries to use on the source")
	writeQueryMaxRows := subFlags.Int("write_query_max_rows", defaultWriteQueryMaxRows, "maximum number of rows per write query")
	writeQueryMaxSize := subFlags.Int("write_query_max_size", defaultWriteQueryMaxSize, "maximum size (in bytes) per write query")
	destinationWriterCount := subFlags.Int("destination_writer_count", defaultDestinationWriterCount, "number of concurrent RPCs to execute on the destination")
	maxTPS := subFlags.Int64("max_tps", defaultMaxTPS, "rate limit of maximum number of (write) transactions/second on the destination (unlimited by default)")
	maxReplicationLag := subFlags.Int64("max_replication_lag", defaultMaxReplicationLag, "if set, the adapative throttler will be enabled and automatically adjust the write rate to keep the lag below the set value in seconds (disabled by default)")
	skipNullRows := subFlags.Bool("skip_null_rows", defaultSkipNullRows, "Skip rows where the lookup sharding key is NULL. Note that such rows aren't especially helpful in a lookup in the first place.")
	if err := subFlags.Parse(args); err != nil {
		return nil, err
	}
	if subFlags.NArg() != 1 {
		subFlags.Usage()
		return nil, fmt.Errorf("command BackfillLookup requires <keyspace/vindexName>")
	}

	keyspace := subFlags.Arg(0)
	if keyspace == "" {
		return nil, fmt.Errorf("have to pass keyspace argument")
	}

	vindexName := subFlags.Arg(1)
	if vindexName == "" {
		return nil, fmt.Errorf("have to pass vindex argument")
	}

	tabletType, ok := topodatapb.TabletType_value[*tabletTypeStr]
	if !ok {
		return nil, fmt.Errorf("cannot parse tabletType: %s", *tabletTypeStr)
	}

	worker, err := newBackfillLookupWorker(wr, wi.cell, keyspace, vindexName, topodatapb.TabletType(tabletType), *chunkCount, *minRowsPerChunk, *sourceReaderCount, *writeQueryMaxRows, *writeQueryMaxSize, *destinationWriterCount, *maxTPS, *maxReplicationLag, *skipNullRows)
	if err != nil {
		return nil, fmt.Errorf("cannot create Backfill Lookup worker: %v", err)
	}
	return worker, nil
}

func writeOnlyVindexes(ctx context.Context, wr *wrangler.Wrangler) ([]map[string]string, error) {
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
			vschema, err := wr.TopoServer().GetVSchema(shortCtx, keyspace)
			cancel()
			if err != nil {
				rec.RecordError(err)
				return
			}
			for name, vindex := range vschema.Vindexes {
				writeOnly, err := boolFromMap(vindex.Params, "write_only")
				if err != nil {
					rec.RecordError(err)
					return
				}
				if writeOnly && vindex.Type == "lookup_hash" {
					mu.Lock()
					result = append(result, map[string]string{
						"Keyspace": keyspace,
						"Vindex":   name,
					})
					mu.Unlock()
				}
			}
		}(keyspace)
	}
	wg.Wait()

	if rec.HasErrors() {
		return nil, rec.Error()
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("there are no keyspaces with lookup_hash vindexes with write_only set to true")
	}
	return result, nil
}

func interactiveBackfillLookup(ctx context.Context, wi *Instance, wr *wrangler.Wrangler, w http.ResponseWriter, r *http.Request) (Worker, *template.Template, map[string]interface{}, error) {
	if err := r.ParseForm(); err != nil {
		return nil, nil, nil, fmt.Errorf("cannot parse form: %s", err)
	}

	keyspace := r.FormValue("keyspace")
	vindex := r.FormValue("vindex")
	if keyspace == "" || vindex == "" {
		// display the list of possible splits to choose from
		// (just find all the overlapping guys)
		result := make(map[string]interface{})
		choices, err := writeOnlyVindexes(ctx, wr)
		if err != nil {
			result["Error"] = err.Error()
		} else {
			result["Choices"] = choices
		}
		return nil, backfillLookupTemplate, result, nil
	}

	sourceReaderCountStr := r.FormValue("sourceReaderCount")
	if sourceReaderCountStr == "" {
		// display the input form
		result := make(map[string]interface{})
		result["Keyspace"] = keyspace
		result["Vindex"] = vindex
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
		return nil, backfillLookupTemplate2, result, nil
	}

	// get other parameters
	tabletTypeStr := r.FormValue("tabletType")
	tabletType, ok := topodatapb.TabletType_value[tabletTypeStr]
	if !ok {
		return nil, nil, nil, fmt.Errorf("cannot parse tabletType: %s", tabletTypeStr)
	}
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

	skipNullRowsStr := r.FormValue("skipNullRows")
	skipNullRows := skipNullRowsStr == "true"

	// start the backfill job
	wrk, err := newBackfillLookupWorker(wr, wi.cell, keyspace, vindex, topodatapb.TabletType(tabletType), int(chunkCount), int(minRowsPerChunk), int(sourceReaderCount), int(writeQueryMaxRows), int(writeQueryMaxSize), int(destinationWriterCount), maxTPS, maxReplicationLag, skipNullRows)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cannot create worker: %v", err)
	}
	return wrk, nil, nil, nil
}

func boolFromMap(m map[string]string, key string) (bool, error) {
	val, ok := m[key]
	if !ok {
		return false, nil
	}
	switch val {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("%s value must be 'true' or 'false': '%s'", key, val)
	}
}

func init() {
	AddCommand("Backfills", Command{"BackfillLookup",
		commandBackfillLookup, interactiveBackfillLookup,
		"<keyspace> <vindex>",
		"Backfills lookup table for a vindex from the the table data."})
}
