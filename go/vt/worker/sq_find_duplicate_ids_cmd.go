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

const findDuplicateIdsHTML = `
<!DOCTYPE html>
<head>
  <title>Find Duplicate IDs Action</title>
</head>
<body>
  <h1>Find Duplicate IDs Action</h1>

    {{if .Error}}
      <b>Error:</b> {{.Error}}</br>
    {{else}}
      <p>Choose the destination keyspace for this action.</p>
      <ul>
      {{range $i, $si := .Choices}}
        <li><a href="/Square/FindDuplicateIds?keyspace={{$si.Keyspace}}&table={{$si.Table}}">{{$si.Keyspace}}.{{$si.Table}}</a></li>
      {{end}}
      </ul>
    {{end}}
</body>
`

const findDuplicateIdsHTML2 = `
<!DOCTYPE html>
<head>
  <title>FindDuplicateIds</title>
</head>
<body>
  <p>Table to process: {{.Keyspace}}.{{.Table}}</p>
  <h1>Find Duplicate IDs </h1>
    <form action="/Square/FindDuplicateIds" method="post">
      <LABEL for="token">Extra column to remove dupes by (often a "token" column): </LABEL>
        <INPUT type="text" id="token" name="token" value="{{.Token}}"></BR>
      <LABEL for="shardingColumn">Sharding column (TODO figure this out from the vschema): </LABEL>
        <INPUT type="text" id="shardingColumn" name="shardingColumn" value="{{.ShardingColumn}}"></BR>
      <LABEL for="tabletType">Tablet Type:</LABEL>
			<SELECT id="tabletType" name="tabletType">
			  <OPTION value="MASTER">MASTER</OPTION>
  			<OPTION selected value="RDONLY">RDONLY</OPTION>
  			<OPTION value="REPLICA">REPLICA</OPTION>
			</SELECT>
			</BR>
      <LABEL for="chunkCount">Chunk Count: </LABEL>
        <INPUT type="text" id="chunkCount" name="chunkCount" value="{{.DefaultChunkCount}}"></BR>
      <LABEL for="minRowsPerChunk">Minimum Number of Rows per Chunk (may reduce the Chunk Count): </LABEL>
        <INPUT type="text" id="minRowsPerChunk" name="minRowsPerChunk" value="{{.DefaultMinRowsPerChunk}}"></BR>
      <LABEL for="sourceRange">Source keyspace range to process: </LABEL>
        <INPUT type="text" id="sourceRange" name="sourceRange" value="{{.DefaultSourceRange}}"></BR>
      <LABEL for="sourceReaderCount">Source Reader Count: </LABEL>
        <INPUT type="text" id="sourceReaderCount" name="sourceReaderCount" value="{{.DefaultSourceReaderCount}}"></BR>
      <INPUT type="hidden" name="keyspace" value="{{.Keyspace}}"/>
      <INPUT type="hidden" name="table" value="{{.Table}}"/>
      <INPUT type="submit" value="Run"/>
    </form>
  </body>`

var findDuplicateIdsTemplate = mustParseTemplate("findDuplicateIds", findDuplicateIdsHTML)
var findDuplicateIdsTemplate2 = mustParseTemplate("findDuplicateIds2", findDuplicateIdsHTML2)

func commandFindDuplicateIds(wi *Instance, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (Worker, error) {
	tabletTypeStr := subFlags.String("tablet_type", "RDONLY", "type of tablet used")
	token := subFlags.String("token", "token", "extra column to remove dupes by (often a \"token\" column)")
	shardingColumn := subFlags.String("shardingColumn", "customer_id", "sharding column (TODO figure this out from the vschema)")
	chunkCount := subFlags.Int("chunk_count", defaultChunkCount, "number of chunks per table")
	minRowsPerChunk := subFlags.Int("min_rows_per_chunk", defaultMinRowsPerChunk, "minimum number of rows per chunk (may reduce --chunk_count)")
	sourceReaderCount := subFlags.Int("source_reader_count", defaultSourceReaderCount, "number of concurrent streaming queries to use on the source")
	sourceRange := subFlags.String("source_range", defaultSourceRange, "range of the source keyspace to copy from")
	if err := subFlags.Parse(args); err != nil {
		return nil, err
	}
	if subFlags.NArg() != 2 {
		subFlags.Usage()
		return nil, fmt.Errorf("command FindDuplicateIds requires <keyspace> <table>")
	}

	keyspace := subFlags.Arg(0)
	if keyspace == "" {
		return nil, fmt.Errorf("have to pass keyspace argument")
	}

	table := subFlags.Arg(1)
	if table == "" {
		return nil, fmt.Errorf("have to pass table argument")
	}

	tabletType, ok := topodatapb.TabletType_value[*tabletTypeStr]
	if !ok {
		return nil, fmt.Errorf("cannot parse tabletType: %s", *tabletTypeStr)
	}

	worker, err := newFindDuplicateIdsWorker(wr, wi.cell, keyspace, table, *shardingColumn, *token, topodatapb.TabletType(tabletType), *sourceRange, *chunkCount, *minRowsPerChunk, *sourceReaderCount)
	if err != nil {
		return nil, fmt.Errorf("cannot create worker: %v", err)
	}
	return worker, nil
}

func allTables(ctx context.Context, wr *wrangler.Wrangler) ([]map[string]string, error) {
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
			for name := range vschema.Tables {
				mu.Lock()
				result = append(result, map[string]string{
					"Keyspace": keyspace,
					"Table":    name,
				})
				mu.Unlock()
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

func interactiveFindDuplicateIds(ctx context.Context, wi *Instance, wr *wrangler.Wrangler, w http.ResponseWriter, r *http.Request) (Worker, *template.Template, map[string]interface{}, error) {
	if err := r.ParseForm(); err != nil {
		return nil, nil, nil, fmt.Errorf("cannot parse form: %s", err)
	}

	keyspace := r.FormValue("keyspace")
	table := r.FormValue("table")
	token := r.FormValue("token")
	shardingColumn := r.FormValue("shardingColumn")
	if keyspace == "" || table == "" {
		result := make(map[string]interface{})
		choices, err := allTables(ctx, wr)
		if err != nil {
			result["Error"] = err.Error()
		} else {
			result["Choices"] = choices
		}
		return nil, findDuplicateIdsTemplate, result, nil
	}

	sourceReaderCountStr := r.FormValue("sourceReaderCount")
	if sourceReaderCountStr == "" {
		// display the input form
		result := make(map[string]interface{})
		result["Keyspace"] = keyspace
		result["Table"] = table
		result["Token"] = "token"
		result["ShardingColumn"] = "customer_id"
		result["DefaultOnline"] = defaultOnline
		result["DefaultOffline"] = defaultOffline
		result["DefaultChunkCount"] = fmt.Sprintf("%v", defaultChunkCount)
		result["DefaultMinRowsPerChunk"] = fmt.Sprintf("%v", defaultMinRowsPerChunk)
		result["DefaultSourceRange"] = fmt.Sprintf("%v", defaultSourceRange)
		result["DefaultSourceReaderCount"] = fmt.Sprintf("%v", defaultSourceReaderCount)
		result["DefaultWriteQueryMaxRows"] = fmt.Sprintf("%v", defaultWriteQueryMaxRows)
		result["DefaultWriteQueryMaxSize"] = fmt.Sprintf("%v", defaultWriteQueryMaxSize)
		result["DefaultDestinationWriterCount"] = fmt.Sprintf("%v", defaultDestinationWriterCount)
		result["DefaultMinHealthyRdonlyTablets"] = fmt.Sprintf("%v", defaultMinHealthyTablets)
		result["DefaultMaxTPS"] = fmt.Sprintf("%v", defaultMaxTPS)
		result["DefaultMaxReplicationLag"] = fmt.Sprintf("%v", defaultMaxReplicationLag)
		return nil, findDuplicateIdsTemplate2, result, nil
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
	sourceRange := r.FormValue("sourceRange")
	if sourceRange == "" {
		sourceRange = defaultSourceRange
	}
	sourceReaderCount, err := strconv.ParseInt(sourceReaderCountStr, 0, 64)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cannot parse sourceReaderCount: %s", err)
	}

	// start the job
	wrk, err := newFindDuplicateIdsWorker(wr, wi.cell, keyspace, table, shardingColumn, token, topodatapb.TabletType(tabletType), sourceRange, int(chunkCount), int(minRowsPerChunk), int(sourceReaderCount))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cannot create worker: %v", err)
	}
	return wrk, nil, nil, nil
}

func init() {
	AddCommand("Square", Command{"FindDuplicateIds",
		commandFindDuplicateIds, interactiveFindDuplicateIds,
		"<keyspace> <table>",
		"Finds duplicate IDs across all shards"})
}
