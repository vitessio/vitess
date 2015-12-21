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
	"github.com/youtube/vitess/go/vt/wrangler"
	"golang.org/x/net/context"
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
        <INPUT type="text" id="strategy" name="strategy" value="-populate_blp_checkpoint"></BR>
      <LABEL for="sourceReaderCount">Source Reader Count: </LABEL>
        <INPUT type="text" id="sourceReaderCount" name="sourceReaderCount" value="{{.DefaultSourceReaderCount}}"></BR>
      <LABEL for="destinationPackCount">Destination Pack Count: </LABEL>
        <INPUT type="text" id="destinationPackCount" name="destinationPackCount" value="{{.DefaultDestinationPackCount}}"></BR>
      <LABEL for="minTableSizeForSplit">Minimun Table Size For Split: </LABEL>
        <INPUT type="text" id="minTableSizeForSplit" name="minTableSizeForSplit" value="{{.DefaultMinTableSizeForSplit}}"></BR>
      <LABEL for="destinationWriterCount">Destination Writer Count: </LABEL>
        <INPUT type="text" id="destinationWriterCount" name="destinationWriterCount" value="{{.DefaultDestinationWriterCount}}"></BR>
      <INPUT type="hidden" name="keyspace" value="{{.Keyspace}}"/>
      <INPUT type="submit" value="Clone"/>
    </form>

  <h1>Help</h1>
    <p>Strategy can have the following values, comma separated:</p>
    <ul>
      <li><b>populateBlpCheckpoint</b>: creates (if necessary) and populates the blp_checkpoint table in the destination. Required for filtered replication to start.</li>
      <li><b>dontStartBinlogPlayer</b>: (requires populateBlpCheckpoint) will setup, but not start binlog replication on the destination. The flag has to be manually cleared from the _vt.blp_checkpoint table.</li>
      <li><b>skipSetSourceShards</b>: we won't set SourceShards on the destination shards, disabling filtered replication. Useful for worker tests.</li>
    </ul>
  </body>
`

var verticalSplitCloneTemplate = mustParseTemplate("verticalSplitClone", verticalSplitCloneHTML)
var verticalSplitCloneTemplate2 = mustParseTemplate("verticalSplitClone2", verticalSplitCloneHTML2)

func commandVerticalSplitClone(wi *Instance, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (Worker, error) {
	tables := subFlags.String("tables", "", "comma separated list of tables to replicate (used for vertical split)")
	strategy := subFlags.String("strategy", "", "which strategy to use for restore, use 'vtworker VerticalSplitClone --strategy=-help k/s' for more info")
	sourceReaderCount := subFlags.Int("source_reader_count", defaultSourceReaderCount, "number of concurrent streaming queries to use on the source")
	destinationPackCount := subFlags.Int("destination_pack_count", defaultDestinationPackCount, "number of packets to pack in one destination insert")
	minTableSizeForSplit := subFlags.Int("min_table_size_for_split", defaultMinTableSizeForSplit, "tables bigger than this size on disk in bytes will be split into source_reader_count chunks if possible")
	destinationWriterCount := subFlags.Int("destination_writer_count", defaultDestinationWriterCount, "number of concurrent RPCs to execute on the destination")
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
	var tableArray []string
	if *tables != "" {
		tableArray = strings.Split(*tables, ",")
	}
	worker, err := NewVerticalSplitCloneWorker(wr, wi.cell, keyspace, shard, tableArray, *strategy, *sourceReaderCount, *destinationPackCount, uint64(*minTableSizeForSplit), *destinationWriterCount)
	if err != nil {
		return nil, fmt.Errorf("cannot create worker: %v", err)
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
			shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
			ki, err := wr.TopoServer().GetKeyspace(shortCtx, keyspace)
			cancel()
			if err != nil {
				rec.RecordError(err)
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
		return nil, fmt.Errorf("There are no keyspaces with ServedFrom")
	}
	return result, nil
}

func interactiveVerticalSplitClone(ctx context.Context, wi *Instance, wr *wrangler.Wrangler, w http.ResponseWriter, r *http.Request) (Worker, *template.Template, map[string]interface{}, error) {
	if err := r.ParseForm(); err != nil {
		return nil, nil, nil, fmt.Errorf("cannot parse form: %s", err)
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
		result["DefaultSourceReaderCount"] = fmt.Sprintf("%v", defaultSourceReaderCount)
		result["DefaultDestinationPackCount"] = fmt.Sprintf("%v", defaultDestinationPackCount)
		result["DefaultMinTableSizeForSplit"] = fmt.Sprintf("%v", defaultMinTableSizeForSplit)
		result["DefaultDestinationWriterCount"] = fmt.Sprintf("%v", defaultDestinationWriterCount)
		return nil, verticalSplitCloneTemplate2, result, nil
	}
	tableArray := strings.Split(tables, ",")

	// get other parameters
	strategy := r.FormValue("strategy")
	sourceReaderCountStr := r.FormValue("sourceReaderCount")
	sourceReaderCount, err := strconv.ParseInt(sourceReaderCountStr, 0, 64)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cannot parse sourceReaderCount: %s", err)
	}
	destinationPackCountStr := r.FormValue("destinationPackCount")
	destinationPackCount, err := strconv.ParseInt(destinationPackCountStr, 0, 64)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cannot parse destinationPackCount: %s", err)
	}
	minTableSizeForSplitStr := r.FormValue("minTableSizeForSplit")
	minTableSizeForSplit, err := strconv.ParseInt(minTableSizeForSplitStr, 0, 64)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cannot parse minTableSizeForSplit: %s", err)
	}
	destinationWriterCountStr := r.FormValue("destinationWriterCount")
	destinationWriterCount, err := strconv.ParseInt(destinationWriterCountStr, 0, 64)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cannot parse destinationWriterCount: %s", err)
	}

	// start the clone job
	wrk, err := NewVerticalSplitCloneWorker(wr, wi.cell, keyspace, "0", tableArray, strategy, int(sourceReaderCount), int(destinationPackCount), uint64(minTableSizeForSplit), int(destinationWriterCount))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cannot create worker: %v", err)
	}
	return wrk, nil, nil, nil
}

func init() {
	AddCommand("Clones", Command{"VerticalSplitClone",
		commandVerticalSplitClone, interactiveVerticalSplitClone,
		"[--tables=''] [--strategy=''] <destination keyspace/shard>",
		"Replicates the data and creates configuration for a vertical split."})
}
