// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/topotools"
	"github.com/youtube/vitess/go/vt/worker"
	"github.com/youtube/vitess/go/vt/wrangler"
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
      <LABEL for="excludeTables">Exclude Tables: </LABEL>
        <INPUT type="text" id="excludeTables" name="excludeTables" value="moving.*"></BR>
      <LABEL for="strategy">Strategy: </LABEL>
        <INPUT type="text" id="strategy" name="strategy" value="populateBlpCheckpoint"></BR>
      <LABEL for="sourceReaderCount">Source Reader Count: </LABEL>
        <INPUT type="text" id="sourceReaderCount" name="sourceReaderCount" value="{{.DefaultSourceReaderCount}}"></BR>
      <LABEL for="minTableSizeForSplit">Minimun Table Size For Split: </LABEL>
        <INPUT type="text" id="minTableSizeForSplit" name="minTableSizeForSplit" value="{{.DefaultMinTableSizeForSplit}}"></BR>
      <LABEL for="destinationWriterCount">Destination Writer Count: </LABEL>
        <INPUT type="text" id="destinationWriterCount" name="destinationWriterCount" value="{{.DefaultDestinationWriterCount}}"></BR>
      <INPUT type="hidden" name="keyspace" value="{{.Keyspace}}"/>
      <INPUT type="hidden" name="shard" value="{{.Shard}}"/>
      <INPUT type="submit" value="Clone"/>
    </form>

  <h1>Help</h1>
    <p>Strategy can have the following values, comma separated:</p>
    <ul>
      <li><b>populateBlpCheckpoint</b>: creates (if necessary) and populates the blp_checkpoint table in the destination. Required for filtered replication to start.</li>
      <li><b>dontStartBinlogPlayer</b>: (requires populateBlpCheckpoint) will setup, but not start binlog replication on the destination. The flag has to be manually cleared from the _vt.blp_checkpoint table.</li>
      <li><b>skipAutoIncrement(TTT)</b>: we won't add the AUTO_INCREMENT back to that table.</li>
      <li><b>skipSetSourceShards</b>: we won't set SourceShards on the destination shards, disabling filtered replication. Usefull for worker tests.</li>
    </ul>
    <p>The following flags are also supported, but their use is very strongly discouraged:</p>
    <ul>
      <li><b>delayPrimaryKey</b>: we won't add the primary key until after the table is populated.</li>
      <li><b>delaySecondaryIndexes</b>: we won't add the secondary indexes until after the table is populated.</li>
      <li><b>useMyIsam</b>: create the table as MyISAM, then convert it to InnoDB after population.</li>
      <li><b>writeBinLogs</b>: write all operations to the binlogs.</li>
    </ul>
  </body>
`

var splitCloneTemplate = loadTemplate("splitClone", splitCloneHTML)
var splitCloneTemplate2 = loadTemplate("splitClone2", splitCloneHTML2)

func commandSplitClone(wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) worker.Worker {
	excludeTables := subFlags.String("exclude_tables", "", "comma separated list of tables to exclude")
	strategy := subFlags.String("strategy", "", "which strategy to use for restore, use 'mysqlctl multirestore -help' for more info")
	sourceReaderCount := subFlags.Int("source_reader_count", defaultSourceReaderCount, "number of concurrent streaming queries to use on the source")
	minTableSizeForSplit := subFlags.Int("min_table_size_for_split", defaultMinTableSizeForSplit, "tables bigger than this size on disk in bytes will be split into source_reader_count chunks if possible")
	destinationWriterCount := subFlags.Int("destination_writer_count", defaultDestinationWriterCount, "number of concurrent RPCs to execute on the destination")
	subFlags.Parse(args)
	if subFlags.NArg() != 1 {
		log.Fatalf("command SplitClone requires <keyspace/shard|zk shard path>")
	}

	keyspace, shard := shardParamToKeyspaceShard(subFlags.Arg(0))
	var excludeTableArray []string
	if *excludeTables != "" {
		excludeTableArray = strings.Split(*excludeTables, ",")
	}
	return worker.NewSplitCloneWorker(wr, *cell, keyspace, shard, excludeTableArray, *strategy, *sourceReaderCount, uint64(*minTableSizeForSplit), *destinationWriterCount)
}

func keyspacesWithOverlappingShards(wr *wrangler.Wrangler) ([]map[string]string, error) {
	keyspaces, err := wr.TopoServer().GetKeyspaces()
	if err != nil {
		return nil, err
	}

	wg := sync.WaitGroup{}
	mu := sync.Mutex{} // protects result
	result := make([]map[string]string, 0, len(keyspaces))
	rec := concurrency.AllErrorRecorder{}
	for _, keyspace := range keyspaces {
		wg.Add(1)
		go func(keyspace string) {
			defer wg.Done()
			osList, err := topotools.FindOverlappingShards(wr.TopoServer(), keyspace)
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

func interactiveSplitClone(wr *wrangler.Wrangler, w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		httpError(w, "cannot parse form: %s", err)
		return
	}

	keyspace := r.FormValue("keyspace")
	shard := r.FormValue("shard")
	if keyspace == "" || shard == "" {
		// display the list of possible splits to choose from
		// (just find all the overlapping guys)
		result := make(map[string]interface{})
		choices, err := keyspacesWithOverlappingShards(wr)
		if err != nil {
			result["Error"] = err.Error()
		} else {
			result["Choices"] = choices
		}

		executeTemplate(w, splitCloneTemplate, result)
		return
	}

	sourceReaderCountStr := r.FormValue("sourceReaderCount")
	if sourceReaderCountStr == "" {
		// display the input form
		result := make(map[string]interface{})
		result["Keyspace"] = keyspace
		result["Shard"] = shard
		result["DefaultSourceReaderCount"] = fmt.Sprintf("%v", defaultSourceReaderCount)
		result["DefaultMinTableSizeForSplit"] = fmt.Sprintf("%v", defaultMinTableSizeForSplit)
		result["DefaultDestinationWriterCount"] = fmt.Sprintf("%v", defaultDestinationWriterCount)
		executeTemplate(w, splitCloneTemplate2, result)
		return
	}

	// get other parameters
	excludeTables := r.FormValue("excludeTables")
	excludeTableArray := strings.Split(excludeTables, ",")
	strategy := r.FormValue("strategy")
	sourceReaderCount, err := strconv.ParseInt(sourceReaderCountStr, 0, 64)
	if err != nil {
		httpError(w, "cannot parse sourceReaderCount: %s", err)
		return
	}
	minTableSizeForSplitStr := r.FormValue("minTableSizeForSplit")
	minTableSizeForSplit, err := strconv.ParseInt(minTableSizeForSplitStr, 0, 64)
	if err != nil {
		httpError(w, "cannot parse minTableSizeForSplit: %s", err)
		return
	}
	destinationWriterCountStr := r.FormValue("destinationWriterCount")
	destinationWriterCount, err := strconv.ParseInt(destinationWriterCountStr, 0, 64)
	if err != nil {
		httpError(w, "cannot parse destinationWriterCount: %s", err)
		return
	}

	// start the clone job
	wrk := worker.NewSplitCloneWorker(wr, *cell, keyspace, shard, excludeTableArray, strategy, int(sourceReaderCount), uint64(minTableSizeForSplit), int(destinationWriterCount))
	if _, err := setAndStartWorker(wrk); err != nil {
		httpError(w, "cannot set worker: %s", err)
		return
	}

	http.Redirect(w, r, servenv.StatusURLPath(), http.StatusTemporaryRedirect)
}

func init() {
	addCommand("Clones", command{"SplitClone",
		commandSplitClone, interactiveSplitClone,
		"[--exclude_tables=''] [--strategy=''] <keyspace/shard|zk shard path>",
		"Replicates the data and creates configuration for a horizontal split."})
}
