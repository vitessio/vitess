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

// TODO(mberlin): Remove this file when SplitClone supports merge-sorting
// primary key columns based on the MySQL collation.

import (
	"flag"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"

	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/wrangler"
	"golang.org/x/net/context"
)

const legacySplitCloneHTML = `
<!DOCTYPE html>
<head>
  <title>Legacy Split Clone Action</title>
</head>
<body>
  <h1>Legacy Split Clone Action</h1>
  <h2>Use this command only when the 'SplitClone' command told you so. Otherwise use the 'SplitClone' command.</h2>

    {{if .Error}}
      <b>Error:</b> {{.Error}}</br>
    {{else}}
      <p>Choose the destination keyspace for this action.</p>
      <ul>
      {{range $i, $si := .Choices}}
        <li><a href="/Clones/LegacySplitClone?keyspace={{$si.Keyspace}}&shard={{$si.Shard}}">{{$si.Keyspace}}/{{$si.Shard}}</a></li>
      {{end}}
      </ul>
    {{end}}
</body>
`

const legacySplitCloneHTML2 = `
<!DOCTYPE html>
<head>
  <title>Legacy Split Clone Action</title>
</head>
<body>
  <p>Shard involved: {{.Keyspace}}/{{.Shard}}</p>
  <h1>Split Clone Action</h1>
    <form action="/Clones/LegacySplitClone" method="post">
      <LABEL for="excludeTables">Exclude Tables: </LABEL>
        <INPUT type="text" id="excludeTables" name="excludeTables" value="/ignored/"></BR>
      <LABEL for="strategy">Strategy: </LABEL>
        <INPUT type="text" id="strategy" name="strategy" value=""></BR>
      <LABEL for="sourceReaderCount">Source Reader Count: </LABEL>
        <INPUT type="text" id="sourceReaderCount" name="sourceReaderCount" value="{{.DefaultSourceReaderCount}}"></BR>
      <LABEL for="destinationPackCount">Destination Pack Count: </LABEL>
        <INPUT type="text" id="destinationPackCount" name="destinationPackCount" value="{{.DefaultDestinationPackCount}}"></BR>
      <LABEL for="destinationWriterCount">Destination Writer Count: </LABEL>
        <INPUT type="text" id="destinationWriterCount" name="destinationWriterCount" value="{{.DefaultDestinationWriterCount}}"></BR>
      <LABEL for="minHealthyRdonlyTablets">Minimum Number of required healthy RDONLY tablets: </LABEL>
        <INPUT type="text" id="minHealthyRdonlyTablets" name="minHealthyRdonlyTablets" value="{{.DefaultMinHealthyRdonlyTablets}}"></BR>
      <LABEL for="maxTPS">Maximum Write Transactions/second (If non-zero, writes on the destination will be throttled. Unlimited by default.): </LABEL>
        <INPUT type="text" id="maxTPS" name="maxTPS" value="{{.DefaultMaxTPS}}"></BR>
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

var legacySplitCloneTemplate = mustParseTemplate("splitClone", legacySplitCloneHTML)
var legacySplitCloneTemplate2 = mustParseTemplate("splitClone2", legacySplitCloneHTML2)

func commandLegacySplitClone(wi *Instance, wr *wrangler.Wrangler, subFlags *flag.FlagSet, args []string) (Worker, error) {
	excludeTables := subFlags.String("exclude_tables", "", "comma separated list of tables to exclude. Each is either an exact match, or a regular expression of the form /regexp/")
	strategy := subFlags.String("strategy", "", "which strategy to use for restore, use 'vtworker LegacySplitClone --strategy=-help k/s' for more info")
	sourceReaderCount := subFlags.Int("source_reader_count", defaultSourceReaderCount, "number of concurrent streaming queries to use on the source")
	destinationPackCount := subFlags.Int("destination_pack_count", defaultDestinationPackCount, "number of packets to pack in one destination insert")
	destinationWriterCount := subFlags.Int("destination_writer_count", defaultDestinationWriterCount, "number of concurrent RPCs to execute on the destination")
	minHealthyRdonlyTablets := subFlags.Int("min_healthy_rdonly_tablets", defaultMinHealthyRdonlyTablets, "minimum number of healthy RDONLY tablets before taking out one")
	maxTPS := subFlags.Int64("max_tps", defaultMaxTPS, "if non-zero, limit copy to maximum number of (write) transactions/second on the destination (unlimited by default)")
	if err := subFlags.Parse(args); err != nil {
		return nil, err
	}
	if subFlags.NArg() != 1 {
		subFlags.Usage()
		return nil, fmt.Errorf("command LegacySplitClone requires <keyspace/shard>")
	}

	keyspace, shard, err := topoproto.ParseKeyspaceShard(subFlags.Arg(0))
	if err != nil {
		return nil, err
	}
	var excludeTableArray []string
	if *excludeTables != "" {
		excludeTableArray = strings.Split(*excludeTables, ",")
	}
	worker, err := NewLegacySplitCloneWorker(wr, wi.cell, keyspace, shard, excludeTableArray, *strategy, *sourceReaderCount, *destinationPackCount, *destinationWriterCount, *minHealthyRdonlyTablets, *maxTPS)
	if err != nil {
		return nil, fmt.Errorf("cannot create split clone worker: %v", err)
	}
	return worker, nil
}

func interactiveLegacySplitClone(ctx context.Context, wi *Instance, wr *wrangler.Wrangler, w http.ResponseWriter, r *http.Request) (Worker, *template.Template, map[string]interface{}, error) {
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
		return nil, legacySplitCloneTemplate, result, nil
	}

	sourceReaderCountStr := r.FormValue("sourceReaderCount")
	if sourceReaderCountStr == "" {
		// display the input form
		result := make(map[string]interface{})
		result["Keyspace"] = keyspace
		result["Shard"] = shard
		result["DefaultSourceReaderCount"] = fmt.Sprintf("%v", defaultSourceReaderCount)
		result["DefaultDestinationPackCount"] = fmt.Sprintf("%v", defaultDestinationPackCount)
		result["DefaultDestinationWriterCount"] = fmt.Sprintf("%v", defaultDestinationWriterCount)
		result["DefaultMinHealthyRdonlyTablets"] = fmt.Sprintf("%v", defaultMinHealthyRdonlyTablets)
		result["DefaultMaxTPS"] = fmt.Sprintf("%v", defaultMaxTPS)
		return nil, legacySplitCloneTemplate2, result, nil
	}

	// get other parameters
	destinationPackCountStr := r.FormValue("destinationPackCount")
	excludeTables := r.FormValue("excludeTables")
	var excludeTableArray []string
	if excludeTables != "" {
		excludeTableArray = strings.Split(excludeTables, ",")
	}
	strategy := r.FormValue("strategy")
	sourceReaderCount, err := strconv.ParseInt(sourceReaderCountStr, 0, 64)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cannot parse sourceReaderCount: %s", err)
	}
	destinationPackCount, err := strconv.ParseInt(destinationPackCountStr, 0, 64)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cannot parse destinationPackCount: %s", err)
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

	// start the clone job
	wrk, err := NewLegacySplitCloneWorker(wr, wi.cell, keyspace, shard, excludeTableArray, strategy, int(sourceReaderCount), int(destinationPackCount), int(destinationWriterCount), int(minHealthyRdonlyTablets), maxTPS)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cannot create worker: %v", err)
	}
	return wrk, nil, nil, nil
}

func init() {
	AddCommand("Clones", Command{"LegacySplitClone",
		commandLegacySplitClone, interactiveLegacySplitClone,
		"[--exclude_tables=''] [--strategy=''] <keyspace/shard>",
		"Old SplitClone code which supports VARCHAR primary key columns. Use this ONLY if SplitClone failed for you."})
}
