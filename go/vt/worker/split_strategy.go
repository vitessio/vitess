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
	"strings"

	"github.com/youtube/vitess/go/vt/logutil"
)

// splitStrategy is the configuration for a split clone.
type splitStrategy struct {
	// skipPopulateBlpCheckpoint will skip the population of the blp_checkpoint table
	skipPopulateBlpCheckpoint bool

	// dontStartBinlogPlayer will delay starting the binlog replication
	dontStartBinlogPlayer bool

	// skipSetSourceShards will not set the source shards at the end of restore
	skipSetSourceShards bool
}

func newSplitStrategy(logger logutil.Logger, argsStr string) (*splitStrategy, error) {
	var args []string
	if argsStr != "" {
		args = strings.Split(argsStr, " ")
	}
	flagSet := flag.NewFlagSet("strategy", flag.ContinueOnError)
	flagSet.SetOutput(logutil.NewLoggerWriter(logger))
	flagSet.Usage = func() {
		logger.Printf("Strategy flag has the following options:\n")
		flagSet.PrintDefaults()
	}
	skipPopulateBlpCheckpoint := flagSet.Bool("skip_populate_blp_checkpoint", false, "do not populate the blp checkpoint table")
	dontStartBinlogPlayer := flagSet.Bool("dont_start_binlog_player", false, "do not start the binlog player after restore is complete")
	skipSetSourceShards := flagSet.Bool("skip_set_source_shards", false, "do not set the SourceShar field on destination shards")
	if err := flagSet.Parse(args); err != nil {
		return nil, fmt.Errorf("cannot parse strategy: %v", err)
	}
	if flagSet.NArg() > 0 {
		return nil, fmt.Errorf("strategy doesn't have positional arguments")
	}

	return &splitStrategy{
		skipPopulateBlpCheckpoint: *skipPopulateBlpCheckpoint,
		dontStartBinlogPlayer:     *dontStartBinlogPlayer,
		skipSetSourceShards:       *skipSetSourceShards,
	}, nil
}

func (strategy *splitStrategy) String() string {
	var result []string
	if strategy.skipPopulateBlpCheckpoint {
		result = append(result, "-skip_populate_blp_checkpoint")
	}
	if strategy.dontStartBinlogPlayer {
		result = append(result, "-dont_start_binlog_player")
	}
	if strategy.skipSetSourceShards {
		result = append(result, "-skip_set_source_shards")
	}
	return strings.Join(result, " ")
}
