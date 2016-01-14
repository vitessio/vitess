// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
