// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"flag"
	"fmt"
	"strings"

	"github.com/youtube/vitess/go/vt/logutil"
)

// SplitStrategy is the configuration for a split clone.
type SplitStrategy struct {
	// PopulateBlpCheckpoint will drive the population of the blp_checkpoint table
	PopulateBlpCheckpoint bool

	// DontStartBinlogPlayer will delay starting the binlog replication
	DontStartBinlogPlayer bool

	// SkipSetSourceShards will not set the source shards at the end of restore
	SkipSetSourceShards bool
}

func NewSplitStrategy(logger logutil.Logger, argsStr string) (*SplitStrategy, error) {
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
	populateBlpCheckpoint := flagSet.Bool("populate_blp_checkpoint", false, "populates the blp checkpoint table")
	dontStartBinlogPlayer := flagSet.Bool("dont_start_binlog_player", false, "do not start the binlog player after restore is complete")
	skipSetSourceShards := flagSet.Bool("skip_set_source_shards", false, "do not set the SourceShar field on destination shards")
	if err := flagSet.Parse(args); err != nil {
		return nil, fmt.Errorf("cannot parse strategy: %v", err)
	}
	if flagSet.NArg() > 0 {
		return nil, fmt.Errorf("strategy doesn't have positional arguments")
	}

	return &SplitStrategy{
		PopulateBlpCheckpoint: *populateBlpCheckpoint,
		DontStartBinlogPlayer: *dontStartBinlogPlayer,
		SkipSetSourceShards:   *skipSetSourceShards,
	}, nil
}

func (strategy *SplitStrategy) String() string {
	var result []string
	if strategy.PopulateBlpCheckpoint {
		result = append(result, "-populate_blp_checkpoint")
	}
	if strategy.DontStartBinlogPlayer {
		result = append(result, "-dont_start_binlog_player")
	}
	if strategy.SkipSetSourceShards {
		result = append(result, "-skip_set_source_shards")
	}
	return strings.Join(result, " ")
}
