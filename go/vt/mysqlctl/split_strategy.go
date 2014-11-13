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
	// DelayPrimaryKey will create the table without primary keys,
	// and then apply them later as an alter
	DelayPrimaryKey bool

	// DelaySecondaryIndexes will delay creating the secondary indexes
	DelaySecondaryIndexes bool

	// SkipAutoIncrement will remove the auto increment field
	// form the given tables
	SkipAutoIncrement []string

	// UseMyIsam will use MyIsam tables to restore, then convert to InnoDB
	UseMyIsam bool

	// DelayAutoIncrement will delay the addition of the auto-increment column
	DelayAutoIncrement bool

	// WriteBinLogs will enable writing to the bin logs
	WriteBinLogs bool

	// PopulateBlpCheckpoint will drive the population of the blp_checkpoint table
	PopulateBlpCheckpoint bool

	// DontStartBinlogPlayer will delay starting the binlog replication
	DontStartBinlogPlayer bool

	// SkipSetSourceShards will not set the source shards at the end of restore
	SkipSetSourceShards bool

	// WriteMastersOnly will write only to the master of the destination shard, with binlog enabled so that replicas can catch up
	WriteMastersOnly bool
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
	delayPrimaryKey := flagSet.Bool("delay_primary_key", false, "delays the application of the primary key until after the data population")
	delaySecondaryIndexes := flagSet.Bool("delay_secondary_indexes", false, "delays the application of secondary indexes until after the data population")
	skipAutoIncrementStr := flagSet.String("skip_auto_increment", "", "comma spearated list of tables on which not to re-introduce auto-increment")
	useMyIsam := flagSet.Bool("use_my_isam", false, "uses MyISAM table types for restores, then switches to InnoDB")
	delayAutoIncrement := flagSet.Bool("delay_auto_increment", false, "don't add auto_increment at table creation, but re-introduces them later")
	writeBinLogs := flagSet.Bool("write_bin_logs", false, "write write to the binlogs on the destination")
	populateBlpCheckpoint := flagSet.Bool("populate_blp_checkpoint", false, "populates the blp checkpoint table")
	dontStartBinlogPlayer := flagSet.Bool("dont_start_binlog_player", false, "do not start the binlog player after restore is complete")
	skipSetSourceShards := flagSet.Bool("skip_set_source_shards", false, "do not set the SourceShar field on destination shards")
	writeMastersOnly := flagSet.Bool("write_masters_only", false, "rite only to the master of the destination shard, with binlog enabled so that replicas can catch up")
	if err := flagSet.Parse(args); err != nil {
		return nil, fmt.Errorf("cannot parse strategy: %v", err)
	}
	if flagSet.NArg() > 0 {
		return nil, fmt.Errorf("strategy doesn't have positional arguments")
	}
	var skipAutoIncrement []string
	if *skipAutoIncrementStr != "" {
		skipAutoIncrement = strings.Split(*skipAutoIncrementStr, ",")
	}
	return &SplitStrategy{
		DelayPrimaryKey:       *delayPrimaryKey,
		DelaySecondaryIndexes: *delaySecondaryIndexes,
		SkipAutoIncrement:     skipAutoIncrement,
		UseMyIsam:             *useMyIsam,
		DelayAutoIncrement:    *delayAutoIncrement,
		WriteBinLogs:          *writeBinLogs,
		PopulateBlpCheckpoint: *populateBlpCheckpoint,
		DontStartBinlogPlayer: *dontStartBinlogPlayer,
		SkipSetSourceShards:   *skipSetSourceShards,
		WriteMastersOnly:      *writeMastersOnly,
	}, nil
}

func (strategy *SplitStrategy) SkipAutoIncrementOnTable(table string) bool {
	for _, t := range strategy.SkipAutoIncrement {
		if t == table {
			return true
		}
	}
	return false
}

func (strategy *SplitStrategy) String() string {
	var result []string
	if strategy.DelayPrimaryKey {
		result = append(result, "-delay_primary_key")
	}
	if strategy.DelaySecondaryIndexes {
		result = append(result, "-delay_secondary_indexes")
	}
	if len(strategy.SkipAutoIncrement) > 0 {
		result = append(result, "-skip_auto_increment="+strings.Join(strategy.SkipAutoIncrement, ","))
	}
	if strategy.UseMyIsam {
		result = append(result, "-use_my_isam")
	}
	if strategy.DelayAutoIncrement {
		result = append(result, "-delay_auto_increment")
	}
	if strategy.WriteBinLogs {
		result = append(result, "-write_bin_logs")
	}
	if strategy.PopulateBlpCheckpoint {
		result = append(result, "-populate_blp_checkpoint")
	}
	if strategy.DontStartBinlogPlayer {
		result = append(result, "-dont_start_binlog_player")
	}
	if strategy.SkipSetSourceShards {
		result = append(result, "-skip_set_source_shards")
	}
	if strategy.WriteMastersOnly {
		result = append(result, "-write_masters_only")
	}
	return strings.Join(result, " ")
}
