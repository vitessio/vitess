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
	"fmt"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/topo"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

// Backfiller will iterate through source data, generate a new row to be inserted
// into destination shards and route it to the correct insert channel.
type Backfiller struct {
	reader       *RowReader
	pkFieldCount int
	status       *backfillStatus
	// router returns for a row to which destination shard index it should go.
	router RowRouter
	// aggregators are keyed by destination shard and DiffType.
	aggregators []*RowAggregator
	// tableName is required to update "equalRowsStatsCounters".
	tableName    string
	transform    func([]sqltypes.Value) ([]sqltypes.Value, bool, error)
	skipNullRows bool
}

// NewBackfiller returns a new RowDiffer2.
// We assume that the indexes of the slice parameters always correspond to the
// same shard e.g. insertChannels[0] refers to destinationShards[0] and so on.
// The column list td.Columns must be have all primary key columns first and
// then the non-primary-key columns. The columns in the rows returned by
// the ResultReader must have the same order as td.Columns.
func NewBackfiller(ctx context.Context, input ResultReader, destinationTableDefinition *tabletmanagerdatapb.TableDefinition, backfillStatus *backfillStatus, skipNullRows bool,
	transform func([]sqltypes.Value) ([]sqltypes.Value, bool, error),
	// Parameters required by RowRouter.
	destinationShards []*topo.ShardInfo, keyResolver keyspaceIDResolver,
	// Parameters required by RowAggregator.
	insertChannels []chan string, dbNames []string, writeQueryMaxRows, writeQueryMaxSize int, statsCounters *stats.CountersWithSingleLabel) (*Backfiller, error) {

	// Create a RowAggregator for each destination shard and DiffType.
	aggregators := make([]*RowAggregator, len(destinationShards))
	for i := range destinationShards {
		maxRows := writeQueryMaxRows
		aggregators[i] = NewInsertIgnoringRowAggregator(ctx, maxRows, writeQueryMaxSize,
			insertChannels[i], dbNames[i], destinationTableDefinition, statsCounters)
	}

	return &Backfiller{
		reader:       NewRowReader(input),
		pkFieldCount: len(destinationTableDefinition.PrimaryKeyColumns),
		status:       backfillStatus,
		router:       NewRowRouter(destinationShards, keyResolver),
		aggregators:  aggregators,
		tableName:    destinationTableDefinition.Name,
		transform:    transform,
		skipNullRows: skipNullRows,
	}, nil
}

// Backfill runs the backfill
// If an error occurs, it will return and stop.
func (b *Backfiller) Backfill() (DiffReport, error) {
	var dr DiffReport

	dr.startingTime = time.Now()
	defer dr.ComputeQPS()

	for {
		row, err := b.reader.Next()
		if err != nil {
			return dr, err
		}
		if row == nil {
			break
		}

		transform, isNull, err := b.transform(row)
		if err != nil {
			return dr, err
		}
		if b.skipNullRows && isNull {
			dr.processedRows++
			continue
		}
		if err := b.addRow(transform); err != nil {
			return dr, err
		}
		dr.processedRows++
	}

	// Flush all aggregators in case they have buffered queries left.
	for _, aggregator := range b.aggregators {
		if err := aggregator.Flush(); err != nil {
			return dr, err
		}
	}

	return dr, nil
}

func (b *Backfiller) addRow(row []sqltypes.Value) error {
	destShardIndex, err := b.router.Route(row)
	if err != nil {
		return fmt.Errorf("failed to route row (%v) to correct shard: %v", row, err)
	}

	if err := b.aggregators[destShardIndex].Add(row); err != nil {
		return fmt.Errorf("failed to add row update to RowAggregator: %v", err)
	}
	// TODO(mberlin): Add more fine granular stats here.
	b.status.addCopiedRows(1)
	return nil
}
