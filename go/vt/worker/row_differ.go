// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"fmt"
	"time"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// repairType specifies how a different or missing row must be repaired.
type repairType int

const (
	insert repairType = iota
	update
	// delet is misspelled because "delete" is a reserved word.
	delet
)

var repairTypes = []repairType{insert, update, delet}

// RowDiffer2 will compare and repair two sides. It assumes that the left side
// is the source of truth and necessary repairs have to be applied to the right
// side.
// It also assumes left and right are sorted by ascending primary key.
type RowDiffer2 struct {
	left         *RowReader
	right        *RowReader
	pkFieldCount int
	// tableStatusList is used to report the number of repaired rows.
	tableStatusList *tableStatusList
	// tableIndex is the index of the table in the schema. It is required for
	// reporting the number of repaired rows to tableStatusList.
	tableIndex int
	// router returns for a row to which destination shard index it should go.
	router RowRouter
	// aggregators are keyed by destination shard and repair type.
	aggregators [][]*RowAggregator
}

// NewRowDiffer2 returns a new RowDiffer2.
// We assume that the indexes of the slice parameters always correspond to the
// same shard e.g. insertChannels[0] refers to destinationShards[0] and so on.
func NewRowDiffer2(left, right ResultReader, td *tabletmanagerdatapb.TableDefinition, tableStatusList *tableStatusList, tableIndex int,
	// Parameters required by RowRouter.
	destinationShards []*topo.ShardInfo, keyResolver keyspaceIDResolver,
	// Parameters required by RowAggregator.
	insertChannels []chan string, abort <-chan struct{}, dbNames []string, writeQueryMaxRows, writeQueryMaxSize int, statCounters []*stats.Counters) (*RowDiffer2, error) {

	if err := compareFields(left.Fields(), right.Fields()); err != nil {
		return nil, err
	}

	// Create a RowAggregator for each destination shard and repair type.
	aggregators := make([][]*RowAggregator, len(destinationShards))
	for i := range destinationShards {
		aggregators[i] = make([]*RowAggregator, len(repairTypes))
		for _, typ := range repairTypes {
			aggregators[i][typ] = NewRowAggregator(writeQueryMaxRows, writeQueryMaxSize,
				topoproto.KeyspaceShardString(destinationShards[i].Keyspace(), destinationShards[i].ShardName()),
				insertChannels[i], abort, dbNames[i], td, typ, statCounters)
		}
	}

	return &RowDiffer2{
		left:            NewRowReader(left),
		right:           NewRowReader(right),
		pkFieldCount:    len(td.PrimaryKeyColumns),
		tableStatusList: tableStatusList,
		tableIndex:      tableIndex,
		router:          NewRowRouter(destinationShards, keyResolver),
		aggregators:     aggregators,
	}, nil
}

func compareFields(left, right []*querypb.Field) error {
	if len(left) != len(right) {
		return fmt.Errorf("Cannot diff inputs with different number of fields: left: %v right: %v", left, right)
	}
	for i, field := range left {
		if field.Type != right[i].Type {
			return fmt.Errorf("Cannot diff inputs with different types: field %v types are %v and %v", i, field.Type, right[i].Type)
		}
	}
	return nil
}

// Diff runs the diff and repair.
// If an error occurs, it will return and stop.
func (rd *RowDiffer2) Diff() (DiffReport, error) {
	var dr DiffReport
	var err error

	dr.startingTime = time.Now()
	defer dr.ComputeQPS()

	fields := rd.left.Fields()
	var left []sqltypes.Value
	var right []sqltypes.Value
	advanceLeft := true
	advanceRight := true
	for {
		if advanceLeft {
			if left, err = rd.left.Next(); err != nil {
				return dr, err
			}
			advanceLeft = false
		}
		if advanceRight {
			if right, err = rd.right.Next(); err != nil {
				return dr, err
			}
			advanceRight = false
		}
		dr.processedRows++
		if left == nil && right == nil {
			// No more rows from either side. We're done.
			break
		}
		if left == nil {
			// No more rows on the left side.
			// We know we have at least one row on the right side left.
			// Delete the row from the destination.
			if err := rd.repairRow(right, delet); err != nil {
				return dr, err
			}
			dr.extraRowsRight++
			advanceRight = true
			continue
		}
		if right == nil {
			// No more rows on the right side.
			// We know we have at least one row on the left side left.
			// Add the row on the destination.
			if err := rd.repairRow(left, insert); err != nil {
				return dr, err
			}
			dr.extraRowsLeft++
			advanceLeft = true
			continue
		}

		// we have both left and right, compare
		f := RowsEqual(left, right)
		if f == -1 {
			// rows are the same, next
			dr.matchingRows++
			advanceLeft = true
			advanceRight = true
			continue
		}

		if f >= rd.pkFieldCount {
			// rows have the same primary key, only content is different
			dr.mismatchedRows++
			advanceLeft = true
			advanceRight = true
			// Update the row on the destination.
			if err := rd.repairRow(left, update); err != nil {
				return dr, err
			}
			continue
		}

		// have to find the 'smallest' row and advance it
		c, err := CompareRows(fields, rd.pkFieldCount, left, right)
		if err != nil {
			return dr, err
		}
		if c < 0 {
			dr.extraRowsLeft++
			advanceLeft = true
			// Add the row on the destination.
			if err := rd.repairRow(left, insert); err != nil {
				return dr, err
			}
			continue
		} else if c > 0 {
			dr.extraRowsRight++
			advanceRight = true
			// Delete the row from the destination.
			if err := rd.repairRow(right, delet); err != nil {
				return dr, err
			}
			continue
		}

		// Values of the primary key columns were not binary equal but their parsed
		// values are.
		// This happens when the raw values returned by MySQL were different but
		// they became equal after we parsed them into ints/floats
		// (due to leading/trailing zeros, for example). So this can happen if MySQL
		// is inconsistent in how it prints a given number.
		dr.mismatchedRows++
		advanceLeft = true
		advanceRight = true
		// Update the row on the destination.
		if err := rd.repairRow(right, update); err != nil {
			return dr, err
		}
	}

	// Close and flush all aggregators in case they have data buffered.
	for i := range rd.aggregators {
		for _, aggregator := range rd.aggregators[i] {
			if abort := aggregator.Close(); abort {
				return dr, fmt.Errorf("failed to flush RowAggregator for destination shard: %v", aggregator.KeyspaceAndShard())
			}
		}
	}

	return dr, nil
}

func (rd *RowDiffer2) repairRow(row []sqltypes.Value, typ repairType) error {
	destShardIndex, err := rd.router.Route(row)
	if err != nil {
		return err
	}

	if abort := rd.aggregators[destShardIndex][typ].Add(row); abort {
		return fmt.Errorf("failed to repair row for destination shard: %v because the context was canceled", rd.aggregators[destShardIndex][typ].KeyspaceAndShard())
	}
	// TODO(mberlin): Add more fine granular stats here.
	rd.tableStatusList.addCopiedRows(rd.tableIndex, 1)
	return nil
}

// RowRouter allows to find out which shard's key range contains a given
// keyspace ID.
type RowRouter struct {
	keyResolver keyspaceIDResolver
	keyRanges   []*topodatapb.KeyRange
}

// NewRowRouter creates a RowRouter.
// We assume that the key ranges in shardInfo cover the full keyrange i.e.
// for any possible keyspaceID there is a shard we can route to.
func NewRowRouter(shardInfos []*topo.ShardInfo, keyResolver keyspaceIDResolver) RowRouter {
	keyRanges := make([]*topodatapb.KeyRange, len(shardInfos))
	for i, si := range shardInfos {
		keyRanges[i] = si.KeyRange
	}
	return RowRouter{keyResolver, keyRanges}
}

// Route returns which shard (specified by the index of the list of shards
// passed in NewRowRouter) contains the given row.
func (rr *RowRouter) Route(row []sqltypes.Value) (int, error) {
	k, err := rr.keyResolver.keyspaceID(row)
	if err != nil {
		return -1, err
	}
	for i, kr := range rr.keyRanges {
		if key.KeyRangeContains(kr, k) {
			return i, nil
		}
	}
	return -1, fmt.Errorf("no shard's key range includes the keyspace id: %v for the row: %#v", k, row)
}
