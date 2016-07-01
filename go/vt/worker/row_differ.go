// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"fmt"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// DiffType specifies why a specific row was found as different when comparing
// a left and right side.
type DiffType int

const (
	// DiffMissing is returned when the row is missing on the right side.
	DiffMissing DiffType = iota
	// DiffNotEqual is returned when the row on the left and right side are
	// not equal.
	DiffNotEqual
	// DiffExtraneous is returned when the row exists on the right side, but not
	// on the left side.
	DiffExtraneous
)

// DiffTypes has the list of available DiffType values, ordered by their value.
var DiffTypes = []DiffType{DiffMissing, DiffNotEqual, DiffExtraneous}

// RowDiffer2 will compare and reconcile two sides. It assumes that the left
// side is the source of truth and necessary reconciliations have to be applied
// to the right side.
// It also assumes left and right are sorted by ascending primary key.
type RowDiffer2 struct {
	left         *RowReader
	right        *RowReader
	pkFieldCount int
	// tableStatusList is used to report the number of reconciled rows.
	tableStatusList *tableStatusList
	// tableIndex is the index of the table in the schema. It is required for
	// reporting the number of reconciled rows to tableStatusList.
	tableIndex int
	// router returns for a row to which destination shard index it should go.
	router RowRouter
	// aggregators are keyed by destination shard and DiffType.
	aggregators [][]*RowAggregator
}

// NewRowDiffer2 returns a new RowDiffer2.
// We assume that the indexes of the slice parameters always correspond to the
// same shard e.g. insertChannels[0] refers to destinationShards[0] and so on.
// The column list td.Columns must be have all primary key columns first and
// then the non-primary-key columns. The columns in the rows returned by
// both ResultReader must have the same order as td.Columns.
func NewRowDiffer2(ctx context.Context, left, right ResultReader, td *tabletmanagerdatapb.TableDefinition, tableStatusList *tableStatusList, tableIndex int,
	// Parameters required by RowRouter.
	destinationShards []*topo.ShardInfo, keyResolver keyspaceIDResolver,
	// Parameters required by RowAggregator.
	insertChannels []chan string, abort <-chan struct{}, dbNames []string, writeQueryMaxRows, writeQueryMaxSize, writeQueryMaxRowsDelete int, statCounters []*stats.Counters) (*RowDiffer2, error) {

	if err := compareFields(left.Fields(), right.Fields()); err != nil {
		return nil, err
	}

	// Create a RowAggregator for each destination shard and DiffType.
	aggregators := make([][]*RowAggregator, len(destinationShards))
	for i := range destinationShards {
		aggregators[i] = make([]*RowAggregator, len(DiffTypes))
		for _, typ := range DiffTypes {
			maxRows := writeQueryMaxRows
			if typ == DiffExtraneous {
				maxRows = writeQueryMaxRowsDelete
			}
			aggregators[i][typ] = NewRowAggregator(ctx, maxRows, writeQueryMaxSize,
				insertChannels[i], dbNames[i], td, typ, statCounters)
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

// Diff runs the diff and reconcile.
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
			if err := rd.reconcileRow(right, DiffExtraneous); err != nil {
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
			if err := rd.reconcileRow(left, DiffMissing); err != nil {
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
			if err := rd.reconcileRow(left, DiffNotEqual); err != nil {
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
			if err := rd.reconcileRow(left, DiffMissing); err != nil {
				return dr, err
			}
			continue
		} else if c > 0 {
			dr.extraRowsRight++
			advanceRight = true
			// Delete the row from the destination.
			if err := rd.reconcileRow(right, DiffExtraneous); err != nil {
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
		if err := rd.reconcileRow(right, DiffNotEqual); err != nil {
			return dr, err
		}
	}

	// Flush all aggregators in case they have buffered queries left.
	for i := range rd.aggregators {
		for _, aggregator := range rd.aggregators[i] {
			if err := aggregator.Flush(); err != nil {
				return dr, err
			}
		}
	}

	return dr, nil
}

func (rd *RowDiffer2) reconcileRow(row []sqltypes.Value, typ DiffType) error {
	destShardIndex, err := rd.router.Route(row)
	if err != nil {
		return fmt.Errorf("failed to route row (%v) to correct shard: %v", row, err)
	}

	if err := rd.aggregators[destShardIndex][typ].Add(row); err != nil {
		return fmt.Errorf("failed to add row update to RowAggregator: %v", err)
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
