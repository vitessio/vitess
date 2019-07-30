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

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/topo"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
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
	// DiffEqual is returned when the rows left and right are equal.
	DiffEqual
)

// DiffTypes has the list of available DiffType values, ordered by their value.
var DiffTypes = []DiffType{DiffMissing, DiffNotEqual, DiffExtraneous, DiffEqual}

// DiffFoundTypes has the list of DiffType values which represent that a
// difference was found. The list is ordered by the values of the types.
var DiffFoundTypes = []DiffType{DiffMissing, DiffNotEqual, DiffExtraneous}

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
	// equalRowsStatsCounters tracks per table how many rows are equal.
	equalRowsStatsCounters *stats.CountersWithSingleLabel
	// tableName is required to update "equalRowsStatsCounters".
	tableName string
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
	insertChannels []chan string, abort <-chan struct{}, dbNames []string, writeQueryMaxRows, writeQueryMaxSize int, statsCounters []*stats.CountersWithSingleLabel) (*RowDiffer2, error) {

	if len(statsCounters) != len(DiffTypes) {
		panic(fmt.Sprintf("statsCounter has the wrong number of elements. got = %v, want = %v", len(statsCounters), len(DiffTypes)))
	}

	if err := compareFields(left.Fields(), right.Fields()); err != nil {
		return nil, err
	}

	// Create a RowAggregator for each destination shard and DiffType.
	aggregators := make([][]*RowAggregator, len(destinationShards))
	for i := range destinationShards {
		aggregators[i] = make([]*RowAggregator, len(DiffFoundTypes))
		for _, typ := range DiffFoundTypes {
			maxRows := writeQueryMaxRows
			aggregators[i][typ] = NewRowAggregator(ctx, maxRows, writeQueryMaxSize,
				insertChannels[i], dbNames[i], td, typ, statsCounters[typ])
		}
	}

	return &RowDiffer2{
		left:                   NewRowReader(left),
		right:                  NewRowReader(right),
		pkFieldCount:           len(td.PrimaryKeyColumns),
		tableStatusList:        tableStatusList,
		tableIndex:             tableIndex,
		router:                 NewRowRouter(destinationShards, keyResolver),
		aggregators:            aggregators,
		equalRowsStatsCounters: statsCounters[DiffEqual],
		tableName:              td.Name,
	}, nil
}

func compareFields(left, right []*querypb.Field) error {
	if len(left) != len(right) {
		return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "cannot diff inputs with different number of fields: left: %v right: %v", left, right)
	}
	for i, field := range left {
		if field.Type != right[i].Type {
			return vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "cannot diff inputs with different types: field %v types are %v and %v", i, field.Type, right[i].Type)
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
		if left == nil && right == nil {
			// No more rows from either side. We're done.
			break
		}
		dr.processedRows++
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
			rd.skipRow()
			continue
		}

		if f >= rd.pkFieldCount {
			// rows have the same primary key, only content is different
			dr.mismatchedRows++
			advanceLeft = true
			advanceRight = true
			// Update the row on the destination.
			if err := rd.updateRow(left, right, DiffNotEqual); err != nil {
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
		if err := rd.updateRow(left, right, DiffNotEqual); err != nil {
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

// skipRow is used for the DiffType DiffEqual.
// Currently, it only updates the statistics and therefore does not require the
// row as input.
func (rd *RowDiffer2) skipRow() {
	rd.equalRowsStatsCounters.Add(rd.tableName, 1)
	rd.tableStatusList.addCopiedRows(rd.tableIndex, 1)
}

// reconcileRow is used for the DiffType DiffMissing and DiffExtraneous.
func (rd *RowDiffer2) reconcileRow(row []sqltypes.Value, typ DiffType) error {
	if typ == DiffNotEqual {
		panic(fmt.Sprintf("reconcileRow() called with wrong type: %v", typ))
	}
	destShardIndex, err := rd.router.Route(row)
	if err != nil {
		return vterrors.Wrapf(err, "failed to route row (%v) to correct shard", row)
	}

	if err := rd.aggregators[destShardIndex][typ].Add(row); err != nil {
		return vterrors.Wrap(err, "failed to add row update to RowAggregator")
	}
	// TODO(mberlin): Add more fine granular stats here.
	rd.tableStatusList.addCopiedRows(rd.tableIndex, 1)
	return nil
}

// updateRow is used for the DiffType DiffNotEqual.
// It needs to look at the row of the source (newRow) and destination (oldRow)
// to detect if the keyspace_id has changed in the meantime.
// If that's the case, we cannot UPDATE the row. Instead, we must DELETE
// the old row and INSERT the new row to the respective destination shards.
func (rd *RowDiffer2) updateRow(newRow, oldRow []sqltypes.Value, typ DiffType) error {
	if typ != DiffNotEqual {
		panic(fmt.Sprintf("updateRow() called with wrong type: %v", typ))
	}
	destShardIndexOld, err := rd.router.Route(oldRow)
	if err != nil {
		return vterrors.Wrapf(err, "failed to route old row (%v) to correct shard", oldRow)
	}
	destShardIndexNew, err := rd.router.Route(newRow)
	if err != nil {
		return vterrors.Wrapf(err, "failed to route new row (%v) to correct shard", newRow)
	}

	if destShardIndexOld == destShardIndexNew {
		// keyspace_id has not changed. Update the row in place on the destination.
		if err := rd.aggregators[destShardIndexNew][typ].Add(newRow); err != nil {
			return vterrors.Wrap(err, "failed to add row update to RowAggregator (keyspace_id not changed)")
		}
	} else {
		// keyspace_id has changed. Delete the old row and insert the new one.
		if err := rd.aggregators[destShardIndexOld][DiffExtraneous].Add(oldRow); err != nil {
			return vterrors.Wrap(err, "failed to add row update to RowAggregator (keyspace_id changed, deleting old row)")
		}
		if err := rd.aggregators[destShardIndexNew][DiffMissing].Add(newRow); err != nil {
			return vterrors.Wrap(err, "failed to add row update to RowAggregator (keyspace_id changed, inserting new row)")
		}
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
	if len(rr.keyRanges) == 1 {
		// Fast path when there is only one destination shard.
		return 0, nil
	}

	k, err := rr.keyResolver.keyspaceID(row)
	if err != nil {
		return -1, err
	}
	for i, kr := range rr.keyRanges {
		if key.KeyRangeContains(kr, k) {
			return i, nil
		}
	}
	return -1, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "no shard's key range includes the keyspace id: %v for the row: %#v", k, row)
}
