// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/logutil"
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
	delete
)

var repairTypes = []repairType{insert, update, delete}

// RowDiffer2 will compare and repair two sides. It assumes that the left side
// is the source of truth and necessary repairs have to be applied to the right
// side.
// It also assumes left and right are sorted by ascending primary key.
type RowDiffer2 struct {
	left         *RowReader
	right        *RowReader
	pkFieldCount int
	// tableStatusList is used to report the number of repaired rows.
	tableStatusList tableStatusList
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
func NewRowDiffer2(left, right ResultReader, td *tabletmanagerdatapb.TableDefinition, tableStatusList tableStatusList, tableIndex int,
	// Parameters required by RowRouter.
	destinationShards []*topo.ShardInfo, keyResolver keyspaceIDResolver,
	// Parameters required by RowAggregator.
	insertChannels []chan string, abort <-chan struct{}, dbNames []string, destinationPackCount int) (*RowDiffer2, error) {

	if err := compareFields(left.Fields(), right.Fields()); err != nil {
		return nil, err
	}

	// Create a RowAggregator for each destination shard and repair type.
	// Aggregate changes up to 4k * pack count bytes. 4k because we assume that as
	// the default size of a single response of a streaming read.
	// TODO(mberlin): Replace the -destination_pack_count with a flag which bounds
	// the size of the destination writes (in bytes).
	limit := 4096 * destinationPackCount
	aggregators := make([][]*RowAggregator, len(destinationShards))
	for i := range destinationShards {
		aggregators[i] = make([]*RowAggregator, len(repairTypes))
		for _, typ := range repairTypes {
			aggregators[i][typ] = NewRowAggregator(limit,
				topoproto.KeyspaceShardString(destinationShards[i].Keyspace(), destinationShards[i].ShardName()),
				insertChannels[i], abort, dbNames[i], td, typ)
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
		return fmt.Errorf("Cannot diff inputs with different types")
	}
	for i, field := range left {
		if field.Type != right[i].Type {
			return fmt.Errorf("Cannot diff inputs with different types: field %v types are %v and %v", i, field.Type, right[i].Type)
		}
	}
	return nil
}

// Go runs the diff and repair.
// If an error occurs, it will return and stop.
func (rd *RowDiffer2) Go(log logutil.Logger) (DiffReport, error) {
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
			if err := rd.repairRow(right, delete); err != nil {
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
			if dr.mismatchedRows < 10 {
				log.Errorf("Different content %v in same PK (only the first 10 errors of this type will be logged): %v != %v", dr.mismatchedRows, left, right)
			}
			dr.mismatchedRows++
			advanceLeft = true
			advanceRight = true
			// Update the row on the destination.
			if err := rd.repairRow(right, update); err != nil {
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
			if dr.extraRowsLeft < 10 {
				log.Errorf("Extra row %v on left (only the first 10 errors of this type will be logged): %v", dr.extraRowsLeft, left)
			}
			dr.extraRowsLeft++
			advanceLeft = true
			// Add the row on the destination.
			if err := rd.repairRow(left, insert); err != nil {
				return dr, err
			}
			continue
		} else if c > 0 {
			if dr.extraRowsRight < 10 {
				log.Errorf("Extra row %v on right (only the first 10 errors of this type will be logged): %v", dr.extraRowsRight, right)
			}
			dr.extraRowsRight++
			advanceRight = true
			// Delete the row from the destination.
			if err := rd.repairRow(right, delete); err != nil {
				return dr, err
			}
			continue
		}

		// After looking at primary keys more carefully,
		// they're the same. Logging a regular difference
		// then, and advancing both.
		// TODO(mberlin): Does this case happen in practice?
		if dr.mismatchedRows < 10 {
			log.Errorf("Different content %v in same PK (only the first 10 errors of this type will be logged): %v != %v", dr.mismatchedRows, left, right)
		}
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

// RowAggregator aggregates SQL repair statements into one statement.
// Once the limit (in bytes) is reached, the statement will be sent to the
// destination's insertChannel.
// RowAggregator is also aware of the type of repair statement and constructs
// the necessary SQL command based on that.
// Aggregating multiple statements is done to improve the overall performance.
// One RowAggregator instance is specific to one destination shard and repair
// type.
// Important: The SQL statement generation assumes that the fields of the
// provided row are in the same order as orderedColumns(td) would return on the
// provided "td" (TableDefinition). That means primary key fields come first and
// then all other fields. The order of these two sets of fields is identical
// to the order in td.PrimaryKeyColumns and td.Columns respectively.
type RowAggregator struct {
	limit int
	// keyspaceAndShard is only used for error messages.
	keyspaceAndShard string
	insertChannel    chan string
	abort            <-chan struct{}
	td               *tabletmanagerdatapb.TableDefinition
	// nonPrimaryKeyColumns is td.Columns filtered by td.PrimaryKeyColumns.
	// The order of td.Columns is preserved.
	nonPrimaryKeyColumns []string
	repairType           repairType
	baseCmdHead          string
	baseCmdTail          string

	buffer bytes.Buffer
}

// NewRowAggregator returns a RowAggregator.
func NewRowAggregator(limit int, keyspaceAndShard string, insertChannel chan string, abort <-chan struct{}, dbName string, td *tabletmanagerdatapb.TableDefinition, repairType repairType) *RowAggregator {
	// Construct head and tail base commands for the repair statement.
	var baseCmdHead, baseCmdTail string
	switch repairType {
	case insert:
		// Example: INSERT INTO test (id, sub_id, msg) VALUES (0, 10, 'a'), (1, 11, 'b')
		baseCmdHead = "INSERT INTO `" + dbName + "`." + td.Name + " (" + strings.Join(td.Columns, ", ") + ") VALUES "
	case update:
		// Example: UPDATE test SET msg='a' WHERE id=0 AND sub_id=10
		baseCmdHead = "UPDATE `" + dbName + "`." + td.Name + " SET "
		// UPDATE ... SET does not support multiple rows as input.
		limit = 1
		// Note: We cannot use INSERT INTO ... ON DUPLICATE KEY UPDATE here because
		// it's not recommended for tables with more than one unique (i.e. the
		// primary key) index. That's because the update function would also be
		// triggered if a unique, non-primary key index matches the row. In that
		// case, we would update the wrong row (it gets selected by the unique key
		// and not the primary key).
	case delete:
		// Example: DELETE FROM test WHERE (id, sub_id) IN ((0, 10), (1, 11))
		baseCmdHead = "DELETE FROM `" + dbName + "`." + td.Name + " WHERE (" + strings.Join(td.PrimaryKeyColumns, ", ") + ") IN ("
		baseCmdTail = ")"
	default:
		panic(fmt.Sprintf("unknown repairType: %v", repairType))
	}

	// Build list of non-primary key columns (required for update statements).
	nonPrimaryKeyColumns := orderedColumnsWithoutPrimaryKeyColumns(td)

	return &RowAggregator{
		limit:                limit,
		keyspaceAndShard:     keyspaceAndShard,
		insertChannel:        insertChannel,
		abort:                abort,
		td:                   td,
		nonPrimaryKeyColumns: nonPrimaryKeyColumns,
		repairType:           repairType,
		baseCmdHead:          baseCmdHead,
		baseCmdTail:          baseCmdTail,
	}
}

// Add will add a new row which must be repaired.
// It returns true if the pipeline should be aborted. If that happens,
// RowAggregator will be in an undefined state and must not be used any longer.
func (ra *RowAggregator) Add(row []sqltypes.Value) bool {
	if ra.buffer.Len() == 0 {
		ra.buffer.WriteString(ra.baseCmdHead)
	}

	switch ra.repairType {
	case insert:
		// Example: (0, 10, 'a'), (1, 11, 'b')
		ra.buffer.WriteByte('(')
		for i, value := range row {
			if i > 0 {
				ra.buffer.WriteByte(',')
			}
			value.EncodeSQL(&ra.buffer)
		}
		ra.buffer.WriteByte(')')
	case update:
		// Example: msg='a' WHERE id=0 AND sub_id=10
		nonPrimaryOffset := len(ra.td.PrimaryKeyColumns)
		for i, column := range ra.nonPrimaryKeyColumns {
			if i > 0 {
				ra.buffer.WriteByte(',')
			}
			ra.buffer.WriteString(column)
			ra.buffer.WriteByte('=')
			row[nonPrimaryOffset+i].EncodeSQL(&ra.buffer)
		}
		ra.buffer.WriteString(" WHERE ")
		for i, pkColumn := range ra.td.PrimaryKeyColumns {
			if i > 0 {
				ra.buffer.WriteString(" AND ")
			}
			ra.buffer.WriteString(pkColumn)
			ra.buffer.WriteByte('=')
			row[i].EncodeSQL(&ra.buffer)
		}
	case delete:
		// Example: (0, 10), (1, 11)
		ra.buffer.WriteByte('(')
		for i := 0; i < len(ra.td.PrimaryKeyColumns); i++ {
			if i > 0 {
				ra.buffer.WriteByte(',')
			}
			row[i].EncodeSQL(&ra.buffer)
		}
		ra.buffer.WriteByte(')')
	}

	if ra.buffer.Len() >= ra.limit {
		if abort := ra.flush(); abort {
			return true
		}
	} else {
		// There will be another value. Separate it by a comma.
		ra.buffer.WriteByte(',')
	}

	return false
}

// Close fluses any pending aggregates which haven't been sent out yet.
// It returns true if the pipeline was aborted.
func (ra *RowAggregator) Close() bool {
	return ra.flush()
}

// KeyspaceAndShard returns the keyspace/shard string the aggregator is used for.
func (ra *RowAggregator) KeyspaceAndShard() string {
	return ra.keyspaceAndShard
}

// flush sends out the current aggregation buffer. It returns true when the
// pipeline was aborted.
func (ra *RowAggregator) flush() bool {
	ra.buffer.WriteString(ra.baseCmdTail)
	// select blocks until sending the SQL succeeded or the context was canceled.
	select {
	case ra.insertChannel <- ra.buffer.String():
	case <-ra.abort:
		return true
	}
	ra.buffer.Reset()
	return false
}
