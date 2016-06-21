// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/youtube/vitess/go/sqltypes"

	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
)

// RowAggregator aggregates SQL repair statements into one statement.
// Once a limit (maxRows or maxSize) is reached, the statement will be sent to
// the destination's insertChannel.
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
	maxRows int
	maxSize int
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

	buffer       bytes.Buffer
	bufferedRows int
}

// NewRowAggregator returns a RowAggregator.
func NewRowAggregator(maxRows, maxSize int, keyspaceAndShard string, insertChannel chan string, abort <-chan struct{}, dbName string, td *tabletmanagerdatapb.TableDefinition, repairType repairType) *RowAggregator {
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
		maxRows = 1
		// Note: We cannot use INSERT INTO ... ON DUPLICATE KEY UPDATE here because
		// it's not recommended for tables with more than one unique (i.e. the
		// primary key) index. That's because the update function would also be
		// triggered if a unique, non-primary key index matches the row. In that
		// case, we would update the wrong row (it gets selected by the unique key
		// and not the primary key).
	case delet:
		// Example: DELETE FROM test WHERE (id, sub_id) IN ((0, 10), (1, 11))
		baseCmdHead = "DELETE FROM `" + dbName + "`." + td.Name + " WHERE (" + strings.Join(td.PrimaryKeyColumns, ", ") + ") IN ("
		baseCmdTail = ")"
	default:
		panic(fmt.Sprintf("unknown repairType: %v", repairType))
	}

	// Build list of non-primary key columns (required for update statements).
	nonPrimaryKeyColumns := orderedColumnsWithoutPrimaryKeyColumns(td)

	return &RowAggregator{
		maxRows:              maxRows,
		maxSize:              maxSize,
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
		if ra.bufferedRows >= 1 {
			// Second row or higher. Separate it by a comma.
			ra.buffer.WriteByte(',')
		}
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
	case delet:
		// Example: (0, 10), (1, 11)
		if ra.bufferedRows >= 1 {
			// Second row or higher. Separate it by a comma.
			ra.buffer.WriteByte(',')
		}
		ra.buffer.WriteByte('(')
		for i := 0; i < len(ra.td.PrimaryKeyColumns); i++ {
			if i > 0 {
				ra.buffer.WriteByte(',')
			}
			row[i].EncodeSQL(&ra.buffer)
		}
		ra.buffer.WriteByte(')')
	}
	ra.bufferedRows++

	if ra.bufferedRows >= ra.maxRows || ra.buffer.Len() >= ra.maxSize {
		if abort := ra.flush(); abort {
			return true
		}
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
	if ra.buffer.Len() == 0 {
		// Already flushed. Not aborted.
		return false
	}

	ra.buffer.WriteString(ra.baseCmdTail)
	// select blocks until sending the SQL succeeded or the context was canceled.
	select {
	case ra.insertChannel <- ra.buffer.String():
	case <-ra.abort:
		return true
	}
	ra.buffer.Reset()
	ra.bufferedRows = 0
	return false
}
