// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"bytes"
	"fmt"
	"strings"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"

	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
)

// RowAggregator aggregates SQL reconciliation statements into one statement.
// Once a limit (maxRows or maxSize) is reached, the statement will be sent to
// the destination's insertChannel.
// RowAggregator is also aware of the type of statement (DiffType) and
// constructs the necessary SQL command based on that.
// Aggregating multiple statements is done to improve the overall performance.
// One RowAggregator instance is specific to one destination shard and diff
// type.
// Important: The SQL statement generation assumes that the fields of the
// provided row are in the same order as "td.Columns".
type RowAggregator struct {
	ctx           context.Context
	maxRows       int
	maxSize       int
	insertChannel chan string
	td            *tabletmanagerdatapb.TableDefinition
	diffType      DiffType
	builder       QueryBuilder
	// statsCounters has a "diffType" specific stats.Counters object to track how
	// many rows were changed per table.
	statsCounters *stats.Counters

	buffer       bytes.Buffer
	bufferedRows int
}

// NewRowAggregator returns a RowAggregator.
// The index of the elements in statCounters must match the elements
// in "DiffTypes" i.e. the first counter is for inserts, second for updates
// and the third for deletes.
func NewRowAggregator(ctx context.Context, maxRows, maxSize int, insertChannel chan string, dbName string, td *tabletmanagerdatapb.TableDefinition, diffType DiffType, statsCounters *stats.Counters) *RowAggregator {
	// Construct head and tail base commands for the reconciliation statement.
	var builder QueryBuilder
	switch diffType {
	case DiffMissing:
		// Example: INSERT INTO test (id, sub_id, msg) VALUES (0, 10, 'a'), (1, 11, 'b')
		builder = NewInsertsQueryBuilder(dbName, td)
	case DiffNotEqual:
		// Example: UPDATE test SET msg='a' WHERE id=0 AND sub_id=10
		builder = NewUpdatesQueryBuilder(dbName, td)
		// UPDATE ... SET does not support multiple rows as input.
		maxRows = 1
	case DiffExtraneous:
		// Example: DELETE FROM test WHERE (id, sub_id) IN ((0, 10), (1, 11))
		builder = NewDeletesQueryBuilder(dbName, td)
	default:
		panic(fmt.Sprintf("unknown DiffType: %v", diffType))
	}

	return &RowAggregator{
		ctx:           ctx,
		maxRows:       maxRows,
		maxSize:       maxSize,
		insertChannel: insertChannel,
		td:            td,
		diffType:      diffType,
		builder:       builder,
		statsCounters: statsCounters,
	}
}

// Add will add a new row which must be reconciled.
// If an error is returned, RowAggregator will be in an undefined state and must
// not be used any longer.
func (ra *RowAggregator) Add(row []sqltypes.Value) error {
	if ra.buffer.Len() == 0 {
		ra.builder.WriteHead(&ra.buffer)
	}

	if ra.bufferedRows >= 1 {
		ra.builder.WriteSeparator(&ra.buffer)
	}
	ra.builder.WriteRow(&ra.buffer, row)
	ra.bufferedRows++

	if ra.bufferedRows >= ra.maxRows || ra.buffer.Len() >= ra.maxSize {
		if err := ra.Flush(); err != nil {
			return err
		}
	}

	return nil
}

// Flush sends out the current aggregation buffer.
func (ra *RowAggregator) Flush() error {
	if ra.buffer.Len() == 0 {
		// Already flushed.
		return nil
	}

	ra.builder.WriteTail(&ra.buffer)
	// select blocks until sending the SQL succeeded or the context was canceled.
	select {
	case ra.insertChannel <- ra.buffer.String():
	case <-ra.ctx.Done():
		return fmt.Errorf("failed to flush RowAggregator and send the query to a writer thread channel: %v", ra.ctx.Err())
	}

	// Update our statistics.
	ra.statsCounters.Add(ra.td.Name, int64(ra.bufferedRows))

	ra.buffer.Reset()
	ra.bufferedRows = 0
	return nil
}

// QueryBuilder defines for a given reconciliation type how we have to
// build the SQL query for one or more rows.
type QueryBuilder interface {
	// WriteHead writes the beginning of the query into the buffer.
	WriteHead(*bytes.Buffer)
	// WriteTail writes any required tailing string into the buffer.
	WriteTail(*bytes.Buffer)
	// Write the separator between two rows.
	WriteSeparator(*bytes.Buffer)
	// Write the row itself.
	WriteRow(*bytes.Buffer, []sqltypes.Value)
}

// BaseQueryBuilder partially implements the QueryBuilder interface.
// It can be used by other QueryBuilder implementations to avoid repeating
// code.
type BaseQueryBuilder struct {
	head      string
	tail      string
	separator string
}

// WriteHead implements the QueryBuilder interface.
func (b *BaseQueryBuilder) WriteHead(buffer *bytes.Buffer) {
	buffer.WriteString(b.head)
}

// WriteTail implements the QueryBuilder interface.
func (b *BaseQueryBuilder) WriteTail(buffer *bytes.Buffer) {
	buffer.WriteString(b.tail)
}

// WriteSeparator implements the QueryBuilder interface.
func (b *BaseQueryBuilder) WriteSeparator(buffer *bytes.Buffer) {
	if b.separator == "" {
		panic("BaseQueryBuilder.WriteSeparator(): separator not defined")
	}
	buffer.WriteString(b.separator)
}

// InsertsQueryBuilder implements the QueryBuilder interface for INSERT queries.
type InsertsQueryBuilder struct {
	BaseQueryBuilder
}

// NewInsertsQueryBuilder creates a new InsertsQueryBuilder.
func NewInsertsQueryBuilder(dbName string, td *tabletmanagerdatapb.TableDefinition) *InsertsQueryBuilder {
	// Example: INSERT INTO test (id, sub_id, msg) VALUES (0, 10, 'a'), (1, 11, 'b')
	return &InsertsQueryBuilder{
		BaseQueryBuilder{
			head:      "INSERT INTO " + escape(dbName) + "." + escape(td.Name) + " (" + strings.Join(escapeAll(td.Columns), ", ") + ") VALUES ",
			separator: ",",
		},
	}
}

// WriteRow implements the QueryBuilder interface.
func (*InsertsQueryBuilder) WriteRow(buffer *bytes.Buffer, row []sqltypes.Value) {
	// Example: (0, 10, 'a'), (1, 11, 'b')
	buffer.WriteByte('(')
	for i, value := range row {
		if i > 0 {
			buffer.WriteByte(',')
		}
		value.EncodeSQL(buffer)
	}
	buffer.WriteByte(')')
}

// UpdatesQueryBuilder implements the QueryBuilder interface for UPDATE queries.
type UpdatesQueryBuilder struct {
	BaseQueryBuilder
	td *tabletmanagerdatapb.TableDefinition
	// nonPrimaryKeyColumns is td.Columns filtered by td.PrimaryKeyColumns.
	// The order of td.Columns is preserved.
	nonPrimaryKeyColumns []string
}

// NewUpdatesQueryBuilder creates a new UpdatesQueryBuilder.
func NewUpdatesQueryBuilder(dbName string, td *tabletmanagerdatapb.TableDefinition) *UpdatesQueryBuilder {
	// Example: UPDATE test SET msg='a' WHERE id=0 AND sub_id=10
	//
	// Note: We cannot use INSERT INTO ... ON DUPLICATE KEY UPDATE here because
	// it's not recommended for tables with more than one unique (i.e. the
	// primary key) index. That's because the update function would also be
	// triggered if a unique, non-primary key index matches the row. In that
	// case, we would update the wrong row (it gets selected by the unique key
	// and not the primary key).
	return &UpdatesQueryBuilder{
		BaseQueryBuilder: BaseQueryBuilder{
			head: "UPDATE " + escape(dbName) + "." + escape(td.Name) + " SET ",
		},
		td: td,
		// Build list of non-primary key columns (required for update statements).
		nonPrimaryKeyColumns: orderedColumnsWithoutPrimaryKeyColumns(td),
	}
}

// WriteSeparator implements the QueryBuilder interface and overrides
// the BaseQueryBuilder implementation.
func (b *UpdatesQueryBuilder) WriteSeparator(buffer *bytes.Buffer) {
	panic("UpdatesQueryBuilder does not support aggregating multiple rows in one query")
}

// WriteRow implements the QueryBuilder interface.
func (b *UpdatesQueryBuilder) WriteRow(buffer *bytes.Buffer, row []sqltypes.Value) {
	// Example: msg='a' WHERE id=0 AND sub_id=10
	nonPrimaryOffset := len(b.td.PrimaryKeyColumns)
	for i, column := range b.nonPrimaryKeyColumns {
		if i > 0 {
			buffer.WriteByte(',')
		}
		writeEscaped(buffer, column)
		buffer.WriteByte('=')
		row[nonPrimaryOffset+i].EncodeSQL(buffer)
	}
	buffer.WriteString(" WHERE ")
	for i, pkColumn := range b.td.PrimaryKeyColumns {
		if i > 0 {
			buffer.WriteString(" AND ")
		}
		writeEscaped(buffer, pkColumn)
		buffer.WriteByte('=')
		row[i].EncodeSQL(buffer)
	}
}

// DeletesQueryBuilder implements the QueryBuilder interface for DELETE queries.
type DeletesQueryBuilder struct {
	BaseQueryBuilder
	td *tabletmanagerdatapb.TableDefinition
}

// NewDeletesQueryBuilder creates a new DeletesQueryBuilder.
func NewDeletesQueryBuilder(dbName string, td *tabletmanagerdatapb.TableDefinition) *DeletesQueryBuilder {
	// Example: DELETE FROM test WHERE (id=0 AND sub_id=10) OR (id=1 AND sub_id=11)
	//
	// Note that we don't do multi row DELETEs with an IN expression because
	// there are reports in the wild that MySQL 5.6 would do a full table scan
	// for such a query. (We haven't confirmed this ourselves.)
	return &DeletesQueryBuilder{
		BaseQueryBuilder: BaseQueryBuilder{
			head:      "DELETE FROM " + escape(dbName) + "." + escape(td.Name) + " WHERE ",
			separator: " OR ",
		},
		td: td,
	}
}

// WriteRow implements the QueryBuilder interface.
func (b *DeletesQueryBuilder) WriteRow(buffer *bytes.Buffer, row []sqltypes.Value) {
	// Example: (id=0 AND sub_id=10) OR (id=1 AND sub_id=11)
	buffer.WriteByte('(')
	for i, pkColumn := range b.td.PrimaryKeyColumns {
		if i > 0 {
			buffer.WriteString(" AND ")
		}
		writeEscaped(buffer, pkColumn)
		buffer.WriteByte('=')
		row[i].EncodeSQL(buffer)
	}
	buffer.WriteByte(')')
}
