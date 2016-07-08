// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// RestartableResultReader will stream all rows within a chunk.
// If the streaming query gets interrupted, it can resume the stream after
// the last row which was read.
type RestartableResultReader struct {
	ctx         context.Context
	logger      logutil.Logger
	tabletAlias *topodatapb.TabletAlias
	// td is used to get the list of primary key columns at a restart.
	td    *tabletmanagerdatapb.TableDefinition
	chunk chunk

	query string

	conn    tabletconn.TabletConn
	fields  []*querypb.Field
	output  sqltypes.ResultStream
	lastRow []sqltypes.Value
}

// NewRestartableResultReader creates a new RestartableResultReader for
// the provided tablet and chunk.
// It will automatically create the necessary query to read all rows within
// the chunk.
// NOTE: We assume that the Columns field in "td" was ordered by a preceding
// call to reorderColumnsPrimaryKeyFirst().
func NewRestartableResultReader(ctx context.Context, logger logutil.Logger, ts topo.Server, tabletAlias *topodatapb.TabletAlias, td *tabletmanagerdatapb.TableDefinition, chunk chunk) (*RestartableResultReader, error) {
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	tablet, err := ts.GetTablet(shortCtx, tabletAlias)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("tablet=%v table=%v chunk=%v: Failed to resolve tablet alias: %v", topoproto.TabletAliasString(tabletAlias), td.Name, chunk, err)
	}

	conn, err := tabletconn.GetDialer()(ctx, tablet.Tablet, *remoteActionsTimeout)
	if err != nil {
		return nil, fmt.Errorf("tablet=%v table=%v chunk=%v: Failed to get dialer for tablet: %v", topoproto.TabletAliasString(tabletAlias), td.Name, chunk, err)
	}

	r := &RestartableResultReader{
		ctx:         ctx,
		logger:      logger,
		tabletAlias: tabletAlias,
		td:          td,
		chunk:       chunk,
		conn:        conn,
	}

	if err := r.startStream(); err != nil {
		return nil, err
	}
	logger.Infof("tablet=%v table=%v chunk=%v: Starting to stream rows using query '%v'.", topoproto.TabletAliasString(tabletAlias), td.Name, chunk, r.query)
	return r, nil
}

// Next returns the next result on the stream. It implements ResultReader.
func (r *RestartableResultReader) Next() (*sqltypes.Result, error) {
	result, err := r.output.Recv()
	if err != nil && err != io.EOF {
		r.logger.Infof("tablet=%v table=%v chunk=%v: Failed to read next rows from active streaming query. Trying to restart stream. Original Error: %v", topoproto.TabletAliasString(r.tabletAlias), r.td.Name, r.chunk, err)
		// Restart streaming query.
		// Note that we intentionally don't reset "r.conn" here. This restart
		// mechanism is only meant to fix transient problems which go away at the
		// next retry. For example, when MySQL killed the vttablet connection due
		// to net_write_timeout being reached.
		if err := r.startStream(); err != nil {
			return nil, err
		}
		result, err = r.output.Recv()
		if err == nil || err == io.EOF {
			r.logger.Infof("tablet=%v table=%v chunk=%v: Successfully restarted streaming query with query '%v'.", topoproto.TabletAliasString(r.tabletAlias), r.td.Name, r.chunk, r.query)
		} else {
			// We won't retry a second time and fail for good.
			// TODO(mberlin): When we have a chunk pipeline restart mechanism,
			// mention that restart mechanism in the log here.
			r.logger.Infof("tablet=%v table=%v chunk=%v: Failed to restart streaming query with query '%v'. Error: %v", topoproto.TabletAliasString(r.tabletAlias), r.td.Name, r.chunk, r.query, err)
		}
	}
	if result != nil && len(result.Rows) > 0 {
		r.lastRow = result.Rows[len(result.Rows)-1]
	}
	return result, err
}

// Fields returns the field data. It implements ResultReader.
func (r *RestartableResultReader) Fields() []*querypb.Field {
	return r.fields
}

// Close closes the connection to the tablet.
func (r *RestartableResultReader) Close() {
	r.conn.Close()
}

func (r *RestartableResultReader) startStream() error {
	r.generateQuery()
	stream, err := r.conn.StreamExecute(r.ctx, r.query, make(map[string]interface{}))
	if err != nil {
		return fmt.Errorf("tablet=%v table=%v chunk=%v: failed to call StreamExecute() for query '%v': %v", topoproto.TabletAliasString(r.tabletAlias), r.td.Name, r.chunk, r.query, err)
	}

	// Read the fields information. Fail and do not restart if there is an error.
	cols, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("tablet=%v table=%v chunk=%v: cannot read Fields for query '%v': %v", topoproto.TabletAliasString(r.tabletAlias), r.td.Name, r.chunk, r.query, err)
	}

	r.fields = cols.Fields
	r.output = stream
	return nil
}

func (r *RestartableResultReader) generateQuery() {
	query := "SELECT " + strings.Join(r.td.Columns, ",") + " FROM " + r.td.Name

	// Build WHERE clauses.
	var clauses []string

	// start value.
	if r.lastRow == nil {
		// Initial query.
		if !r.chunk.start.IsNull() {
			var b bytes.Buffer
			b.WriteString(r.td.PrimaryKeyColumns[0])
			b.WriteString(">=")
			r.chunk.start.EncodeSQL(&b)
			clauses = append(clauses, b.String())
		}
	} else {
		// This is a restart. Read after the last row.
		// Note that we don't have to be concerned that the new start might be > end
		// because lastRow < end is always true. That's because the initial query
		// had the clause 'WHERE PrimaryKeyColumns[0] < end'.
		// TODO(mberlin): Write an e2e test to verify that restarts also work with
		// string types and MySQL collation rules.
		clauses = append(clauses, greaterThanTupleWhereClause(r.td.PrimaryKeyColumns, r.lastRow)...)
	}

	// end value.
	if !r.chunk.end.IsNull() {
		var b bytes.Buffer
		b.WriteString(r.td.PrimaryKeyColumns[0])
		b.WriteString("<")
		r.chunk.end.EncodeSQL(&b)
		clauses = append(clauses, b.String())
	}

	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	if len(r.td.PrimaryKeyColumns) > 0 {
		query += " ORDER BY " + strings.Join(r.td.PrimaryKeyColumns, ",")
	}
	r.query = query
}

// greaterThanTupleWhereClause builds a greater than (">") WHERE clause
// expression for the first "columns" in "row".
// The caller has to ensure that "columns" matches with the values in "row".
// Examples:
// one column:    a > 1
// two columns:   a>=1 AND (a,b) > (1,2)
//   (Input for that would be: columns{"a", "b"}, row{1, 2}.)
// three columns: a>=1 AND (a,b,c) > (1,2,3)
//
// Note that we are using the short-form for row comparisons. This is defined
// by MySQL. See: http://dev.mysql.com/doc/refman/5.5/en/comparison-operators.html
// <quote>
//   For row comparisons, (a, b) < (x, y) is equivalent to:
//   (a < x) OR ((a = x) AND (b < y))
// </quote>
//
// NOTE: If there is more than one column, we add an extra clause for the
// first column because older MySQL versions seem to do a full table scan
// when we use the short-form. With the additional clause we skip the full
// table scan up the primary key we're interested it.
func greaterThanTupleWhereClause(columns []string, row []sqltypes.Value) []string {
	var clauses []string

	// Additional clause on the first column for multi-columns.
	if len(columns) > 1 {
		var b bytes.Buffer
		b.WriteString(columns[0])
		b.WriteString(">=")
		row[0].EncodeSQL(&b)
		clauses = append(clauses, b.String())
	}

	var b bytes.Buffer
	// List of columns.
	if len(columns) > 1 {
		b.WriteByte('(')
	}
	b.WriteString(strings.Join(columns, ","))
	if len(columns) > 1 {
		b.WriteByte(')')
	}

	// Operator.
	b.WriteString(">")

	// List of values.
	if len(columns) > 1 {
		b.WriteByte('(')
	}
	for i := 0; i < len(columns); i++ {
		if i != 0 {
			b.WriteByte(',')
		}
		row[i].EncodeSQL(&b)
	}
	if len(columns) > 1 {
		b.WriteByte(')')
	}
	clauses = append(clauses, b.String())

	return clauses
}
