/*
Copyright 2019 The Vitess Authors.

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

package vstreamer

import (
	"context"
	"fmt"
	"time"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// RowStreamer exposes an externally usable interface to rowStreamer.
type RowStreamer interface {
	Stream() error
	Cancel()
}

var (
	copyLimit = 10000
)

// NewRowStreamer returns a RowStreamer
func NewRowStreamer(ctx context.Context, cp dbconfigs.Connector, se *schema.Engine, query string, lastpk []sqltypes.Value, send func(*binlogdatapb.VStreamRowsResponse) error, vse *Engine) RowStreamer {
	return newRowStreamer(ctx, cp, se, query, lastpk, &localVSchema{vschema: &vindexes.VSchema{}}, send, vse)
}

// rowStreamer is used for copying the existing rows of a table
// before vreplication begins streaming binlogs. The rowStreamer
// responds to a request with the GTID position as of which it
// streams the rows of a table. This allows vreplication to synchronize
// its events as of the returned GTID before adding the new rows.
// For every set of rows sent, the last pk value is also sent.
// This allows for the streaming to be resumed based on the last
// pk value processed.
type rowStreamer struct {
	ctx    context.Context
	cancel func()

	cp      dbconfigs.Connector
	se      *schema.Engine
	query   string
	lastpk  []sqltypes.Value
	maxpk   []sqltypes.Value
	send    func(*binlogdatapb.VStreamRowsResponse) error
	vschema *localVSchema

	// strictSnapshot indicates whether we run a strict (table lock, begin transaction with consistent snapshot) single query,
	// or whether we stream rows by running multiple SELECTs of limited size.
	// The strict snapshot is the original logiv for rowstreamer, and is required for:
	// - Materialize queries which use GROUP BY
	// - Online DDL which adds a UNIQUE KEY
	// See https://github.com/vitessio/vitess/issues/8056
	strictSnapshot bool

	plan      *Plan
	pkColumns []int
	sendQuery string
	vse       *Engine
	pktsize   PacketSizer
}

func newRowStreamer(ctx context.Context, cp dbconfigs.Connector, se *schema.Engine, query string, lastpk []sqltypes.Value, vschema *localVSchema, send func(*binlogdatapb.VStreamRowsResponse) error, vse *Engine) *rowStreamer {
	ctx, cancel := context.WithCancel(ctx)
	return &rowStreamer{
		ctx:     ctx,
		cancel:  cancel,
		cp:      cp,
		se:      se,
		query:   query,
		lastpk:  lastpk,
		send:    send,
		vschema: vschema,
		vse:     vse,
		pktsize: DefaultPacketSizer(),
	}
}

func (rs *rowStreamer) Cancel() {
	log.Info("Rowstreamer Cancel() called")
	rs.cancel()
}

func (rs *rowStreamer) Stream() error {
	// Ensure sh is Open. If vttablet came up in a non_serving role,
	// the schema engine may not have been initialized.
	if err := rs.se.Open(); err != nil {
		return err
	}
	if err := rs.buildPlan(); err != nil {
		return err
	}
	conn, err := snapshotConnect(rs.ctx, rs.cp)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecuteFetch("set names binary", 1, false); err != nil {
		return err
	}
	return rs.streamQuery(conn, rs.send)
}

func (rs *rowStreamer) buildPlan() error {
	// This pre-parsing is required to extract the table name
	// and create its metadata.
	sel, fromTable, err := analyzeSelect(rs.query)
	if err != nil {
		return err
	}
	st, err := rs.se.GetTableForPos(fromTable, "")
	if err != nil {
		return err
	}
	ti := &Table{
		Name:   st.Name,
		Fields: st.Fields,
	}
	// The plan we build is identical to the one for vstreamer.
	// This is because the row format of a read is identical
	// to the row format of a binlog event. So, the same
	// filtering will work.
	rs.plan, err = buildTablePlan(ti, rs.vschema, rs.query)
	if err != nil {
		log.Errorf("%s", err.Error())
		return err
	}
	rs.pkColumns, err = buildPKColumns(st)
	if err != nil {
		return err
	}

	directives := sqlparser.ExtractCommentDirectives(sel.Comments)
	rs.strictSnapshot = directives.IsTrue("strictSnapshot")
	return nil
}

func buildPKColumns(st *binlogdatapb.MinimalTable) ([]int, error) {
	var pkColumns = make([]int, 0)
	if len(st.PKColumns) == 0 {
		pkColumns = make([]int, len(st.Fields))
		for i := range st.Fields {
			pkColumns[i] = i
		}
		return pkColumns, nil
	}
	for _, pk := range st.PKColumns {
		if pk >= int64(len(st.Fields)) {
			return nil, fmt.Errorf("primary key %d refers to non-existent column", pk)
		}
		pkColumns = append(pkColumns, int(pk))
	}
	return pkColumns, nil
}

// buildSelect creates the SELECT query which this streamer streams down to the target for copying table rows.
// depending on rs.strictSnapshot, we either:
// - generate a single query to SELECT all table rows (and so this function is only called once), or
// - generate multiple queries to SELECT "copyLimit" rows at a time, thus this function gets called repetitively until rows are exhausted
func (rs *rowStreamer) buildSelect(startWithPk []sqltypes.Value) (string, error) {
	buf := sqlparser.NewTrackedBuffer(nil)
	// We could have used select *, but being explicit is more predictable.
	buf.Myprintf("select ")
	prefix := ""
	for _, col := range rs.plan.Table.Fields {
		if rs.plan.isConvertColumnUsingUTF8(col.Name) {
			buf.Myprintf("%sconvert(%v using utf8mb4)", prefix, sqlparser.NewColIdent(col.Name))
		} else {
			buf.Myprintf("%s%v", prefix, sqlparser.NewColIdent(col.Name))
		}
		prefix = ", "
	}
	wherePrinted := false
	buf.Myprintf(" from %v", sqlparser.NewTableIdent(rs.plan.Table.Name))
	if len(startWithPk) != 0 {
		if len(startWithPk) != len(rs.pkColumns) {
			return "", fmt.Errorf("primary key values don't match length: %v vs %v", startWithPk, rs.pkColumns)
		}
		buf.WriteString(" where ")
		buf.Myprintf("(")
		wherePrinted = true
		prefix := ""
		// This loop handles the case for composite pks. For example,
		// if lastpk was (1,2), the where clause would be:
		// (col1 = 1 and col2 > 2) or (col1 > 1).
		// A tuple inequality like (col1,col2) > (1,2) ends up
		// being a full table scan for mysql.
		for lastcol := len(rs.pkColumns) - 1; lastcol >= 0; lastcol-- {
			buf.Myprintf("%s(", prefix)
			prefix = " or "
			for i, pk := range rs.pkColumns[:lastcol] {
				buf.Myprintf("%v = ", sqlparser.NewColIdent(rs.plan.Table.Fields[pk].Name))
				startWithPk[i].EncodeSQL(buf)
				buf.Myprintf(" and ")
			}
			buf.Myprintf("%v > ", sqlparser.NewColIdent(rs.plan.Table.Fields[rs.pkColumns[lastcol]].Name))
			startWithPk[lastcol].EncodeSQL(buf)
			buf.Myprintf(")")
		}
		buf.Myprintf(")")
	}
	if !rs.strictSnapshot {
		// a strict snapshot reads all rows without limit.
		// but in a non-strict snapshot we must limit our query to a predefined upper bound: the max PRIMARY KEY
		// evaluated _once_ in streamQuery() (see comments in that function)
		if rs.maxpk != nil && len(rs.maxpk) != 0 {
			if len(rs.maxpk) != len(rs.pkColumns) {
				return "", fmt.Errorf("primary key values don't match length: %v vs %v", rs.maxpk, rs.pkColumns)
			}
			if wherePrinted {
				buf.WriteString(" and ")
			} else {
				buf.WriteString(" where ")
			}
			buf.Myprintf("(")
			{
				buf.Myprintf("(")
				for i, pk := range rs.pkColumns {
					if i > 0 {
						buf.Myprintf(" and ")
					}
					buf.Myprintf("%v = ", sqlparser.NewColIdent(rs.plan.Table.Fields[pk].Name))
					rs.maxpk[i].EncodeSQL(buf)
				}
				buf.Myprintf(") or ")
			}
			prefix := ""
			// This loop handles the case for composite pks. For example,
			// if lastpk was (1,2), the where clause would be:
			// (col1 = 1 and col2 = 2) or (col1 = 1 and col2 < 2) or (col1 < 1)
			// A tuple inequality like (col1,col2) < (1,2) ends up
			// being a full table scan for mysql.
			for lastcol := len(rs.pkColumns) - 1; lastcol >= 0; lastcol-- {
				buf.Myprintf("%s(", prefix)
				prefix = " or "
				for i, pk := range rs.pkColumns[:lastcol] {
					buf.Myprintf("%v = ", sqlparser.NewColIdent(rs.plan.Table.Fields[pk].Name))
					rs.maxpk[i].EncodeSQL(buf)
					buf.Myprintf(" and ")
				}
				buf.Myprintf("%v < ", sqlparser.NewColIdent(rs.plan.Table.Fields[rs.pkColumns[lastcol]].Name))
				rs.maxpk[lastcol].EncodeSQL(buf)
				buf.Myprintf(")")
			}
			buf.Myprintf(")")
		}
	}
	buf.Myprintf(" order by ", sqlparser.NewTableIdent(rs.plan.Table.Name))
	prefix = ""
	for _, pk := range rs.pkColumns {
		buf.Myprintf("%s%v", prefix, sqlparser.NewColIdent(rs.plan.Table.Fields[pk].Name))
		prefix = ", "
	}
	if copyLimit > 0 && !rs.strictSnapshot {
		// a strict snapshot reads all rows without limit
		buf.WriteString(fmt.Sprintf(" limit %d", copyLimit))
	}
	return buf.String(), nil
}

// buildSelectMaxPK builds a SELECT query that returns the max PRIMARY KEY value (very last tuple of PK columns)
func (rs *rowStreamer) buildSelectMaxPK() (string, error) {
	buf := sqlparser.NewTrackedBuffer(nil)
	// We could have used select *, but being explicit is more predictable.
	buf.Myprintf("select ")
	prefix := ""
	for _, pk := range rs.pkColumns {
		buf.Myprintf("%s%v", prefix, sqlparser.NewColIdent(rs.plan.Table.Fields[pk].Name))
		prefix = ", "
	}
	buf.Myprintf(" from %v", sqlparser.NewTableIdent(rs.plan.Table.Name))
	buf.Myprintf(" order by ", sqlparser.NewTableIdent(rs.plan.Table.Name))
	prefix = ""
	for _, pk := range rs.pkColumns {
		buf.Myprintf("%s%v desc", prefix, sqlparser.NewColIdent(rs.plan.Table.Fields[pk].Name))
		prefix = ", "
	}
	buf.WriteString(" limit 1")
	return buf.String(), nil
}

// executeStreamQuery runs the given query using a snapshotConn; it chooses to either:
// - use a strict snapshot (lock table, begin transaction with consistent snapshot; gtid strictly represents the time query is executed)
// - run the query without a snapshot; returned gtid is <= time of query
func (rs *rowStreamer) executeStreamQuery(ctx context.Context, conn *snapshotConn, table, query string) (gtid string, err error) {
	if rs.strictSnapshot {
		return conn.streamWithSnapshot(rs.ctx, rs.plan.Table.Name, rs.sendQuery)
	}
	return conn.streamWithoutSnapshot(rs.ctx, rs.plan.Table.Name, rs.sendQuery)
}

func (rs *rowStreamer) streamQuery(conn *snapshotConn, send func(*binlogdatapb.VStreamRowsResponse) error) (err error) {
	log.Infof("Streaming query: %v\n", rs.sendQuery)
	if !rs.strictSnapshot {
		// We evaluate the current max primary key, just once. In a strict snapshot this is not necessary, because we issue
		// a single SELECT that reads all rows. But in non-strict snpashot, where we run multiple iterations of limited SELECTs,
		// we really need to set an uppser limit up front, or else, as the table grows, we may chase its tail for eternity.
		maxPKQuery, err := rs.buildSelectMaxPK()
		if err != nil {
			return err
		}
		maxPKRS, err := conn.ExecuteFetch(maxPKQuery, 1, true)
		if err != nil {
			return err
		}
		if len(maxPKRS.Rows) == 1 {
			mysqlrow := maxPKRS.Rows[0]

			rs.maxpk = make([]sqltypes.Value, len(rs.pkColumns))
			for i := range rs.pkColumns {
				rs.maxpk[i] = mysqlrow[i]
			}
		}
	}
	rs.sendQuery, err = rs.buildSelect(rs.lastpk)
	if err != nil {
		return err
	}
	gtid, err := rs.executeStreamQuery(rs.ctx, conn, rs.plan.Table.Name, rs.sendQuery)
	if err != nil {
		return err
	}

	// first call the callback with the fields
	flds, err := conn.Fields()
	if err != nil {
		return err
	}
	pkfields := make([]*querypb.Field, len(rs.pkColumns))
	for i, pk := range rs.pkColumns {
		pkfields[i] = &querypb.Field{
			Name: flds[pk].Name,
			Type: flds[pk].Type,
		}
	}

	err = send(&binlogdatapb.VStreamRowsResponse{
		Fields:   rs.plan.fields(),
		Pkfields: pkfields,
		Gtid:     gtid,
	})
	if err != nil {
		return fmt.Errorf("stream send error: %v", err)
	}

	var response binlogdatapb.VStreamRowsResponse
	var rows []*querypb.Row
	var rowCount int
	var mysqlrow []sqltypes.Value

	filtered := make([]sqltypes.Value, len(rs.plan.ColExprs))
	lastpk := make([]sqltypes.Value, len(rs.pkColumns))
	byteCount := 0
	for {
		//log.Infof("StreamResponse for loop iteration starts")
		if rs.ctx.Err() != nil {
			log.Infof("Stream ended because of ctx.Done")
			return fmt.Errorf("stream ended: %v", rs.ctx.Err())
		}

		// check throttler.
		if !rs.vse.throttlerClient.ThrottleCheckOKOrWait(rs.ctx) {
			continue
		}

		if mysqlrow != nil {
			mysqlrow = mysqlrow[:0]
		}
		mysqlrow, err = conn.FetchNext(mysqlrow)
		if err != nil {
			return err
		}
		if mysqlrow == nil {
			if rs.strictSnapshot {
				// with a strict snapshot we SELECT all table rows, just once. So if we've exhausted the rows, our job is done, we've streamed all rows.
				break
			}
			// if !rs.strictSnapshot, then we've only read/SELECTed a chunk of rows, and we want to move on to reading the next chunk
			// attempt next round
			nextQuery, err := rs.buildSelect(lastpk)
			if err != nil {
				return err
			}
			if nextQuery == rs.sendQuery {
				// loop termination condition:
				// No change in query; we're at same lastPK; that means we've exhausted all rows
				// We can argue that this is an ugly way to detect end of loop (query text comparison). Should we perhaps, instead, compare the value of lastPK?
				// I (shlomi) feel like `rs.buildSelect(lastpk)` is an existing mechanism that just works.
				break
			}
			rs.sendQuery = nextQuery
			// Read next chunk of rows
			_, err = rs.executeStreamQuery(rs.ctx, conn, rs.plan.Table.Name, rs.sendQuery)
			if err != nil {
				return err
			}
			mysqlrow = []sqltypes.Value{}
			continue
		}
		// Compute lastpk here, because we'll need it at the end after the loop exits.
		for i, pk := range rs.pkColumns {
			lastpk[i] = mysqlrow[pk]
		}
		// Reuse the vstreamer's filter.
		ok, err := rs.plan.filter(mysqlrow, filtered)
		if err != nil {
			return err
		}
		if ok {
			if rowCount >= len(rows) {
				rows = append(rows, &querypb.Row{})
			}
			byteCount += sqltypes.RowToProto3Inplace(filtered, rows[rowCount])
			rowCount++
		}

		if rs.pktsize.ShouldSend(byteCount) {
			response.Rows = rows[:rowCount]
			response.Lastpk = sqltypes.RowToProto3(lastpk)

			rs.vse.rowStreamerNumRows.Add(int64(len(response.Rows)))
			rs.vse.rowStreamerNumPackets.Add(int64(1))

			startSend := time.Now()
			err = send(&response)
			if err != nil {
				log.Infof("Rowstreamer send returned error %v", err)
				return err
			}
			rs.pktsize.Record(byteCount, time.Since(startSend))
			rowCount = 0
			byteCount = 0
		}
	}

	if rowCount > 0 {
		response.Rows = rows[:rowCount]
		response.Lastpk = sqltypes.RowToProto3(lastpk)

		rs.vse.rowStreamerNumRows.Add(int64(len(response.Rows)))
		err = send(&response)
		if err != nil {
			return err
		}
	}

	return nil
}
