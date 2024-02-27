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
	"net/url"
	"sync"
	"time"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

var (
	rowStreamertHeartbeatInterval = 10 * time.Second
)

type RowStreamerMode int32

const (
	RowStreamerModeSingleTable RowStreamerMode = iota
	RowStreamerModeAllTables
)

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
	send    func(*binlogdatapb.VStreamRowsResponse) error
	vschema *localVSchema

	plan          *Plan
	pkColumns     []int
	ukColumnNames []string
	sendQuery     string
	vse           *Engine
	pktsize       PacketSizer

	mode RowStreamerMode
	conn *snapshotConn
}

func newRowStreamer(ctx context.Context, cp dbconfigs.Connector, se *schema.Engine, query string,
	lastpk []sqltypes.Value, vschema *localVSchema, send func(*binlogdatapb.VStreamRowsResponse) error, vse *Engine,
	mode RowStreamerMode, conn *snapshotConn) *rowStreamer {

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
		mode:    mode,
		conn:    conn,
	}
}

func (rs *rowStreamer) Cancel() {
	log.Info("Rowstreamer Cancel() called")
	rs.cancel()
}

func (rs *rowStreamer) Stream() error {
	// Ensure se is Open. If vttablet came up in a non_serving role,
	// the schema engine may not have been initialized.
	if err := rs.se.Open(); err != nil {
		return err
	}
	if err := rs.buildPlan(); err != nil {
		return err
	}
	if rs.conn == nil {
		conn, err := snapshotConnect(rs.ctx, rs.cp)
		if err != nil {
			return err
		}
		rs.conn = conn
		defer rs.conn.Close()
		if _, err := rs.conn.ExecuteFetch("set names 'binary'", 1, false); err != nil {
			return err
		}
		if _, err := conn.ExecuteFetch(fmt.Sprintf("set @@session.net_read_timeout = %v", vttablet.VReplicationNetReadTimeout), 1, false); err != nil {
			return err
		}
		if _, err := conn.ExecuteFetch(fmt.Sprintf("set @@session.net_write_timeout = %v", vttablet.VReplicationNetWriteTimeout), 1, false); err != nil {
			return err
		}
	}
	return rs.streamQuery(rs.send)
}

func (rs *rowStreamer) buildPlan() error {
	// This pre-parsing is required to extract the table name
	// and create its metadata.
	sel, fromTable, err := analyzeSelect(rs.query, rs.se.Environment().Parser())
	if err != nil {
		return err
	}

	st, err := rs.se.GetTableForPos(fromTable, "")
	if err != nil {
		// There is a scenario where vstreamer's table state can be out-of-date, and this happens
		// with vitess migrations, based on vreplication.
		// Vitess migrations use an elaborate cut-over flow where tables are swapped away while traffic is
		// being blocked. The RENAME flow is such that at some point the table is renamed away, leaving a
		// "puncture"; this is an event that is captured by vstreamer. The completion of the flow fixes the
		// puncture, and places a new table under the original table's name, but the way it is done does not
		// cause vstreamer to refresh schema state.
		// There is therefore a reproducible valid sequence of events where vstreamer thinks a table does not
		// exist, where it in fact does exist.
		// For this reason we give vstreamer a "second chance" to review the up-to-date state of the schema.
		// In the future, we will reduce this operation to reading a single table rather than the entire schema.
		rs.se.ReloadAt(context.Background(), replication.Position{})
		st, err = rs.se.GetTableForPos(fromTable, "")
	}
	if err != nil {
		return err
	}
	ti := &Table{
		Name: st.Name,
	}

	ti.Fields, err = getFields(rs.ctx, rs.cp, rs.vse.se, st.Name, rs.cp.DBName(), st.Fields)
	if err != nil {
		return err
	}

	// The plan we build is identical to the one for vstreamer.
	// This is because the row format of a read is identical
	// to the row format of a binlog event. So, the same
	// filtering will work.
	rs.plan, err = buildTablePlan(rs.se.Environment(), ti, rs.vschema, rs.query)
	if err != nil {
		log.Errorf("%s", err.Error())
		return err
	}

	directives := sel.Comments.Directives()
	if s, found := directives.GetString("ukColumns", ""); found {
		rs.ukColumnNames, err = textutil.SplitUnescape(s, ",")
		if err != nil {
			return err
		}
	}
	if s, found := directives.GetString("ukForce", ""); found {
		st.PKIndexName, err = url.QueryUnescape(s)
		if err != nil {
			return err
		}
	}
	rs.pkColumns, err = rs.buildPKColumns(st)
	if err != nil {
		return err
	}
	rs.sendQuery, err = rs.buildSelect(st)
	if err != nil {
		return err
	}
	return err
}

// buildPKColumnsFromUniqueKey assumes a unique key is indicated,
func (rs *rowStreamer) buildPKColumnsFromUniqueKey() ([]int, error) {
	var pkColumns = make([]int, 0)
	// We wish to utilize a UNIQUE KEY which is not the PRIMARY KEY/

	for _, colName := range rs.ukColumnNames {
		index := rs.plan.Table.FindColumn(sqlparser.NewIdentifierCI(colName))
		if index < 0 {
			return pkColumns, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "column %v is listed as unique key, but not present in table %v", colName, rs.plan.Table.Name)
		}
		pkColumns = append(pkColumns, index)
	}
	return pkColumns, nil
}

func (rs *rowStreamer) buildPKColumns(st *binlogdatapb.MinimalTable) ([]int, error) {
	if len(rs.ukColumnNames) > 0 {
		return rs.buildPKColumnsFromUniqueKey()
	}
	var pkColumns = make([]int, 0)
	if len(st.PKColumns) == 0 {
		// Use a PK equivalent if one exists.
		pkColumns, err := rs.vse.mapPKEquivalentCols(rs.ctx, rs.cp, st)
		if err == nil && len(pkColumns) != 0 {
			return pkColumns, nil
		}

		// Fall back to using every column in the table if there's no PK or PKE.
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
	st.PKIndexName = "PRIMARY"
	return pkColumns, nil
}

func (rs *rowStreamer) buildSelect(st *binlogdatapb.MinimalTable) (string, error) {
	buf := sqlparser.NewTrackedBuffer(nil)
	// We could have used select *, but being explicit is more predictable.
	buf.Myprintf("select %s", GetVReplicationMaxExecutionTimeQueryHint())
	prefix := ""
	for _, col := range rs.plan.Table.Fields {
		if rs.plan.isConvertColumnUsingUTF8(col.Name) {
			buf.Myprintf("%sconvert(%v using utf8mb4) as %v", prefix, sqlparser.NewIdentifierCI(col.Name), sqlparser.NewIdentifierCI(col.Name))
		} else if funcExpr := rs.plan.getColumnFuncExpr(col.Name); funcExpr != nil {
			buf.Myprintf("%s%s as %v", prefix, sqlparser.String(funcExpr), sqlparser.NewIdentifierCI(col.Name))
		} else {
			buf.Myprintf("%s%v", prefix, sqlparser.NewIdentifierCI(col.Name))
		}
		prefix = ", "
	}
	// If we know the index name that we should be using then tell MySQL
	// to use it if possible. This helps to ensure that we are able to
	// leverage the ordering from the index itself and avoid having to
	// do a FILESORT of all the results. This index should contain all
	// of the PK columns which are used in the ORDER BY clause below.
	var indexHint string
	if st.PKIndexName != "" {
		escapedPKIndexName, err := sqlescape.EnsureEscaped(st.PKIndexName)
		if err != nil {
			return "", err
		}
		indexHint = fmt.Sprintf(" force index (%s)", escapedPKIndexName)
	}
	buf.Myprintf(" from %v%s", sqlparser.NewIdentifierCS(rs.plan.Table.Name), indexHint)
	if len(rs.lastpk) != 0 {
		if len(rs.lastpk) != len(rs.pkColumns) {
			return "", fmt.Errorf("primary key values don't match length: %v vs %v", rs.lastpk, rs.pkColumns)
		}
		buf.WriteString(" where ")
		prefix := ""
		// This loop handles the case for composite PKs. For example,
		// if lastpk was (1,2), the where clause would be:
		// (col1 = 1 and col2 > 2) or (col1 > 1).
		// A tuple inequality like (col1,col2) > (1,2) ends up
		// being a full table scan for MySQL.
		for lastcol := len(rs.pkColumns) - 1; lastcol >= 0; lastcol-- {
			buf.Myprintf("%s(", prefix)
			prefix = " or "
			for i, pk := range rs.pkColumns[:lastcol] {
				buf.Myprintf("%v = ", sqlparser.NewIdentifierCI(rs.plan.Table.Fields[pk].Name))
				rs.lastpk[i].EncodeSQL(buf)
				buf.Myprintf(" and ")
			}
			buf.Myprintf("%v > ", sqlparser.NewIdentifierCI(rs.plan.Table.Fields[rs.pkColumns[lastcol]].Name))
			rs.lastpk[lastcol].EncodeSQL(buf)
			buf.Myprintf(")")
		}
	}
	buf.Myprintf(" order by ", sqlparser.NewIdentifierCS(rs.plan.Table.Name))
	prefix = ""
	for _, pk := range rs.pkColumns {
		buf.Myprintf("%s%v", prefix, sqlparser.NewIdentifierCI(rs.plan.Table.Fields[pk].Name))
		prefix = ", "
	}
	return buf.String(), nil
}

func (rs *rowStreamer) streamQuery(send func(*binlogdatapb.VStreamRowsResponse) error) error {
	throttleResponseRateLimiter := timer.NewRateLimiter(rowStreamertHeartbeatInterval)
	defer throttleResponseRateLimiter.Stop()

	var sendMu sync.Mutex
	safeSend := func(r *binlogdatapb.VStreamRowsResponse) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return send(r)
	}
	// Let's wait until MySQL is in good shape to stream rows
	if err := rs.vse.waitForMySQL(rs.ctx, rs.cp, rs.plan.Table.Name); err != nil {
		return err
	}
	var (
		gtid       string
		rotatedLog bool
		err        error
	)
	log.Infof("Streaming query: %v\n", rs.sendQuery)
	if rs.mode == RowStreamerModeSingleTable {
		gtid, rotatedLog, err = rs.conn.streamWithSnapshot(rs.ctx, rs.plan.Table.Name, rs.sendQuery)
		if err != nil {
			return err
		}
		if rotatedLog {
			rs.vse.vstreamerFlushedBinlogs.Add(1)
		}
	} else {
		// Comes here when we stream all tables. The snapshot is created just once at the start.
		if err := rs.conn.ExecuteStreamFetch(rs.query); err != nil {
			return err
		}
	}

	pkfields := make([]*querypb.Field, len(rs.pkColumns))
	for i, pk := range rs.pkColumns {
		pkfields[i] = &querypb.Field{
			Name:    rs.plan.Table.Fields[pk].Name,
			Type:    rs.plan.Table.Fields[pk].Type,
			Charset: rs.plan.Table.Fields[pk].Charset,
			Flags:   rs.plan.Table.Fields[pk].Flags,
		}
	}

	charsets := make([]collations.ID, len(rs.plan.Table.Fields))
	for i, fld := range rs.plan.Table.Fields {
		charsets[i] = collations.ID(fld.Charset)
	}

	err = safeSend(&binlogdatapb.VStreamRowsResponse{
		Fields:   rs.plan.fields(),
		Pkfields: pkfields,
		Gtid:     gtid,
	})
	if err != nil {
		return fmt.Errorf("stream send error: %v", err)
	}

	// streamQuery sends heartbeats as long as it operates
	heartbeatTicker := time.NewTicker(rowStreamertHeartbeatInterval)
	defer heartbeatTicker.Stop()
	go func() {
		select {
		case <-rs.ctx.Done():
			return
		case <-heartbeatTicker.C:
			safeSend(&binlogdatapb.VStreamRowsResponse{Heartbeat: true})
		}
	}()

	var (
		response binlogdatapb.VStreamRowsResponse
		rows     []*querypb.Row
		rowCount int
		mysqlrow []sqltypes.Value
	)

	filtered := make([]sqltypes.Value, len(rs.plan.ColExprs))
	lastpk := make([]sqltypes.Value, len(rs.pkColumns))
	byteCount := 0
	for {
		if rs.ctx.Err() != nil {
			log.Infof("Stream ended because of ctx.Done")
			return fmt.Errorf("stream ended: %v", rs.ctx.Err())
		}

		// check throttler.
		if !rs.vse.throttlerClient.ThrottleCheckOKOrWaitAppName(rs.ctx, throttlerapp.RowStreamerName) {
			throttleResponseRateLimiter.Do(func() error {
				return safeSend(&binlogdatapb.VStreamRowsResponse{Throttled: true})
			})
			continue
		}

		if mysqlrow != nil {
			mysqlrow = mysqlrow[:0]
		}
		mysqlrow, err = rs.conn.FetchNext(mysqlrow)
		if err != nil {
			return err
		}
		if mysqlrow == nil {
			break
		}
		// Compute lastpk here, because we'll need it
		// at the end after the loop exits.
		for i, pk := range rs.pkColumns {
			lastpk[i] = mysqlrow[pk]
		}
		// Reuse the vstreamer's filter.
		ok, err := rs.plan.filter(mysqlrow, filtered, charsets)
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
			err = safeSend(&response)
			if err != nil {
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
		err = safeSend(&response)
		if err != nil {
			return err
		}
	}

	return nil
}

func GetVReplicationMaxExecutionTimeQueryHint() string {
	return fmt.Sprintf("/*+ MAX_EXECUTION_TIME(%v) */ ", vttablet.CopyPhaseDuration.Milliseconds())
}
