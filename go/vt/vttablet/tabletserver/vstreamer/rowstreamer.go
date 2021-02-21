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
	send    func(*binlogdatapb.VStreamRowsResponse) error
	vschema *localVSchema

	plan      *Plan
	pkColumns []int
	sendQuery string
	vse       *Engine
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
	_, fromTable, err := analyzeSelect(rs.query)
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
	rs.sendQuery, err = rs.buildSelect()
	if err != nil {
		return err
	}
	return err
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

func (rs *rowStreamer) buildSelect() (string, error) {
	buf := sqlparser.NewTrackedBuffer(nil)
	// We could have used select *, but being explicit is more predictable.
	buf.Myprintf("select ")
	prefix := ""
	for _, col := range rs.plan.Table.Fields {
		buf.Myprintf("%s%v", prefix, sqlparser.NewColIdent(col.Name))
		prefix = ", "
	}
	buf.Myprintf(" from %v", sqlparser.NewTableIdent(rs.plan.Table.Name))
	if len(rs.lastpk) != 0 {
		if len(rs.lastpk) != len(rs.pkColumns) {
			return "", fmt.Errorf("primary key values don't match length: %v vs %v", rs.lastpk, rs.pkColumns)
		}
		buf.WriteString(" where ")
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
				rs.lastpk[i].EncodeSQL(buf)
				buf.Myprintf(" and ")
			}
			buf.Myprintf("%v > ", sqlparser.NewColIdent(rs.plan.Table.Fields[rs.pkColumns[lastcol]].Name))
			rs.lastpk[lastcol].EncodeSQL(buf)
			buf.Myprintf(")")
		}
	}
	buf.Myprintf(" order by ", sqlparser.NewTableIdent(rs.plan.Table.Name))
	prefix = ""
	for _, pk := range rs.pkColumns {
		buf.Myprintf("%s%v", prefix, sqlparser.NewColIdent(rs.plan.Table.Fields[pk].Name))
		prefix = ", "
	}
	return buf.String(), nil
}

func (rs *rowStreamer) streamQuery(conn *snapshotConn, send func(*binlogdatapb.VStreamRowsResponse) error) error {
	log.Infof("Streaming query: %v\n", rs.sendQuery)
	gtid, err := conn.streamWithSnapshot(rs.ctx, rs.plan.Table.Name, rs.sendQuery)
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

	response := &binlogdatapb.VStreamRowsResponse{}
	lastpk := make([]sqltypes.Value, len(rs.pkColumns))
	byteCount := 0
	for {
		//log.Infof("StreamResponse for loop iteration starts")
		select {
		case <-rs.ctx.Done():
			log.Infof("Stream ended because of ctx.Done")
			return fmt.Errorf("stream ended: %v", rs.ctx.Err())
		default:
		}

		// check throttler.
		if !rs.vse.throttlerClient.ThrottleCheckOKOrWait(rs.ctx) {
			continue
		}

		row, err := conn.FetchNext()
		if err != nil {
			return err
		}
		if row == nil {
			break
		}
		// Compute lastpk here, because we'll need it
		// at the end after the loop exits.
		for i, pk := range rs.pkColumns {
			lastpk[i] = row[pk]
		}
		// Reuse the vstreamer's filter.
		ok, filtered, err := rs.plan.filter(row)
		if err != nil {
			return err
		}
		if ok {
			response.Rows = append(response.Rows, sqltypes.RowToProto3(filtered))
			for _, s := range filtered {
				byteCount += s.Len()
			}
		}

		if byteCount >= *PacketSize {
			rs.vse.rowStreamerNumRows.Add(int64(len(response.Rows)))
			rs.vse.rowStreamerNumPackets.Add(int64(1))

			response.Lastpk = sqltypes.RowToProto3(lastpk)
			err = send(response)
			if err != nil {
				log.Infof("Rowstreamer send returned error %v", err)
				return err
			}
			// empty the rows so we start over, but we keep the
			// same capacity
			response.Rows = nil
			byteCount = 0
		}
	}

	if len(response.Rows) > 0 {
		rs.vse.rowStreamerNumRows.Add(int64(len(response.Rows)))
		response.Lastpk = sqltypes.RowToProto3(lastpk)
		err = send(response)
		if err != nil {
			return err
		}
	}

	return nil
}
