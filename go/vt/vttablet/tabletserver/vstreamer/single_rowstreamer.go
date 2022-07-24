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

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

// singleRowStreamer streams rows for a single table (via a select on that single table). It acts as a
// subsidery to parallelRowStreamer.
type singleRowStreamer struct {
	lastpk []sqltypes.Value

	plan          *Plan
	pkColumns     []int
	ukColumnNames []string
	sendQuery     string

	conn    *snapshotConn
	pktsize PacketSizer

	pstreamer *parallelRowStreamer
}

// newSingleRowStreamer creates a streamer using an analyzed plan
func newSingleRowStreamer(parallelStreamer *parallelRowStreamer, plan *Plan, lastpk []sqltypes.Value) *singleRowStreamer {
	return &singleRowStreamer{
		pstreamer: parallelStreamer,
		plan:      plan,
		lastpk:    lastpk,
		pktsize:   DefaultPacketSizer(),
	}
}

// tableName returns the name of streamed table
func (si *singleRowStreamer) tableName() string {
	return si.plan.Table.Name
}

// analyzeCommentDirectives reads any special directives from parsed query comments
// specifically, we're looking for a unique key directive, which indicates the unique key column names
// we should use instead of PK columns
func (si *singleRowStreamer) analyzeCommentDirectives(comments *sqlparser.ParsedComments) (err error) {
	if s := comments.Directives().GetString("ukColumns", ""); s != "" {
		si.ukColumnNames, err = textutil.SplitUnescape(s, ",")
		if err != nil {
			return err
		}
	}
	return nil
}

// buildPKColumnsFromUniqueKey assumes a unique key is indicated,
func (si *singleRowStreamer) buildPKColumnsFromUniqueKey() error {
	var pkColumns = make([]int, 0)
	// We wish to utilize a UNIQUE KEY which is not the PRIMARY KEY

	for _, colName := range si.ukColumnNames {
		index := si.plan.Table.FindColumn(sqlparser.NewIdentifierCI(colName))
		if index < 0 {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "column %v is listed as unique key, but not present in table %v", colName, si.tableName())
		}
		pkColumns = append(pkColumns, index)
	}
	si.pkColumns = pkColumns
	return nil
}

// buildPKColumns evaluates indices of PK or UK columns
func (si *singleRowStreamer) buildPKColumns(st *binlogdatapb.MinimalTable) error {
	if len(si.ukColumnNames) > 0 {
		return si.buildPKColumnsFromUniqueKey()
	}
	if len(st.PKColumns) == 0 {
		si.pkColumns = make([]int, len(st.Fields))
		for i := range st.Fields {
			si.pkColumns[i] = i
		}
		return nil
	}
	si.pkColumns = make([]int, 0)
	for _, pk := range st.PKColumns {
		if pk >= int64(len(st.Fields)) {
			return fmt.Errorf("primary key %d refers to non-existent column", pk)
		}
		si.pkColumns = append(si.pkColumns, int(pk))
	}
	return nil
}

func (si *singleRowStreamer) buildSelect() error {
	buf := sqlparser.NewTrackedBuffer(nil)
	// We could have used select *, but being explicit is more predictable.
	buf.Myprintf("select ")
	prefix := ""
	for _, col := range si.plan.Table.Fields {
		if si.plan.isConvertColumnUsingUTF8(col.Name) {
			buf.Myprintf("%sconvert(%v using utf8mb4) as %v", prefix, sqlparser.NewIdentifierCI(col.Name), sqlparser.NewIdentifierCI(col.Name))
		} else {
			buf.Myprintf("%s%v", prefix, sqlparser.NewIdentifierCI(col.Name))
		}
		prefix = ", "
	}
	buf.Myprintf(" from %v", sqlparser.NewIdentifierCI(si.tableName()))
	if len(si.lastpk) != 0 {
		if len(si.lastpk) != len(si.pkColumns) {
			return fmt.Errorf("primary key values don't match length: %v vs %v", si.lastpk, si.pkColumns)
		}
		buf.WriteString(" where ")
		prefix := ""
		// This loop handles the case for composite pks. For example,
		// if lastpk was (1,2), the where clause would be:
		// (col1 = 1 and col2 > 2) or (col1 > 1).
		// A tuple inequality like (col1,col2) > (1,2) ends up
		// being a full table scan for mysql.
		for lastcol := len(si.pkColumns) - 1; lastcol >= 0; lastcol-- {
			buf.Myprintf("%s(", prefix)
			prefix = " or "
			for i, pk := range si.pkColumns[:lastcol] {
				buf.Myprintf("%v = ", sqlparser.NewIdentifierCI(si.plan.Table.Fields[pk].Name))
				si.lastpk[i].EncodeSQL(buf)
				buf.Myprintf(" and ")
			}
			buf.Myprintf("%v > ", sqlparser.NewIdentifierCI(si.plan.Table.Fields[si.pkColumns[lastcol]].Name))
			si.lastpk[lastcol].EncodeSQL(buf)
			buf.Myprintf(")")
		}
	}
	buf.Myprintf(" order by ", sqlparser.NewIdentifierCI(si.tableName()))
	prefix = ""
	for _, pk := range si.pkColumns {
		buf.Myprintf("%s%v", prefix, sqlparser.NewIdentifierCI(si.plan.Table.Fields[pk].Name))
		prefix = ", "
	}
	si.sendQuery = buf.String()
	return nil
}

func (si *singleRowStreamer) streamQuery(ctx context.Context, gtid string) error {
	log.Infof("Streaming query: %v\n", si.sendQuery)
	if err := si.conn.ExecuteStreamFetch(si.sendQuery); err != nil {
		return err
	}

	// first call the callback with the fields
	flds, err := si.conn.Fields()
	if err != nil {
		return err
	}
	pkfields := make([]*querypb.Field, len(si.pkColumns))
	for i, pk := range si.pkColumns {
		pkfields[i] = &querypb.Field{
			Name: flds[pk].Name,
			Type: flds[pk].Type,
		}
	}
	charsets := make([]collations.ID, len(flds))
	for i, fld := range flds {
		charsets[i] = collations.ID(fld.Charset)
	}

	err = si.pstreamer.sendResponse(&binlogdatapb.VStreamRowsResponse{
		Fields:    si.plan.fields(),
		Pkfields:  pkfields,
		Gtid:      gtid,
		TableName: si.tableName(),
	})
	if err != nil {
		return fmt.Errorf("stream send error: %v", err)
	}

	var response = &binlogdatapb.VStreamRowsResponse{
		TableName: si.tableName(),
	}
	var rows []*querypb.Row
	var rowCount int
	var mysqlrow []sqltypes.Value

	filtered := make([]sqltypes.Value, len(si.plan.ColExprs))
	lastpk := make([]sqltypes.Value, len(si.pkColumns))
	byteCount := 0
	for {
		//log.Infof("StreamResponse for loop iteration starts")
		if ctx.Err() != nil {
			log.Infof("Stream ended because of ctx.Done")
			return fmt.Errorf("stream ended: %v", ctx.Err())
		}

		// check throttler.
		if !si.pstreamer.vse.throttlerClient.ThrottleCheckOKOrWait(ctx) {
			continue
		}

		if mysqlrow != nil {
			mysqlrow = mysqlrow[:0]
		}
		mysqlrow, err = si.conn.FetchNext(mysqlrow)
		if err != nil {
			return err
		}
		if mysqlrow == nil {
			break
		}
		// Compute lastpk here, because we'll need it
		// at the end after the loop exits.
		for i, pk := range si.pkColumns {
			lastpk[i] = mysqlrow[pk]
		}
		// Reuse the vstreamer's filter.
		ok, err := si.plan.filter(mysqlrow, filtered, charsets)
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

		if si.pktsize.ShouldSend(byteCount) {
			response.Rows = rows[:rowCount]
			response.Lastpk = sqltypes.RowToProto3(lastpk)

			si.pstreamer.vse.rowStreamerNumRows.Add(int64(len(response.Rows)))
			si.pstreamer.vse.rowStreamerNumPackets.Add(int64(1))

			startSend := time.Now()
			err = si.pstreamer.sendResponse(response)
			if err != nil {
				log.Infof("Rowstreamer send returned error %v", err)
				return err
			}
			si.pktsize.Record(byteCount, time.Since(startSend))
			rowCount = 0
			byteCount = 0
		}
	}

	if rowCount > 0 {
		response.Rows = rows[:rowCount]
		response.Lastpk = sqltypes.RowToProto3(lastpk)

		si.pstreamer.vse.rowStreamerNumRows.Add(int64(len(response.Rows)))
		err = si.pstreamer.sendResponse(response)
		if err != nil {
			return err
		}
	}

	return nil
}
