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
	"sync"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type rowStreamer struct {
	ctx    context.Context
	cancel func()

	cp      *mysql.ConnParams
	se      *schema.Engine
	query   string
	lastpk  []sqltypes.Value
	send    func(*binlogdatapb.VStreamRowsResponse) error
	kschema *vindexes.KeyspaceSchema

	plan      *Plan
	pkColumns []int
	sendQuery string
}

func newRowStreamer(ctx context.Context, cp *mysql.ConnParams, se *schema.Engine, query string, lastpk []sqltypes.Value, kschema *vindexes.KeyspaceSchema, send func(*binlogdatapb.VStreamRowsResponse) error) *rowStreamer {
	ctx, cancel := context.WithCancel(ctx)
	return &rowStreamer{
		ctx:     ctx,
		cancel:  cancel,
		cp:      cp,
		se:      se,
		query:   query,
		lastpk:  lastpk,
		send:    send,
		kschema: kschema,
	}
}

func (rs *rowStreamer) Cancel() {
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

	conn, err := rs.mysqlConnect()
	if err != nil {
		return err
	}
	defer conn.Close()
	return rs.streamQuery(conn, rs.send)
}

func (rs *rowStreamer) buildPlan() error {
	// This pre-parsing is required to extract the table name
	// and create its metadata.
	_, fromTable, err := analyzeSelect(rs.query)
	if err != nil {
		return err
	}
	st := rs.se.GetTable(fromTable)
	if st == nil {
		return fmt.Errorf("unknown table %v in schema", fromTable)
	}
	ti := &Table{
		Name:    st.Name.String(),
		Columns: st.Columns,
	}
	rs.plan, err = buildTablePlan(ti, rs.kschema, rs.query)
	if err != nil {
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

func buildPKColumns(st *schema.Table) ([]int, error) {
	if len(st.PKColumns) == 0 {
		pkColumns := make([]int, len(st.Columns))
		for i := range st.Columns {
			pkColumns[i] = i
		}
		return pkColumns, nil
	}
	for _, pk := range st.PKColumns {
		if pk >= len(st.Columns) {
			return nil, fmt.Errorf("primary key %d refers to non-existent column", pk)
		}
	}
	return st.PKColumns, nil
}

func (rs *rowStreamer) buildSelect() (string, error) {
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("select ")
	prefix := ""
	for _, col := range rs.plan.Table.Columns {
		buf.Myprintf("%s%v", prefix, col.Name)
		prefix = ", "
	}
	buf.Myprintf(" from %v", sqlparser.NewTableIdent(rs.plan.Table.Name))
	if len(rs.lastpk) != 0 {
		if len(rs.lastpk) != len(rs.pkColumns) {
			return "", fmt.Errorf("primary key values don't match length: %v vs %v", rs.lastpk, rs.pkColumns)
		}
		buf.WriteString(" where (")
		prefix := ""
		for _, pk := range rs.pkColumns {
			buf.Myprintf("%s%v", prefix, rs.plan.Table.Columns[pk].Name)
			prefix = ","
		}
		buf.WriteString(") > (")
		prefix = ""
		for _, val := range rs.lastpk {
			buf.WriteString(prefix)
			prefix = ","
			val.EncodeSQL(buf)
		}
		buf.WriteString(")")
	}
	buf.Myprintf(" order by ", sqlparser.NewTableIdent(rs.plan.Table.Name))
	prefix = ""
	for _, pk := range rs.pkColumns {
		buf.Myprintf("%s%v", prefix, rs.plan.Table.Columns[pk].Name)
		prefix = ", "
	}
	return buf.String(), nil
}

func (rs *rowStreamer) streamQuery(conn *mysql.Conn, send func(*binlogdatapb.VStreamRowsResponse) error) error {
	unlock, gtid, err := rs.lockTable()
	if err != nil {
		return err
	}
	defer unlock()

	if err := conn.ExecuteStreamFetch(rs.sendQuery); err != nil {
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
	if err := unlock(); err != nil {
		return err
	}

	response := &binlogdatapb.VStreamRowsResponse{}
	lastpk := make([]sqltypes.Value, len(rs.pkColumns))
	byteCount := 0
	for {
		select {
		case <-rs.ctx.Done():
			return fmt.Errorf("stream ended: %v", rs.ctx.Err())
		default:
		}

		row, err := conn.FetchNext()
		if err != nil {
			return err
		}
		if row == nil {
			break
		}
		for i, pk := range rs.pkColumns {
			lastpk[i] = row[pk]
		}
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
			response.Lastpk = sqltypes.RowToProto3(lastpk)
			err = send(response)
			if err != nil {
				return err
			}
			// empty the rows so we start over, but we keep the
			// same capacity
			response.Rows = response.Rows[:0]
			byteCount = 0
		}
	}

	if len(response.Rows) > 0 {
		response.Lastpk = sqltypes.RowToProto3(lastpk)
		err = send(response)
		if err != nil {
			return err
		}
	}

	return nil
}

func (rs *rowStreamer) lockTable() (unlock func() error, gtid string, err error) {
	conn, err := rs.mysqlConnect()
	if err != nil {
		return nil, "", err
	}
	// mysql recommends this before locking tables.
	if _, err := conn.ExecuteFetch("set autocommit=0", 0, false); err != nil {
		return nil, "", err
	}
	if _, err := conn.ExecuteFetch(fmt.Sprintf("lock tables %s read", sqlparser.String(sqlparser.NewTableIdent(rs.plan.Table.Name))), 0, false); err != nil {
		return nil, "", err
	}
	var once sync.Once
	unlock = func() error {
		var err error
		once.Do(func() {
			_, err = conn.ExecuteFetch("unlock tables", 0, false)
			conn.Close()
		})
		return err
	}
	pos, err := conn.MasterPosition()
	if err != nil {
		unlock()
		return nil, "", err
	}
	return unlock, mysql.EncodePosition(pos), nil
}

func (rs *rowStreamer) mysqlConnect() (*mysql.Conn, error) {
	cp, err := dbconfigs.WithCredentials(rs.cp)
	if err != nil {
		return nil, err
	}
	return mysql.Connect(rs.ctx, cp)
}
