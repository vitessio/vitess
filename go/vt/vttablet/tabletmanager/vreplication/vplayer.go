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

package vreplication

import (
	"bytes"
	"fmt"
	"io"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

type vplayer struct {
	id           uint32
	source       *binlogdatapb.BinlogSource
	sourceTablet *topodatapb.Tablet
	stats        *binlogplayer.Stats
	dbClient     binlogplayer.DBClient
	mysqld       mysqlctl.MysqlDaemon

	pos     mysql.Position
	stopPos mysql.Position
	pplan   *playerPlan
	tplans  map[string]*tablePlan

	retryDelay time.Duration
}

func newVPlayer(id uint32, source *binlogdatapb.BinlogSource, sourceTablet *topodatapb.Tablet, stats *binlogplayer.Stats, dbClient binlogplayer.DBClient, mysqld mysqlctl.MysqlDaemon) *vplayer {
	return &vplayer{
		id:           id,
		source:       source,
		sourceTablet: sourceTablet,
		stats:        stats,
		dbClient:     dbClient,
		mysqld:       mysqld,
		retryDelay:   1 * time.Second,
		tplans:       make(map[string]*tablePlan),
	}
}

func (vp *vplayer) Play(ctx context.Context) error {
	vp.setState(binlogplayer.BlpRunning, "")
	if err := vp.play(ctx); err != nil {
		msg := err.Error()
		vp.stats.History.Add(&binlogplayer.StatsHistoryRecord{
			Time:    time.Now(),
			Message: msg,
		})
		vp.setState(binlogplayer.BlpError, msg)
		return err
	}
	return nil
}

func (vp *vplayer) play(ctx context.Context) error {
	startPos, stopPos, _, _, err := binlogplayer.ReadVRSettings(vp.dbClient, vp.id)
	if err != nil {
		return fmt.Errorf("error reading VReplication settings: %v", err)
	}
	vp.pos, err = mysql.DecodePosition(startPos)
	if err != nil {
		return fmt.Errorf("error decoding start position %v: %v", startPos, err)
	}
	if stopPos != "" {
		vp.stopPos, err = mysql.DecodePosition(stopPos)
		if err != nil {
			return fmt.Errorf("error decoding stop position %v: %v", stopPos, err)
		}
	}
	if !vp.stopPos.IsZero() {
		if vp.pos.AtLeast(vp.stopPos) {
			vp.setState(binlogplayer.BlpStopped, fmt.Sprintf("Stop position %v already reached: %v", vp.pos, vp.stopPos))
			return nil
		}
	}
	log.Infof("Starting VReplication player id: %v, startPos: %v, stop: %v, source: %v", vp.id, startPos, vp.stopPos, vp.sourceTablet)

	plan, err := buildPlayerPlan(vp.source.Filter)
	if err != nil {
		return err
	}
	vp.pplan = plan

	vsClient, err := tabletconn.GetDialer()(vp.sourceTablet, grpcclient.FailFast(false))
	if err != nil {
		return fmt.Errorf("error dialing tablet: %v", err)
	}
	target := &querypb.Target{
		Keyspace:   vp.sourceTablet.Keyspace,
		Shard:      vp.sourceTablet.Shard,
		TabletType: vp.sourceTablet.Type,
	}
	return vsClient.VStream(ctx, target, startPos, plan.vstreamFilter, func(events []*binlogdatapb.VEvent) error {
		for _, event := range events {
			select {
			case <-ctx.Done():
				return io.EOF
			default:
			}
			if err := vp.applyEvent(event); err != nil {
				return err
			}
		}
		return nil
	})
}

func (vp *vplayer) applyEvent(event *binlogdatapb.VEvent) error {
	switch event.Type {
	case binlogdatapb.VEventType_GTID:
		pos, err := mysql.DecodePosition(event.Gtid)
		if err != nil {
			return err
		}
		vp.pos = pos
		if vp.stopPos.IsZero() {
			return nil
		}
		if !vp.pos.Equal(vp.stopPos) && vp.pos.AtLeast(vp.stopPos) {
			return fmt.Errorf("next event position %v exceeds stop pos %v, exiting without applying", vp.pos, vp.stopPos)
		}
	case binlogdatapb.VEventType_BEGIN:
		if err := vp.dbClient.Begin(); err != nil {
			return err
		}
	case binlogdatapb.VEventType_COMMIT:
		if err := vp.updatePos(event.Timestamp); err != nil {
			return err
		}
		if err := vp.dbClient.Commit(); err != nil {
			return err
		}
	case binlogdatapb.VEventType_ROLLBACK:
		// This code is unreachable. It's just here as failsafe.
		_ = vp.dbClient.Rollback()
	case binlogdatapb.VEventType_FIELD:
		if err := vp.updatePlan(event.FieldEvent); err != nil {
			return err
		}
	case binlogdatapb.VEventType_ROW:
		if err := vp.applyRowEvent(event.RowEvent); err != nil {
			return err
		}
	case binlogdatapb.VEventType_DDL:
		switch vp.source.OnDdl {
		case binlogdatapb.OnDDLAction_IGNORE:
			// no-op
		case binlogdatapb.OnDDLAction_STOP:
			if err := vp.dbClient.Begin(); err != nil {
				return err
			}
			if err := vp.updatePos(event.Timestamp); err != nil {
				return err
			}
			vp.setState(binlogplayer.BlpStopped, fmt.Sprintf("stopped at DDL %s", event.Ddl))
			if err := vp.dbClient.Commit(); err != nil {
				return err
			}
		case binlogdatapb.OnDDLAction_EXEC:
			if err := vp.exec(event.Ddl); err != nil {
				return err
			}
			if err := vp.savePos(event.Timestamp); err != nil {
				return err
			}
		case binlogdatapb.OnDDLAction_EXEC_IGNORE:
			if err := vp.exec(event.Ddl); err != nil {
				log.Infof("Ignoring error: %v for DDL: %s", err, event.Ddl)
			}
			if err := vp.savePos(event.Timestamp); err != nil {
				return err
			}
		}
	}
	return nil
}

func (vp *vplayer) setState(state, message string) {
	if state == binlogplayer.BlpStopped {
	}
	if err := binlogplayer.SetVReplicationState(vp.dbClient, vp.id, state, message); err != nil {
		log.Errorf("Error writing state: %s, msg: %s, err: %v", state, message, err)
	}
}

func (vp *vplayer) updatePlan(fieldEvent *binlogdatapb.FieldEvent) error {
	prelim := vp.pplan.tablePlans[fieldEvent.TableName]
	tplan := &tablePlan{
		name: fieldEvent.TableName,
	}
	if prelim != nil {
		*tplan = *prelim
	}
	tplan.fields = fieldEvent.Fields

	if tplan.colExprs == nil {
		tplan.colExprs = make([]*colExpr, len(tplan.fields))
		for i, field := range tplan.fields {
			tplan.colExprs[i] = &colExpr{
				colname: sqlparser.NewColIdent(field.Name),
				colnum:  i,
			}
		}
	} else {
		if len(tplan.fields) != len(tplan.colExprs) {
			return fmt.Errorf("columns received from vreplication: %v, do not match expected: %v", tplan.fields, tplan.colExprs)
		}
		for i, field := range tplan.fields {
			if tplan.colExprs[i].colname.EqualString(field.Name) {
				return fmt.Errorf("column name from vreplication field %d: %s, does not match expected: %s", i, field.Name, tplan.colExprs[i].colname)
			}
		}
	}

	qr, err := vp.dbClient.ExecuteFetch("select database()", 1)
	if err != nil {
		return err
	}
	if len(qr.Rows) == 0 || len(qr.Rows[0]) == 0 {
		return fmt.Errorf("unexpected result from 'select database()': %v", qr)
	}
	dbname := qr.Rows[0][0].ToString()
	pkcols, err := vp.mysqld.GetPrimaryKeyColumns(dbname, tplan.name)
	if err != nil {
		return fmt.Errorf("error fetching pk columns for %s: %v", tplan.name, err)
	}
	for _, pkcol := range pkcols {
		found := false
		for i, cExpr := range tplan.colExprs {
			if cExpr.colname.EqualString(pkcol) {
				found = true
				tplan.pkCols = append(tplan.pkCols, &colExpr{
					colname: cExpr.colname,
					colnum:  i,
				})
				break
			}
		}
		if !found {
			return fmt.Errorf("primary key column %s missing from select list for table %s", pkcol, tplan.name)
		}
	}
	vp.tplans[fieldEvent.TableName] = tplan
	return nil
}

func (vp *vplayer) applyRowEvent(rowEvent *binlogdatapb.RowEvent) error {
	tplan := vp.tplans[rowEvent.TableName]
	if tplan == nil {
		return fmt.Errorf("unexpected event on table %s", rowEvent.TableName)
	}
	for _, change := range rowEvent.RowChanges {
		if err := vp.applyRowChange(tplan, change); err != nil {
			return err
		}
	}
	return nil
}

func (vp *vplayer) applyRowChange(tplan *tablePlan, rowChange *binlogdatapb.RowChange) error {
	// MakeRowTrusted is needed here because because Proto3ToResult is not convenient.
	var before, after []sqltypes.Value
	if rowChange.Before != nil {
		before = sqltypes.MakeRowTrusted(tplan.fields, rowChange.Before)
	}
	if rowChange.After != nil {
		after = sqltypes.MakeRowTrusted(tplan.fields, rowChange.After)
	}
	var query string
	switch {
	case before == nil && after != nil:
		query = vp.generateInsert(tplan, after)
	case before != nil && after != nil:
		query = vp.generateUpdate(tplan, before, after)
	case before != nil && after == nil:
		query = vp.generateDelete(tplan, before)
	case before == nil && after == nil:
		// unreachable
	}
	return vp.exec(query)
}

func (vp *vplayer) generateInsert(tplan *tablePlan, after []sqltypes.Value) string {
	sql := sqlparser.NewTrackedBuffer(nil)
	if tplan.onInsert == insertIgnore {
		sql.Myprintf("insert ignore into %v set ", sqlparser.NewTableIdent(tplan.name))
	} else {
		sql.Myprintf("insert into %v set ", sqlparser.NewTableIdent(tplan.name))
	}
	vp.writeInsertValues(sql, tplan, after)
	if tplan.onInsert == insertOndup {
		sql.Myprintf(" on duplicate key update ")
		vp.writeUpdateValues(sql, tplan, nil, after)
	}
	return sql.String()
}

func (vp *vplayer) generateUpdate(tplan *tablePlan, before, after []sqltypes.Value) string {
	if tplan.onInsert == insertIgnore {
		return ""
	}
	sql := sqlparser.NewTrackedBuffer(nil)
	sql.Myprintf("update %v set ", sqlparser.NewTableIdent(tplan.name))
	vp.writeUpdateValues(sql, tplan, before, after)
	sql.Myprintf(" where ")
	vp.writeWhereValues(sql, tplan, before)
	return sql.String()
}

func (vp *vplayer) generateDelete(tplan *tablePlan, before []sqltypes.Value) string {
	sql := sqlparser.NewTrackedBuffer(nil)
	if tplan.onInsert == insertNormal {
		sql.Myprintf("delete from %v where ", sqlparser.NewTableIdent(tplan.name))
		vp.writeWhereValues(sql, tplan, before)
		return sql.String()
	}
	sql.Myprintf("update %v set ", sqlparser.NewTableIdent(tplan.name))
	vp.writeUpdateValues(sql, tplan, before, nil)
	sql.Myprintf(" where ")
	vp.writeWhereValues(sql, tplan, before)
	return sql.String()
}

func (vp *vplayer) writeInsertValues(sql *sqlparser.TrackedBuffer, tplan *tablePlan, after []sqltypes.Value) {
	separator := ""
	for _, cExpr := range tplan.colExprs {
		sql.Myprintf("%s%v=", separator, cExpr.colname)
		if separator == "" {
			separator = ", "
		}
		if cExpr.op == opCount {
			sql.WriteString("1")
		} else {
			encodeValue(sql, after[cExpr.colnum])
		}
	}
}

func (vp *vplayer) writeUpdateValues(sql *sqlparser.TrackedBuffer, tplan *tablePlan, before, after []sqltypes.Value) {
	separator := ""
	for _, cExpr := range tplan.colExprs {
		if cExpr.isGrouped {
			continue
		}
		sql.Myprintf("%s%v=", separator, cExpr.colname)
		if separator == "" {
			separator = ", "
		}
		if cExpr.op == opCount || cExpr.op == opSum {
			sql.Myprintf("%s", cExpr.colname)
		}
		if len(before) != 0 {
			switch cExpr.op {
			case opNone:
				if len(after) == 0 {
					sql.WriteString("NULL")
				}
			case opCount:
				sql.WriteString("-1")
			case opSum:
				sql.WriteString("-")
				encodeValue(sql, before[cExpr.colnum])
			}
		}
		if len(after) != 0 {
			switch cExpr.op {
			case opNone:
				encodeValue(sql, after[cExpr.colnum])
			case opCount:
				sql.WriteString("+1")
			case opSum:
				sql.WriteString("+")
				encodeValue(sql, after[cExpr.colnum])
			}
		}
	}
}

func (vp *vplayer) writeWhereValues(sql *sqlparser.TrackedBuffer, tplan *tablePlan, before []sqltypes.Value) {
	separator := ""
	for _, cExpr := range tplan.pkCols {
		sql.Myprintf("%s%v=", separator, cExpr.colname)
		if separator == "" {
			separator = " AND "
		}
		encodeValue(sql, before[cExpr.colnum])
	}
}

// savePos performs an updatePos in its own transaction.
func (vp *vplayer) savePos(ts int64) error {
	if err := vp.dbClient.Begin(); err != nil {
		return err
	}
	if err := vp.updatePos(ts); err != nil {
		return err
	}
	return vp.dbClient.Commit()
}

func (vp *vplayer) updatePos(ts int64) error {
	updatePos := binlogplayer.GenerateUpdatePos(vp.id, vp.pos, time.Now().Unix(), ts)
	if _, err := vp.dbClient.ExecuteFetch(updatePos, 0); err != nil {
		_ = vp.dbClient.Rollback()
		return fmt.Errorf("error %v updating position", err)
	}
	return nil
}

func (vp *vplayer) exec(sql string) error {
	vp.stats.Timings.Record("query", time.Now())
	_, err := vp.dbClient.ExecuteFetch(sql, 0)
	return err
}

func encodeValue(sql *sqlparser.TrackedBuffer, value sqltypes.Value) {
	if value.Type() == querypb.Type_TIMESTAMP && !bytes.HasPrefix(value.ToBytes(), mysql.ZeroTimestamp) {
		// Values in the binary log are UTC. Let's convert them
		// to whatever timezone the connection is using,
		// so MySQL properly converts them back to UTC.
		sql.WriteString("convert_tz(")
		value.EncodeSQL(sql)
		sql.WriteString(", '+00:00', @@session.time_zone)")
	} else {
		value.EncodeSQL(sql)
	}
}
