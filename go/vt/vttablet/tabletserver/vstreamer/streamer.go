/*
Copyright 2018 The Vitess Authors.

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
	"strings"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

// statementPrefixes are normal sql statement prefixes.
var statementPrefixes = map[string]binlogdatapb.VEventType{
	"begin":    binlogdatapb.VEventType_BEGIN,
	"commit":   binlogdatapb.VEventType_COMMIT,
	"rollback": binlogdatapb.VEventType_ROLLBACK,
	"insert":   binlogdatapb.VEventType_INSERT,
	"update":   binlogdatapb.VEventType_UPDATE,
	"delete":   binlogdatapb.VEventType_DELETE,
	"create":   binlogdatapb.VEventType_DDL,
	"alter":    binlogdatapb.VEventType_DDL,
	"drop":     binlogdatapb.VEventType_DDL,
	"truncate": binlogdatapb.VEventType_DDL,
	"rename":   binlogdatapb.VEventType_DDL,
	"set":      binlogdatapb.VEventType_SET,
}

// getStatementCategory returns the binlogdatapb.BL_* category for a SQL statement.
func getStatementCategory(sql string) binlogdatapb.VEventType {
	if i := strings.IndexByte(sql, byte(' ')); i >= 0 {
		sql = sql[:i]
	}
	return statementPrefixes[strings.ToLower(sql)]
}

type vstreamer struct {
	ctx    context.Context
	cancel func()

	cp       *mysql.ConnParams
	se       *schema.Engine
	startPos mysql.Position
	filter   *binlogdatapb.Filter
	send     func(*binlogdatapb.VEvent) error

	kevents chan *vindexes.KeyspaceSchema
	kschema *vindexes.KeyspaceSchema
	plans   map[uint64]*Plan

	// format and pos are updated by parseEvent.
	format mysql.BinlogFormat
	pos    mysql.Position
}

func newVStreamer(ctx context.Context, cp *mysql.ConnParams, se *schema.Engine, startPos mysql.Position, filter *binlogdatapb.Filter, kschema *vindexes.KeyspaceSchema, send func(*binlogdatapb.VEvent) error) *vstreamer {
	ctx, cancel := context.WithCancel(ctx)
	return &vstreamer{
		ctx:      ctx,
		cancel:   cancel,
		cp:       cp,
		se:       se,
		startPos: startPos,
		filter:   filter,
		send:     send,
		kevents:  make(chan *vindexes.KeyspaceSchema, 1),
		kschema:  kschema,
		plans:    make(map[uint64]*Plan),
	}
}

func (vs *vstreamer) SetKSchema(kschema *vindexes.KeyspaceSchema) {
	// Since vs.Stream is a single-threaded loop. We just send an event to
	// that thread, which helps us avoid mutexes to update the plans.
	select {
	case vs.kevents <- kschema:
	case <-vs.ctx.Done():
	}
}

func (vs *vstreamer) Cancel() {
	vs.cancel()
}

// Stream runs a single-threaded loop.
func (vs *vstreamer) Stream() error {
	defer vs.cancel()
	vs.pos = vs.startPos

	// Ensure se is Open. If vttablet came up in a non_serving role,
	// the schema engine may not have been initialized.
	if err := vs.se.Open(); err != nil {
		return wrapError(err, vs.pos)
	}

	conn, err := binlog.NewSlaveConnection(vs.cp)
	if err != nil {
		return wrapError(err, vs.pos)
	}
	defer conn.Close()

	events, err := conn.StartBinlogDumpFromPosition(vs.ctx, vs.pos)
	if err != nil {
		return wrapError(err, vs.pos)
	}
	err = vs.parseEvents(vs.ctx, events)
	return wrapError(err, vs.pos)
}

func (vs *vstreamer) parseEvents(ctx context.Context, events <-chan mysql.BinlogEvent) error {
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				return fmt.Errorf("server EOF")
			}
			vevents, err := vs.parseEvent(ev)
			if err != nil {
				return err
			}
			for _, vevent := range vevents {
				if err := vs.send(vevent); err != nil {
					return fmt.Errorf("error sending event: %v", err)
				}
			}
		case vs.kschema = <-vs.kevents:
			if err := vs.rebuildPlans(); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (vs *vstreamer) parseEvent(ev mysql.BinlogEvent) ([]*binlogdatapb.VEvent, error) {
	// Validate the buffer before reading fields from it.
	if !ev.IsValid() {
		return nil, fmt.Errorf("can't parse binlog event: invalid data: %#v", ev)
	}

	// We need to keep checking for FORMAT_DESCRIPTION_EVENT even after we've
	// seen one, because another one might come along (e.g. on log rotate due to
	// binlog settings change) that changes the format.
	if ev.IsFormatDescription() {
		var err error
		vs.format, err = ev.Format()
		if err != nil {
			return nil, fmt.Errorf("can't parse FORMAT_DESCRIPTION_EVENT: %v, event data: %#v", err, ev)
		}
		return nil, nil
	}

	// We can't parse anything until we get a FORMAT_DESCRIPTION_EVENT that
	// tells us the size of the event header.
	if vs.format.IsZero() {
		// The only thing that should come before the FORMAT_DESCRIPTION_EVENT
		// is a fake ROTATE_EVENT, which the master sends to tell us the name
		// of the current log file.
		if ev.IsRotate() {
			return nil, nil
		}
		return nil, fmt.Errorf("got a real event before FORMAT_DESCRIPTION_EVENT: %#v", ev)
	}

	// Strip the checksum, if any. We don't actually verify the checksum, so discard it.
	ev, _, err := ev.StripChecksum(vs.format)
	if err != nil {
		return nil, fmt.Errorf("can't strip checksum from binlog event: %v, event data: %#v", err, ev)
	}
	var vevents []*binlogdatapb.VEvent
	switch {
	case ev.IsPseudo() || ev.IsGTID():
		gtid, hasBegin, err := ev.GTID(vs.format)
		if err != nil {
			return nil, fmt.Errorf("can't get GTID from binlog event: %v, event data: %#v", err, ev)
		}
		if hasBegin {
			vevents = append(vevents, &binlogdatapb.VEvent{
				Type: binlogdatapb.VEventType_BEGIN,
			})
		}
		vs.pos = mysql.AppendGTID(vs.pos, gtid)
		vevents = append(vevents, &binlogdatapb.VEvent{
			Type: binlogdatapb.VEventType_GTID,
			Gtid: mysql.EncodePosition(vs.pos),
		})
	case ev.IsXID():
		vevents = append(vevents, &binlogdatapb.VEvent{
			Type: binlogdatapb.VEventType_COMMIT,
		})
	case ev.IsQuery():
		q, err := ev.Query(vs.format)
		if err != nil {
			return nil, fmt.Errorf("can't get query from binlog event: %v, event data: %#v", err, ev)
		}
		switch cat := getStatementCategory(q.SQL); cat {
		case binlogdatapb.VEventType_BEGIN, binlogdatapb.VEventType_COMMIT, binlogdatapb.VEventType_ROLLBACK:
			vevents = append(vevents, &binlogdatapb.VEvent{
				Type: cat,
			})
		case binlogdatapb.VEventType_DDL:
			vevents = append(vevents, &binlogdatapb.VEvent{
				Type: cat,
				Ddl:  q.SQL,
			})
		default:
			return nil, fmt.Errorf("unexpected event type %v in row-based replication: %#v", cat, ev)
		}
	case ev.IsTableMap():
		id := ev.TableID(vs.format)
		tm, err := ev.TableMap(vs.format)
		if err != nil {
			return nil, err
		}
		if tm.Database != "" && tm.Database != vs.cp.DbName {
			return nil, nil
		}
		ti := vs.se.GetTable(sqlparser.NewTableIdent(tm.Name))
		if ti == nil {
			return nil, fmt.Errorf("unknown table %v in schema", tm.Name)
		}
		if len(ti.Columns) < len(tm.Types) {
			return nil, fmt.Errorf("cannot determine table columns for %s: event has %d columns, current schema has %d: %#v", tm.Name, len(tm.Types), len(ti.Columns), ev)
		}
		table := &Table{
			TableMap: tm,
			Columns:  ti.Columns,
		}
		plan, err := buildPlan(table, vs.kschema, vs.filter)
		if err != nil {
			return nil, err
		}
		if plan != nil {
			vs.plans[id] = plan
		}
	case ev.IsWriteRows() || ev.IsDeleteRows() || ev.IsUpdateRows():
		// The existence of before and after images can be used to
		// identify statememt types. It's also possible that the
		// before and after images end up going to different shards.
		// If so, an update will be treated as delete on one shard
		// and insert on the other.
		id := ev.TableID(vs.format)
		plan, ok := vs.plans[id]
		if !ok {
			return nil, nil
		}
		rows, err := ev.Rows(vs.format, plan.Table.TableMap)
		if err != nil {
			return nil, err
		}
		rowEvents := make([]*binlogdatapb.RowEvent, 0, len(rows.Rows))
		for _, row := range rows.Rows {
			beforeOK, beforeValues, err := vs.extractRowAndFilter(plan, row.Identify, rows.IdentifyColumns, row.NullIdentifyColumns)
			if err != nil {
				return nil, err
			}
			afterOK, afterValues, err := vs.extractRowAndFilter(plan, row.Data, rows.DataColumns, row.NullColumns)
			if err != nil {
				return nil, err
			}
			if !beforeOK && !afterOK {
				continue
			}
			rowEvent := &binlogdatapb.RowEvent{}
			if beforeOK {
				rowEvent.Before = sqltypes.RowToProto3(beforeValues)
			}
			if afterOK {
				rowEvent.After = sqltypes.RowToProto3(afterValues)
			}
			rowEvents = append(rowEvents, rowEvent)
		}
		vevents = append(vevents, &binlogdatapb.VEvent{
			Type:      binlogdatapb.VEventType_ROW,
			RowEvents: rowEvents,
		})
	}
	return vevents, nil
}

func (vs *vstreamer) rebuildPlans() error {
	for id, plan := range vs.plans {
		newPlan, err := buildPlan(plan.Table, vs.kschema, vs.filter)
		if err != nil {
			return err
		}
		vs.plans[id] = newPlan
	}
	return nil
}

func (vs *vstreamer) extractRowAndFilter(plan *Plan, data []byte, dataColumns, nullColumns mysql.Bitmap) (bool, []sqltypes.Value, error) {
	if len(data) == 0 {
		return false, nil, nil
	}
	values := make([]sqltypes.Value, dataColumns.Count())
	valueIndex := 0
	pos := 0
	for colNum := 0; colNum < dataColumns.Count(); colNum++ {
		if !dataColumns.Bit(colNum) {
			return false, nil, fmt.Errorf("partial row image encountered: ensure binlog_row_image is set to 'full'")
		}
		if nullColumns.Bit(valueIndex) {
			valueIndex++
			continue
		}
		value, l, err := mysql.CellValue(data, pos, plan.Table.Types[colNum], plan.Table.Metadata[colNum], plan.Table.Columns[colNum].Type)
		if err != nil {
			return false, nil, err
		}
		pos += l
		values[colNum] = value
		valueIndex++
	}
	return plan.filter(values)
}

func wrapError(err error, stopPos mysql.Position) error {
	if err != nil {
		err = fmt.Errorf("stream error @ %v: %v", stopPos, err)
	}
	log.Infof("stream ended @ %v, err: %v", stopPos, err)
	return err
}
