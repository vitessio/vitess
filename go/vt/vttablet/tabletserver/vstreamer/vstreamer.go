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
	"flag"
	"fmt"
	"io"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

// PacketSize is the suggested packet size for VReplication streamer.
var PacketSize = flag.Int("vstream_packet_size", 30000, "Suggested packet size for VReplication streamer. This is used only as a recommendation. The actual packet size may be more or less than this amount.")

// HeartbeatTime is set to slightly below 1s, compared to idleTimeout
// set by VPlayer at slightly above 1s. This minimizes conflicts
// between the two timeouts.
var HeartbeatTime = 900 * time.Millisecond

type vstreamer struct {
	ctx    context.Context
	cancel func()

	cp       *mysql.ConnParams
	se       *schema.Engine
	startPos string
	filter   *binlogdatapb.Filter
	send     func([]*binlogdatapb.VEvent) error

	// A kschema is a VSchema for just one keyspace.
	kevents chan *vindexes.KeyspaceSchema
	kschema *vindexes.KeyspaceSchema
	plans   map[uint64]*streamerPlan

	// format and pos are updated by parseEvent.
	format mysql.BinlogFormat
	pos    mysql.Position
}

// streamerPlan extends the original plan to also include
// the TableMap which is used to extract values from the binlog events.
type streamerPlan struct {
	*Plan
	TableMap *mysql.TableMap
}

func newVStreamer(ctx context.Context, cp *mysql.ConnParams, se *schema.Engine, startPos string, filter *binlogdatapb.Filter, kschema *vindexes.KeyspaceSchema, send func([]*binlogdatapb.VEvent) error) *vstreamer {
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
		plans:    make(map[uint64]*streamerPlan),
	}
}

// SetKSchema updates all existing against the new kschema.
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

	pos, err := mysql.DecodePosition(vs.startPos)
	if err != nil {
		return err
	}
	vs.pos = pos

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
	// bufferAndTransmit uses bufferedEvents and curSize to buffer events.
	var (
		bufferedEvents []*binlogdatapb.VEvent
		curSize        int
	)
	// Buffering only takes row lenghts into consideration.
	// Length of other events is considered negligible.
	// If a new row event causes the packet size to be exceeded,
	// all existing rows are sent without the new row.
	// If a single row exceeds the packet size, it will be in its own packet.
	bufferAndTransmit := func(vevent *binlogdatapb.VEvent) error {
		switch vevent.Type {
		case binlogdatapb.VEventType_GTID, binlogdatapb.VEventType_BEGIN, binlogdatapb.VEventType_FIELD:
			// We never have to send GTID, BEGIN or FIELD events on their own.
			bufferedEvents = append(bufferedEvents, vevent)
		case binlogdatapb.VEventType_COMMIT, binlogdatapb.VEventType_DDL, binlogdatapb.VEventType_HEARTBEAT:
			// COMMIT, DDL and HEARTBEAT must be immediately sent.
			bufferedEvents = append(bufferedEvents, vevent)
			vevents := bufferedEvents
			bufferedEvents = nil
			curSize = 0
			return vs.send(vevents)
		case binlogdatapb.VEventType_ROW:
			// ROW events happen inside transactions. So, we can chunk them.
			// Buffer everything until packet size is reached, and then send.
			newSize := 0
			for _, rowChange := range vevent.RowEvent.RowChanges {
				if rowChange.Before != nil {
					newSize += len(rowChange.Before.Values)
				}
				if rowChange.After != nil {
					newSize += len(rowChange.After.Values)
				}
			}
			if curSize+newSize > *PacketSize {
				vevents := bufferedEvents
				bufferedEvents = []*binlogdatapb.VEvent{vevent}
				curSize = newSize
				return vs.send(vevents)
			}
			curSize += newSize
			bufferedEvents = append(bufferedEvents, vevent)
		default:
			return fmt.Errorf("unexpected event: %v", vevent)
		}
		return nil
	}

	// Main loop: calls bufferAndTransmit as events arrive.
	timer := time.NewTimer(HeartbeatTime)
	defer timer.Stop()
	for {
		timer.Reset(HeartbeatTime)
		// Drain event if timer fired before reset.
		select {
		case <-timer.C:
		default:
		}

		select {
		case ev, ok := <-events:
			if !ok {
				select {
				case <-ctx.Done():
					return nil
				default:
				}
				return fmt.Errorf("unexpected server EOF")
			}
			vevents, err := vs.parseEvent(ev)
			if err != nil {
				return err
			}
			for _, vevent := range vevents {
				if err := bufferAndTransmit(vevent); err != nil {
					if err == io.EOF {
						return nil
					}
					return fmt.Errorf("error sending event: %v", err)
				}
			}
		case vs.kschema = <-vs.kevents:
			if err := vs.rebuildPlans(); err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		case <-timer.C:
			now := time.Now().UnixNano()
			if err := bufferAndTransmit(&binlogdatapb.VEvent{
				Type:        binlogdatapb.VEventType_HEARTBEAT,
				Timestamp:   now / 1e9,
				CurrentTime: now,
			}); err != nil {
				if err == io.EOF {
					return nil
				}
				return fmt.Errorf("error sending event: %v", err)
			}
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
	case ev.IsGTID():
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
		switch cat := sqlparser.Preview(q.SQL); cat {
		case sqlparser.StmtBegin:
			vevents = append(vevents, &binlogdatapb.VEvent{
				Type: binlogdatapb.VEventType_BEGIN,
			})
		case sqlparser.StmtCommit:
			vevents = append(vevents, &binlogdatapb.VEvent{
				Type: binlogdatapb.VEventType_COMMIT,
			})
		case sqlparser.StmtDDL:
			if mustSendDDL(q, vs.cp.DbName, vs.filter) {
				vevents = append(vevents, &binlogdatapb.VEvent{
					Type: binlogdatapb.VEventType_DDL,
					Ddl:  q.SQL,
				})
			} else {
				vevents = append(vevents,
					&binlogdatapb.VEvent{
						Type: binlogdatapb.VEventType_BEGIN,
					},
					&binlogdatapb.VEvent{
						Type: binlogdatapb.VEventType_COMMIT,
					})
			}
			// Proactively reload schema.
			// If the DDL adds a column, comparing with an older snapshot of the
			// schema will make us think that a column was dropped and error out.
			vs.se.Reload(vs.ctx)
		case sqlparser.StmtOther:
			// These are DBA statements like REPAIR that can be ignored.
		default:
			return nil, fmt.Errorf("unexpected statement type %s in row-based replication: %q", sqlparser.StmtType(cat), q.SQL)
		}
	case ev.IsTableMap():
		// This is very frequent. It precedes every row event.
		id := ev.TableID(vs.format)
		tm, err := ev.TableMap(vs.format)
		if err != nil {
			return nil, err
		}
		// We have to build a plan only for new ids.
		if _, ok := vs.plans[id]; ok {
			return nil, nil
		}
		if tm.Database != "" && tm.Database != vs.cp.DbName {
			vs.plans[id] = nil
			return nil, nil
		}
		st := vs.se.GetTable(sqlparser.NewTableIdent(tm.Name))
		if st == nil {
			return nil, fmt.Errorf("unknown table %v in schema", tm.Name)
		}
		if len(st.Columns) < len(tm.Types) {
			return nil, fmt.Errorf("cannot determine table columns for %s: event has %d columns, current schema has %d: %#v", tm.Name, len(tm.Types), len(st.Columns), ev)
		}
		table := &Table{
			Name: st.Name.String(),
			// Columns should be truncated to match those in tm.
			Columns: st.Columns[:len(tm.Types)],
		}
		plan, err := buildPlan(table, vs.kschema, vs.filter)
		if err != nil {
			return nil, err
		}
		if plan == nil {
			vs.plans[id] = nil
			return nil, nil
		}
		vs.plans[id] = &streamerPlan{
			Plan:     plan,
			TableMap: tm,
		}
		vevents = append(vevents, &binlogdatapb.VEvent{
			Type: binlogdatapb.VEventType_FIELD,
			FieldEvent: &binlogdatapb.FieldEvent{
				TableName: plan.Table.Name,
				Fields:    plan.fields(),
			},
		})
	case ev.IsWriteRows() || ev.IsDeleteRows() || ev.IsUpdateRows():
		// The existence of before and after images can be used to
		// identify statememt types. It's also possible that the
		// before and after images end up going to different shards.
		// If so, an update will be treated as delete on one shard
		// and insert on the other.
		id := ev.TableID(vs.format)
		plan := vs.plans[id]
		if plan == nil {
			return nil, nil
		}
		rows, err := ev.Rows(vs.format, plan.TableMap)
		if err != nil {
			return nil, err
		}
		rowChanges := make([]*binlogdatapb.RowChange, 0, len(rows.Rows))
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
			rowChange := &binlogdatapb.RowChange{}
			if beforeOK {
				rowChange.Before = sqltypes.RowToProto3(beforeValues)
			}
			if afterOK {
				rowChange.After = sqltypes.RowToProto3(afterValues)
			}
			rowChanges = append(rowChanges, rowChange)
		}
		if len(rowChanges) != 0 {
			vevents = append(vevents, &binlogdatapb.VEvent{
				Type: binlogdatapb.VEventType_ROW,
				RowEvent: &binlogdatapb.RowEvent{
					TableName:  plan.Table.Name,
					RowChanges: rowChanges,
				},
			})
		}
	}
	for _, vevent := range vevents {
		vevent.Timestamp = int64(ev.Timestamp())
		vevent.CurrentTime = time.Now().UnixNano()
	}
	return vevents, nil
}

func (vs *vstreamer) rebuildPlans() error {
	for id, plan := range vs.plans {
		if plan == nil {
			// If a table has no plan, a kschema change will not
			// cause that to change.
			continue
		}
		newPlan, err := buildPlan(plan.Table, vs.kschema, vs.filter)
		if err != nil {
			return err
		}
		vs.plans[id] = &streamerPlan{
			Plan:     newPlan,
			TableMap: plan.TableMap,
		}
	}
	return nil
}

func (vs *vstreamer) extractRowAndFilter(plan *streamerPlan, data []byte, dataColumns, nullColumns mysql.Bitmap) (bool, []sqltypes.Value, error) {
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
		value, l, err := mysql.CellValue(data, pos, plan.TableMap.Types[colNum], plan.TableMap.Metadata[colNum], plan.Table.Columns[colNum].Type)
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
		log.Error(err)
		return err
	}
	log.Infof("stream ended @ %v", stopPos)
	return nil
}
