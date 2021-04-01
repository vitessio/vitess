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
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	vtschema "vitess.io/vitess/go/vt/schema"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/binlog"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// PacketSize is the suggested packet size for VReplication streamer.
var PacketSize = flag.Int("vstream_packet_size", 250000, "Suggested packet size for VReplication streamer. This is used only as a recommendation. The actual packet size may be more or less than this amount.")

// HeartbeatTime is set to slightly below 1s, compared to idleTimeout
// set by VPlayer at slightly above 1s. This minimizes conflicts
// between the two timeouts.
var HeartbeatTime = 900 * time.Millisecond

// vschemaUpdateCount is for testing only.
// vstreamer is a mutex free data structure. So, it's not safe to access its members
// from a test. Since VSchema gets updated asynchronously, there's no way for a test
// to wait for it. Instead, the code that updates the vschema increments this atomic
// counter, which will let the tests poll for it to change.
// TODO(sougou): find a better way for this.
var vschemaUpdateCount sync2.AtomicInt64

// vstreamer is for serving a single vreplication stream on the source side.
type vstreamer struct {
	ctx    context.Context
	cancel func()

	cp       dbconfigs.Connector
	se       *schema.Engine
	startPos string
	filter   *binlogdatapb.Filter
	send     func([]*binlogdatapb.VEvent) error

	vevents        chan *localVSchema
	vschema        *localVSchema
	plans          map[uint64]*streamerPlan
	journalTableID uint64
	versionTableID uint64

	// format and pos are updated by parseEvent.
	format  mysql.BinlogFormat
	pos     mysql.Position
	stopPos string

	phase string
	vse   *Engine
}

// CopyState contains the last PK for tables to be copied
type CopyState map[string][]*sqltypes.Result

// streamerPlan extends the original plan to also include
// the TableMap, which comes from the binlog. It's used
// to extract values from the ROW events.
type streamerPlan struct {
	*Plan
	TableMap *mysql.TableMap
}

// newVStreamer creates a new vstreamer.
// cp: the mysql conn params.
// sh: the schema engine. The vstreamer uses it to convert the TableMap into field info.
// startPos: a flavor compliant position to stream from. This can also contain the special
//   value "current", which means start from the current position.
// filter: the list of filtering rules. If a rule has a select expression for its filter,
//   the select list can only reference direct columns. No other expressions are allowed.
//   The select expression is allowed to contain the special 'keyspace_id()' function which
//   will return the keyspace id of the row. Examples:
//   "select * from t", same as an empty Filter,
//   "select * from t where in_keyrange('-80')", same as "-80",
//   "select * from t where in_keyrange(col1, 'hash', '-80')",
//   "select col1, col2 from t where...",
//   "select col1, keyspace_id() from t where...".
//   Only "in_keyrange" expressions are supported in the where clause.
//   Other constructs like joins, group by, etc. are not supported.
// vschema: the current vschema. This value can later be changed through the SetVSchema method.
// send: callback function to send events.
func newVStreamer(ctx context.Context, cp dbconfigs.Connector, se *schema.Engine, startPos string, stopPos string, filter *binlogdatapb.Filter, vschema *localVSchema, send func([]*binlogdatapb.VEvent) error, phase string, vse *Engine) *vstreamer {
	ctx, cancel := context.WithCancel(ctx)
	return &vstreamer{
		ctx:      ctx,
		cancel:   cancel,
		cp:       cp,
		se:       se,
		startPos: startPos,
		stopPos:  stopPos,
		filter:   filter,
		send:     send,
		vevents:  make(chan *localVSchema, 1),
		vschema:  vschema,
		plans:    make(map[uint64]*streamerPlan),
		phase:    phase,
		vse:      vse,
	}
}

// SetVSchema updates the vstreamer against the new vschema.
func (vs *vstreamer) SetVSchema(vschema *localVSchema) {
	// Since vs.Stream is a single-threaded loop. We just send an event to
	// that thread, which helps us avoid mutexes to update the plans.
	select {
	case vs.vevents <- vschema:
	case <-vs.ctx.Done():
	}
}

// Cancel stops the streaming.
func (vs *vstreamer) Cancel() {
	vs.cancel()
}

// Stream streams binlog events.
func (vs *vstreamer) Stream() error {
	//defer vs.cancel()
	ctx := context.Background()
	defer ctx.Done()
	vs.vse.vstreamersCreated.Add(1)
	log.Infof("Starting Stream() with startPos %s", vs.startPos)
	pos, err := mysql.DecodePosition(vs.startPos)
	if err != nil {
		vs.vse.errorCounts.Add("StreamRows", 1)
		vs.vse.vstreamersEndedWithErrors.Add(1)
		return err
	}
	vs.pos = pos
	return vs.replicate(ctx)
}

// Stream streams binlog events.
func (vs *vstreamer) replicate(ctx context.Context) error {
	// Ensure se is Open. If vttablet came up in a non_serving role,
	// the schema engine may not have been initialized.
	if err := vs.se.Open(); err != nil {
		return wrapError(err, vs.pos, vs.vse)
	}

	conn, err := binlog.NewBinlogConnection(vs.cp)
	if err != nil {
		return wrapError(err, vs.pos, vs.vse)
	}
	defer conn.Close()

	events, err := conn.StartBinlogDumpFromPosition(vs.ctx, vs.pos)
	if err != nil {
		return wrapError(err, vs.pos, vs.vse)
	}
	err = vs.parseEvents(vs.ctx, events)
	return wrapError(err, vs.pos, vs.vse)
}

// parseEvents parses and sends events.
func (vs *vstreamer) parseEvents(ctx context.Context, events <-chan mysql.BinlogEvent) error {
	// bufferAndTransmit uses bufferedEvents and curSize to buffer events.
	var (
		bufferedEvents []*binlogdatapb.VEvent
		curSize        int
	)
	// Only the following patterns are possible:
	// BEGIN->ROWs or Statements->GTID->COMMIT. In the case of large transactions, this can be broken into chunks.
	// BEGIN->JOURNAL->GTID->COMMIT
	// GTID->DDL
	// GTID->OTHER
	// HEARTBEAT is issued if there's inactivity, which is likely
	// to heppend between one group of events and another.
	//
	// Buffering only takes row or statement lengths into consideration.
	// Length of other events is considered negligible.
	// If a new row event causes the packet size to be exceeded,
	// all existing rows are sent without the new row.
	// If a single row exceeds the packet size, it will be in its own packet.
	bufferAndTransmit := func(vevent *binlogdatapb.VEvent) error {
		switch vevent.Type {
		case binlogdatapb.VEventType_GTID, binlogdatapb.VEventType_BEGIN, binlogdatapb.VEventType_FIELD,
			binlogdatapb.VEventType_JOURNAL:
			// We never have to send GTID, BEGIN, FIELD events on their own.
			// A JOURNAL event is always preceded by a BEGIN and followed by a COMMIT.
			// So, we don't have to send it right away.
			bufferedEvents = append(bufferedEvents, vevent)
		case binlogdatapb.VEventType_COMMIT, binlogdatapb.VEventType_DDL, binlogdatapb.VEventType_OTHER,
			binlogdatapb.VEventType_HEARTBEAT, binlogdatapb.VEventType_VERSION:
			// COMMIT, DDL, OTHER and HEARTBEAT must be immediately sent.
			// Although unlikely, it's possible to get a HEARTBEAT in the middle
			// of a transaction. If so, we still send the partial transaction along
			// with the heartbeat.
			bufferedEvents = append(bufferedEvents, vevent)
			vevents := bufferedEvents
			bufferedEvents = nil
			curSize = 0
			return vs.send(vevents)
		case binlogdatapb.VEventType_INSERT, binlogdatapb.VEventType_DELETE, binlogdatapb.VEventType_UPDATE, binlogdatapb.VEventType_REPLACE:
			newSize := len(vevent.GetDml())
			if curSize+newSize > *PacketSize {
				vs.vse.vstreamerNumPackets.Add(1)
				vevents := bufferedEvents
				bufferedEvents = []*binlogdatapb.VEvent{vevent}
				curSize = newSize
				return vs.send(vevents)
			}
			curSize += newSize
			bufferedEvents = append(bufferedEvents, vevent)
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
				vs.vse.vstreamerNumPackets.Add(1)
				vevents := bufferedEvents
				bufferedEvents = []*binlogdatapb.VEvent{vevent}
				curSize = newSize
				return vs.send(vevents)
			}
			curSize += newSize
			bufferedEvents = append(bufferedEvents, vevent)
		case binlogdatapb.VEventType_SAVEPOINT:
			bufferedEvents = append(bufferedEvents, vevent)
		default:
			vs.vse.errorCounts.Add("BufferAndTransmit", 1)
			return fmt.Errorf("unexpected event: %v", vevent)
		}
		return nil
	}

	// Main loop: calls bufferAndTransmit as events arrive.
	timer := time.NewTimer(HeartbeatTime)
	defer timer.Stop()

	// throttledEvents can be read just like you would read from events
	// throttledEvents pulls data from events, but throttles pulling data,
	// which in turn blocks the BinlogConnection from pushing events to the channel
	throttledEvents := make(chan mysql.BinlogEvent)
	go func() {
		for {
			// check throttler.
			if !vs.vse.throttlerClient.ThrottleCheckOKOrWait(ctx) {
				continue
			}

			ev, ok := <-events
			if ok {
				throttledEvents <- ev
			} else {
				close(throttledEvents)
				return
			}
		}
	}()
	for {
		timer.Reset(HeartbeatTime)
		// Drain event if timer fired before reset.
		select {
		case <-timer.C:
		default:
		}

		select {
		case ev, ok := <-throttledEvents:
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
				vs.vse.errorCounts.Add("ParseEvent", 1)
				return err
			}
			for _, vevent := range vevents {
				if err := bufferAndTransmit(vevent); err != nil {
					if err == io.EOF {
						return nil
					}
					vs.vse.errorCounts.Add("BufferAndTransmit", 1)
					return fmt.Errorf("error sending event: %v", err)
				}
			}
		case vs.vschema = <-vs.vevents:
			if err := vs.rebuildPlans(); err != nil {
				return err
			}
			// Increment this counter for testing.
			vschemaUpdateCount.Add(1)
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
				vs.vse.errorCounts.Add("Send", 1)
				return fmt.Errorf("error sending event: %v", err)
			}
		}
	}
}

// parseEvent parses an event from the binlog and converts it to a list of VEvents.
func (vs *vstreamer) parseEvent(ev mysql.BinlogEvent) ([]*binlogdatapb.VEvent, error) {
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
		vs.pos = mysql.AppendGTID(vs.pos, gtid) //TODO: #sugu why Append?
	case ev.IsXID():
		vevents = append(vevents, &binlogdatapb.VEvent{
			Type: binlogdatapb.VEventType_GTID,
			Gtid: mysql.EncodePosition(vs.pos),
		}, &binlogdatapb.VEvent{
			Type: binlogdatapb.VEventType_COMMIT,
		})
	case ev.IsQuery():
		q, err := ev.Query(vs.format)
		if err != nil {
			return nil, fmt.Errorf("can't get query from binlog event: %v, event data: %#v", err, ev)
		}
		// Insert/Delete/Update are supported only to be used in the context of external mysql streams where source databases
		// could be using SBR. Vitess itself will never run into cases where it needs to consume non rbr statements.

		switch cat := sqlparser.Preview(q.SQL); cat {
		case sqlparser.StmtInsert:
			mustSend := mustSendStmt(q, vs.cp.DBName())
			if mustSend {
				vevents = append(vevents, &binlogdatapb.VEvent{
					Type: binlogdatapb.VEventType_INSERT,
					Dml:  q.SQL,
				})
			}
		case sqlparser.StmtUpdate:
			mustSend := mustSendStmt(q, vs.cp.DBName())
			if mustSend {
				vevents = append(vevents, &binlogdatapb.VEvent{
					Type: binlogdatapb.VEventType_UPDATE,
					Dml:  q.SQL,
				})
			}
		case sqlparser.StmtDelete:
			mustSend := mustSendStmt(q, vs.cp.DBName())
			if mustSend {
				vevents = append(vevents, &binlogdatapb.VEvent{
					Type: binlogdatapb.VEventType_DELETE,
					Dml:  q.SQL,
				})
			}
		case sqlparser.StmtReplace:
			mustSend := mustSendStmt(q, vs.cp.DBName())
			if mustSend {
				vevents = append(vevents, &binlogdatapb.VEvent{
					Type: binlogdatapb.VEventType_REPLACE,
					Dml:  q.SQL,
				})
			}
		case sqlparser.StmtBegin:
			vevents = append(vevents, &binlogdatapb.VEvent{
				Type: binlogdatapb.VEventType_BEGIN,
			})
		case sqlparser.StmtCommit:
			vevents = append(vevents, &binlogdatapb.VEvent{
				Type: binlogdatapb.VEventType_COMMIT,
			})
		case sqlparser.StmtDDL:
			if mustSendDDL(q, vs.cp.DBName(), vs.filter) {
				vevents = append(vevents, &binlogdatapb.VEvent{
					Type: binlogdatapb.VEventType_GTID,
					Gtid: mysql.EncodePosition(vs.pos),
				}, &binlogdatapb.VEvent{
					Type:      binlogdatapb.VEventType_DDL,
					Statement: q.SQL,
				})
			} else {
				// If the DDL need not be sent, send a dummy OTHER event.
				vevents = append(vevents, &binlogdatapb.VEvent{
					Type: binlogdatapb.VEventType_GTID,
					Gtid: mysql.EncodePosition(vs.pos),
				}, &binlogdatapb.VEvent{
					Type: binlogdatapb.VEventType_OTHER,
				})
			}
			vs.se.ReloadAt(context.Background(), vs.pos)
		case sqlparser.StmtSavepoint:
			mustSend := mustSendStmt(q, vs.cp.DBName())
			if mustSend {
				vevents = append(vevents, &binlogdatapb.VEvent{
					Type:      binlogdatapb.VEventType_SAVEPOINT,
					Statement: q.SQL,
				})
			}
		case sqlparser.StmtOther, sqlparser.StmtPriv, sqlparser.StmtSet, sqlparser.StmtComment, sqlparser.StmtFlush:
			// These are either:
			// 1) DBA statements like REPAIR that can be ignored.
			// 2) Privilege-altering statements like GRANT/REVOKE
			//    that we want to keep out of the stream for now.
			vevents = append(vevents, &binlogdatapb.VEvent{
				Type: binlogdatapb.VEventType_GTID,
				Gtid: mysql.EncodePosition(vs.pos),
			}, &binlogdatapb.VEvent{
				Type: binlogdatapb.VEventType_OTHER,
			})
		default:
			return nil, fmt.Errorf("unexpected statement type %s in row-based replication: %q", cat, q.SQL)
		}
	case ev.IsTableMap():
		// This is very frequent. It precedes every row event.
		// If it's the first time for a table, we generate a FIELD
		// event, and also cache the plan. Subsequent TableMap events
		// for that table id don't generate VEvents.
		// A schema change will result in a change in table id, which
		// will generate a new plan and FIELD event.
		id := ev.TableID(vs.format)

		if _, ok := vs.plans[id]; ok {
			return nil, nil
		}

		tm, err := ev.TableMap(vs.format)
		if err != nil {
			return nil, err
		}
		if tm.Database == "_vt" && tm.Name == "resharding_journal" {
			// A journal is a special case that generates a JOURNAL event.
			return nil, vs.buildJournalPlan(id, tm)
		} else if tm.Database == "_vt" && tm.Name == "schema_version" && !vs.se.SkipMetaCheck {
			// Generates a Version event when it detects that a schema is stored in the schema_version table.
			return nil, vs.buildVersionPlan(id, tm)
		}
		if tm.Database != "" && tm.Database != vs.cp.DBName() {
			vs.plans[id] = nil
			return nil, nil
		}
		if vtschema.IsInternalOperationTableName(tm.Name) { // ignore tables created by onlineddl/gh-ost/pt-osc
			vs.plans[id] = nil
			return nil, nil
		}
		if !ruleMatches(tm.Name, vs.filter) {
			return nil, nil
		}

		vevent, err := vs.buildTablePlan(id, tm)
		if err != nil {
			vs.vse.errorCounts.Add("TablePlan", 1)
			return nil, err
		}
		if vevent != nil {
			vevents = append(vevents, vevent)
		}
	case ev.IsWriteRows() || ev.IsDeleteRows() || ev.IsUpdateRows():
		// The existence of before and after images can be used to
		// identify statement types. It's also possible that the
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
		if id == vs.journalTableID {
			vevents, err = vs.processJournalEvent(vevents, plan, rows)
		} else if id == vs.versionTableID {
			vs.se.RegisterVersionEvent()
			vevent := &binlogdatapb.VEvent{
				Type: binlogdatapb.VEventType_VERSION,
			}
			vevents = append(vevents, vevent)

		} else {
			vevents, err = vs.processRowEvent(vevents, plan, rows)
		}
		if err != nil {
			return nil, err
		}
	case ev.IsCompressed():
		log.Errorf("VReplication does not handle binlog compression")
		return nil, fmt.Errorf("VReplication does not handle binlog compression")
	}
	for _, vevent := range vevents {
		vevent.Timestamp = int64(ev.Timestamp())
		vevent.CurrentTime = time.Now().UnixNano()
	}
	return vevents, nil
}

func (vs *vstreamer) buildJournalPlan(id uint64, tm *mysql.TableMap) error {
	conn, err := vs.cp.Connect(vs.ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	qr, err := conn.ExecuteFetch("select * from _vt.resharding_journal where 1 != 1", 1, true)
	if err != nil {
		return err
	}
	fields := qr.Fields
	if len(fields) < len(tm.Types) {
		return fmt.Errorf("cannot determine table columns for %s: event has %v, schema as %v", tm.Name, tm.Types, fields)
	}
	table := &Table{
		Name:   "_vt.resharding_journal",
		Fields: fields[:len(tm.Types)],
	}
	// Build a normal table plan, which means, return all rows
	// and columns as is. Special handling is done when we actually
	// receive the row event. We'll build a JOURNAL event instead.
	plan, err := buildREPlan(table, nil, "")
	if err != nil {
		return err
	}
	vs.plans[id] = &streamerPlan{
		Plan:     plan,
		TableMap: tm,
	}
	vs.journalTableID = id
	return nil
}

func (vs *vstreamer) buildVersionPlan(id uint64, tm *mysql.TableMap) error {
	conn, err := vs.cp.Connect(vs.ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	qr, err := conn.ExecuteFetch("select * from _vt.schema_version where 1 != 1", 1, true)
	if err != nil {
		return err
	}
	fields := qr.Fields
	if len(fields) < len(tm.Types) {
		return fmt.Errorf("cannot determine table columns for %s: event has %v, schema as %v", tm.Name, tm.Types, fields)
	}
	table := &Table{
		Name:   "_vt.schema_version",
		Fields: fields[:len(tm.Types)],
	}
	// Build a normal table plan, which means, return all rows
	// and columns as is. Special handling is done when we actually
	// receive the row event. We'll build a JOURNAL event instead.
	plan, err := buildREPlan(table, nil, "")
	if err != nil {
		return err
	}
	vs.plans[id] = &streamerPlan{
		Plan:     plan,
		TableMap: tm,
	}
	vs.versionTableID = id
	return nil
}

func (vs *vstreamer) buildTablePlan(id uint64, tm *mysql.TableMap) (*binlogdatapb.VEvent, error) {
	cols, err := vs.buildTableColumns(tm)
	if err != nil {
		return nil, err
	}

	table := &Table{
		Name:   tm.Name,
		Fields: cols,
	}
	plan, err := buildPlan(table, vs.vschema, vs.filter)
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
	return &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_FIELD,
		FieldEvent: &binlogdatapb.FieldEvent{
			TableName: plan.Table.Name,
			Fields:    plan.fields(),
		},
	}, nil
}

func (vs *vstreamer) buildTableColumns(tm *mysql.TableMap) ([]*querypb.Field, error) {
	var fields []*querypb.Field
	for i, typ := range tm.Types {
		t, err := sqltypes.MySQLToType(int64(typ), 0)
		if err != nil {
			return nil, fmt.Errorf("unsupported type: %d, position: %d", typ, i)
		}
		fields = append(fields, &querypb.Field{
			Name: fmt.Sprintf("@%d", i+1),
			Type: t,
		})
	}
	st, err := vs.se.GetTableForPos(sqlparser.NewTableIdent(tm.Name), mysql.EncodePosition(vs.pos))
	if err != nil {
		if vs.filter.FieldEventMode == binlogdatapb.Filter_ERR_ON_MISMATCH {
			log.Infof("No schema found for table %s", tm.Name)
			return nil, fmt.Errorf("unknown table %v in schema", tm.Name)
		}
		return fields, nil
	}

	if len(st.Fields) < len(tm.Types) {
		if vs.filter.FieldEventMode == binlogdatapb.Filter_ERR_ON_MISMATCH {
			log.Infof("Cannot determine columns for table %s", tm.Name)
			return nil, fmt.Errorf("cannot determine table columns for %s: event has %v, schema as %v", tm.Name, tm.Types, st.Fields)
		}
		return fields, nil
	}

	// check if the schema returned by schema.Engine matches with row.
	for i := range tm.Types {
		if !sqltypes.AreTypesEquivalent(fields[i].Type, st.Fields[i].Type) {
			return fields, nil
		}
	}

	// Columns should be truncated to match those in tm.
	fields = st.Fields[:len(tm.Types)]
	extColInfos, err := vs.getExtColInfos(tm.Name, tm.Database)
	if err != nil {
		return nil, err
	}
	for _, field := range fields {
		typ := strings.ToLower(field.Type.String())
		if typ == "enum" || typ == "set" {
			if extColInfo, ok := extColInfos[field.Name]; ok {
				field.ColumnType = extColInfo.columnType
			}
		}
	}
	return fields, nil
}

// additional column attributes from information_schema.columns. Currently only column_type is used, but
// we expect to add more in the future
type extColInfo struct {
	columnType string
}

func encodeString(in string) string {
	buf := bytes.NewBuffer(nil)
	sqltypes.NewVarChar(in).EncodeSQL(buf)
	return buf.String()
}

func (vs *vstreamer) getExtColInfos(table, database string) (map[string]*extColInfo, error) {
	extColInfos := make(map[string]*extColInfo)
	conn, err := vs.cp.Connect(vs.ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	queryTemplate := "select column_name, column_type from information_schema.columns where table_schema=%s and table_name=%s;"
	query := fmt.Sprintf(queryTemplate, encodeString(database), encodeString(table))
	qr, err := conn.ExecuteFetch(query, 10000, false)
	if err != nil {
		return nil, err
	}
	for _, row := range qr.Rows {
		extColInfo := &extColInfo{
			columnType: row[1].ToString(),
		}
		extColInfos[row[0].ToString()] = extColInfo
	}
	return extColInfos, nil
}

func (vs *vstreamer) processJournalEvent(vevents []*binlogdatapb.VEvent, plan *streamerPlan, rows mysql.Rows) ([]*binlogdatapb.VEvent, error) {
	// Get DbName
	params, err := vs.cp.MysqlParams()
	if err != nil {
		return nil, err
	}
nextrow:
	for _, row := range rows.Rows {
		afterOK, afterValues, err := vs.extractRowAndFilter(plan, row.Data, rows.DataColumns, row.NullColumns)
		if err != nil {
			return nil, err
		}
		if !afterOK {
			// This can happen if someone manually deleted rows.
			continue
		}
		// Exclude events that don't match the db_name.
		for i, fld := range plan.fields() {
			if fld.Name == "db_name" && afterValues[i].ToString() != params.DbName {
				continue nextrow
			}
		}
		for i, fld := range plan.fields() {
			if fld.Name != "val" {
				continue
			}
			journal := &binlogdatapb.Journal{}
			if err := proto.UnmarshalText(afterValues[i].ToString(), journal); err != nil {
				return nil, err
			}
			vevents = append(vevents, &binlogdatapb.VEvent{
				Type:    binlogdatapb.VEventType_JOURNAL,
				Journal: journal,
			})
		}
	}
	return vevents, nil
}

func (vs *vstreamer) processRowEvent(vevents []*binlogdatapb.VEvent, plan *streamerPlan, rows mysql.Rows) ([]*binlogdatapb.VEvent, error) {
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
	return vevents, nil
}

func (vs *vstreamer) rebuildPlans() error {
	for id, plan := range vs.plans {
		if plan == nil {
			// If a table has no plan, a vschema change will not
			// cause that to change.
			continue
		}
		newPlan, err := buildPlan(plan.Table, vs.vschema, vs.filter)
		if err != nil {
			return err
		}
		if newPlan == nil {
			continue
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
		value, l, err := mysql.CellValue(data, pos, plan.TableMap.Types[colNum], plan.TableMap.Metadata[colNum], plan.Table.Fields[colNum].Type)
		if err != nil {
			return false, nil, err
		}
		pos += l
		values[colNum] = value
		valueIndex++
	}
	return plan.filter(values)
}

func wrapError(err error, stopPos mysql.Position, vse *Engine) error {
	if err != nil {
		vse.vstreamersEndedWithErrors.Add(1)
		vse.errorCounts.Add("StreamEnded", 1)
		err = fmt.Errorf("stream (at source tablet) error @ %v: %v", stopPos, err)
		log.Error(err)
		return err
	}
	log.Infof("stream (at source tablet) ended @ %v", stopPos)
	return nil
}
