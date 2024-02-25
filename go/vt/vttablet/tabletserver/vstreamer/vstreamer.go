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
	"fmt"
	"io"
	"time"

	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/mysql"
	mysqlbinlog "vitess.io/vitess/go/mysql/binlog"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/binlog"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	vtschema "vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

const (
	trxHistoryLenQuery = `select count as history_len from information_schema.INNODB_METRICS where name = 'trx_rseg_history_len'`
	replicaLagQuery    = `show slave status`
	hostQuery          = `select @@hostname as hostname, @@port as port`
)

// HeartbeatTime is set to slightly below 1s, compared to idleTimeout
// set by VPlayer at slightly above 1s. This minimizes conflicts
// between the two timeouts.
var HeartbeatTime = 900 * time.Millisecond

// vstreamer is for serving a single vreplication stream on the source side.
type vstreamer struct {
	ctx    context.Context
	cancel func()

	cp           dbconfigs.Connector
	se           *schema.Engine
	startPos     string
	filter       *binlogdatapb.Filter
	send         func([]*binlogdatapb.VEvent) error
	throttlerApp throttlerapp.Name

	vevents        chan *localVSchema
	vschema        *localVSchema
	plans          map[uint64]*streamerPlan
	journalTableID uint64
	versionTableID uint64

	// format and pos are updated by parseEvent.
	format  mysql.BinlogFormat
	pos     replication.Position
	stopPos string

	phase string
	vse   *Engine
}

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
//
//	value "current", which means start from the current position.
//
// filter: the list of filtering rules. If a rule has a select expression for its filter,
//
//	the select list can only reference direct columns. No other expressions are allowed.
//	The select expression is allowed to contain the special 'keyspace_id()' function which
//	will return the keyspace id of the row. Examples:
//	"select * from t", same as an empty Filter,
//	"select * from t where in_keyrange('-80')", same as "-80",
//	"select * from t where in_keyrange(col1, 'hash', '-80')",
//	"select col1, col2 from t where...",
//	"select col1, keyspace_id() from t where...".
//	Only "in_keyrange" and limited comparison operators (see enum Opcode in planbuilder.go) are supported in the where clause.
//	Other constructs like joins, group by, etc. are not supported.
//
// vschema: the current vschema. This value can later be changed through the SetVSchema method.
// send: callback function to send events.
func newVStreamer(ctx context.Context, cp dbconfigs.Connector, se *schema.Engine, startPos string, stopPos string, filter *binlogdatapb.Filter, vschema *localVSchema, throttlerApp throttlerapp.Name, send func([]*binlogdatapb.VEvent) error, phase string, vse *Engine) *vstreamer {
	ctx, cancel := context.WithCancel(ctx)
	return &vstreamer{
		ctx:          ctx,
		cancel:       cancel,
		cp:           cp,
		se:           se,
		startPos:     startPos,
		stopPos:      stopPos,
		throttlerApp: throttlerApp,
		filter:       filter,
		send:         send,
		vevents:      make(chan *localVSchema, 1),
		vschema:      vschema,
		plans:        make(map[uint64]*streamerPlan),
		phase:        phase,
		vse:          vse,
	}
}

// SetVSchema updates the vstreamer against the new vschema.
func (vs *vstreamer) SetVSchema(vschema *localVSchema) {
	// Since vs.Stream is a single-threaded loop. We just send an event to
	// that thread, which helps us avoid mutexes to update the plans.
	select {
	case vs.vevents <- vschema:
	case <-vs.ctx.Done():
	default: // if there is a pending vschema in the channel, drain it and update it with the latest one
		select {
		case <-vs.vevents:
			vs.vevents <- vschema
		default:
		}
	}
}

// Cancel stops the streaming.
func (vs *vstreamer) Cancel() {
	vs.cancel()
}

// Stream streams binlog events.
func (vs *vstreamer) Stream() error {
	ctx := context.Background()
	vs.vse.vstreamerCount.Add(1)
	defer func() {
		ctx.Done()
		vs.vse.vstreamerCount.Add(-1)
	}()
	vs.vse.vstreamersCreated.Add(1)
	log.Infof("Starting Stream() with startPos %s", vs.startPos)
	pos, err := replication.DecodePosition(vs.startPos)
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

	events, errs, err := conn.StartBinlogDumpFromPosition(vs.ctx, "", vs.pos)
	if err != nil {
		return wrapError(err, vs.pos, vs.vse)
	}
	err = vs.parseEvents(vs.ctx, events, errs)
	return wrapError(err, vs.pos, vs.vse)
}

// parseEvents parses and sends events.
func (vs *vstreamer) parseEvents(ctx context.Context, events <-chan mysql.BinlogEvent, errs <-chan error) error {
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
	// to happen between one group of events and another.
	//
	// Buffering only takes row or statement lengths into consideration.
	// Length of other events is considered negligible.
	// If a new row event causes the packet size to be exceeded,
	// all existing rows are sent without the new row.
	// If a single row exceeds the packet size, it will be in its own packet.
	bufferAndTransmit := func(vevent *binlogdatapb.VEvent) error {
		vevent.Keyspace = vs.vse.keyspace
		vevent.Shard = vs.vse.shard

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
			if curSize+newSize > defaultPacketSize {
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
			if curSize+newSize > defaultPacketSize {
				vs.vse.vstreamerNumPackets.Add(1)
				vevents := bufferedEvents
				bufferedEvents = []*binlogdatapb.VEvent{vevent}
				curSize = newSize
				return vs.send(vevents)
			}
			curSize += newSize
			bufferedEvents = append(bufferedEvents, vevent)
		default:
			vs.vse.errorCounts.Add("BufferAndTransmit", 1)
			return fmt.Errorf("unexpected event: %v", vevent)
		}
		return nil
	}

	// Main loop: calls bufferAndTransmit as events arrive.
	hbTimer := time.NewTimer(HeartbeatTime)
	defer hbTimer.Stop()

	injectHeartbeat := func(throttled bool) error {
		now := time.Now().UnixNano()
		select {
		case <-ctx.Done():
			return vterrors.Errorf(vtrpcpb.Code_CANCELED, "context has expired")
		default:
			err := bufferAndTransmit(&binlogdatapb.VEvent{
				Type:        binlogdatapb.VEventType_HEARTBEAT,
				Timestamp:   now / 1e9,
				CurrentTime: now,
				Throttled:   throttled,
			})
			return err
		}
	}

	throttleEvents := func(throttledEvents chan mysql.BinlogEvent) {
		throttledHeartbeatsRateLimiter := timer.NewRateLimiter(HeartbeatTime)
		defer throttledHeartbeatsRateLimiter.Stop()
		for {
			// check throttler.
			if !vs.vse.throttlerClient.ThrottleCheckOKOrWaitAppName(ctx, vs.throttlerApp) {
				// make sure to leave if context is cancelled
				select {
				case <-ctx.Done():
					return
				default:
					// do nothing special
				}
				throttledHeartbeatsRateLimiter.Do(func() error {
					return injectHeartbeat(true)
				})
				// we won't process events, until we're no longer throttling
				continue
			}
			select {
			case ev, ok := <-events:
				if ok {
					select {
					case throttledEvents <- ev:
					case <-ctx.Done():
						return
					}
				} else {
					close(throttledEvents)
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}
	// throttledEvents can be read just like you would read from events
	// throttledEvents pulls data from events, but throttles pulling data,
	// which in turn blocks the BinlogConnection from pushing events to the channel
	throttledEvents := make(chan mysql.BinlogEvent)
	go throttleEvents(throttledEvents)

	for {
		hbTimer.Reset(HeartbeatTime)
		// Drain event if timer fired before reset.
		select {
		case <-hbTimer.C:
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
			select {
			case <-ctx.Done():
				return nil
			default:
				if err := vs.rebuildPlans(); err != nil {
					return err
				}
			}
		case err := <-errs:
			return err
		case <-ctx.Done():
			return nil
		case <-hbTimer.C:
			if err := injectHeartbeat(false); err != nil {
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
		// is a fake ROTATE_EVENT, which the primary sends to tell us the name
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
		vs.pos = replication.AppendGTID(vs.pos, gtid)
	case ev.IsXID():
		vevents = append(vevents, &binlogdatapb.VEvent{
			Type: binlogdatapb.VEventType_GTID,
			Gtid: replication.EncodePosition(vs.pos),
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
			if mustSendDDL(q, vs.cp.DBName(), vs.filter, vs.vse.env.Environment().Parser()) {
				vevents = append(vevents, &binlogdatapb.VEvent{
					Type: binlogdatapb.VEventType_GTID,
					Gtid: replication.EncodePosition(vs.pos),
				}, &binlogdatapb.VEvent{
					Type:      binlogdatapb.VEventType_DDL,
					Statement: q.SQL,
				})
			} else {
				// If the DDL need not be sent, send a dummy OTHER event.
				vevents = append(vevents, &binlogdatapb.VEvent{
					Type: binlogdatapb.VEventType_GTID,
					Gtid: replication.EncodePosition(vs.pos),
				}, &binlogdatapb.VEvent{
					Type: binlogdatapb.VEventType_OTHER,
				})
			}
			if schema.MustReloadSchemaOnDDL(q.SQL, vs.cp.DBName(), vs.vse.env.Environment().Parser()) {
				vs.se.ReloadAt(context.Background(), vs.pos)
			}
		case sqlparser.StmtSavepoint:
			// We currently completely skip `SAVEPOINT ...` statements.
			//
			// MySQL inserts `SAVEPOINT ...` statements into the binlog in row based, statement based
			// and in mixed replication modes, but only ever writes `ROLLBACK TO ...` statements to the
			// binlog in mixed or statement based replication modes. Without `ROLLBACK TO ...` statements,
			// savepoints are side-effect free.
			//
			// Vitess only supports row based replication, so skipping the creation of savepoints
			// reduces the amount of data send over to vplayer.
		case sqlparser.StmtOther, sqlparser.StmtAnalyze, sqlparser.StmtPriv, sqlparser.StmtSet, sqlparser.StmtComment, sqlparser.StmtFlush:
			// These are either:
			// 1) DBA statements like REPAIR that can be ignored.
			// 2) Privilege-altering statements like GRANT/REVOKE
			//    that we want to keep out of the stream for now.
			vevents = append(vevents, &binlogdatapb.VEvent{
				Type: binlogdatapb.VEventType_GTID,
				Gtid: replication.EncodePosition(vs.pos),
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

		tm, err := ev.TableMap(vs.format)
		if err != nil {
			return nil, err
		}

		if plan, ok := vs.plans[id]; ok {
			// When the underlying mysql server restarts the table map can change.
			// Usually the vstreamer will also error out when this happens, and vstreamer re-initializes its table map.
			// But if the vstreamer is not aware of the restart, we could get an id that matches one in the cache, but
			// is for a different table. We then invalidate and recompute the plan for this id.
			if plan == nil || plan.Table.Name == tm.Name {
				return nil, nil
			}
			vs.plans[id] = nil
			log.Infof("table map changed: id %d for %s has changed to %s", id, plan.Table.Name, tm.Name)
		}

		if tm.Database == sidecar.GetName() && tm.Name == "resharding_journal" {
			// A journal is a special case that generates a JOURNAL event.
			return nil, vs.buildJournalPlan(id, tm)
		} else if tm.Database == sidecar.GetName() && tm.Name == "schema_version" && !vs.se.SkipMetaCheck {
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
	case ev.IsTransactionPayload():
		if !vs.pos.MatchesFlavor(replication.Mysql56FlavorID) {
			return nil, fmt.Errorf("compressed transaction payload events are not supported with database flavor %s",
				vs.vse.env.Config().DB.Flavor)
		}
		tpevents, err := ev.TransactionPayload(vs.format)
		if err != nil {
			return nil, err
		}
		// Events inside the payload don't have their own checksum.
		ogca := vs.format.ChecksumAlgorithm
		defer func() { vs.format.ChecksumAlgorithm = ogca }()
		vs.format.ChecksumAlgorithm = mysql.BinlogChecksumAlgOff
		for _, tpevent := range tpevents {
			tpvevents, err := vs.parseEvent(tpevent)
			if err != nil {
				return nil, vterrors.Wrap(err, "failed to parse transaction payload's internal event")
			}
			vevents = append(vevents, tpvevents...)
		}
		vs.vse.vstreamerCompressedTransactionsDecoded.Add(1)
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
	qr, err := conn.ExecuteFetch(sqlparser.BuildParsedQuery("select * from %s.resharding_journal where 1 != 1",
		sidecar.GetIdentifier()).Query, 1, true)
	if err != nil {
		return err
	}
	fields := qr.Fields
	if len(fields) < len(tm.Types) {
		return fmt.Errorf("cannot determine table columns for %s: event has %v, schema has %v", tm.Name, tm.Types, fields)
	}
	table := &Table{
		Name:   fmt.Sprintf("%s.resharding_journal", sidecar.GetIdentifier()),
		Fields: fields[:len(tm.Types)],
	}
	// Build a normal table plan, which means, return all rows
	// and columns as is. Special handling is done when we actually
	// receive the row event. We'll build a JOURNAL event instead.
	plan, err := buildREPlan(vs.se.Environment(), table, nil, "")
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
	qr, err := conn.ExecuteFetch(sqlparser.BuildParsedQuery("select * from %s.schema_version where 1 != 1",
		sidecar.GetIdentifier()).Query, 1, true)
	if err != nil {
		return err
	}
	fields := qr.Fields
	if len(fields) < len(tm.Types) {
		return fmt.Errorf("cannot determine table columns for %s: event has %v, schema has %v", tm.Name, tm.Types, fields)
	}
	table := &Table{
		Name:   fmt.Sprintf("%s.schema_version", sidecar.GetIdentifier()),
		Fields: fields[:len(tm.Types)],
	}
	// Build a normal table plan, which means, return all rows
	// and columns as is. Special handling is done when we actually
	// receive the row event. We'll build a JOURNAL event instead.
	plan, err := buildREPlan(vs.se.Environment(), table, nil, "")
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
	plan, err := buildPlan(vs.se.Environment(), table, vs.vschema, vs.filter)
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
			Keyspace:  vs.vse.keyspace,
			Shard:     vs.vse.shard,
		},
	}, nil
}

func (vs *vstreamer) buildTableColumns(tm *mysql.TableMap) ([]*querypb.Field, error) {
	var fields []*querypb.Field
	var txtFieldIdx int
	for i, typ := range tm.Types {
		t, err := sqltypes.MySQLToType(typ, 0)
		if err != nil {
			return nil, fmt.Errorf("unsupported type: %d, position: %d", typ, i)
		}
		// Use the the collation inherited or the one specified explicitly for the
		// column if one was provided in the event's optional metadata (MySQL only
		// provides this for text based columns).
		var coll collations.ID
		switch {
		case sqltypes.IsText(t) && len(tm.ColumnCollationIDs) > txtFieldIdx:
			coll = tm.ColumnCollationIDs[txtFieldIdx]
			txtFieldIdx++
		case t == sqltypes.TypeJSON:
			// JSON is a blob at this (storage) layer -- vs the connection/query serving
			// layer which CollationForType seems primarily concerned about and JSON at
			// the response layer should be using utf-8 as that's the standard -- so we
			// should NOT use utf8mb4 as the collation in MySQL for a JSON column is
			// NULL, meaning there is not one (same as for int) and we should use binary.
			coll = collations.CollationBinaryID
		default: // Use the server defined default for the column's type
			coll = collations.CollationForType(t, vs.se.Environment().CollationEnv().DefaultConnectionCharset())
		}
		fields = append(fields, &querypb.Field{
			Name:    fmt.Sprintf("@%d", i+1),
			Type:    t,
			Charset: uint32(coll),
			Flags:   mysql.FlagsForColumn(t, coll),
		})
	}
	st, err := vs.se.GetTableForPos(sqlparser.NewIdentifierCS(tm.Name), replication.EncodePosition(vs.pos))
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
			return nil, fmt.Errorf("cannot determine table columns for %s: event has %v, schema has %v", tm.Name, tm.Types, st.Fields)
		}
		return fields, nil
	}

	// Check if the schema returned by schema.Engine matches with row.
	for i := range tm.Types {
		if !sqltypes.AreTypesEquivalent(fields[i].Type, st.Fields[i].Type) {
			return fields, nil
		}
	}

	// Columns should be truncated to match those in tm.
	// This uses the historian which queries the columns in the table and uses the
	// generated fields metadata. This means that the fields for text types are
	// initially using collations for the column types based on the *connection
	// collation* and not the actual *column collation*.
	// But because we now get the correct collation for the actual column from
	// mysqld in getExtColsInfo we know this is the correct one for the vstream
	// target and we use that rather than any that were in the binlog events,
	// which were for the source and which can be using a different collation
	// than the target.
	fieldsCopy, err := getFields(vs.ctx, vs.cp, vs.se, tm.Name, tm.Database, st.Fields[:len(tm.Types)])
	if err != nil {
		return nil, err
	}

	return fieldsCopy, nil
}

func getExtColInfos(ctx context.Context, cp dbconfigs.Connector, se *schema.Engine, table, database string) (map[string]*extColInfo, error) {
	extColInfos := make(map[string]*extColInfo)
	conn, err := cp.Connect(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	queryTemplate := "select column_name, column_type, collation_name from information_schema.columns where table_schema=%s and table_name=%s;"
	query := fmt.Sprintf(queryTemplate, encodeString(database), encodeString(table))
	qr, err := conn.ExecuteFetch(query, 10000, false)
	if err != nil {
		return nil, err
	}
	for _, row := range qr.Rows {
		extColInfo := &extColInfo{
			columnType: row[1].ToString(),
		}
		collationName := row[2].ToString()
		var coll collations.ID
		if row[2].IsNull() || collationName == "" {
			coll = collations.CollationBinaryID
		} else {
			coll = se.Environment().CollationEnv().LookupByName(collationName)
		}
		extColInfo.collationID = coll
		extColInfos[row[0].ToString()] = extColInfo
	}
	return extColInfos, nil
}

func getFields(ctx context.Context, cp dbconfigs.Connector, se *schema.Engine, table, database string, fields []*querypb.Field) ([]*querypb.Field, error) {
	// Make a deep copy of the schema.Engine fields as they are pointers and
	// will be modified by adding ColumnType below
	fieldsCopy := make([]*querypb.Field, len(fields))
	for i, field := range fields {
		fieldsCopy[i] = field.CloneVT()
	}
	extColInfos, err := getExtColInfos(ctx, cp, se, table, database)
	if err != nil {
		return nil, err
	}
	for _, field := range fieldsCopy {
		if colInfo, ok := extColInfos[field.Name]; ok {
			field.ColumnType = colInfo.columnType
			field.Charset = uint32(colInfo.collationID)
		}
	}
	return fieldsCopy, nil
}

// Additional column attributes to get from information_schema.columns.
type extColInfo struct {
	columnType  string
	collationID collations.ID
}

func encodeString(in string) string {
	buf := bytes.NewBuffer(nil)
	sqltypes.NewVarChar(in).EncodeSQL(buf)
	return buf.String()
}

func (vs *vstreamer) processJournalEvent(vevents []*binlogdatapb.VEvent, plan *streamerPlan, rows mysql.Rows) ([]*binlogdatapb.VEvent, error) {
	// Get DbName
	params, err := vs.cp.MysqlParams()
	if err != nil {
		return nil, err
	}
nextrow:
	for _, row := range rows.Rows {
		afterOK, afterValues, _, err := vs.extractRowAndFilter(plan, row.Data, rows.DataColumns, row.NullColumns)
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
			avBytes, err := afterValues[i].ToBytes()
			if err != nil {
				return nil, err
			}
			if err := prototext.Unmarshal(avBytes, journal); err != nil {
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
		beforeOK, beforeValues, _, err := vs.extractRowAndFilter(plan, row.Identify, rows.IdentifyColumns, row.NullIdentifyColumns)
		if err != nil {
			return nil, err
		}
		afterOK, afterValues, partial, err := vs.extractRowAndFilter(plan, row.Data, rows.DataColumns, row.NullColumns)
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
			if (vttablet.VReplicationExperimentalFlags /**/ & /**/ vttablet.VReplicationExperimentalFlagAllowNoBlobBinlogRowImage != 0) &&
				partial {

				rowChange.DataColumns = &binlogdatapb.RowChange_Bitmap{
					Count: int64(rows.DataColumns.Count()),
					Cols:  rows.DataColumns.Bits(),
				}
			}
		}
		rowChanges = append(rowChanges, rowChange)
	}
	if len(rowChanges) != 0 {
		vevents = append(vevents, &binlogdatapb.VEvent{
			Type: binlogdatapb.VEventType_ROW,
			RowEvent: &binlogdatapb.RowEvent{
				TableName:  plan.Table.Name,
				RowChanges: rowChanges,
				Keyspace:   vs.vse.keyspace,
				Shard:      vs.vse.shard,
				Flags:      uint32(rows.Flags),
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
		newPlan, err := buildPlan(vs.se.Environment(), plan.Table, vs.vschema, vs.filter)
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

// extractRowAndFilter takes the data and bitmaps from the binlog events and returns the following
//   - true, if row needs to be skipped because of workflow filter rules
//   - data values, array of one value per column
//   - true, if the row image was partial (i.e. binlog_row_image=noblob and dml doesn't update one or more blob/text columns)
func (vs *vstreamer) extractRowAndFilter(plan *streamerPlan, data []byte, dataColumns, nullColumns mysql.Bitmap) (bool, []sqltypes.Value, bool, error) {
	if len(data) == 0 {
		return false, nil, false, nil
	}
	values := make([]sqltypes.Value, dataColumns.Count())
	charsets := make([]collations.ID, len(values))
	valueIndex := 0
	pos := 0
	partial := false
	for colNum := 0; colNum < dataColumns.Count(); colNum++ {
		if !dataColumns.Bit(colNum) {
			if vttablet.VReplicationExperimentalFlags /**/ & /**/ vttablet.VReplicationExperimentalFlagAllowNoBlobBinlogRowImage == 0 {
				return false, nil, false, fmt.Errorf("partial row image encountered: ensure binlog_row_image is set to 'full'")
			} else {
				partial = true
			}
			continue
		}
		if nullColumns.Bit(valueIndex) {
			valueIndex++
			continue
		}
		value, l, err := mysqlbinlog.CellValue(data, pos, plan.TableMap.Types[colNum], plan.TableMap.Metadata[colNum], plan.Table.Fields[colNum])
		if err != nil {
			log.Errorf("extractRowAndFilter: %s, table: %s, colNum: %d, fields: %+v, current values: %+v",
				err, plan.Table.Name, colNum, plan.Table.Fields, values)
			return false, nil, false, err
		}
		pos += l

		charsets[colNum] = collations.ID(plan.Table.Fields[colNum].Charset)
		values[colNum] = value
		valueIndex++
	}
	filtered := make([]sqltypes.Value, len(plan.ColExprs))
	ok, err := plan.filter(values, filtered, charsets)
	return ok, filtered, partial, err
}

func wrapError(err error, stopPos replication.Position, vse *Engine) error {
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
