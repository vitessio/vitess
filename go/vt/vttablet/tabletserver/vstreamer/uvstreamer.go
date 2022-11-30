/*
Copyright 2020 The Vitess Authors.

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
	"io"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
)

var uvstreamerTestMode = false // Only used for testing

type tablePlan struct {
	tablePK *binlogdatapb.TableLastPK
	rule    *binlogdatapb.Rule
}

type uvstreamer struct {
	ctx    context.Context
	cancel func()

	// input parameters
	vse        *Engine
	send       func([]*binlogdatapb.VEvent) error
	cp         dbconfigs.Connector
	se         *schema.Engine
	startPos   string
	filter     *binlogdatapb.Filter
	inTablePKs []*binlogdatapb.TableLastPK

	vschema *localVSchema

	// map holds tables remaining to be fully copied, it is depleted as each table gets completely copied
	plans        map[string]*tablePlan
	tablesToCopy []string

	// changes for each table being copied
	fields   []*querypb.Field
	pkfields []*querypb.Field

	// current position in the binlog for this streamer
	pos mysql.Position

	// fast forward uses this to stop replicating upto the point of the last snapshot
	stopPos mysql.Position

	// lastTimestampNs is the last timestamp seen so far.
	lastTimestampNs       int64
	ReplicationLagSeconds int64
	mu                    sync.Mutex

	config *uvstreamerConfig

	vs *vstreamer //last vstreamer created in uvstreamer
}

type uvstreamerConfig struct {
	MaxReplicationLag time.Duration
	CatchupRetryTime  time.Duration
}

func newUVStreamer(ctx context.Context, vse *Engine, cp dbconfigs.Connector, se *schema.Engine, startPos string, tablePKs []*binlogdatapb.TableLastPK, filter *binlogdatapb.Filter, vschema *localVSchema, send func([]*binlogdatapb.VEvent) error) *uvstreamer {
	ctx, cancel := context.WithCancel(ctx)
	config := &uvstreamerConfig{
		MaxReplicationLag: 1 * time.Nanosecond,
		CatchupRetryTime:  1 * time.Second,
	}
	send2 := func(evs []*binlogdatapb.VEvent) error {
		vse.vstreamerEventsStreamed.Add(int64(len(evs)))
		for _, ev := range evs {
			ev.Keyspace = vse.keyspace
			ev.Shard = vse.shard
		}
		return send(evs)
	}
	uvs := &uvstreamer{
		ctx:        ctx,
		cancel:     cancel,
		vse:        vse,
		send:       send2,
		cp:         cp,
		se:         se,
		startPos:   startPos,
		filter:     filter,
		vschema:    vschema,
		config:     config,
		inTablePKs: tablePKs,
	}

	return uvs
}

// buildTablePlan identifies the tables for the copy phase and creates the plans which consist of the lastPK seen
// for a table and its Rule (for filtering purposes by the vstreamer engine)
// it can be called
//
//	the first time, with just the filter and an empty pos
//	during a restart, with both the filter and list of TableLastPK from the vgtid
func (uvs *uvstreamer) buildTablePlan() error {
	uvs.plans = make(map[string]*tablePlan)
	tableLastPKs := make(map[string]*binlogdatapb.TableLastPK)
	for _, tablePK := range uvs.inTablePKs {
		tableLastPKs[tablePK.TableName] = tablePK
	}
	tables := uvs.se.GetSchema()
	for range tables {
		for _, rule := range uvs.filter.Rules {
			if !strings.HasPrefix(rule.Match, "/") {
				_, ok := tables[rule.Match]
				if !ok {
					return fmt.Errorf("table %s is not present in the database", rule.Match)
				}
			}
		}
	}
	for tableName := range tables {
		rule, err := matchTable(tableName, uvs.filter, tables)
		if err != nil {
			return err
		}
		if rule == nil {
			continue
		}
		plan := &tablePlan{
			tablePK: nil,
			rule: &binlogdatapb.Rule{
				Filter: rule.Filter,
				Match:  rule.Match,
			},
		}
		tablePK, ok := tableLastPKs[tableName]
		if !ok {
			tablePK = &binlogdatapb.TableLastPK{
				TableName: tableName,
				Lastpk:    nil,
			}
		}
		plan.tablePK = tablePK
		uvs.plans[tableName] = plan
		uvs.tablesToCopy = append(uvs.tablesToCopy, tableName)

	}
	sort.Strings(uvs.tablesToCopy)
	return nil
}

// check which rule matches table, validate table is in schema
func matchTable(tableName string, filter *binlogdatapb.Filter, tables map[string]*schema.Table) (*binlogdatapb.Rule, error) {
	if tableName == "dual" {
		return nil, nil
	}
	found := false
	for _, rule := range filter.Rules {

		switch {
		case tableName == rule.Match:
			found = true
		case strings.HasPrefix(rule.Match, "/"):
			expr := strings.Trim(rule.Match, "/")
			result, err := regexp.MatchString(expr, tableName)
			if err != nil {
				return nil, err
			}
			if !result {
				continue
			}
			found = true
		}
		if found {
			return &binlogdatapb.Rule{
				Match:  tableName,
				Filter: getQuery(tableName, rule.Filter),
			}, nil
		}
	}

	return nil, nil
}

// generate equivalent select statement if filter is empty or a keyrange.
func getQuery(tableName string, filter string) string {
	query := filter
	switch {
	case filter == "":
		buf := sqlparser.NewTrackedBuffer(nil)
		buf.Myprintf("select * from %v", sqlparser.NewIdentifierCS(tableName))
		query = buf.String()
	case key.IsKeyRange(filter):
		buf := sqlparser.NewTrackedBuffer(nil)
		buf.Myprintf("select * from %v where in_keyrange(%v)",
			sqlparser.NewIdentifierCS(tableName), sqlparser.NewStrLiteral(filter))
		query = buf.String()
	}
	return query
}

func (uvs *uvstreamer) Cancel() {
	log.Infof("uvstreamer context is being cancelled")
	uvs.cancel()
}

// We have not yet implemented the logic to check if an event is for a row that is already copied,
// so we always return true so that we send all events for this table and so we don't miss events.
func (uvs *uvstreamer) isRowCopied(tableName string, ev *binlogdatapb.VEvent) bool {
	return true
}

// Only send catchup/fastforward events for tables whose copy phase is complete or in progress.
// This ensures we fulfill the at-least-once delivery semantics for events.
// TODO: filter out events for rows not yet copied. Note that we can only do this as a best-effort
// for comparable PKs.
func (uvs *uvstreamer) shouldSendEventForTable(tableName string, ev *binlogdatapb.VEvent) bool {
	table, ok := uvs.plans[tableName]
	// Event is for a table which is not in its copy phase.
	if !ok {
		return true
	}

	// if table copy was not started and no tablePK was specified we can ignore catchup/fastforward events for it
	if table.tablePK == nil || table.tablePK.Lastpk == nil {
		return false
	}

	// Table is currently in its copy phase. We have not yet implemented the logic to
	// check if an event is for a row that is already copied, so we always return true
	// there so that we don't miss events.
	// We may send duplicate insert events or update/delete events for rows not yet seen
	// to the client for the table being copied. This is ok as the client is expected to be
	// idempotent: we only promise at-least-once semantics for VStream API (not exactly-once).
	// Aside: vreplication workflows handle at-least-once by adding where clauses that render
	// DML queries, related to events for rows not yet copied, as no-ops.
	return uvs.isRowCopied(tableName, ev)
}

// Do not send internal heartbeat events. Filter out events for tables whose copy has not been started.
func (uvs *uvstreamer) filterEvents(evs []*binlogdatapb.VEvent) []*binlogdatapb.VEvent {
	if len(uvs.plans) == 0 {
		return evs
	}
	var evs2 []*binlogdatapb.VEvent
	var tableName string
	var shouldSend bool

	for _, ev := range evs {
		switch ev.Type {
		case binlogdatapb.VEventType_ROW:
			tableName = ev.RowEvent.TableName
		case binlogdatapb.VEventType_FIELD:
			tableName = ev.FieldEvent.TableName
		default:
			tableName = ""
		}
		switch ev.Type {
		case binlogdatapb.VEventType_HEARTBEAT:
			shouldSend = false
		default:
			shouldSend = uvs.shouldSendEventForTable(tableName, ev)
		}

		if shouldSend {
			evs2 = append(evs2, ev)
		}
	}
	return evs2
}

// wraps the send parameter and filters events. called by fastforward/catchup
func (uvs *uvstreamer) send2(evs []*binlogdatapb.VEvent) error {
	if len(evs) == 0 {
		return nil
	}
	ev := evs[len(evs)-1]
	if ev.Timestamp != 0 {
		uvs.lastTimestampNs = ev.Timestamp * 1e9
	}
	behind := time.Now().UnixNano() - uvs.lastTimestampNs
	uvs.setReplicationLagSeconds(behind / 1e9)
	//log.Infof("sbm set to %d", uvs.ReplicationLagSeconds)
	var evs2 []*binlogdatapb.VEvent
	if len(uvs.plans) > 0 {
		evs2 = uvs.filterEvents(evs)
	}
	err := uvs.send(evs2)
	if err != nil && err != io.EOF {
		return err
	}
	for _, ev := range evs2 {
		if ev.Type == binlogdatapb.VEventType_GTID {
			uvs.pos, _ = mysql.DecodePosition(ev.Gtid)
			if !uvs.stopPos.IsZero() && uvs.pos.AtLeast(uvs.stopPos) {
				err = io.EOF
			}
		}
	}
	if err != nil {
		uvs.vse.errorCounts.Add("Send", 1)
	}
	return err
}

func (uvs *uvstreamer) sendEventsForCurrentPos() error {
	log.Infof("sendEventsForCurrentPos")
	evs := []*binlogdatapb.VEvent{{
		Type: binlogdatapb.VEventType_GTID,
		Gtid: mysql.EncodePosition(uvs.pos),
	}, {
		Type: binlogdatapb.VEventType_OTHER,
	}}
	if err := uvs.send(evs); err != nil {
		return wrapError(err, uvs.pos, uvs.vse)
	}
	return nil
}

func (uvs *uvstreamer) setStreamStartPosition() error {
	curPos, err := uvs.currentPosition()
	if err != nil {
		return vterrors.Wrap(err, "could not obtain current position")
	}
	if uvs.startPos == "current" {
		uvs.pos = curPos
		if err := uvs.sendEventsForCurrentPos(); err != nil {
			return err
		}
		return nil
	}
	pos, err := mysql.DecodePosition(uvs.startPos)
	if err != nil {
		return vterrors.Wrap(err, "could not decode position")
	}
	if !curPos.AtLeast(pos) {
		uvs.vse.errorCounts.Add("GTIDSet Mismatch", 1)
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
			"GTIDSet Mismatch: requested source position:%v, current target vrep position: %v",
			mysql.EncodePosition(pos), mysql.EncodePosition(curPos))
	}
	uvs.pos = pos
	return nil
}

func (uvs *uvstreamer) currentPosition() (mysql.Position, error) {
	conn, err := uvs.cp.Connect(uvs.ctx)
	if err != nil {
		return mysql.Position{}, err
	}
	defer conn.Close()
	return conn.PrimaryPosition()
}

// Possible states:
// 1. TablePKs nil, startPos set to gtid or "current" => start replicating from pos
// 2. TablePKs nil, startPos empty => full table copy of tables matching filter
// 3. TablePKs not nil, startPos empty => table copy (for pks > lastPK)
// 4. TablePKs not nil, startPos set => run catchup from startPos, then table copy  (for pks > lastPK)
func (uvs *uvstreamer) init() error {
	if uvs.startPos == "" /* full copy */ || len(uvs.inTablePKs) > 0 /* resume copy */ {
		if err := uvs.buildTablePlan(); err != nil {
			return err
		}
	}
	if uvs.startPos != "" {
		if err := uvs.setStreamStartPosition(); err != nil {
			return err
		}
	}
	if uvs.pos.IsZero() && (len(uvs.plans) == 0) {
		return fmt.Errorf("stream needs a position or a table to copy")
	}
	return nil
}

// Stream streams binlog events.
func (uvs *uvstreamer) Stream() error {
	log.Info("Stream() called")
	if err := uvs.init(); err != nil {
		return err
	}
	if len(uvs.plans) > 0 {
		log.Info("TablePKs is not nil: starting vs.copy()")
		if err := uvs.copy(uvs.ctx); err != nil {
			log.Infof("uvstreamer.Stream() copy returned with err %s", err)
			uvs.vse.errorCounts.Add("Copy", 1)
			return err
		}
		if err := uvs.allCopyComplete(); err != nil {
			return err
		}
	}
	vs := newVStreamer(uvs.ctx, uvs.cp, uvs.se, mysql.EncodePosition(uvs.pos), mysql.EncodePosition(uvs.stopPos),
		uvs.filter, uvs.getVSchema(), uvs.send, "replicate", uvs.vse)

	uvs.setVs(vs)
	return vs.Stream()
}

func (uvs *uvstreamer) lock(msg string) {
	uvs.mu.Lock()
}

func (uvs *uvstreamer) unlock(msg string) {
	uvs.mu.Unlock()
}

func (uvs *uvstreamer) setVs(vs *vstreamer) {
	uvs.lock("setVs")
	defer uvs.unlock("setVs")
	uvs.vs = vs
}

// SetVSchema updates the vstreamer against the new vschema.
func (uvs *uvstreamer) SetVSchema(vschema *localVSchema) {
	uvs.lock("SetVSchema")
	defer uvs.unlock("SetVSchema")
	uvs.vschema = vschema
	if uvs.vs != nil {
		uvs.vs.SetVSchema(vschema)
	}
}

func (uvs *uvstreamer) getVSchema() *localVSchema {
	uvs.lock("getVSchema")
	defer uvs.unlock("getVSchema")
	return uvs.vschema
}

func (uvs *uvstreamer) setCopyState(tableName string, qr *querypb.QueryResult) {
	uvs.plans[tableName].tablePK.Lastpk = qr
}

func (uvs *uvstreamer) allCopyComplete() error {
	ev := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_COPY_COMPLETED,
	}

	if err := uvs.send([]*binlogdatapb.VEvent{ev}); err != nil {
		return err
	}
	return nil
}

// dummy event sent only in test mode
func (uvs *uvstreamer) sendTestEvent(msg string) {
	if !uvstreamerTestMode {
		return
	}
	ev := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_OTHER,
		Gtid: msg,
	}

	if err := uvs.send([]*binlogdatapb.VEvent{ev}); err != nil {
		return
	}
}

func (uvs *uvstreamer) copyComplete(tableName string) error {
	evs := []*binlogdatapb.VEvent{
		{Type: binlogdatapb.VEventType_BEGIN},
		{
			Type: binlogdatapb.VEventType_LASTPK,
			LastPKEvent: &binlogdatapb.LastPKEvent{
				TableLastPK: &binlogdatapb.TableLastPK{
					TableName: tableName,
					Lastpk:    nil,
				},
				Completed: true,
			},
		},
		{Type: binlogdatapb.VEventType_COMMIT},
	}
	if err := uvs.send(evs); err != nil {
		return err
	}

	delete(uvs.plans, tableName)
	uvs.tablesToCopy = uvs.tablesToCopy[1:]
	return nil
}

func (uvs *uvstreamer) setPosition(gtid string, isInTx bool) error {
	if gtid == "" {
		return fmt.Errorf("empty gtid passed to setPosition")
	}
	pos, err := mysql.DecodePosition(gtid)
	if err != nil {
		return err
	}
	if pos.Equal(uvs.pos) {
		return nil
	}
	gtidEvent := &binlogdatapb.VEvent{
		Type:     binlogdatapb.VEventType_GTID,
		Gtid:     gtid,
		Keyspace: uvs.vse.keyspace,
		Shard:    uvs.vse.shard,
	}

	var evs []*binlogdatapb.VEvent
	if !isInTx {
		evs = append(evs, &binlogdatapb.VEvent{
			Type:     binlogdatapb.VEventType_BEGIN,
			Keyspace: uvs.vse.keyspace,
			Shard:    uvs.vse.shard,
		})
	}
	evs = append(evs, gtidEvent)
	if !isInTx {
		evs = append(evs, &binlogdatapb.VEvent{
			Type:     binlogdatapb.VEventType_COMMIT,
			Keyspace: uvs.vse.keyspace,
			Shard:    uvs.vse.shard,
		})
	}
	if err := uvs.send(evs); err != nil {
		return err
	}
	uvs.pos = pos
	return nil
}

func (uvs *uvstreamer) getReplicationLagSeconds() int64 {
	uvs.mu.Lock()
	defer uvs.mu.Unlock()
	return uvs.ReplicationLagSeconds
}

func (uvs *uvstreamer) setReplicationLagSeconds(sbm int64) {
	uvs.mu.Lock()
	defer uvs.mu.Unlock()
	uvs.ReplicationLagSeconds = sbm
}
