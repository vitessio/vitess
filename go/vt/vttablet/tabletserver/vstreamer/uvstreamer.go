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
	lastTimestampNs     int64
	secondsBehindMaster int64
	mu                  sync.Mutex

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
//		the first time, with just the filter and an empty pos
//		during a restart, with both the filter and list of TableLastPK from the vgtid
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
		buf.Myprintf("select * from %v", sqlparser.NewTableIdent(tableName))
		query = buf.String()
	case key.IsKeyRange(filter):
		buf := sqlparser.NewTrackedBuffer(nil)
		buf.Myprintf("select * from %v where in_keyrange(%v)", sqlparser.NewTableIdent(tableName), sqlparser.NewStrLiteral(filter))
		query = buf.String()
	}
	return query
}

func (uvs *uvstreamer) Cancel() {
	log.Infof("uvstreamer context is being cancelled")
	uvs.cancel()
}

// during copy phase only send streaming events (during catchup/fastforward) for pks already seen
func (uvs *uvstreamer) filterEvents(evs []*binlogdatapb.VEvent) []*binlogdatapb.VEvent {
	if len(uvs.plans) == 0 {
		return evs
	}
	var evs2 []*binlogdatapb.VEvent
	var tableName string
	var shouldSend bool

	for _, ev := range evs {
		shouldSend = false
		tableName = ""
		switch ev.Type {
		case binlogdatapb.VEventType_ROW:
			tableName = ev.RowEvent.TableName
		case binlogdatapb.VEventType_FIELD:
			tableName = ev.FieldEvent.TableName
		case binlogdatapb.VEventType_HEARTBEAT:
			shouldSend = false
		default:
			shouldSend = true
		}
		if !shouldSend && tableName != "" {
			shouldSend = true
			_, ok := uvs.plans[tableName]
			if ok {
				shouldSend = false
			}
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
	uvs.setSecondsBehindMaster(behind / 1e9)
	//log.Infof("sbm set to %d", uvs.secondsBehindMaster)
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
		return fmt.Errorf("GTIDSet Mismatch: requested source position:%v, current target vrep position: %v", mysql.EncodePosition(pos), mysql.EncodePosition(curPos))
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
	return conn.MasterPosition()
}

func (uvs *uvstreamer) init() error {
	if uvs.startPos != "" {
		if err := uvs.setStreamStartPosition(); err != nil {
			return err
		}
	} else if uvs.startPos == "" || len(uvs.inTablePKs) > 0 {
		if err := uvs.buildTablePlan(); err != nil {
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
		uvs.sendTestEvent("Copy Done")
	}
	vs := newVStreamer(uvs.ctx, uvs.cp, uvs.se, mysql.EncodePosition(uvs.pos), mysql.EncodePosition(uvs.stopPos), uvs.filter, uvs.getVSchema(), uvs.send, "replicate", uvs.vse)

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
		Type: binlogdatapb.VEventType_GTID,
		Gtid: gtid,
	}

	var evs []*binlogdatapb.VEvent
	if !isInTx {
		evs = append(evs, &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_BEGIN})
	}
	evs = append(evs, gtidEvent)
	if !isInTx {
		evs = append(evs, &binlogdatapb.VEvent{Type: binlogdatapb.VEventType_COMMIT})
	}
	if err := uvs.send(evs); err != nil {
		return err
	}
	uvs.pos = pos
	return nil
}

func (uvs *uvstreamer) getSecondsBehindMaster() int64 {
	uvs.mu.Lock()
	defer uvs.mu.Unlock()
	return uvs.secondsBehindMaster
}

func (uvs *uvstreamer) setSecondsBehindMaster(sbm int64) {
	uvs.mu.Lock()
	defer uvs.mu.Unlock()
	uvs.secondsBehindMaster = sbm
}
