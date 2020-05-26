package vstreamer

import (
	"context"
	"fmt"
	"sort"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
)

type tablePlan struct {
	tablePK *TableLastPK
	rule    *binlogdatapb.Rule
}

type uvstreamer struct {
	ctx    context.Context
	cancel func()

	// input parameters
	vse      *Engine
	send     func([]*binlogdatapb.VEvent) error
	cp       dbconfigs.Connector
	se       *schema.Engine
	sh       schema.Historian
	startPos string
	filter   *binlogdatapb.Filter
	vschema  *localVSchema

	//map holds tables remaining to be fully copied, it is depleted as each table gets completely copied
	plans        map[string]*tablePlan
	tablesToCopy []string

	//changes for each table being copied
	fields   []*querypb.Field
	pkfields []*querypb.Field

	// current position in the binlog for this streamer
	pos mysql.Position

	// fast forward uses this to stop replicating upto the point of the last snapshot
	stopPos string

	// lastTimestampNs is the last timestamp seen so far.
	lastTimestampNs int64
	// timeOffsetNs keeps track of the clock difference with respect to source tablet.
	timeOffsetNs        int64
	secondsBehindMaster int64

	config *uvstreamerConfig
}

type uvstreamerConfig struct {
	MaxRows           int64 //todo ??
	MaxReplicationLag time.Duration
	CatchupRetryTime  time.Duration
}

func newUVStreamer(ctx context.Context, vse *Engine, cp dbconfigs.Connector, se *schema.Engine, sh schema.Historian, startPos string, tablePKs []*TableLastPK, filter *binlogdatapb.Filter, vschema *localVSchema, send func([]*binlogdatapb.VEvent) error) *uvstreamer {
	ctx, cancel := context.WithCancel(ctx)
	config := &uvstreamerConfig{
		MaxReplicationLag: 1 * time.Nanosecond,
		CatchupRetryTime:  1 * time.Second,
	}
	uvs := &uvstreamer{
		ctx:      ctx,
		cancel:   cancel,
		vse:      vse,
		send:     send,
		cp:       cp,
		se:       se,
		sh:       sh,
		startPos: startPos,
		filter:   filter,
		vschema:  vschema,
		config:   config,
	}
	if len(tablePKs) > 0 {
		uvs.plans = make(map[string]*tablePlan)
		for _, rule := range filter.Rules {
			plan := &tablePlan{
				tablePK: nil,
				rule:    rule,
			}
			uvs.plans[rule.Match] = plan //TODO: only handles actual table name now, no regular expressions
		}
		for _, tablePK := range tablePKs {
			uvs.plans[tablePK.name].tablePK = tablePK
			uvs.tablesToCopy = append(uvs.tablesToCopy, tablePK.name)
		}
		sort.Strings(uvs.tablesToCopy)
	}
	//TODO table pk validations
	return uvs
}

func (uvs *uvstreamer) Cancel() {
	log.Infof("uvstreamer context is being cancelled")
	uvs.cancel()
}

func (uvs *uvstreamer) filterEvents(evs []*binlogdatapb.VEvent) ([]*binlogdatapb.VEvent, error) {
	var evs2 []*binlogdatapb.VEvent
	var tableName string
	var shouldSend bool
	for _, ev := range evs {
		shouldSend = false
		switch ev.Type {
		case binlogdatapb.VEventType_ROW:
			tableName = ev.RowEvent.TableName
		case binlogdatapb.VEventType_FIELD:
			tableName = ev.FieldEvent.TableName
		default:
			tableName = ""
		}
		if tableName != "" {
			shouldSend = true
			_, ok := uvs.plans[tableName]
			if ok && uvs.plans[tableName].tablePK.lastPK.Rows[0] == nil {
				shouldSend = false
			}
		}
		if shouldSend {
			evs2 = append(evs2, ev)
			//log.Infof("shouldSend: sending %v table %s", ev.String(), tableName)
		} else {
			//log.Infof("shouldSend: filtering out %v", ev.String())
		}
	}
	return evs2, nil
}

func (uvs *uvstreamer) send2(evs []*binlogdatapb.VEvent) error {
	if len(evs) == 0 {
		return nil
	}
	ev := evs[len(evs)-1]
	if ev.Timestamp != 0 {
		uvs.lastTimestampNs = ev.Timestamp * 1e9
	}
	behind := time.Now().UnixNano() - uvs.lastTimestampNs
	uvs.secondsBehindMaster = behind / 1e9
	log.Infof("sbm set to %d", uvs.secondsBehindMaster)
	if len(uvs.plans) > 0 {
		evs, _ = uvs.filterEvents(evs)
	}
	return uvs.send(evs)
}

func (uvs *uvstreamer) sendEventsForCurrentPos() error {
	vevents := []*binlogdatapb.VEvent{{
		Type: binlogdatapb.VEventType_GTID,
		Gtid: mysql.EncodePosition(uvs.pos),
	}, {
		Type: binlogdatapb.VEventType_OTHER,
	}}
	if err := uvs.send(vevents); err != nil {
		return wrapError(err, uvs.pos)
	}
	return nil
}

func (uvs *uvstreamer) setStreamPosition() error {
	if uvs.startPos != "" {
		curPos, err := uvs.currentPosition()
		if err != nil {
			return vterrors.Wrap(err, "could not obtain current position")
		}
		if uvs.startPos == "current" {
			uvs.pos = curPos
			return nil
		}
		pos, err := mysql.DecodePosition(uvs.startPos)
		if err != nil {
			return vterrors.Wrap(err, "could not decode position")
		}
		if !curPos.AtLeast(pos) {
			return fmt.Errorf("requested position %v is ahead of current position %v", mysql.EncodePosition(pos), mysql.EncodePosition(curPos))
		}
		log.Infof("Setting stream position to %s", uvs.pos)
		uvs.pos = pos
	}
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
	if err := uvs.setStreamPosition(); err != nil {
		return err
	} //startpos validation for tablepk != nil
	if uvs.pos.IsZero() && (len(uvs.plans) == 0) {
		return fmt.Errorf("Stream needs atleast a position or a table to copy")
	}
	return nil
}

// Stream streams binlog events.
func (uvs *uvstreamer) Stream() error {
	if err := uvs.init(); err != nil {
		return err
	}
	if len(uvs.plans) > 0 {
		log.Info("TablePKs is not nil: starting vs.copy()")
		if err := uvs.copy(uvs.ctx); err != nil {
			log.Infof("uvstreamer.Stream() copy returned with err %s", err)
			return err
		}
		ev := &binlogdatapb.VEvent{
			Type: binlogdatapb.VEventType_GTID,
			Gtid: mysql.EncodePosition(uvs.pos),
		}
		uvs.send([]*binlogdatapb.VEvent{ev})
	}
	log.Infof("Starting replicate in uvstreamer.Stream()")
	vs := newVStreamer(uvs.ctx, uvs.cp, uvs.se, uvs.sh, mysql.EncodePosition(uvs.pos), uvs.stopPos, uvs.filter, uvs.vschema, uvs.send)

	return vs.Stream()
}

// SetVSchema updates the vstreamer against the new vschema.
func (uvs *uvstreamer) SetVSchema(vschema *localVSchema) {
	uvs.vschema = vschema
	//FIXME: #sugu need to pass it on to running vstreamer
}

func (uvs *uvstreamer) setPos(pos mysql.Position) {
	uvs.pos = pos
	ev := &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_GTID,
		Gtid: mysql.EncodePosition(pos),
	}
	uvs.send([]*binlogdatapb.VEvent{ev})
}

func (uvs *uvstreamer) setCopyState(tableName string, lastPK *sqltypes.Result) {
	uvs.plans[tableName].tablePK.lastPK = lastPK
	qr := sqltypes.ResultToProto3(lastPK)
	lastPKEvent := &binlogdatapb.LastPKEvent{
		TableLastPK: &binlogdatapb.TableLastPK{
			TableName: tableName,
			Lastpk:    qr,
		},
		Completed: false,
	}
	ev := &binlogdatapb.VEvent{
		Type:        binlogdatapb.VEventType_LASTPK,
		LastPKEvent: lastPKEvent,
	}
	uvs.send([]*binlogdatapb.VEvent{ev})
}
