package vstreamer

import (
	"context"
	"fmt"
	"io"
	"math"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func (uvs *uvstreamer) copy(ctx context.Context) error {
	for len(uvs.tablesToCopy) > 0 {
		tableName := uvs.tablesToCopy[0]
		log.Infof("Copystate not empty starting catchupAndCopy on table %s", tableName)
		if err := uvs.catchupAndCopy(ctx, tableName); err != nil {
			return err
		}
		//time.Sleep(2*time.Second) //FIXME for debugging
	}
	log.Info("No tables left to copy")
	return nil
}

func (uvs *uvstreamer) catchupAndCopy(ctx context.Context, tableName string) error {
	log.Infof("catchupAndCopy for %s", tableName)
	if !uvs.pos.IsZero() {
		if err := uvs.catchup(ctx); err != nil {
			log.Infof("catchupAndCopy: catchup returned %v", err)
			return err
		}
	}

	log.Infof("catchupAndCopy: before copyTable %s", tableName)
	uvs.fields = nil
	return uvs.copyTable(ctx, tableName)
}

func (uvs *uvstreamer) catchup(ctx context.Context) error {
	log.Infof("starting catchup ...")
	uvs.secondsBehindMaster = math.MaxInt64
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errch := make(chan error, 1)
	go func() {
		startPos := mysql.EncodePosition(uvs.pos)
		vs := newVStreamer(ctx, uvs.cp, uvs.se, uvs.sh, startPos, "", uvs.filter, uvs.vschema, uvs.send2)
		errch <- vs.Stream()
		uvs.vs = nil
		log.Infof("catchup vs.stream returned with vs.pos %s", vs.pos.String())
	}()

	// Wait for catchup.
	tkr := time.NewTicker(uvs.config.CatchupRetryTime)
	defer tkr.Stop()
	seconds := int64(uvs.config.MaxReplicationLag / time.Second)
	for {
		sbm := uvs.secondsBehindMaster //TODO #sugu
		log.Infof("Checking sbm %d vs config %d", sbm, seconds)
		if sbm <= seconds {
			log.Infof("Canceling context because lag is %d:%d", sbm, seconds)
			cancel()
			// Make sure vplayer returns before returning.
			<-errch
			return nil
		}
		select {
		case err := <-errch:
			if err != nil {
				return err
			}
			return io.EOF
		case <-ctx.Done():
			// Make sure vplayer returns before returning.
			<-errch
			return io.EOF
		case <-tkr.C:
		}
	}
}

func (uvs *uvstreamer) sendFieldEvent(ctx context.Context, gtid string, fieldEvent *binlogdatapb.FieldEvent) error {
	ev := &binlogdatapb.VEvent{
		Type:       binlogdatapb.VEventType_FIELD,
		FieldEvent: fieldEvent,
	}
	log.Infof("Sending field event %v, gtid is %s", fieldEvent, gtid)
	uvs.send([]*binlogdatapb.VEvent{ev})
	pos, _ := mysql.DecodePosition(gtid)
	uvs.pos = pos
	return nil

}

func (uvs *uvstreamer) sendEventsForRows(ctx context.Context, tableName string, rows *binlogdatapb.VStreamRowsResponse) error {
	var evs []*binlogdatapb.VEvent
	for _, row := range rows.Rows {
		ev := &binlogdatapb.VEvent{
			Type: binlogdatapb.VEventType_ROW,
			RowEvent: &binlogdatapb.RowEvent{
				TableName: tableName,
				RowChanges: []*binlogdatapb.RowChange{{
					Before: nil,
					After:  row,
				}},
			},
		}
		evs = append(evs, ev)
	}
	uvs.send(evs)
	return nil
}

func (uvs *uvstreamer) copyTable(ctx context.Context, tableName string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var err error
	var newLastPK *sqltypes.Result
	lastPK := uvs.plans[tableName].tablePK.lastPK.Rows[0]
	filter := uvs.plans[tableName].rule.Filter
	log.Infof("Starting copyTable for %s, PK %v", tableName, lastPK)
	uvs.sendTestEvent(fmt.Sprintf("Copy Start %s", tableName))

	//TODO: PASS and return VGTID during gtid events
	err = uvs.vse.StreamRows(ctx, filter, lastPK, func(rows *binlogdatapb.VStreamRowsResponse) error {
		select {
		case <-ctx.Done():
			log.Infof("Returning io.EOF in StreamRows")
			return io.EOF
		default:
		}
		if uvs.fields == nil {
			if len(rows.Fields) == 0 {
				return fmt.Errorf("expecting field event first, got: %v", rows)
			}
			pos, _ := mysql.DecodePosition(rows.Gtid)
			if !uvs.pos.IsZero() && !uvs.pos.AtLeast(pos) {
				uvs.fastForward(ctx, rows.Gtid)
			}
			if mysql.EncodePosition(uvs.pos) != rows.Gtid {
				log.Errorf("Position after fastforward was %s but stopPos was %s", uvs.pos, rows.Gtid)
			}

			fieldEvent := &binlogdatapb.FieldEvent{
				TableName: tableName,
				Fields:    rows.Fields,
			}
			uvs.fields = rows.Fields
			uvs.pkfields = rows.Pkfields
			uvs.sendFieldEvent(ctx, rows.Gtid, fieldEvent)
		}
		if len(rows.Rows) == 0 {
			log.Infof("0 rows returned for table %s", tableName)
			return nil
		}
		uvs.sendEventsForRows(ctx, tableName, rows)

		newLastPK = sqltypes.CustomProto3ToResult(uvs.pkfields, &querypb.QueryResult{
			Fields: rows.Fields,
			Rows:   []*querypb.Row{rows.Lastpk},
		})
		uvs.setCopyState(tableName, newLastPK)
		log.Infof("NewLastPK: %v", newLastPK)
		return nil
	})
	// If there was a timeout, return without an error.
	select {
	case <-ctx.Done():
		log.Infof("Context done: Copy of %v stopped at lastpk: %v", tableName, newLastPK)
		return ctx.Err()
	default:
	}
	if err != nil {
		return err
	}
	log.Infof("Copy of %v finished at lastpk: %v", tableName, newLastPK)
	delete(uvs.plans, tableName)
	uvs.tablesToCopy = uvs.tablesToCopy[1:]
	return nil
}

func (uvs *uvstreamer) fastForward(ctx context.Context, stopPos string) error {
	log.Infof("starting fastForward from %s upto pos %s", mysql.EncodePosition(uvs.pos), stopPos)
	uvs.stopPos, _ = mysql.DecodePosition(stopPos)
	vs := newVStreamer(uvs.ctx, uvs.cp, uvs.se, uvs.sh, mysql.EncodePosition(uvs.pos), "", uvs.filter, uvs.vschema, uvs.send2)
	uvs.vs = vs
	return vs.Stream()
}
