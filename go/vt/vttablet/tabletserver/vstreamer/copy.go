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
	"math"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// starts the copy phase for the first table in the (sorted) list.
// can be continuing the copy of a partially completed table or start a new one
func (uvs *uvstreamer) copy(ctx context.Context) error {
	for len(uvs.tablesToCopy) > 0 {
		tableName := uvs.tablesToCopy[0]
		log.V(2).Infof("Copystate not empty starting catchupAndCopy on table %s", tableName)
		if err := uvs.catchupAndCopy(ctx, tableName); err != nil {
			uvs.vse.errorCounts.Add("Copy", 1)
			return err
		}
	}
	log.Info("No tables left to copy")
	return nil
}

// first does a catchup for tables already fully or partially copied (upto last pk)
func (uvs *uvstreamer) catchupAndCopy(ctx context.Context, tableName string) error {
	log.Infof("catchupAndCopy for %s", tableName)
	if !uvs.pos.IsZero() {
		if err := uvs.catchup(ctx); err != nil {
			log.Infof("catchupAndCopy: catchup returned %v", err)
			uvs.vse.errorCounts.Add("Catchup", 1)
			return err
		}
	}
	log.Infof("catchupAndCopy: before copyTable %s", tableName)
	uvs.fields = nil
	return uvs.copyTable(ctx, tableName)
}

// catchup on events for tables already fully or partially copied (upto last pk) until replication lag is small
func (uvs *uvstreamer) catchup(ctx context.Context) error {
	log.Infof("starting catchup ...")
	uvs.setSecondsBehindMaster(math.MaxInt64)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer func() {
		uvs.vse.vstreamerPhaseTimings.Record("catchup", time.Now())
	}()

	errch := make(chan error, 1)
	go func() {
		startPos := mysql.EncodePosition(uvs.pos)
		vs := newVStreamer(ctx, uvs.cp, uvs.se, startPos, "", uvs.filter, uvs.getVSchema(), uvs.send2, "catchup", uvs.vse)
		uvs.setVs(vs)
		errch <- vs.Stream()
		uvs.setVs(nil)
		log.Infof("catchup vs.stream returned with vs.pos %s", vs.pos.String())
	}()

	// Wait for catchup.
	tkr := time.NewTicker(uvs.config.CatchupRetryTime)
	defer tkr.Stop()
	seconds := int64(uvs.config.MaxReplicationLag / time.Second)
	for {
		sbm := uvs.getSecondsBehindMaster()
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

// field event is sent for every new rowevent or set of rowevents
func (uvs *uvstreamer) sendFieldEvent(ctx context.Context, gtid string, fieldEvent *binlogdatapb.FieldEvent) error {
	evs := []*binlogdatapb.VEvent{{
		Type: binlogdatapb.VEventType_BEGIN,
	}, {
		Type:       binlogdatapb.VEventType_FIELD,
		FieldEvent: fieldEvent,
	}}
	log.V(2).Infof("Sending field event %v, gtid is %s", fieldEvent, gtid)
	uvs.send(evs)

	if err := uvs.setPosition(gtid, true); err != nil {
		log.Infof("setPosition returned error %v", err)
		return err
	}
	return nil
}

// send one RowEvent per row, followed by a LastPK (merged in VTGate with vgtid)
func (uvs *uvstreamer) sendEventsForRows(ctx context.Context, tableName string, rows *binlogdatapb.VStreamRowsResponse, qr *querypb.QueryResult) error {
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
	evs = append(evs, ev)
	evs = append(evs, &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_COMMIT,
	})

	if err := uvs.send(evs); err != nil {
		log.Infof("send returned error %v", err)
		return err
	}
	return nil
}

// converts lastpk from proto to value
func getLastPKFromQR(qr *querypb.QueryResult) []sqltypes.Value {
	if qr == nil {
		return nil
	}
	var lastPK []sqltypes.Value
	r := sqltypes.Proto3ToResult(qr)
	if len(r.Rows) != 1 {
		log.Errorf("unexpected lastpk input: %v", qr)
		return nil
	}
	lastPK = r.Rows[0]
	return lastPK
}

// converts lastpk from value to proto
func getQRFromLastPK(fields []*querypb.Field, lastPK []sqltypes.Value) *querypb.QueryResult {
	row := sqltypes.RowToProto3(lastPK)
	qr := &querypb.QueryResult{
		Fields: fields,
		Rows:   []*querypb.Row{row},
	}
	return qr
}

// gets batch of rows to copy. size of batch is determined by max packetsize
func (uvs *uvstreamer) copyTable(ctx context.Context, tableName string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer func() {
		uvs.vse.vstreamerPhaseTimings.Record("copy", time.Now())
	}()

	var newLastPK *sqltypes.Result
	lastPK := getLastPKFromQR(uvs.plans[tableName].tablePK.Lastpk)
	filter := uvs.plans[tableName].rule.Filter

	log.Infof("Starting copyTable for %s, PK %v", tableName, lastPK)
	uvs.sendTestEvent(fmt.Sprintf("Copy Start %s", tableName))

	err := uvs.vse.StreamRows(ctx, filter, lastPK, func(rows *binlogdatapb.VStreamRowsResponse) error {
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
				if err := uvs.fastForward(rows.Gtid); err != nil {
					uvs.setVs(nil)
					log.Infof("fastForward returned error %v", err)
					return err
				}
				uvs.setVs(nil)
				if mysql.EncodePosition(uvs.pos) != rows.Gtid {
					return fmt.Errorf("position after fastforward was %s but stopPos was %s", uvs.pos, rows.Gtid)
				}
				if err := uvs.setPosition(rows.Gtid, false); err != nil {
					return err
				}
			} else {
				log.V(2).Infof("Not starting fastforward pos is %s, uvs.pos is %s, rows.gtid %s", pos, uvs.pos, rows.Gtid)
			}

			fieldEvent := &binlogdatapb.FieldEvent{
				TableName: tableName,
				Fields:    rows.Fields,
			}
			uvs.fields = rows.Fields
			uvs.pkfields = rows.Pkfields
			if err := uvs.sendFieldEvent(ctx, rows.Gtid, fieldEvent); err != nil {
				log.Infof("sendFieldEvent returned error %v", err)
				return err
			}
		}
		if len(rows.Rows) == 0 {
			log.V(2).Infof("0 rows returned for table %s", tableName)
			return nil
		}

		newLastPK = sqltypes.CustomProto3ToResult(uvs.pkfields, &querypb.QueryResult{
			Fields: rows.Fields,
			Rows:   []*querypb.Row{rows.Lastpk},
		})
		qrLastPK := sqltypes.ResultToProto3(newLastPK)
		log.V(2).Infof("Calling sendEventForRows with gtid %s", rows.Gtid)
		if err := uvs.sendEventsForRows(ctx, tableName, rows, qrLastPK); err != nil {
			log.Infof("sendEventsForRows returned error %v", err)
			return err
		}

		uvs.setCopyState(tableName, qrLastPK)
		log.V(2).Infof("NewLastPK: %v", qrLastPK)
		return nil
	})
	if err != nil {
		uvs.vse.errorCounts.Add("StreamRows", 1)
		return err
	}

	select {
	case <-ctx.Done():
		log.Infof("Context done: Copy of %v stopped at lastpk: %v", tableName, newLastPK)
		return ctx.Err()
	default:
	}

	log.Infof("Copy of %v finished at lastpk: %v", tableName, newLastPK)
	if err := uvs.copyComplete(tableName); err != nil {
		return err
	}
	return nil
}

// processes events between when a table was caught up and when a snapshot is taken for streaming a batch of rows
func (uvs *uvstreamer) fastForward(stopPos string) error {
	defer func() {
		uvs.vse.vstreamerPhaseTimings.Record("fastforward", time.Now())
	}()
	log.Infof("starting fastForward from %s upto pos %s", mysql.EncodePosition(uvs.pos), stopPos)
	uvs.stopPos, _ = mysql.DecodePosition(stopPos)
	vs := newVStreamer(uvs.ctx, uvs.cp, uvs.se, mysql.EncodePosition(uvs.pos), "", uvs.filter, uvs.getVSchema(), uvs.send2, "fastforward", uvs.vse)
	uvs.setVs(vs)
	return vs.Stream()
}
