package vstreamer

import (
	"context"
	"fmt"
	"io"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func (uvs *uvstreamer) initCopyState() error {
	if len(uvs.tablePKs) == 0 {
		return nil
	}

	copyState := make(map[string]*sqltypes.Result)
	for _, tablePK := range uvs.tablePKs {
		copyState[tablePK.name] = tablePK.lastPK
	}
	uvs.copyState = copyState //FIXME mutex
	return nil
}

func (uvs *uvstreamer) copy(ctx context.Context) error {
	if err := uvs.initCopyState(); err != nil {
		return err
	}
	log.Infof("Inited copy state to %v", uvs.copyState)

	for len(uvs.copyState) > 0 {
		var tableName string
		for k := range uvs.copyState {
			tableName = k
			break
		}
		if err := uvs.catchupAndCopy(ctx, tableName); err != nil {
			return err
		}
	}
	log.Info("No tables left to copy")
	return nil
}

func (uvs *uvstreamer) catchupAndCopy(ctx context.Context, tableName string) error {
	log.Infof("catchupAndCopy for %s", tableName)
	if !uvs.vs.pos.IsZero() {
		if err := uvs.catchup(ctx); err != nil {
			return err
		}
	}
	return uvs.copyTable(ctx, tableName)
}

const (
	waitRetryTime       = 1
	replicaLagTolerance = 0
)

func (uvs *uvstreamer) catchup(ctx context.Context) error {
	log.Infof("starting catchup ...")
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errch := make(chan error, 1)
	go func() {
		errch <- uvs.vs.replicate(ctx) //FIXME: can be more efficient? currently creating new slave each time
	}()

	// Wait for catchup.
	tkr := time.NewTicker(waitRetryTime)
	defer tkr.Stop()
	seconds := int64(replicaLagTolerance / time.Second)
	for {
		sbm := uvs.getSecondsBehindMaster()
		if sbm < seconds {
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
		Gtid:       "",
		FieldEvent: fieldEvent,
	}
	log.Infof("Sending field event %v", fieldEvent)
	uvs.send([]*binlogdatapb.VEvent{ev})
	return nil

}
func (uvs *uvstreamer) sendEventsForRows(ctx context.Context, tableName string, rows *binlogdatapb.VStreamRowsResponse) error {
	var evs []*binlogdatapb.VEvent
	for i, row := range rows.Rows {
		//begin, rows, copystate, commit
		log.Infof("ROW %d : %v", i+1, row)
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

//TODO figure out how table plan is got from source
func (uvs *uvstreamer) copyTable(ctx context.Context, tableName string) error {
	var err error
	var newLastPK *sqltypes.Result
	log.Infof("Starting copyTable for %s, PK %v", tableName, uvs.copyState[tableName].Rows[0])
	//TODO: need to "filter" filter by tablename in case of multiple filters per table? or just validate matching filters to tablepk map at start?
	//FIXME: getting first filter ... for POC
	err = uvs.vse.StreamRows(ctx, uvs.filter.Rules[0].Filter, uvs.copyState[tableName].Rows[0], func(rows *binlogdatapb.VStreamRowsResponse) error {
		select {
		case <-ctx.Done():
			return io.EOF
		default:
		}
		if uvs.fields == nil {
			//TODO add fast forward
			if len(rows.Fields) == 0 {
				return fmt.Errorf("expecting field event first, got: %v", rows)
			}
			//FIXME: fastforward removed for now
			fieldEvent := &binlogdatapb.FieldEvent{
				TableName: tableName,
				Fields:    rows.Fields,
			}
			uvs.fields = rows.Fields
			uvs.pkfields = rows.Pkfields
			uvs.sendFieldEvent(ctx, rows.Gtid, fieldEvent)  //FIXME: gtid should be different for each row
			uvs.vs.pos, _ = mysql.DecodePosition(rows.Gtid) //FIXME
		}
		log.Infof("After batch of rows is sent 1, lastpk is %v, %s, %s", rows.Lastpk, rows.Pkfields, rows.Fields)
		if len(rows.Rows) == 0 {
			return nil
		}

		uvs.sendEventsForRows(ctx, rows.Gtid, rows)
		log.Infof("After batch of rows is sent 2, lastpk is %v, %s, %s", rows.Lastpk, rows.Pkfields, rows.Fields)

		newLastPK = sqltypes.CustomProto3ToResult(uvs.pkfields, &querypb.QueryResult{
			Fields: uvs.fields,
			Rows:   []*querypb.Row{rows.Lastpk},
		})
		uvs.copyState[tableName] = newLastPK
		log.Infof("NewLastPK: %v", newLastPK)
		return nil
	})
	// If there was a timeout, return without an error.
	select {
	case <-ctx.Done():
		log.Infof("Copy of %v stopped at lastpk: %v", tableName, newLastPK)
		return nil
	default:
	}
	if err != nil {
		return err
	}
	log.Infof("Copy of %v finished at lastpk: %v", tableName, newLastPK)
	delete(uvs.copyState, tableName)
	return nil
}

func (uvs *uvstreamer) getSecondsBehindMaster() int64 {
	return 0 //FIXME
}
