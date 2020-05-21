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

type unified struct {
}

func (vs *vstreamer) initCopyState() error {
	if len(vs.tablePKs) == 0 {
		return nil
	}

	copyState := make(map[string]*sqltypes.Result)
	for _, tablePK := range vs.tablePKs {
		copyState[tablePK.name] = tablePK.lastPK
	}
	vs.copyState = copyState //FIXME mutex
	return nil
}

func (vs *vstreamer) copy(ctx context.Context) error {
	if err := vs.initCopyState(); err != nil {
		return err
	}
	log.Infof("Inited copy state to %v", vs.copyState)

	for len(vs.copyState) > 0 {
		var tableName string
		for k := range vs.copyState {
			tableName = k
			break
		}
		if err := vs.catchupAndCopy(ctx, tableName); err != nil {
			return err
		}
	}
	log.Info("No tables left to copy")
	return nil
}

func (vs *vstreamer) catchupAndCopy(ctx context.Context, tableName string) error {
	log.Infof("catchupAndCopy for %s", tableName)
	if !vs.pos.IsZero() {
		if err := vs.catchup(ctx); err != nil {
			return err
		}
	}
	return vs.copyTable(ctx, tableName)
}

const (
	waitRetryTime       = 1
	replicaLagTolerance = 0
)

func (vs *vstreamer) catchup(ctx context.Context) error {
	log.Infof("starting catchup ...")
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errch := make(chan error, 1)
	go func() {
		errch <- vs.replicate(ctx) //FIXME: can be more efficient? currently creating new slave each time
	}()

	// Wait for catchup.
	tkr := time.NewTicker(waitRetryTime)
	defer tkr.Stop()
	seconds := int64(replicaLagTolerance / time.Second)
	for {
		sbm := vs.getSecondsBehindMaster()
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
func (vs *vstreamer) sendFieldEvent(ctx context.Context, gtid string, fieldEvent *binlogdatapb.FieldEvent) error {
	ev := &binlogdatapb.VEvent{
		Type:       binlogdatapb.VEventType_FIELD,
		Gtid:       "",
		FieldEvent: fieldEvent,
	}
	log.Infof("Sending field event %v", fieldEvent)
	vs.send([]*binlogdatapb.VEvent{ev})
	return nil

}
func (vs *vstreamer) sendEventsForRows(ctx context.Context, tableName string, rows *binlogdatapb.VStreamRowsResponse) error {
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
	vs.send(evs)
	return nil
}

//TODO figure out how table plan is got from source
func (vs *vstreamer) copyTable(ctx context.Context, tableName string) error {
	var err error
	var newLastPK *sqltypes.Result
	log.Infof("Starting copyTable for %s, PK %v", tableName, vs.copyState[tableName].Rows[0])
	//TODO: need to "filter" filter by tablename in case of multiple filters per table? or just validate matching filters to tablepk map at start?
	//FIXME: getting first filter ... for POC
	err = vs.vse.StreamRows(ctx, vs.filter.Rules[0].Filter, vs.copyState[tableName].Rows[0], func(rows *binlogdatapb.VStreamRowsResponse) error {
		select {
		case <-ctx.Done():
			return io.EOF
		default:
		}
		if vs.fields == nil {
			if len(rows.Fields) == 0 {
				return fmt.Errorf("expecting field event first, got: %v", rows)
			}
			//FIXME: fastforward removed for now
			fieldEvent := &binlogdatapb.FieldEvent{
				TableName: tableName,
				Fields:    rows.Fields,
			}
			vs.fields = rows.Fields
			vs.pkfields = rows.Pkfields
			vs.sendFieldEvent(ctx, rows.Gtid, fieldEvent) //FIXME: gtid should be different for each row
			vs.pos, _ = mysql.DecodePosition(rows.Gtid)   //FIXME
		}
		log.Infof("After batch of rows is sent 1, lastpk is %v, %s, %s", rows.Lastpk, rows.Pkfields, rows.Fields)
		if len(rows.Rows) == 0 {
			return nil
		}

		vs.sendEventsForRows(ctx, rows.Gtid, rows)
		log.Infof("After batch of rows is sent 2, lastpk is %v, %s, %s", rows.Lastpk, rows.Pkfields, rows.Fields)

		newLastPK = sqltypes.CustomProto3ToResult(vs.pkfields, &querypb.QueryResult{
			Fields: vs.fields,
			Rows:   []*querypb.Row{rows.Lastpk},
		})
		vs.copyState[tableName] = newLastPK
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
	delete(vs.copyState, tableName)
	return nil
}

func (vs *vstreamer) getSecondsBehindMaster() int64 {
	return 0 //FIXME
}
