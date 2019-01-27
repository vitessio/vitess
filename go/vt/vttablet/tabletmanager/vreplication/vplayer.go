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

package vreplication

import (
	"errors"
	"fmt"
	"io"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	idleTimeout      = 1 * time.Second
	dbLockRetryDelay = 1 * time.Second
	relayLogMaxSize  = 10000
	relayLogMaxItems = 1000
)

type vplayer struct {
	id           uint32
	source       *binlogdatapb.BinlogSource
	sourceTablet *topodatapb.Tablet
	stats        *binlogplayer.Stats
	dbClient     *retryableClient
	// mysqld is used to fetch the local schema.
	mysqld mysqlctl.MysqlDaemon

	pos mysql.Position
	// unsavedGTID when we receive a GTID event and reset
	// if it gets saved. If Fetch returns on idleTimeout,
	// we save the last unsavedGTID.
	unsavedGTID *binlogdatapb.VEvent
	// timeLastSaved is set every time a GTID is saved.
	timeLastSaved time.Time
	stopPos       mysql.Position

	// pplan is built based on the source Filter at the beginning.
	pplan *PlayerPlan
	// tplans[table] is built for each table based on pplan and schema info
	// about the table.
	tplans map[string]*TablePlan
}

func newVPlayer(id uint32, source *binlogdatapb.BinlogSource, sourceTablet *topodatapb.Tablet, stats *binlogplayer.Stats, dbClient binlogplayer.DBClient, mysqld mysqlctl.MysqlDaemon) *vplayer {
	return &vplayer{
		id:            id,
		source:        source,
		sourceTablet:  sourceTablet,
		stats:         stats,
		dbClient:      &retryableClient{DBClient: dbClient},
		mysqld:        mysqld,
		timeLastSaved: time.Now(),
		tplans:        make(map[string]*TablePlan),
	}
}

func (vp *vplayer) Play(ctx context.Context) error {
	if err := vp.setState(binlogplayer.BlpRunning, ""); err != nil {
		return err
	}
	if err := vp.play(ctx); err != nil {
		msg := err.Error()
		vp.stats.History.Add(&binlogplayer.StatsHistoryRecord{
			Time:    time.Now(),
			Message: msg,
		})
		if err := vp.setState(binlogplayer.BlpError, msg); err != nil {
			return err
		}
		return err
	}
	return nil
}

func (vp *vplayer) play(ctx context.Context) error {
	startPos, stopPos, _, _, err := binlogplayer.ReadVRSettings(vp.dbClient, vp.id)
	if err != nil {
		return vp.setState(binlogplayer.BlpStopped, fmt.Sprintf("error reading VReplication settings: %v", err))
	}
	vp.pos, err = mysql.DecodePosition(startPos)
	if err != nil {
		return vp.setState(binlogplayer.BlpStopped, fmt.Sprintf("error decoding start position %v: %v", startPos, err))
	}
	if stopPos != "" {
		vp.stopPos, err = mysql.DecodePosition(stopPos)
		if err != nil {
			return vp.setState(binlogplayer.BlpStopped, fmt.Sprintf("error decoding stop position %v: %v", stopPos, err))
		}
	}
	if !vp.stopPos.IsZero() {
		if vp.pos.AtLeast(vp.stopPos) {
			return vp.setState(binlogplayer.BlpStopped, fmt.Sprintf("Stop position %v already reached: %v", vp.pos, vp.stopPos))
		}
	}
	log.Infof("Starting VReplication player id: %v, startPos: %v, stop: %v, source: %v, filter: %v", vp.id, startPos, vp.stopPos, vp.sourceTablet, vp.source)

	plan, err := buildPlayerPlan(vp.source.Filter)
	if err != nil {
		return err
	}
	vp.pplan = plan

	vsClient, err := tabletconn.GetDialer()(vp.sourceTablet, grpcclient.FailFast(false))
	if err != nil {
		return fmt.Errorf("error dialing tablet: %v", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	relay := newRelayLog(ctx, relayLogMaxItems, relayLogMaxSize)

	target := &querypb.Target{
		Keyspace:   vp.sourceTablet.Keyspace,
		Shard:      vp.sourceTablet.Shard,
		TabletType: vp.sourceTablet.Type,
	}
	log.Infof("Sending vstream command: %v", plan.VStreamFilter)
	streamErr := make(chan error, 1)
	go func() {
		streamErr <- vsClient.VStream(ctx, target, startPos, plan.VStreamFilter, func(events []*binlogdatapb.VEvent) error {
			return relay.Send(events)
		})
	}()

	applyErr := make(chan error, 1)
	go func() {
		applyErr <- vp.applyEvents(ctx, relay)
	}()

	select {
	case err := <-applyErr:
		defer func() {
			// cancel and wait for the other thread to finish.
			cancel()
			<-streamErr
		}()
		// If the apply thread ends with io.EOF, it means either the Engine
		// is shutting down and canceled the context, or stop position was reached.
		// If so, we return nil which will cause the controller to not retry.
		if err == io.EOF {
			return nil
		}
		return err
	case err := <-streamErr:
		defer func() {
			// cancel and wait for the other thread to finish.
			cancel()
			<-applyErr
		}()
		// If context is done, don't return an error.
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		// If the stream ends normally we have to return an error indicating
		// that the controller has to retry a different vttablet.
		if err == nil || err == io.EOF {
			return errors.New("vstream ended")
		}
		return err
	}
}

func (vp *vplayer) applyEvents(ctx context.Context, relay *relayLog) error {
	for {
		items, err := relay.Fetch()
		if err != nil {
			return err
		}
		// This covers two situations:
		// 1. Fetch was idle for idleTimeout.
		// 2. We've been receiving empty events for longer than idleTimeout.
		// In both cases, now > timeLastSaved. If so, any unsaved GTID should be saved.
		if time.Now().Sub(vp.timeLastSaved) >= idleTimeout && vp.unsavedGTID != nil {
			// Although unlikely, we should not save if a transaction is still open.
			// This can happen if a large transaction is split as multiple events.
			if !vp.dbClient.InTransaction {
				if err := vp.updatePos(vp.unsavedGTID.Timestamp); err != nil {
					return err
				}
			}
		}
		for i, events := range items {
			for j, event := range events {
				mustSave := false
				switch event.Type {
				case binlogdatapb.VEventType_COMMIT:
					if vp.pos.Equal(vp.stopPos) {
						mustSave = true
						break
					}
					if hasAnotherCommit(items, i, j+1) {
						continue
					}
				}
				if err := vp.applyEvent(ctx, event, mustSave); err != nil {
					return err
				}
			}
		}
	}
}

func hasAnotherCommit(items [][]*binlogdatapb.VEvent, i, j int) bool {
	for i < len(items) {
		for j < len(items[i]) {
			// We ignore GTID, BEGIN, FIELD and ROW.
			switch items[i][j].Type {
			case binlogdatapb.VEventType_COMMIT:
				return true
			case binlogdatapb.VEventType_DDL:
				return false
			}
			j++
		}
		j = 0
		i++
	}
	return false
}

func (vp *vplayer) applyEvent(ctx context.Context, event *binlogdatapb.VEvent, mustSave bool) error {
	switch event.Type {
	case binlogdatapb.VEventType_GTID:
		pos, err := mysql.DecodePosition(event.Gtid)
		if err != nil {
			return err
		}
		vp.pos = pos
		vp.unsavedGTID = event
		if vp.stopPos.IsZero() {
			return nil
		}
		if !vp.pos.Equal(vp.stopPos) && vp.pos.AtLeast(vp.stopPos) {
			// Code is unreachable, but bad data can cause this to happen.
			if err := vp.setState(binlogplayer.BlpStopped, fmt.Sprintf("next event position %v exceeds stop pos %v, exiting without applying", vp.pos, vp.stopPos)); err != nil {
				return err
			}
			return io.EOF
		}
	case binlogdatapb.VEventType_BEGIN:
		// No-op: begin is called as needed.
	case binlogdatapb.VEventType_COMMIT:
		if mustSave {
			if err := vp.dbClient.Begin(); err != nil {
				return err
			}
		}

		if !vp.dbClient.InTransaction {
			return nil
		}
		if err := vp.updatePos(event.Timestamp); err != nil {
			return err
		}
		posReached := !vp.stopPos.IsZero() && vp.pos.Equal(vp.stopPos)
		if posReached {
			if err := vp.setState(binlogplayer.BlpStopped, fmt.Sprintf("Stopped at position %v", vp.stopPos)); err != nil {
				return err
			}
		}
		if err := vp.dbClient.Commit(); err != nil {
			return err
		}
		if posReached {
			return io.EOF
		}
	case binlogdatapb.VEventType_FIELD:
		if err := vp.dbClient.Begin(); err != nil {
			return err
		}
		if err := vp.updatePlan(event.FieldEvent); err != nil {
			return err
		}
	case binlogdatapb.VEventType_ROW:
		if err := vp.dbClient.Begin(); err != nil {
			return err
		}
		if err := vp.applyRowEvent(ctx, event.RowEvent); err != nil {
			return err
		}
	case binlogdatapb.VEventType_DDL:
		if vp.dbClient.InTransaction {
			return fmt.Errorf("unexpected state: DDL encountered in the middle of a transaction: %v", event.Ddl)
		}
		switch vp.source.OnDdl {
		case binlogdatapb.OnDDLAction_IGNORE:
			// no-op
		case binlogdatapb.OnDDLAction_STOP:
			if err := vp.dbClient.Begin(); err != nil {
				return err
			}
			if err := vp.updatePos(event.Timestamp); err != nil {
				return err
			}
			if err := vp.setState(binlogplayer.BlpStopped, fmt.Sprintf("Stopped at DDL %s", event.Ddl)); err != nil {
				return err
			}
			if err := vp.dbClient.Commit(); err != nil {
				return err
			}
			return io.EOF
		case binlogdatapb.OnDDLAction_EXEC:
			if err := vp.exec(ctx, event.Ddl); err != nil {
				return err
			}
			if err := vp.updatePos(event.Timestamp); err != nil {
				return err
			}
		case binlogdatapb.OnDDLAction_EXEC_IGNORE:
			if err := vp.exec(ctx, event.Ddl); err != nil {
				log.Infof("Ignoring error: %v for DDL: %s", err, event.Ddl)
			}
			if err := vp.updatePos(event.Timestamp); err != nil {
				return err
			}
		}
	}
	return nil
}

func (vp *vplayer) setState(state, message string) error {
	return binlogplayer.SetVReplicationState(vp.dbClient, vp.id, state, message)
}

func (vp *vplayer) updatePlan(fieldEvent *binlogdatapb.FieldEvent) error {
	prelim := vp.pplan.TablePlans[fieldEvent.TableName]
	tplan := &TablePlan{
		Name: fieldEvent.TableName,
	}
	if prelim != nil {
		*tplan = *prelim
	}
	tplan.Fields = fieldEvent.Fields

	if tplan.ColExprs == nil {
		tplan.ColExprs = make([]*ColExpr, len(tplan.Fields))
		for i, field := range tplan.Fields {
			tplan.ColExprs[i] = &ColExpr{
				ColName: sqlparser.NewColIdent(field.Name),
				ColNum:  i,
			}
		}
	} else {
		for _, cExpr := range tplan.ColExprs {
			if cExpr.ColNum >= len(tplan.Fields) {
				// Unreachable code.
				return fmt.Errorf("columns received from vreplication: %v, do not match expected: %v", tplan.Fields, tplan.ColExprs)
			}
		}
	}

	pkcols, err := vp.mysqld.GetPrimaryKeyColumns(vp.dbClient.DBName(), tplan.Name)
	if err != nil {
		return fmt.Errorf("error fetching pk columns for %s: %v", tplan.Name, err)
	}
	if len(pkcols) == 0 {
		// If the table doesn't have a PK, then we treat all columns as PK.
		pkcols, err = vp.mysqld.GetColumns(vp.dbClient.DBName(), tplan.Name)
		if err != nil {
			return fmt.Errorf("error fetching pk columns for %s: %v", tplan.Name, err)
		}
	}
	for _, pkcol := range pkcols {
		found := false
		for i, cExpr := range tplan.ColExprs {
			if cExpr.ColName.EqualString(pkcol) {
				found = true
				tplan.PKCols = append(tplan.PKCols, &ColExpr{
					ColName: cExpr.ColName,
					ColNum:  i,
				})
				break
			}
		}
		if !found {
			return fmt.Errorf("primary key column %s missing from select list for table %s", pkcol, tplan.Name)
		}
	}
	vp.tplans[fieldEvent.TableName] = tplan
	return nil
}

func (vp *vplayer) applyRowEvent(ctx context.Context, rowEvent *binlogdatapb.RowEvent) error {
	tplan := vp.tplans[rowEvent.TableName]
	if tplan == nil {
		return fmt.Errorf("unexpected event on table %s", rowEvent.TableName)
	}
	for _, change := range rowEvent.RowChanges {
		if err := vp.applyRowChange(ctx, tplan, change); err != nil {
			return err
		}
	}
	return nil
}

func (vp *vplayer) applyRowChange(ctx context.Context, tplan *TablePlan, rowChange *binlogdatapb.RowChange) error {
	// MakeRowTrusted is needed here because because Proto3ToResult is not convenient.
	var before, after []sqltypes.Value
	if rowChange.Before != nil {
		before = sqltypes.MakeRowTrusted(tplan.Fields, rowChange.Before)
	}
	if rowChange.After != nil {
		after = sqltypes.MakeRowTrusted(tplan.Fields, rowChange.After)
	}
	var query string
	switch {
	case before == nil && after != nil:
		query = vp.generateInsert(tplan, after)
	case before != nil && after != nil:
		query = vp.generateUpdate(tplan, before, after)
	case before != nil && after == nil:
		query = vp.generateDelete(tplan, before)
	case before == nil && after == nil:
		// unreachable
	}
	if query == "" {
		return nil
	}
	return vp.exec(ctx, query)
}

func (vp *vplayer) generateInsert(tplan *TablePlan, after []sqltypes.Value) string {
	sql := sqlparser.NewTrackedBuffer(nil)
	if tplan.OnInsert == InsertIgnore {
		sql.Myprintf("insert ignore into %v set ", sqlparser.NewTableIdent(tplan.Name))
	} else {
		sql.Myprintf("insert into %v set ", sqlparser.NewTableIdent(tplan.Name))
	}
	vp.writeInsertValues(sql, tplan, after)
	if tplan.OnInsert == InsertOndup {
		sql.Myprintf(" on duplicate key update ")
		_ = vp.writeUpdateValues(sql, tplan, nil, after)
	}
	return sql.String()
}

func (vp *vplayer) generateUpdate(tplan *TablePlan, before, after []sqltypes.Value) string {
	if tplan.OnInsert == InsertIgnore {
		return vp.generateInsert(tplan, after)
	}
	sql := sqlparser.NewTrackedBuffer(nil)
	sql.Myprintf("update %v set ", sqlparser.NewTableIdent(tplan.Name))
	if ok := vp.writeUpdateValues(sql, tplan, before, after); !ok {
		return ""
	}
	sql.Myprintf(" where ")
	vp.writeWhereValues(sql, tplan, before)
	return sql.String()
}

func (vp *vplayer) generateDelete(tplan *TablePlan, before []sqltypes.Value) string {
	sql := sqlparser.NewTrackedBuffer(nil)
	switch tplan.OnInsert {
	case InsertOndup:
		return vp.generateUpdate(tplan, before, nil)
	case InsertIgnore:
		return ""
	default: // insertNormal
		sql.Myprintf("delete from %v where ", sqlparser.NewTableIdent(tplan.Name))
		vp.writeWhereValues(sql, tplan, before)
	}
	return sql.String()
}

func (vp *vplayer) writeInsertValues(sql *sqlparser.TrackedBuffer, tplan *TablePlan, after []sqltypes.Value) {
	separator := ""
	for _, cExpr := range tplan.ColExprs {
		sql.Myprintf("%s%v=", separator, cExpr.ColName)
		separator = ", "
		if cExpr.Operation == OpCount {
			sql.WriteString("1")
		} else {
			if cExpr.Operation == OpSum && after[cExpr.ColNum].IsNull() {
				sql.WriteString("0")
			} else {
				encodeValue(sql, after[cExpr.ColNum])
			}
		}
	}
}

// writeUpdateValues returns true if at least one value was set. Otherwise, it returns false.
func (vp *vplayer) writeUpdateValues(sql *sqlparser.TrackedBuffer, tplan *TablePlan, before, after []sqltypes.Value) bool {
	separator := ""
	hasSet := false
	for _, cExpr := range tplan.ColExprs {
		if cExpr.IsGrouped {
			continue
		}
		if len(before) != 0 && len(after) != 0 {
			if cExpr.Operation == OpCount {
				continue
			}
			bef := before[cExpr.ColNum]
			aft := after[cExpr.ColNum]
			// If both are null, there's no change
			if bef.IsNull() && aft.IsNull() {
				continue
			}
			// If any one of them is null, something has changed.
			if bef.IsNull() || aft.IsNull() {
				goto mustSet
			}
			// Compare content only if none are null.
			if bef.ToString() == aft.ToString() {
				continue
			}
		}
	mustSet:
		sql.Myprintf("%s%v=", separator, cExpr.ColName)
		separator = ", "
		hasSet = true
		if cExpr.Operation == OpCount || cExpr.Operation == OpSum {
			sql.Myprintf("%v", cExpr.ColName)
		}
		if len(before) != 0 {
			switch cExpr.Operation {
			case OpNone:
				if len(after) == 0 {
					sql.WriteString("NULL")
				}
			case OpCount:
				sql.WriteString("-1")
			case OpSum:
				if !before[cExpr.ColNum].IsNull() {
					sql.WriteString("-")
					encodeValue(sql, before[cExpr.ColNum])
				}
			}
		}
		if len(after) != 0 {
			switch cExpr.Operation {
			case OpNone:
				encodeValue(sql, after[cExpr.ColNum])
			case OpCount:
				sql.WriteString("+1")
			case OpSum:
				if !after[cExpr.ColNum].IsNull() {
					sql.WriteString("+")
					encodeValue(sql, after[cExpr.ColNum])
				}
			}
		}
	}
	return hasSet
}

func (vp *vplayer) writeWhereValues(sql *sqlparser.TrackedBuffer, tplan *TablePlan, before []sqltypes.Value) {
	separator := ""
	for _, cExpr := range tplan.PKCols {
		sql.Myprintf("%s%v=", separator, cExpr.ColName)
		separator = " and "
		encodeValue(sql, before[cExpr.ColNum])
	}
}

func (vp *vplayer) updatePos(ts int64) error {
	updatePos := binlogplayer.GenerateUpdatePos(vp.id, vp.pos, time.Now().Unix(), ts)
	if _, err := vp.dbClient.ExecuteFetch(updatePos, 0); err != nil {
		vp.dbClient.Rollback()
		return fmt.Errorf("error %v updating position", err)
	}
	vp.unsavedGTID = nil
	vp.timeLastSaved = time.Now()
	return nil
}

func (vp *vplayer) exec(ctx context.Context, sql string) error {
	vp.stats.Timings.Record("query", time.Now())
	_, err := vp.dbClient.ExecuteFetch(sql, 0)
	for err != nil {
		// 1213: deadlock, 1205: lock wait timeout
		if sqlErr, ok := err.(*mysql.SQLError); ok && sqlErr.Number() == 1213 || sqlErr.Number() == 1205 {
			log.Infof("retryable error: %v, waiting for %v and retrying", sqlErr, dbLockRetryDelay)
			if err := vp.dbClient.Rollback(); err != nil {
				return err
			}
			time.Sleep(dbLockRetryDelay)
			// Check context here. Otherwise this can become an infinite loop.
			select {
			case <-ctx.Done():
				return io.EOF
			default:
			}
			err = vp.dbClient.Retry()
			continue
		}
		return err
	}
	return nil
}

func encodeValue(sql *sqlparser.TrackedBuffer, value sqltypes.Value) {
	// This is currently a separate function because special handling
	// may be needed for certain types.
	// Previously, this function used to convert timestamp to the session
	// time zone, but we now set the session timezone to UTC. So, the timestamp
	// value we receive as UTC can be sent as is.
	// TODO(sougou): handle BIT data type here?
	value.EncodeSQL(sql)
}
