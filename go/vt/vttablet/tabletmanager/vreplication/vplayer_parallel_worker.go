/*
Copyright 2024 The Vitess Authors.

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
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
)

var (
	terminateWorkerEvent = &binlogdatapb.VEvent{
		Type:           binlogdatapb.VEventType_UNKNOWN,
		LastCommitted:  0,
		SequenceNumber: 0,
	}
)

type parallelWorker struct {
	pool           *parallelWorkersPool
	index          int
	lastCommitted  int64
	sequenceNumber int64
	dbClient       *vdbClient
	queryFunc      func(ctx context.Context, sql string) (*sqltypes.Result, error)
	vp             *vplayer
	wakeup         chan int

	pos replication.Position
	// foreignKeyChecksEnabled is the current state of the foreign key checks for the current session.
	// It reflects what we have set the @@session.foreign_key_checks session variable to.
	foreignKeyChecksEnabled bool
	// foreignKeyChecksStateInitialized is set to true once we have initialized the foreignKeyChecksEnabled.
	// The initialization is done on the first row event that this vplayer sees.
	foreignKeyChecksStateInitialized bool
	events                           chan *binlogdatapb.VEvent
	stats                            *VrLogStats
}

func (w *parallelWorker) recycle() {
	w.pool.pool <- w
}

func (w *parallelWorker) begin() error {
	return w.dbClient.Begin()
}

// applyQueuedStmtEvent applies an actual DML statement received from the source, directly onto the backend database
func (w *parallelWorker) applyQueuedStmtEvent(ctx context.Context, event *binlogdatapb.VEvent) error {
	vp := w.vp
	sql := event.Statement
	if sql == "" {
		sql = event.Dml
	}
	if event.Type == binlogdatapb.VEventType_SAVEPOINT || vp.canAcceptStmtEvents {
		start := time.Now()
		_, err := w.queryFunc(ctx, sql)
		vp.vr.stats.QueryTimings.Record(vp.phase, start)
		vp.vr.stats.QueryCount.Add(vp.phase, 1)
		return err
	}
	return fmt.Errorf("filter rules are not supported for SBR replication: %v", vp.vr.source.Filter.GetRules())
}

// updatePos should get called at a minimum of vreplicationMinimumHeartbeatUpdateInterval.
func (w *parallelWorker) updatePos(ctx context.Context, ts int64) (posReached bool, err error) {
	update := binlogplayer.GenerateUpdatePos(w.vp.vr.id, w.pos, time.Now().Unix(), ts, w.vp.vr.stats.CopyRowCount.Get(), w.vp.vr.workflowConfig.StoreCompressedGTID)
	if _, err := w.queryFunc(ctx, update); err != nil {
		return false, fmt.Errorf("error %v updating position", err)
	}
	w.vp.numAccumulatedHeartbeats = 0
	w.vp.unsavedEvent = nil
	w.vp.timeLastSaved = time.Now()
	w.vp.vr.stats.SetLastPosition(w.pos)
	posReached = !w.vp.stopPos.IsZero() && w.pos.AtLeast(w.vp.stopPos)
	if posReached {
		log.Infof("Stopped at position: %v", w.vp.stopPos)
		if w.vp.saveStop {
			if err := w.vp.vr.setState(binlogdatapb.VReplicationWorkflowState_Stopped, fmt.Sprintf("Stopped at position %v", w.vp.stopPos)); err != nil {
				return false, err
			}
		}
	}
	return posReached, nil
}

// updateFKCheck updates the @@session.foreign_key_checks variable based on the binlog row event flags.
// The function only does it if it has changed to avoid redundant updates, using the cached vplayer.foreignKeyChecksEnabled
// The foreign_key_checks value for a transaction is determined by the 2nd bit (least significant) of the flags:
// - If set (1), foreign key checks are disabled.
// - If unset (0), foreign key checks are enabled.
// updateFKCheck also updates the state for the first row event that this vplayer, and hence the db connection, sees.
func (w *parallelWorker) updateFKCheck(ctx context.Context, flags2 uint32) error {
	mustUpdate := false
	if w.vp.vr.WorkflowSubType == int32(binlogdatapb.VReplicationWorkflowSubType_AtomicCopy) {
		// If this is an atomic copy, we must update the foreign_key_checks state even when the vplayer runs during
		// the copy phase, i.e., for catchup and fastforward.
		mustUpdate = true
	} else if w.vp.vr.state == binlogdatapb.VReplicationWorkflowState_Running {
		// If the vreplication workflow is in Running state, we must update the foreign_key_checks
		// state for all workflow types.
		mustUpdate = true
	}
	if !mustUpdate {
		return nil
	}
	dbForeignKeyChecksEnabled := true
	if flags2&NoForeignKeyCheckFlagBitmask == NoForeignKeyCheckFlagBitmask {
		dbForeignKeyChecksEnabled = false
	}

	if w.foreignKeyChecksStateInitialized /* already set earlier */ &&
		dbForeignKeyChecksEnabled == w.foreignKeyChecksEnabled /* no change in the state, no need to update */ {
		return nil
	}
	log.Infof("Setting this session's foreign_key_checks to %s", strconv.FormatBool(dbForeignKeyChecksEnabled))
	if _, err := w.queryFunc(ctx, "set @@session.foreign_key_checks="+strconv.FormatBool(dbForeignKeyChecksEnabled)); err != nil {
		return fmt.Errorf("failed to set session foreign_key_checks: %w", err)
	}
	w.foreignKeyChecksEnabled = dbForeignKeyChecksEnabled
	if !w.foreignKeyChecksStateInitialized {
		log.Infof("First foreign_key_checks update to: %s", strconv.FormatBool(dbForeignKeyChecksEnabled))
		w.foreignKeyChecksStateInitialized = true
	}
	return nil
}

func (w *parallelWorker) applyEvent(ctx context.Context, event *binlogdatapb.VEvent, mustSave bool) error {
	w.events <- event
	return nil
}

func (w *parallelWorker) applyQueuedEvents(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case vevent := <-w.events:
			if vevent == terminateWorkerEvent {
				// An indication that there are no more events for this worker
				return nil
			}
			if err := w.applyQueuedEvent(ctx, vevent, true); err != nil {
				// wasError = true
				if err != io.EOF {
					w.vp.vr.stats.ErrorCounts.Add([]string{"Apply"}, 1)
					var table, tableLogMsg, gtidLogMsg string
					switch {
					case vevent.GetFieldEvent() != nil:
						table = vevent.GetFieldEvent().TableName
					case vevent.GetRowEvent() != nil:
						table = vevent.GetRowEvent().TableName
					}
					if table != "" {
						tableLogMsg = fmt.Sprintf(" for table %s", table)
					}
					gtidLogMsg = fmt.Sprintf(" while processing position %v", w.pos)
					log.Errorf("Error applying event%s%s: %s", tableLogMsg, gtidLogMsg, err.Error())
					err = vterrors.Wrapf(err, "error applying event%s%s", tableLogMsg, gtidLogMsg)
				}
				return err
			}
			// No error
		}
	}
}

func (w *parallelWorker) applyQueuedCommit(ctx context.Context, vevent *binlogdatapb.VEvent) error {
	// Only the head worker can commit. This is because we want to
	// commit in the order of events. It is theoretically possible to commit
	// out of order, but we want to keep the code simple as well as to comply
	// with vreplication's sequential GTID tracking.
	for {
		headIndex := w.pool.headIndex()
		if w.index == headIndex {
			break
		}
		select {
		case err := <-w.pool.workerErrors:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	// Now that we're at head, we should be able to commit. If we fail to, that's a
	// vreplication / vplayer error.
	if err := w.dbClient.Begin(); err != nil {
		return err
	}

	if !w.dbClient.InTransaction {
		// We're skipping an empty transaction. We may have to save the position on inactivity.
		w.vp.unsavedEvent = vevent
		return nil
	}
	posReached, err := w.updatePos(ctx, vevent.Timestamp)
	if err != nil {
		return err
	}

	if err := w.dbClient.Commit(); err != nil {
		return err
	}

	w.pool.handoverHead(w.index)

	if posReached {
		w.pool.posReached.Store(true)
		return io.EOF
	}
	// No more events for this worker
	return nil
}

func (w *parallelWorker) applyQueuedRowEvent(ctx context.Context, vevent *binlogdatapb.VEvent, applyFunc func(sql string) (*sqltypes.Result, error)) error {
	if err := w.updateFKCheck(ctx, vevent.RowEvent.Flags); err != nil {
		return err
	}
	var tplan *TablePlan
	func() {
		w.vp.planMu.Lock()
		defer w.vp.planMu.Unlock()
		tplan = w.vp.tablePlans[vevent.RowEvent.TableName]
	}()
	if tplan == nil {
		return fmt.Errorf("unexpected event on table %s", vevent.RowEvent.TableName)
	}
	applyFuncWithStats := func(sql string) (*sqltypes.Result, error) {
		stats := NewVrLogStats("ROWCHANGE")
		start := time.Now()
		qr, err := w.queryFunc(ctx, sql)
		w.vp.vr.stats.QueryCount.Add(w.vp.phase, 1)
		w.vp.vr.stats.QueryTimings.Record(w.vp.phase, start)
		stats.Send(sql)
		return qr, err
	}

	rowEvent := vevent.RowEvent
	if w.vp.batchMode && len(rowEvent.RowChanges) > 1 {
		// If we have multiple delete row events for a table with a single PK column
		// then we can perform a simple bulk DELETE using an IN clause.
		if (rowEvent.RowChanges[0].Before != nil && rowEvent.RowChanges[0].After == nil) &&
			tplan.MultiDelete != nil {
			_, err := tplan.applyBulkDeleteChanges(rowEvent.RowChanges, applyFunc, w.dbClient.maxBatchSize)
			return err
		}
		// If we're done with the copy phase then we will be replicating all INSERTS
		// regardless of the PK value and can use a single INSERT statment with
		// multiple VALUES clauses.
		if len(w.vp.copyState) == 0 && (rowEvent.RowChanges[0].Before == nil && rowEvent.RowChanges[0].After != nil) {
			_, err := tplan.applyBulkInsertChanges(rowEvent.RowChanges, applyFunc, w.dbClient.maxBatchSize)
			return err
		}
	}

	currentConcurrency := w.pool.currentConcurrency.Add(1)
	defer w.pool.currentConcurrency.Add(-1)
	if currentConcurrency > w.pool.maxConcurrency.Load() {
		w.pool.maxConcurrency.Store(currentConcurrency)
	}
	for _, change := range vevent.RowEvent.RowChanges {
		if _, err := tplan.applyChange(change, applyFuncWithStats); err != nil {
			return err
		}
	}
	return nil
}

func (w *parallelWorker) applyQueuedEvent(ctx context.Context, event *binlogdatapb.VEvent, mustSave bool) error {
	for !w.pool.isApplicable(w, event.Type) {
		<-w.wakeup
	}
	applyFunc := func(sql string) (*sqltypes.Result, error) {
		return w.queryFunc(ctx, sql)
	}

	stats := NewVrLogStats(event.Type.String())
	switch event.Type {
	case binlogdatapb.VEventType_GTID:
		pos, err := binlogplayer.DecodePosition(event.Gtid)
		if err != nil {
			return err
		}
		w.pos = pos
		// A new position should not be saved until a saveable event occurs.
		w.vp.unsavedEvent = nil
		if w.vp.stopPos.IsZero() {
			return nil
		}
	case binlogdatapb.VEventType_BEGIN:
		// No-op: begin is called as needed.
	case binlogdatapb.VEventType_COMMIT:
		if err := w.applyQueuedCommit(ctx, event); err != nil {
			return err
		}
		w.pool.numCommits.Add(1)
	case binlogdatapb.VEventType_FIELD:
		if err := w.dbClient.Begin(); err != nil {
			return err
		}
		tplan, err := w.vp.replicatorPlan.buildExecutionPlan(event.FieldEvent)
		if err != nil {
			return err
		}
		func() {
			w.vp.planMu.Lock()
			defer w.vp.planMu.Unlock()

			w.vp.tablePlans[event.FieldEvent.TableName] = tplan
		}()
		stats.Send(fmt.Sprintf("%v", event.FieldEvent))

	case binlogdatapb.VEventType_INSERT, binlogdatapb.VEventType_DELETE, binlogdatapb.VEventType_UPDATE,
		binlogdatapb.VEventType_REPLACE, binlogdatapb.VEventType_SAVEPOINT:
		// use event.Statement if available, preparing for deprecation in 8.0
		sql := event.Statement
		if sql == "" {
			sql = event.Dml
		}
		// If the event is for one of the AWS RDS "special" or pt-table-checksum tables, we skip
		if !strings.Contains(sql, " mysql.rds_") && !strings.Contains(sql, " percona.checksums") {
			// This is a player using statement based replication
			if err := w.dbClient.Begin(); err != nil {
				return err
			}
			if err := w.applyQueuedStmtEvent(ctx, event); err != nil {
				return err
			}
			stats.Send(sql)
		}
	case binlogdatapb.VEventType_ROW:
		if err := w.applyQueuedRowEvent(ctx, event, applyFunc); err != nil {
			return err
		}
		// Row event is logged AFTER RowChanges are applied so as to calculate the total elapsed
		// time for the Row event.
		stats.Send(fmt.Sprintf("%v", event.RowEvent))
	case binlogdatapb.VEventType_OTHER:
		if w.dbClient.InTransaction {
			// Unreachable
			log.Errorf("internal error: vplayer is in a transaction on event: %v", event)
			return fmt.Errorf("internal error: vplayer is in a transaction on event: %v", event)
		}
		// Just update the position.
		posReached, err := w.updatePos(ctx, event.Timestamp)
		if err != nil {
			return err
		}
		if posReached {
			return io.EOF
		}
	case binlogdatapb.VEventType_DDL:
		if w.dbClient.InTransaction {
			// Unreachable
			log.Errorf("internal error: vplayer is in a transaction on event: %v", event)
			return fmt.Errorf("internal error: vplayer is in a transaction on event: %v", event)
		}
		w.vp.vr.stats.DDLEventActions.Add(w.vp.vr.source.OnDdl.String(), 1) // Record the DDL handling
		switch w.vp.vr.source.OnDdl {
		case binlogdatapb.OnDDLAction_IGNORE:
			// We still have to update the position.
			posReached, err := w.updatePos(ctx, event.Timestamp)
			if err != nil {
				return err
			}
			if posReached {
				return io.EOF
			}
		case binlogdatapb.OnDDLAction_STOP:
			if err := w.dbClient.Begin(); err != nil {
				return err
			}
			if _, err := w.updatePos(ctx, event.Timestamp); err != nil {
				return err
			}
			if err := w.vp.vr.setState(binlogdatapb.VReplicationWorkflowState_Stopped, fmt.Sprintf("Stopped at DDL %s", event.Statement)); err != nil {
				return err
			}
			if err := w.dbClient.Commit(); err != nil {
				return err
			}
			return io.EOF
		case binlogdatapb.OnDDLAction_EXEC:
			// It's impossible to save the position transactionally with the statement.
			// So, we apply the DDL first, and then save the position.
			// Manual intervention may be needed if there is a partial
			// failure here.
			if _, err := w.queryFunc(ctx, event.Statement); err != nil {
				return err
			}
			stats.Send(fmt.Sprintf("%v", event.Statement))
			posReached, err := w.updatePos(ctx, event.Timestamp)
			if err != nil {
				return err
			}
			if posReached {
				return io.EOF
			}
		case binlogdatapb.OnDDLAction_EXEC_IGNORE:
			if _, err := w.queryFunc(ctx, event.Statement); err != nil {
				log.Infof("Ignoring error: %v for DDL: %s", err, event.Statement)
			}
			stats.Send(fmt.Sprintf("%v", event.Statement))
			posReached, err := w.updatePos(ctx, event.Timestamp)
			if err != nil {
				return err
			}
			if posReached {
				return io.EOF
			}
		}
	case binlogdatapb.VEventType_JOURNAL:
		if w.dbClient.InTransaction {
			// Unreachable
			log.Errorf("internal error: vplayer is in a transaction on event: %v", event)
			return fmt.Errorf("internal error: vplayer is in a transaction on event: %v", event)
		}
		// Ensure that we don't have a partial set of table matches in the journal.
		switch event.Journal.MigrationType {
		case binlogdatapb.MigrationType_SHARDS:
			// All tables of the source were migrated. So, no validation needed.
		case binlogdatapb.MigrationType_TABLES:
			// Validate that all or none of the tables are in the journal.
			jtables := make(map[string]bool)
			for _, table := range event.Journal.Tables {
				jtables[table] = true
			}
			found := false
			notFound := false
			for tableName := range w.vp.replicatorPlan.TablePlans {
				if _, ok := jtables[tableName]; ok {
					found = true
				} else {
					notFound = true
				}
			}
			switch {
			case found && notFound:
				// Some were found and some were not found. We can't handle this.
				if err := w.vp.vr.setState(binlogdatapb.VReplicationWorkflowState_Stopped, "unable to handle journal event: tables were partially matched"); err != nil {
					return err
				}
				return io.EOF
			case notFound:
				// None were found. Ignore journal.
				return nil
			}
			// All were found. We must register journal.
		}
		log.Infof("Binlog event registering journal event %+v", event.Journal)
		if err := w.vp.vr.vre.registerJournal(event.Journal, w.vp.vr.id); err != nil {
			if err := w.vp.vr.setState(binlogdatapb.VReplicationWorkflowState_Stopped, err.Error()); err != nil {
				return err
			}
			return io.EOF
		}
		stats.Send(fmt.Sprintf("%v", event.Journal))
		return io.EOF
	case binlogdatapb.VEventType_HEARTBEAT:
		if event.Throttled {
			if err := w.vp.vr.updateTimeThrottled(throttlerapp.VStreamerName, event.ThrottledReason); err != nil {
				return err
			}
		}
		if !w.dbClient.InTransaction {
			w.vp.numAccumulatedHeartbeats++
			if err := w.vp.recordHeartbeat(); err != nil {
				return err
			}
		}
	}

	return nil
}
