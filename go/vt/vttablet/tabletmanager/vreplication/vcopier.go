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
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/prototext"

	"context"

	"vitess.io/vitess/go/bytes2"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type vcopier struct {
	vr               *vreplicator
	tablePlan        *TablePlan
	throttlerAppName string
}

func newVCopier(vr *vreplicator) *vcopier {
	return &vcopier{
		vr:               vr,
		throttlerAppName: vr.throttlerAppName(),
	}
}

// initTablesForCopy (phase 1) identifies the list of tables to be copied and inserts
// them into copy_state. If there are no tables to copy, it explicitly stops
// the stream. Otherwise, the copy phase (phase 2) may think that all tables are copied.
// This will cause us to go into the replication phase (phase 3) without a starting position.
func (vc *vcopier) initTablesForCopy(ctx context.Context) error {
	defer vc.vr.dbClient.Rollback()

	plan, err := buildReplicatorPlan(vc.vr.source, vc.vr.colInfoMap, nil, vc.vr.stats)
	if err != nil {
		return err
	}
	if err := vc.vr.dbClient.Begin(); err != nil {
		return err
	}
	// Insert the table list only if at least one table matches.
	if len(plan.TargetTables) != 0 {
		var buf strings.Builder
		buf.WriteString("insert into _vt.copy_state(vrepl_id, table_name) values ")
		prefix := ""
		for name := range plan.TargetTables {
			fmt.Fprintf(&buf, "%s(%d, %s)", prefix, vc.vr.id, encodeString(name))
			prefix = ", "
		}
		if _, err := vc.vr.dbClient.Execute(buf.String()); err != nil {
			return err
		}
		if err := vc.vr.setState(binlogplayer.VReplicationCopying, ""); err != nil {
			return err
		}
		if err := vc.vr.insertLog(LogCopyStart, fmt.Sprintf("Copy phase started for %d table(s)",
			len(plan.TargetTables))); err != nil {
			return err
		}
	} else {
		if err := vc.vr.setState(binlogplayer.BlpStopped, "There is nothing to replicate"); err != nil {
			return err
		}
	}
	return vc.vr.dbClient.Commit()
}

// copyNext performs a multi-step process on each iteration.
// Step 1: catchup: During this step, it replicates from the source from the last position.
// This is a partial replication: events are applied only to tables or subsets of tables
// that have already been copied. This goes on until replication catches up.
// Step 2: Start streaming. This returns the initial field info along with the GTID
// as of which the snapshot is being streamed.
// Step 3: fastForward: The target is fast-forwarded to the GTID obtained. This should
// be quick because we were mostly caught up as of step 1. This ensures that the
// snapshot of the rows are consistent with the position where the target stopped.
// Step 4: copy rows: Copy the next set of rows from the stream that was started in Step 2.
// This goes on until all rows are copied, or a timeout. In both cases, copyNext
// returns, and the replicator decides whether to invoke copyNext again, or to
// go to the next phase if all the copying is done.
// Steps 2, 3 and 4 are performed by copyTable.
// copyNext also builds the copyState metadata that contains the tables and their last
// primary key that was copied. A nil Result means that nothing has been copied.
// A table that was fully copied is removed from copyState.
func (vc *vcopier) copyNext(ctx context.Context, settings binlogplayer.VRSettings) error {
	qr, err := vc.vr.dbClient.Execute(fmt.Sprintf("select table_name, lastpk from _vt.copy_state where vrepl_id=%d", vc.vr.id))
	if err != nil {
		return err
	}
	var tableToCopy string
	copyState := make(map[string]*sqltypes.Result)
	for _, row := range qr.Rows {
		tableName := row[0].ToString()
		lastpk := row[1].ToString()
		if tableToCopy == "" {
			tableToCopy = tableName
		}
		copyState[tableName] = nil
		if lastpk != "" {
			var r querypb.QueryResult
			if err := prototext.Unmarshal([]byte(lastpk), &r); err != nil {
				return err
			}
			copyState[tableName] = sqltypes.Proto3ToResult(&r)
		}
	}
	if len(copyState) == 0 {
		return fmt.Errorf("unexpected: there are no tables to copy")
	}
	if err := vc.catchup(ctx, copyState); err != nil {
		return err
	}
	return vc.copyTable(ctx, tableToCopy, copyState)
}

// catchup replays events to the subset of the tables that have been copied
// until replication is caught up. In order to stop, the seconds behind primary has
// to fall below replicationLagTolerance.
func (vc *vcopier) catchup(ctx context.Context, copyState map[string]*sqltypes.Result) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer vc.vr.stats.PhaseTimings.Record("catchup", time.Now())

	settings, err := binlogplayer.ReadVRSettings(vc.vr.dbClient, vc.vr.id)
	if err != nil {
		return err
	}
	// If there's no start position, it means we're copying the
	// first table. So, there's nothing to catch up to.
	if settings.StartPos.IsZero() {
		return nil
	}

	// Start vreplication.
	errch := make(chan error, 1)
	go func() {
		errch <- newVPlayer(vc.vr, settings, copyState, mysql.Position{}, "catchup").play(ctx)
	}()

	// Wait for catchup.
	tkr := time.NewTicker(waitRetryTime)
	defer tkr.Stop()
	seconds := int64(*replicaLagTolerance / time.Second)
	for {
		sbm := vc.vr.stats.ReplicationLagSeconds.Get()
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

// copyTable performs the synchronized copy of the next set of rows from the
// current table being copied. Each batch or rows received is
// non-transactionally committed with the lastpk in that batch. If new rows are
// inserted, but _vt.copy_state fails to update with the lastpk, those rows are
// deleted in the next call to copyTable. This allows for consistent
// resumability.
func (vc *vcopier) copyTable(ctx context.Context, tableName string, copyState map[string]*sqltypes.Result) error {
	defer vc.vr.dbClient.Rollback()
	defer vc.vr.stats.PhaseTimings.Record("copy", time.Now())
	defer vc.vr.stats.CopyLoopCount.Add(1)

	log.Infof("Copying table %s, lastpk: %v", tableName, copyState[tableName])

	var planCopyState map[string]*sqltypes.Result
	if lastpk, ok := copyState[tableName]; ok && lastpk != nil {
		planCopyState = copyState
	}

	plan, err := buildReplicatorPlan(vc.vr.source, vc.vr.colInfoMap, planCopyState, vc.vr.stats)
	if err != nil {
		return err
	}

	initialPlan, ok := plan.TargetTables[tableName]
	if !ok {
		return fmt.Errorf("plan not found for table: %s, current plans are: %#v", tableName, plan.TargetTables)
	}

	ctx, cancel := context.WithTimeout(ctx, *copyPhaseDuration)
	defer cancel()

	// Prepare a pool of db clients. These will be used by async goroutines to
	// insert data into the target.
	dbClientPool := pools.NewResourcePool(
		/* resource factory */
		func(ctx context.Context) (pools.Resource, error) {
			dbClient := vc.vr.dbClientFactory()
			if err := dbClient.Connect(); err != nil {
				return nil, err
			}
			return dbClient, nil
		},
		/* capacity */
		*copyInsertConcurrency,
		/* max capacity */
		*copyInsertConcurrency,
		/* idle timeout */
		0,
		/* prefill parallelism */
		*copyInsertConcurrency,
		/* log wait */
		nil,
		/* refresh check */
		nil,
		/* refresh interval*/
		0,
	)
	defer dbClientPool.Close()

	// The async insertion goroutines will report errors back to this channel.
	errorCh := make(chan error, 2**copyInsertConcurrency)

	// The async insertion goroutines will enqueue SQL statements to update
	// _vt.copy_state to this channel.
	updateCopyStateChCh := make(chan chan string, 1000**copyInsertConcurrency)

	var lastpkpb *querypb.QueryResult
	if lastpkqr := copyState[tableName]; lastpkqr != nil {
		lastpkpb = sqltypes.ResultToProto3(lastpkqr)
	}

	rowsCopiedTicker := time.NewTicker(rowsCopiedUpdateInterval)
	defer rowsCopiedTicker.Stop()

	var pkfields []*querypb.Field
	var updateCopyState *sqlparser.ParsedQuery
	var bv map[string]*querypb.BindVariable
	var sqlbuffer bytes2.Buffer

	// Create a dedicated db client to asynchronously process SQL statements to
	// update _vt.copy_state.
	dbClient := vc.vr.dbClientFactory()
	if err := dbClient.Connect(); err != nil {
		return err
	}
	defer dbClient.Close()
	// Launch async gorouting to process updates to _vt.copy_state.
	go func(ctx context.Context, dbClient *vdbClient, updateCopyStateChCh chan chan string, isClosed func() bool) {
		var updateCopyState string
		var updateCopyStateCh chan string

		idx := 0

		// Before we exit, commit the last captured updateCopyState.
		defer func() {
			if updateCopyState != "" {
				_, err := dbClient.Execute(updateCopyState)
				if err != nil {
					log.Infof("Got an error commiting copy state: %s", err.Error())
					errorCh <- err
					return
				}
			}
		}()

		// Read updateCopyStateCh until there aren't any left and the dbClientPool.IsClosed.
		for {
			select {
			// Exit without processing all updates if we get a timeout.
			case <-ctx.Done():
				return
			case updateCopyStateCh = <-updateCopyStateChCh:
				break
			default:
				// If the dbClientPool is closed, and we aren't getting any more updateStateCh,
				// we can shut down.
				if isClosed() {
					return
				}
			}

			// Wait until we have an updateCopyState.
			for updateCopyStateCh != nil {
				select {
				case updateCopyState = <-updateCopyStateCh:
					updateCopyStateCh = nil
				case <-ctx.Done():
					return
				}
			}

			// Commit initially and then once every 100 copy states.
			if updateCopyState != "" && idx%100 == 0 {
				_, err := dbClient.Execute(updateCopyState)
				updateCopyState = ""
				if err != nil {
					log.Infof("Got an error commiting copy state: %s", err.Error())
					errorCh <- err
					return
				}
			}

			idx++
		}
	}(ctx, dbClient, updateCopyStateChCh, dbClientPool.IsClosed)

	// Stream rows from source and write asynchronously to target.
	streamErr := vc.vr.sourceVStreamer.VStreamRows(ctx, initialPlan.SendRule.Filter, lastpkpb, func(rows *binlogdatapb.VStreamRowsResponse) error {
		if dbClientPool.IsClosed() {
			return io.EOF
		}

		for {
			select {
			// If any errors were collected from async goroutines since the
			// last loop, return the next one. It might be better to collect
			// and aggregate all queued errors.
			case err := <-errorCh:
				if err != nil {
					return err
				}
			case <-rowsCopiedTicker.C:
				update := binlogplayer.GenerateUpdateRowsCopied(vc.vr.id, vc.vr.stats.CopyRowCount.Get())
				_, _ = vc.vr.dbClient.Execute(update)
			case <-ctx.Done():
				return io.EOF
			default:
			}
			if rows.Throttled {
				_ = vc.vr.updateTimeThrottled(RowStreamerComponentName)
				return nil
			}
			if rows.Heartbeat {
				_ = vc.vr.updateHeartbeatTime(time.Now().Unix())
				return nil
			}
			// verify throttler is happy, otherwise keep looping
			if vc.vr.vre.throttlerClient.ThrottleCheckOKOrWaitAppName(ctx, vc.throttlerAppName) {
				break // out of 'for' loop
			} else { // we're throttled
				_ = vc.vr.updateTimeThrottled(VCopierComponentName)
			}
		}
		// This is the first batch of rows in this iteration of copyTable.
		// Build the full table plan, fast forward, and clear out any inserted
		// rows newer than lastpk.
		if vc.tablePlan == nil {
			if len(rows.Fields) == 0 {
				return fmt.Errorf("expecting field event first, got: %v", rows)
			}
			if err := vc.fastForward(ctx, copyState, rows.Gtid); err != nil {
				return err
			}
			fieldEvent := &binlogdatapb.FieldEvent{
				TableName: initialPlan.SendRule.Match,
			}
			fieldEvent.Fields = append(fieldEvent.Fields, rows.Fields...)
			vc.tablePlan, err = plan.buildExecutionPlan(fieldEvent)
			if err != nil {
				return err
			}
			pkfields = append(pkfields, rows.Pkfields...)

			log.Infof("Deleting any rows after the recorded lastpk: %s", vc.tablePlan.DeleteAfterLastpk.Query)
			er, err := vc.vr.dbClient.Execute(vc.tablePlan.DeleteAfterLastpk.Query)
			if err != nil {
				return err
			}
			if er.RowsAffected > 0 {
				log.Infof("Deleted %d rows after the recorded lastpk.", er.RowsAffected)
			}

			buf := sqlparser.NewTrackedBuffer(nil)
			buf.Myprintf("update _vt.copy_state set lastpk=%a where vrepl_id=%s and table_name=%s", ":lastpk", strconv.Itoa(int(vc.vr.id)), encodeString(tableName))
			updateCopyState = buf.ParsedQuery()
		}
		if len(rows.Rows) == 0 {
			return nil
		}

		// Build insert SQL statement.
		if err := vc.tablePlan.writeBulkInsert(&sqlbuffer, rows); err != nil {
			return err
		}
		insertRows := sqlbuffer.String()

		// Build update copy state SQL statement.
		var buf []byte
		buf, err = prototext.Marshal(&querypb.QueryResult{
			Fields: pkfields,
			Rows:   []*querypb.Row{rows.Lastpk},
		})
		if err != nil {
			return err
		}
		bv = map[string]*querypb.BindVariable{
			"lastpk": {
				Type:  sqltypes.VarBinary,
				Value: buf,
			},
		}
		updateState, err := updateCopyState.GenerateQuery(bv, nil)
		if err != nil {
			return err
		}

		// Acquire a db client from the pool.
		poolResource, err := dbClientPool.Get(ctx)
		if err != nil {
			return err
		}

		dbClient := poolResource.(*vdbClient)

		// Synchronously enqueue a channel which will contain the copy state
		// update for this batch of rows.  This helps enforce that:
		// - Updates to _vt.copy_state happen after rows are inserted.
		// - Updates to _vt.copy_state happen in primary-key order.
		updateCopyStateCh := make(chan string, 1)
		updateCopyStateChCh <- updateCopyStateCh

		// Asynchronously insert rows and enqueue copy state update.
		go func(dbClient *vdbClient, insertRows, updateState string, errorCh chan error, _ chan string) {
			defer dbClientPool.Put(dbClient)

			var err error
			var qr *sqltypes.Result
			var start time.Time

			if err = dbClient.Begin(); err != nil {
				goto DONE
			}

			start = time.Now()

			if qr, err = dbClient.ExecuteWithRetry(ctx, insertRows); err != nil {
				goto DONE
			}

			vc.vr.stats.QueryTimings.Record("copy", start)
			vc.vr.stats.CopyRowCount.Add(int64(qr.RowsAffected))
			vc.vr.stats.QueryCount.Add("copy", 1)

			if err = dbClient.Commit(); err != nil {
				goto DONE
			}

			select {
			case updateCopyStateCh <- updateState:
			default:
				// Enqueuing to updateCopyStateCh could become a bottleneck if
				// updates to _vt.copy_state happen slower than inserts into
				// the target table. This could be a good place for a new
				// metric.
				log.Infof("updatecopyStateCh is full, waiting until there's space")
				updateCopyStateCh <- updateState
			}
		DONE:
			// If we got an error, put it into the errorCh. It will be
			// collected in a subsequent loop by the caller of this function.
			if err != nil {
				errorCh <- err
			}
		}(dbClient, insertRows, updateState, errorCh, updateCopyStateCh)

		return nil
	})

	// Close client pool and collect any outstanding errors.
	dbClientPool.Close()
	asyncErrs := make([]error, 0)
	for errorCh != nil {
		select {
		case err := <-errorCh:
			if err != nil {
				asyncErrs = append(asyncErrs, err)
			}
		default:
			errorCh = nil
		}
	}

	// If there was a timeout, return without an error.
	select {
	case <-ctx.Done():
		log.Infof("Copy of %v stopped at lastpk: %v", tableName, bv)
		return nil
	default:
	}

	// If the streamer encountered an error, return it.
	if streamErr != nil {
		return streamErr
	}

	log.Infof("Copy of %v finished at lastpk: %v", tableName, bv)

	// If the sender encountered an error, return it.
	if len(asyncErrs) > 0 {
		return vterrors.Aggregate(asyncErrs)
	}

	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("delete from _vt.copy_state where vrepl_id=%s and table_name=%s", strconv.Itoa(int(vc.vr.id)), encodeString(tableName))
	if _, err := vc.vr.dbClient.Execute(buf.String()); err != nil {
		return err
	}
	return nil
}

func (vc *vcopier) fastForward(ctx context.Context, copyState map[string]*sqltypes.Result, gtid string) error {
	defer vc.vr.stats.PhaseTimings.Record("fastforward", time.Now())
	pos, err := mysql.DecodePosition(gtid)
	if err != nil {
		return err
	}
	settings, err := binlogplayer.ReadVRSettings(vc.vr.dbClient, vc.vr.id)
	if err != nil {
		return err
	}
	if settings.StartPos.IsZero() {
		update := binlogplayer.GenerateUpdatePos(vc.vr.id, pos, time.Now().Unix(), 0, vc.vr.stats.CopyRowCount.Get(), *vreplicationStoreCompressedGTID)
		_, err := vc.vr.dbClient.Execute(update)
		return err
	}
	return newVPlayer(vc.vr, settings, copyState, pos, "fastforward").play(ctx)
}
