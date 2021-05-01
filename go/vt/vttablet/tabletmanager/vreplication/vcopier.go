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
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	strings2 "k8s.io/utils/strings"

	"vitess.io/vitess/go/vt/vterrors"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type vcopier struct {
	vr        *vreplicator
	tablePlan *TablePlan
}

func newVCopier(vr *vreplicator) *vcopier {
	return &vcopier{
		vr: vr,
	}
}

// initTablesForCopy (phase 1) identifies the list of tables to be copied and inserts
// them into copy_state. If there are no tables to copy, it explicitly stops
// the stream. Otherwise, the copy phase (phase 2) may think that all tables are copied.
// This will cause us to go into the replication phase (phase 3) without a starting position.
func (vc *vcopier) initTablesForCopy(ctx context.Context) error {
	defer vc.vr.dbClient.Rollback()

	plan, err := buildReplicatorPlan(vc.vr.source.Filter, vc.vr.pkInfoMap, nil, vc.vr.stats)
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
			if err := proto.UnmarshalText(lastpk, &r); err != nil {
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
// until replication is caught up. In order to stop, the seconds behind master has
// to fall below replicationLagTolerance.
func (vc *vcopier) catchup(ctx context.Context, copyState map[string]*sqltypes.Result) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer func() {
		vc.vr.stats.PhaseTimings.Record("catchup", time.Now())
	}()

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
	seconds := int64(replicaLagTolerance / time.Second)
	for {
		sbm := vc.vr.stats.SecondsBehindMaster.Get()
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

func (vc *vcopier) getClientConnection() (*vdbClient, error) {
	dbc := vc.vr.vre.dbClientFactoryFiltered()
	if err := dbc.Connect(); err != nil {
		return nil, vterrors.Wrap(err, "can't connect to database")
	}
	dbClient := newVDBClient(dbc, vc.vr.stats)
	_, err := dbClient.Execute("set foreign_key_checks=0;")
	if err != nil {
		return nil, err
	}
	return dbClient, nil
}

func (vc *vcopier) updateLastPK(dbClient *vdbClient, updateCopyState *sqlparser.ParsedQuery, pkfields []*querypb.Field, lastpk *querypb.Row) error {
	var buf bytes.Buffer
	err := proto.CompactText(&buf, &querypb.QueryResult{
		Fields: pkfields,
		Rows:   []*querypb.Row{lastpk},
	})
	if err != nil {
		return err
	}
	bv := map[string]*querypb.BindVariable{
		"lastpk": {
			Type:  sqltypes.VarBinary,
			Value: buf.Bytes(),
		},
	}
	updateCopyStateQuery, err := updateCopyState.GenerateQuery(bv, nil)
	if err != nil {
		return err
	}
	log.Infof("update pk query: %s", updateCopyStateQuery)
	if _, err := dbClient.Execute(updateCopyStateQuery); err != nil {
		return err
	}
	return nil
}

func getCopyBatchConcurrency() int {
	concurrentBatches := int(*vreplicationParallelBulkInserts)
	if *vreplicationExperimentalFlags /**/ & /**/ vreplicationExperimentalParallelizeBulkInserts == 0 {
		concurrentBatches = 1
	}
	return concurrentBatches
}

// BatchInfo has the information required to insert one batch during the copy phase
type BatchInfo struct {
	rows     []*querypb.Row
	lastpk   *querypb.Row
	dbClient *vdbClient
}

// BatchesInfo has all the information required to insert a set of batches concurrently during the copy phase
type BatchesInfo struct {
	table   string
	done    chan int
	batches []*BatchInfo
}

// NewBatchesInfo returns an initialized object for all batches to be concurrently inserted
func NewBatchesInfo(table string, batches []*BatchInfo) *BatchesInfo {
	return &BatchesInfo{
		table:   table,
		batches: batches,
		done:    make(chan int, getCopyBatchConcurrency()),
	}
}

func (vc *vcopier) copyBatch(ctx context.Context, batch *BatchInfo, ch chan error) {
	var err error
	var qr *sqltypes.Result
	defer func() {
		log.Infof("Writing %v to channel %v", err, ch)
		ch <- err
	}()

	_, err = vc.tablePlan.applyBulkInsert(batch.rows, func(sql string) (*sqltypes.Result, error) {
		if err = batch.dbClient.Begin(); err != nil {
			log.Infof("Error in Begin() %s", err)
			return nil, err
		}
		qr, err = batch.dbClient.ExecuteWithRetry(ctx, sql)
		if err != nil {
			log.Infof("Error in ExecuteWithRetry() %s", err)
			return nil, err
		}
		return qr, err
	})
}

func (vc *vcopier) copyBatches(ctx context.Context, batchesInfo *BatchesInfo, updateCopyState *sqlparser.ParsedQuery,
	pkfields []*querypb.Field, dbClientPool []*vdbClient) error {

	start := time.Now()
	numBatches := len(batchesInfo.batches)
	if numBatches == 0 {
		return fmt.Errorf("no batches passed to copyBatches")
	}
	totalRows := 0
	chans := make([]chan error, numBatches)
	for i := 0; i < numBatches; i++ {
		chans[i] = make(chan error, 1)
		batch := batchesInfo.batches[i]
		batch.dbClient = dbClientPool[i]
		go vc.copyBatch(ctx, batch, chans[i])
		totalRows += len(batch.rows)
	}

	batchToCommitNext := 0
	for batchToCommitNext < numBatches {
		select {
		case <-ctx.Done():
			err := fmt.Errorf("context canceled, timing out batch copy")
			log.Errorf("%s", err)
			vc.vr.stats.ErrorCounts.Add([]string{"BulkCopy"}, 1)
			return err
		case err := <-chans[batchToCommitNext]:
			if err != nil {
				log.Errorf(strings2.ShortenString(err.Error(), 200))
				vc.vr.stats.ErrorCounts.Add([]string{"BulkCopy"}, 1)
				return err
			}
			batch := batchesInfo.batches[batchToCommitNext]
			if err := vc.updateLastPK(batch.dbClient, updateCopyState, pkfields, batch.lastpk); err != nil {
				err = fmt.Errorf("error updating lastpk to %v: %s", batch.lastpk, err)
				log.Errorf(err.Error())
				return err
			}
			if err := batch.dbClient.Commit(); err != nil {
				log.Errorf("Error committing batch with lastpk %v: %s", batch.lastpk, err)
				return err
			}
			log.Infof("committed batch %d, lastpk %v", batchToCommitNext, batch.lastpk)

			elapsedTime := float64(time.Now().UnixNano()-start.UnixNano()) / 1e9
			bandwidth := int64(float64(len(batch.rows)) / elapsedTime)
			vc.vr.stats.CopyBandwidth.Set(bandwidth)
			vc.vr.stats.CopyRowCount.Add(int64(len(batch.rows)))
			vc.vr.stats.QueryCount.Add("copy", 1)

			batchToCommitNext++
		}
	}

	elapsedTime := float64(time.Now().UnixNano()-start.UnixNano()) / 1e9
	bandwidth := int64(float64(totalRows) / elapsedTime)
	vc.vr.stats.BatchCopyBandwidth.Set(bandwidth)
	vc.vr.stats.BatchCopyLoopCount.Add(int64(1))
	duration, err := time.ParseDuration(fmt.Sprintf("%ds", int(elapsedTime)))
	if err != nil {
		return err
	}
	vc.vr.stats.TableCopyTimings.Add(batchesInfo.table, duration)
	return nil
}

// copyTable performs the synchronized copy of the next set of rows from
// the current table being copied. Each packet received is transactionally
// committed with the lastpk. This allows for consistent resumability.
func (vc *vcopier) copyTable(ctx context.Context, tableName string, copyState map[string]*sqltypes.Result) error {
	defer vc.vr.dbClient.Rollback()
	defer func() {
		vc.vr.stats.PhaseTimings.Record("copy", time.Now())
		vc.vr.stats.CopyLoopCount.Add(1)
	}()

	log.Infof("Copying table %s, lastpk: %v", tableName, copyState[tableName])
	plan, err := buildReplicatorPlan(vc.vr.source.Filter, vc.vr.pkInfoMap, nil, vc.vr.stats)
	if err != nil {
		return err
	}

	initialPlan, ok := plan.TargetTables[tableName]
	if !ok {
		return fmt.Errorf("plan not found for table: %s, current plans are: %#v", tableName, plan.TargetTables)
	}

	ctx, cancel := context.WithTimeout(ctx, copyTimeout)
	defer cancel()

	var currentLastPKpb *querypb.QueryResult
	if lastpkqr := copyState[tableName]; lastpkqr != nil {
		currentLastPKpb = sqltypes.ResultToProto3(lastpkqr)
	}
	var updateCopyState *sqlparser.ParsedQuery
	var pkFields []*querypb.Field
	var batches []*BatchInfo
	var insert bool
	var lastpk *querypb.Row

	concurrentBatches := getCopyBatchConcurrency()
	dbClientPool := make([]*vdbClient, concurrentBatches)
	dbClientPool[0] = vc.vr.dbClient
	for i := 1; i < concurrentBatches; i++ {
		dbClientPool[i], err = vc.getClientConnection()
		if err != nil {
			return err
		}
		_, err := dbClientPool[i].Execute("set foreign_key_checks=0;")
		if err != nil {
			return err
		}
	}

	defer func(dbClientPool []*vdbClient) {
		for i := 1; i < concurrentBatches; i++ {
			dbClientPool[i].Close()
		}
	}(dbClientPool)

	err = vc.vr.sourceVStreamer.VStreamRows(ctx, initialPlan.SendRule.Filter, currentLastPKpb, func(rows *binlogdatapb.VStreamRowsResponse) error {
		for {
			select {
			case <-ctx.Done():
				return io.EOF
			default:
			}
			// verify throttler is happy, otherwise keep looping
			if vc.vr.vre.throttlerClient.ThrottleCheckOKOrWait(ctx) {
				break
			}
		}
		if vc.tablePlan == nil {
			if len(rows.Fields) == 0 {
				return fmt.Errorf("expecting field event first, got: %v", rows)
			}
			if err := vc.fastForward(ctx, copyState, rows.Gtid); err != nil {
				return err
			}
			fieldEvent := &binlogdatapb.FieldEvent{
				TableName: initialPlan.SendRule.Match,
				Fields:    rows.Fields,
			}
			vc.tablePlan, err = plan.buildExecutionPlan(fieldEvent)
			if err != nil {
				return err
			}
			pkFields = rows.Pkfields
			buf := sqlparser.NewTrackedBuffer(nil)
			buf.Myprintf("update _vt.copy_state set lastpk=%a where vrepl_id=%s and table_name=%s", ":lastpk",
				strconv.Itoa(int(vc.vr.id)), encodeString(tableName))
			updateCopyState = buf.ParsedQuery()
		}
		if len(rows.Rows) == 0 {
			if len(batches) == 0 {
				return nil
			}
			insert = true
		} else {
			batch := &BatchInfo{
				rows:   rows.Rows,
				lastpk: rows.Lastpk,
			}
			batches = append(batches, batch)
			if len(batches) >= concurrentBatches {
				insert = true
			}
		}
		if insert {
			err = vc.copyBatches(ctx, NewBatchesInfo(tableName, batches), updateCopyState, pkFields, dbClientPool)
			insert = false
			batches = nil
			if err != nil {
				return err
			}
		}

		// The number of rows we receive depends on the packet size set
		// for the row streamer. Since the packet size is roughly equivalent
		// to data size, this should map to a uniform amount of pages affected
		// per statement. A packet size of 30K will roughly translate to 8
		// mysql pages of 4K each.

		return nil
	})

	// If there was a timeout, return without an error.
	select {
	case <-ctx.Done():
		log.Infof("Copy of %v stopped at lastpk: %v", tableName, lastpk)
		return nil
	default:
	}

	if err != nil {
		log.Errorf(err.Error())
		return err
	}

	if len(batches) > 0 {
		err = vc.copyBatches(ctx, NewBatchesInfo(tableName, batches), updateCopyState, pkFields, dbClientPool)
		if err != nil {
			return err
		}
	}

	log.Infof("Copy of %v finished at lastpk: %v", tableName, lastpk)
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("delete from _vt.copy_state where vrepl_id=%s and table_name=%s", strconv.Itoa(int(vc.vr.id)), encodeString(tableName))
	if _, err := vc.vr.dbClient.Execute(buf.String()); err != nil {
		return err
	}
	return nil
}

func (vc *vcopier) fastForward(ctx context.Context, copyState map[string]*sqltypes.Result, gtid string) error {
	defer func() {
		vc.vr.stats.PhaseTimings.Record("fastforward", time.Now())
	}()
	pos, err := mysql.DecodePosition(gtid)
	if err != nil {
		return err
	}
	settings, err := binlogplayer.ReadVRSettings(vc.vr.dbClient, vc.vr.id)
	if err != nil {
		return err
	}
	if settings.StartPos.IsZero() {
		update := binlogplayer.GenerateUpdatePos(vc.vr.id, pos, time.Now().Unix(), 0)
		_, err := vc.vr.dbClient.Execute(update)
		return err
	}
	return newVPlayer(vc.vr, settings, copyState, pos, "fastforward").play(ctx)
}
