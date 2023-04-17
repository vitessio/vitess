/*
Copyright 2022 The Vitess Authors.

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

package vdiff

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"

	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"
)

// how long to wait for background operations to complete
var BackgroundOperationTimeout = topo.RemoteOperationTimeout * 4

// compareColInfo contains the metadata for a column of the table being diffed
type compareColInfo struct {
	colIndex  int                  // index of the column in the filter's select
	collation collations.Collation // is the collation of the column, if any
	isPK      bool                 // is this column part of the primary key
	colName   string
}

// tableDiffer performs a diff for one table in the workflow.
type tableDiffer struct {
	wd        *workflowDiffer
	tablePlan *tablePlan

	// sourcePrimitive and targetPrimitive are used for streaming
	sourcePrimitive engine.Primitive
	targetPrimitive engine.Primitive

	// sourceQuery is computed from the associated query for this table in the vreplication workflow's Rule Filter
	sourceQuery string
	table       *tabletmanagerdatapb.TableDefinition
	lastPK      *querypb.QueryResult
}

func newTableDiffer(wd *workflowDiffer, table *tabletmanagerdatapb.TableDefinition, sourceQuery string) *tableDiffer {
	return &tableDiffer{wd: wd, table: table, sourceQuery: sourceQuery}
}

// initialize
func (td *tableDiffer) initialize(ctx context.Context) error {
	vdiffEngine := td.wd.ct.vde
	vdiffEngine.snapshotMu.Lock()
	defer vdiffEngine.snapshotMu.Unlock()

	dbClient := td.wd.ct.dbClientFactory()
	if err := dbClient.Connect(); err != nil {
		return err
	}
	defer dbClient.Close()

	targetKeyspace := td.wd.ct.vde.thisTablet.Keyspace
	log.Infof("Locking target keyspace %s", targetKeyspace)
	ctx, unlock, lockErr := td.wd.ct.ts.LockKeyspace(ctx, targetKeyspace, "vdiff")
	if lockErr != nil {
		log.Errorf("LockKeyspace failed: %v", lockErr)
		return lockErr
	}

	var err error
	defer func() {
		unlock(&err)
		if err != nil {
			log.Errorf("UnlockKeyspace %s failed: %v", targetKeyspace, lockErr)
		}
	}()

	if err := td.stopTargetVReplicationStreams(ctx, dbClient); err != nil {
		return err
	}
	defer func() {
		// We use a new context as we want to reset the state even
		// when the parent context has timed out or been canceled.
		log.Infof("Restarting the %q VReplication workflow on target tablets in keyspace %q",
			td.wd.ct.workflow, targetKeyspace)
		restartCtx, restartCancel := context.WithTimeout(context.Background(), BackgroundOperationTimeout)
		defer restartCancel()
		if err := td.restartTargetVReplicationStreams(restartCtx); err != nil {
			log.Errorf("error restarting target streams: %v", err)
		}
	}()

	if err := td.selectTablets(ctx, td.wd.opts.PickerOptions.SourceCell, td.wd.opts.PickerOptions.TabletTypes); err != nil {
		return err
	}
	if err := td.syncSourceStreams(ctx); err != nil {
		return err
	}
	if err := td.startSourceDataStreams(ctx); err != nil {
		return err
	}
	if err := td.syncTargetStreams(ctx); err != nil {
		return err
	}
	if err := td.startTargetDataStream(ctx); err != nil {
		return err
	}
	td.setupRowSorters()
	return nil
}

func (td *tableDiffer) stopTargetVReplicationStreams(ctx context.Context, dbClient binlogplayer.DBClient) error {
	log.Infof("stopTargetVReplicationStreams")
	ct := td.wd.ct
	query := fmt.Sprintf("update _vt.vreplication set state = 'Stopped', message='for vdiff' %s", ct.workflowFilter)
	if _, err := ct.vde.vre.Exec(query); err != nil {
		return err
	}
	// streams are no longer running because vre.Exec would have replaced old controllers and new ones will not start

	// update position of all source streams
	query = fmt.Sprintf("select id, source, pos from _vt.vreplication %s", ct.workflowFilter)
	qr, err := dbClient.ExecuteFetch(query, -1)
	if err != nil {
		return err
	}
	for _, row := range qr.Named().Rows {
		id, _ := row["id"].ToInt64()
		pos := row["pos"].ToString()
		mpos, err := binlogplayer.DecodePosition(pos)
		if err != nil {
			return err
		}
		if mpos.IsZero() {
			return fmt.Errorf("stream %d has not started on tablet %v",
				id, td.wd.ct.vde.thisTablet.Alias)
		}
		sourceBytes, err := row["source"].ToBytes()
		if err != nil {
			return err
		}
		var bls binlogdatapb.BinlogSource
		if err := prototext.Unmarshal(sourceBytes, &bls); err != nil {
			return err
		}
		ct.sources[bls.Shard].position = mpos
	}

	return nil
}

func (td *tableDiffer) forEachSource(cb func(source *migrationSource) error) error {
	ct := td.wd.ct
	var wg sync.WaitGroup
	allErrors := &concurrency.AllErrorRecorder{}
	for _, source := range ct.sources {
		wg.Add(1)
		go func(source *migrationSource) {
			defer wg.Done()
			log.Flush()
			if err := cb(source); err != nil {
				allErrors.RecordError(err)
			}
		}(source)
	}
	wg.Wait()

	return allErrors.AggrError(vterrors.Aggregate)
}

func (td *tableDiffer) selectTablets(ctx context.Context, cell, tabletTypes string) error {
	var wg sync.WaitGroup
	ct := td.wd.ct
	var err1, err2 error

	// For Mount+Migrate, the source tablets will be in a different
	// Vitess cluster with its own TopoServer.
	sourceTopoServer := ct.ts
	if ct.externalCluster != "" {
		extTS, err := ct.ts.OpenExternalVitessClusterServer(ctx, ct.externalCluster)
		if err != nil {
			return err
		}
		sourceTopoServer = extTS
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err1 = td.forEachSource(func(source *migrationSource) error {
			tablet, err := pickTablet(ctx, sourceTopoServer, cell, ct.sourceKeyspace, source.shard, tabletTypes)
			if err != nil {
				return err
			}
			source.tablet = tablet
			return nil
		})
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		tablet, err2 := pickTablet(ctx, ct.ts, td.wd.opts.PickerOptions.TargetCell, ct.vde.thisTablet.Keyspace,
			ct.vde.thisTablet.Shard, td.wd.opts.PickerOptions.TabletTypes)
		if err2 != nil {
			return
		}
		ct.targetShardStreamer = &shardStreamer{
			tablet: tablet,
			shard:  tablet.Shard,
		}
	}()

	wg.Wait()
	if err1 != nil {
		return err1
	}
	return err2
}

func pickTablet(ctx context.Context, ts *topo.Server, cell, keyspace, shard, tabletTypes string) (*topodata.Tablet, error) {
	tp, err := discovery.NewTabletPicker(ts, []string{cell}, keyspace, shard, tabletTypes)
	if err != nil {
		return nil, err
	}
	return tp.PickForStreaming(ctx)
}

func (td *tableDiffer) syncSourceStreams(ctx context.Context) error {
	// source can be replica, wait for them to at least reach max gtid of all target streams
	ct := td.wd.ct
	waitCtx, cancel := context.WithTimeout(ctx, time.Duration(ct.options.CoreOptions.TimeoutSeconds*int64(time.Second)))
	defer cancel()

	if err := td.forEachSource(func(source *migrationSource) error {
		log.Flush()
		if err := ct.tmc.WaitForPosition(waitCtx, source.tablet, mysql.EncodePosition(source.position)); err != nil {
			return vterrors.Wrapf(err, "WaitForPosition for tablet %v", topoproto.TabletAliasString(source.tablet.Alias))
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (td *tableDiffer) syncTargetStreams(ctx context.Context) error {
	ct := td.wd.ct
	waitCtx, cancel := context.WithTimeout(ctx, time.Duration(ct.options.CoreOptions.TimeoutSeconds*int64(time.Second)))
	defer cancel()

	if err := td.forEachSource(func(source *migrationSource) error {
		query := fmt.Sprintf("update _vt.vreplication set state='Running', stop_pos='%s', message='synchronizing for vdiff' where id=%d",
			source.snapshotPosition, source.vrID)
		if _, err := ct.tmc.VReplicationExec(waitCtx, ct.vde.thisTablet, query); err != nil {
			return err
		}
		if err := ct.vde.vre.WaitForPos(waitCtx, source.vrID, source.snapshotPosition); err != nil {
			log.Errorf("WaitForPosition error: %d: %s", source.vrID, err)
			return vterrors.Wrapf(err, "WaitForPosition for stream id %d", source.vrID)
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (td *tableDiffer) startTargetDataStream(ctx context.Context) error {
	ct := td.wd.ct
	gtidch := make(chan string, 1)
	ct.targetShardStreamer.result = make(chan *sqltypes.Result, 1)
	go td.streamOneShard(ctx, ct.targetShardStreamer, td.tablePlan.targetQuery, td.lastPK, gtidch)
	gtid, ok := <-gtidch
	if !ok {
		log.Infof("streaming error: %v", ct.targetShardStreamer.err)
		return ct.targetShardStreamer.err
	}
	ct.targetShardStreamer.snapshotPosition = gtid
	return nil
}

func (td *tableDiffer) startSourceDataStreams(ctx context.Context) error {
	if err := td.forEachSource(func(source *migrationSource) error {
		gtidch := make(chan string, 1)
		source.result = make(chan *sqltypes.Result, 1)
		go td.streamOneShard(ctx, source.shardStreamer, td.tablePlan.sourceQuery, td.lastPK, gtidch)

		gtid, ok := <-gtidch
		if !ok {
			return source.err
		}
		source.snapshotPosition = gtid
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (td *tableDiffer) restartTargetVReplicationStreams(ctx context.Context) error {
	ct := td.wd.ct
	query := fmt.Sprintf("update _vt.vreplication set state='Running', message='', stop_pos='' where db_name=%s and workflow=%s",
		encodeString(ct.vde.dbName), encodeString(ct.workflow))
	log.Infof("Restarting the %q VReplication workflow using %q", ct.workflow, query)
	var err error
	// Let's retry a few times if we get a retryable error.
	for i := 1; i <= 3; i++ {
		_, err := ct.tmc.VReplicationExec(ctx, ct.vde.thisTablet, query)
		if err == nil || !mysql.IsEphemeralError(err) {
			break
		}
		log.Warningf("Encountered the following error while restarting the %q VReplication workflow, will retry (attempt #%d): %v",
			ct.workflow, i, err)
	}
	return err
}

func (td *tableDiffer) streamOneShard(ctx context.Context, participant *shardStreamer, query string, lastPK *querypb.QueryResult, gtidch chan string) {
	log.Infof("streamOneShard Start on %s using query: %s", participant.tablet.Alias.String(), query)
	defer func() {
		log.Infof("streamOneShard End on %s", participant.tablet.Alias.String())
		close(participant.result)
		close(gtidch)
	}()
	participant.err = func() error {
		conn, err := tabletconn.GetDialer()(participant.tablet, false)
		if err != nil {
			return err
		}
		defer conn.Close(ctx)

		target := &querypb.Target{
			Keyspace:   participant.tablet.Keyspace,
			Shard:      participant.shard,
			TabletType: participant.tablet.Type,
		}
		var fields []*querypb.Field
		req := &binlogdatapb.VStreamRowsRequest{Target: target, Query: query, Lastpk: lastPK}
		return conn.VStreamRows(ctx, req, func(vsrRaw *binlogdatapb.VStreamRowsResponse) error {
			// We clone (deep copy) the VStreamRowsResponse -- which contains a vstream packet with N rows and
			// their corresponding GTID position/snapshot along with the LastPK in the row set -- so that we
			// can safely process it while the next VStreamRowsResponse message is getting prepared by the
			// shardStreamer. Without doing this, we would have to serialize the row processing by using
			// unbuffered channels which would present a major performance bottleneck.
			// This need arises from the gRPC VStreamRowsResponse pooling and re-use/recycling done for
			// gRPCQueryClient.VStreamRows() in vttablet/grpctabletconn/conn.
			vsr := proto.Clone(vsrRaw).(*binlogdatapb.VStreamRowsResponse)

			if len(fields) == 0 {
				if len(vsr.Fields) == 0 {
					return fmt.Errorf("did not received expected fields in response %+v on tablet %v",
						vsr, td.wd.ct.vde.thisTablet.Alias)
				}
				fields = vsr.Fields
				gtidch <- vsr.Gtid
			}
			if len(vsr.Rows) == 0 && len(vsr.Fields) == 0 {
				return nil
			}
			p3qr := &querypb.QueryResult{
				Fields: fields,
				Rows:   vsr.Rows,
			}
			result := sqltypes.Proto3ToResult(p3qr)

			// Fields should be received only once, and sent only once.
			if vsr.Fields == nil {
				result.Fields = nil
			}
			select {
			case participant.result <- result:
			case <-ctx.Done():
				return vterrors.Wrap(ctx.Err(), "VStreamRows")
			}
			return nil
		})
	}()
}

func (td *tableDiffer) setupRowSorters() {
	// Combine all sources into a slice and create a merge sorter for it.
	sources := make(map[string]*shardStreamer)
	for shard, source := range td.wd.ct.sources {
		sources[shard] = source.shardStreamer
	}
	td.sourcePrimitive = newMergeSorter(sources, td.tablePlan.comparePKs)

	// Create a merge sorter for the target.
	targets := make(map[string]*shardStreamer)
	targets[td.wd.ct.targetShardStreamer.shard] = td.wd.ct.targetShardStreamer
	td.targetPrimitive = newMergeSorter(targets, td.tablePlan.comparePKs)

	// If there were aggregate expressions, we have to re-aggregate
	// the results, which engine.OrderedAggregate can do.
	if len(td.tablePlan.aggregates) != 0 {
		td.sourcePrimitive = &engine.OrderedAggregate{
			Aggregates:  td.tablePlan.aggregates,
			GroupByKeys: pkColsToGroupByParams(td.tablePlan.pkCols),
			Input:       td.sourcePrimitive,
		}
	}
}

func (td *tableDiffer) diff(ctx context.Context, rowsToCompare int64, debug, onlyPks bool, maxExtraRowsToCompare int64) (*DiffReport, error) {
	dbClient := td.wd.ct.dbClientFactory()
	if err := dbClient.Connect(); err != nil {
		return nil, err
	}
	defer dbClient.Close()

	// We need to continue were we left off when appropriate. This can be an
	// auto-retry on error, or a manual retry via the resume command.
	// Otherwise the existing state will be empty and we start from scratch.
	query := fmt.Sprintf(sqlGetVDiffTable, td.wd.ct.id, encodeString(td.table.Name))
	cs, err := dbClient.ExecuteFetch(query, -1)
	if err != nil {
		return nil, err
	}
	if len(cs.Rows) == 0 {
		return nil, fmt.Errorf("no state found for vdiff table %s for vdiff_id %d on tablet %v",
			td.table.Name, td.wd.ct.id, td.wd.ct.vde.thisTablet.Alias)
	} else if len(cs.Rows) > 1 {
		return nil, fmt.Errorf("invalid state found for vdiff table %s (multiple records) for vdiff_id %d on tablet %v",
			td.table.Name, td.wd.ct.id, td.wd.ct.vde.thisTablet.Alias)
	}
	curState := cs.Named().Row()
	mismatch := curState.AsBool("mismatch", false)
	dr := &DiffReport{}
	if rpt := curState.AsBytes("report", []byte("{}")); json.Valid(rpt) {
		if err = json.Unmarshal(rpt, dr); err != nil {
			return nil, err
		}
	}
	dr.TableName = td.table.Name

	sourceExecutor := newPrimitiveExecutor(ctx, td.sourcePrimitive, "source")
	targetExecutor := newPrimitiveExecutor(ctx, td.targetPrimitive, "target")
	var sourceRow, lastProcessedRow, targetRow []sqltypes.Value
	advanceSource := true
	advanceTarget := true

	// Save our progress when we finish the run
	defer func() {
		if err := td.updateTableProgress(dbClient, dr, lastProcessedRow); err != nil {
			log.Errorf("Failed to update vdiff progress on %s table: %v", td.table.Name, err)
		}
	}()

	for {
		lastProcessedRow = sourceRow

		select {
		case <-ctx.Done():
			return nil, vterrors.Errorf(vtrpcpb.Code_CANCELED, "context has expired")
		default:
		}

		if !mismatch && dr.MismatchedRows > 0 {
			mismatch = true
			log.Infof("Flagging mismatch for %s: %+v", td.table.Name, dr)
			if err := updateTableMismatch(dbClient, td.wd.ct.id, td.table.Name); err != nil {
				return nil, err
			}
		}
		rowsToCompare--
		if rowsToCompare < 0 {
			log.Infof("Stopping vdiff, specified limit reached")
			return dr, nil
		}
		if advanceSource {
			sourceRow, err = sourceExecutor.next()
			if err != nil {
				log.Error(err)
				return nil, err
			}
		}
		if advanceTarget {
			targetRow, err = targetExecutor.next()
			if err != nil {
				log.Error(err)
				return nil, err
			}
		}

		if sourceRow == nil && targetRow == nil {
			return dr, nil
		}

		advanceSource = true
		advanceTarget = true
		if sourceRow == nil {
			diffRow, err := td.genRowDiff(td.tablePlan.sourceQuery, targetRow, debug, onlyPks)
			if err != nil {
				return nil, vterrors.Wrap(err, "unexpected error generating diff")
			}
			dr.ExtraRowsTargetDiffs = append(dr.ExtraRowsTargetDiffs, diffRow)

			// drain target, update count
			count, err := targetExecutor.drain(ctx)
			if err != nil {
				return nil, err
			}
			dr.ExtraRowsTarget += 1 + count
			dr.ProcessedRows += 1 + count
			return dr, nil
		}
		if targetRow == nil {
			// no more rows from the target
			// we know we have rows from source, drain, update count
			diffRow, err := td.genRowDiff(td.tablePlan.sourceQuery, sourceRow, debug, onlyPks)
			if err != nil {
				return nil, vterrors.Wrap(err, "unexpected error generating diff")
			}
			dr.ExtraRowsSourceDiffs = append(dr.ExtraRowsSourceDiffs, diffRow)
			count, err := sourceExecutor.drain(ctx)
			if err != nil {
				return nil, err
			}
			dr.ExtraRowsSource += 1 + count
			dr.ProcessedRows += 1 + count
			return dr, nil
		}

		dr.ProcessedRows++

		// Compare pk values.
		c, err := td.compare(sourceRow, targetRow, td.tablePlan.comparePKs, false)
		switch {
		case err != nil:
			return nil, err
		case c < 0:
			if dr.ExtraRowsSource < maxExtraRowsToCompare {
				diffRow, err := td.genRowDiff(td.tablePlan.sourceQuery, sourceRow, debug, onlyPks)
				if err != nil {
					return nil, vterrors.Wrap(err, "unexpected error generating diff")
				}
				dr.ExtraRowsSourceDiffs = append(dr.ExtraRowsSourceDiffs, diffRow)
			}
			dr.ExtraRowsSource++
			advanceTarget = false
			continue
		case c > 0:
			if dr.ExtraRowsTarget < maxExtraRowsToCompare {
				diffRow, err := td.genRowDiff(td.tablePlan.targetQuery, targetRow, debug, onlyPks)
				if err != nil {
					return nil, vterrors.Wrap(err, "unexpected error generating diff")
				}
				dr.ExtraRowsTargetDiffs = append(dr.ExtraRowsTargetDiffs, diffRow)
			}
			dr.ExtraRowsTarget++
			advanceSource = false
			continue
		}

		// c == 0
		// Compare the non-pk values.
		c, err = td.compare(sourceRow, targetRow, td.tablePlan.compareCols, true)
		switch {
		case err != nil:
			return nil, err
		case c != 0:
			// We don't do a second pass to compare mismatched rows so we can cap the slice here
			if dr.MismatchedRows < maxVDiffReportSampleRows {
				sourceDiffRow, err := td.genRowDiff(td.tablePlan.targetQuery, sourceRow, debug, onlyPks)
				if err != nil {
					return nil, vterrors.Wrap(err, "unexpected error generating diff")
				}
				targetDiffRow, err := td.genRowDiff(td.tablePlan.targetQuery, targetRow, debug, onlyPks)
				if err != nil {
					return nil, vterrors.Wrap(err, "unexpected error generating diff")
				}
				dr.MismatchedRowsDiffs = append(dr.MismatchedRowsDiffs, &DiffMismatch{Source: sourceDiffRow, Target: targetDiffRow})
			}
			dr.MismatchedRows++
		default:
			dr.MatchingRows++
		}

		// Update progress every 10,000 rows as we go along. This will allow us to provide
		// approximate progress information but without too much overhead for when it's not
		// needed or even desired.
		if dr.ProcessedRows%1e4 == 0 {
			if err := td.updateTableProgress(dbClient, dr, sourceRow); err != nil {
				return nil, err
			}
		}
	}
}

func (td *tableDiffer) compare(sourceRow, targetRow []sqltypes.Value, cols []compareColInfo, compareOnlyNonPKs bool) (int, error) {
	for _, col := range cols {
		if col.isPK && compareOnlyNonPKs {
			continue
		}
		compareIndex := col.colIndex
		var (
			c           int
			err         error
			collationID collations.ID
		)
		// If the collation is nil or unknown, use binary collation to compare as bytes.
		if col.collation == nil {
			collationID = collations.CollationBinaryID
		} else {
			collationID = col.collation.ID()
		}
		c, err = evalengine.NullsafeCompare(sourceRow[compareIndex], targetRow[compareIndex], collationID)
		if err != nil {
			return 0, err
		}
		if c != 0 {
			return c, nil
		}
	}
	return 0, nil
}

func (td *tableDiffer) updateTableProgress(dbClient binlogplayer.DBClient, dr *DiffReport, lastRow []sqltypes.Value) error {
	if dr == nil {
		return fmt.Errorf("cannot update progress with a nil diff report")
	}
	var lastPK []byte
	var err error
	var query string
	rpt, err := json.Marshal(dr)
	if err != nil {
		return err
	}
	if lastRow != nil {
		lastPK, err = td.lastPKFromRow(lastRow)
		if err != nil {
			return err
		}

		query = fmt.Sprintf(sqlUpdateTableProgress, dr.ProcessedRows, encodeString(string(lastPK)), encodeString(string(rpt)), td.wd.ct.id, encodeString(td.table.Name))
	} else {
		query = fmt.Sprintf(sqlUpdateTableNoProgress, dr.ProcessedRows, encodeString(string(rpt)), td.wd.ct.id, encodeString(td.table.Name))
	}
	if _, err := dbClient.ExecuteFetch(query, 1); err != nil {
		return err
	}
	return nil
}

func (td *tableDiffer) updateTableState(ctx context.Context, dbClient binlogplayer.DBClient, state VDiffState) error {
	query := fmt.Sprintf(sqlUpdateTableState, encodeString(string(state)), td.wd.ct.id, encodeString(td.table.Name))
	if _, err := dbClient.ExecuteFetch(query, 1); err != nil {
		return err
	}
	insertVDiffLog(ctx, dbClient, td.wd.ct.id, fmt.Sprintf("%s: table %s", state, encodeString(td.table.Name)))

	return nil
}

func (td *tableDiffer) updateTableStateAndReport(ctx context.Context, dbClient binlogplayer.DBClient, state VDiffState, dr *DiffReport) error {
	var report string
	if dr != nil {
		reportJSONBytes, err := json.Marshal(dr)
		if err != nil {
			return err
		}
		report = string(reportJSONBytes)
	} else {
		report = "{}"
	}
	query := fmt.Sprintf(sqlUpdateTableStateAndReport, encodeString(string(state)), dr.ProcessedRows, encodeString(report), td.wd.ct.id, encodeString(td.table.Name))
	if _, err := dbClient.ExecuteFetch(query, 1); err != nil {
		return err
	}
	insertVDiffLog(ctx, dbClient, td.wd.ct.id, fmt.Sprintf("%s: table %s", state, encodeString(td.table.Name)))

	return nil
}

func updateTableMismatch(dbClient binlogplayer.DBClient, vdiffID int64, table string) error {
	query := fmt.Sprintf(sqlUpdateTableMismatch, vdiffID, encodeString(table))
	if _, err := dbClient.ExecuteFetch(query, 1); err != nil {
		return err
	}
	return nil
}

func (td *tableDiffer) lastPKFromRow(row []sqltypes.Value) ([]byte, error) {
	pkColCnt := len(td.tablePlan.pkCols)
	pkFields := make([]*querypb.Field, pkColCnt)
	pkVals := make([]sqltypes.Value, pkColCnt)
	for i, colIndex := range td.tablePlan.pkCols {
		pkFields[i] = td.tablePlan.table.Fields[colIndex]
		pkVals[i] = row[colIndex]
	}
	buf, err := prototext.Marshal(&querypb.QueryResult{
		Fields: pkFields,
		Rows:   []*querypb.Row{sqltypes.RowToProto3(pkVals)},
	})
	return buf, err
}

// If SourceTimeZone is defined in the BinlogSource (_vt.vreplication.source), the
// VReplication workflow would have converted the datetime columns expecting the
// source to have been in the SourceTimeZone and target in TargetTimeZone. We need
// to do the reverse conversion in VDiff before the comparison.
func (td *tableDiffer) adjustForSourceTimeZone(targetSelectExprs sqlparser.SelectExprs, fields map[string]querypb.Type) sqlparser.SelectExprs {
	if td.wd.ct.sourceTimeZone == "" {
		return targetSelectExprs
	}
	log.Infof("source time zone specified: %s", td.wd.ct.sourceTimeZone)
	var newSelectExprs sqlparser.SelectExprs
	var modified bool
	for _, expr := range targetSelectExprs {
		converted := false
		switch selExpr := expr.(type) {
		case *sqlparser.AliasedExpr:
			if colAs, ok := selExpr.Expr.(*sqlparser.ColName); ok {
				var convertTZFuncExpr *sqlparser.FuncExpr
				colName := colAs.Name.Lowered()
				fieldType := fields[colName]
				if fieldType == querypb.Type_DATETIME {
					convertTZFuncExpr = &sqlparser.FuncExpr{
						Name: sqlparser.NewIdentifierCI("convert_tz"),
						Exprs: sqlparser.SelectExprs{
							expr,
							&sqlparser.AliasedExpr{Expr: sqlparser.NewStrLiteral(td.wd.ct.targetTimeZone)},
							&sqlparser.AliasedExpr{Expr: sqlparser.NewStrLiteral(td.wd.ct.sourceTimeZone)},
						},
					}
					log.Infof("converting datetime column %s using convert_tz()", colName)
					newSelectExprs = append(newSelectExprs, &sqlparser.AliasedExpr{Expr: convertTZFuncExpr, As: colAs.Name})
					converted = true
					modified = true
				}
			}
		}
		if !converted { // not datetime
			newSelectExprs = append(newSelectExprs, expr)
		}
	}
	if modified { // at least one datetime was found
		log.Infof("Found datetime columns when SourceTimeZone was set, resetting target SelectExprs after convert_tz()")
		return newSelectExprs
	}
	return targetSelectExprs
}

// updateTableStats runs ANALYZE TABLE on the table in order to update the
// statistics, then it reads those updated stats (specifically the number of
// rows in the table) and saves them in the vdiff_table record.
func (td *tableDiffer) updateTableStats(dbClient binlogplayer.DBClient) error {
	// First update the stats.
	stmt := sqlparser.BuildParsedQuery(sqlAnalyzeTable, td.wd.ct.vde.dbName, td.table.Name)
	if _, err := dbClient.ExecuteFetch(stmt.Query, -1); err != nil {
		return err
	}
	// Now read the updated stats.
	query, err := sqlparser.ParseAndBind(sqlGetTableRows,
		sqltypes.StringBindVariable(td.wd.ct.vde.dbName),
		sqltypes.StringBindVariable(td.table.Name),
	)
	if err != nil {
		return err
	}
	isqr, err := dbClient.ExecuteFetch(query, 1)
	if err != nil {
		return err
	}
	if isqr == nil || len(isqr.Rows) != 1 {
		rows := 0
		if isqr != nil {
			rows = len(isqr.Rows)
		}
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected number of rows returned from %s: %d", query, rows)
	}
	// And finally save the updated stats.
	row := isqr.Named().Row()
	tableRows, err := row.ToInt64("table_rows")
	if err != nil {
		strVal, _ := row.ToString("table_rows")
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid value (%s) returned from %s: %v", strVal, query, err)
	}
	query, err = sqlparser.ParseAndBind(sqlUpdateTableRows,
		sqltypes.Int64BindVariable(tableRows),
		sqltypes.Int64BindVariable(td.wd.ct.id),
		sqltypes.StringBindVariable(td.table.Name),
	)
	if err != nil {
		return err
	}
	if _, err := dbClient.ExecuteFetch(query, 1); err != nil {
		return err
	}
	return nil
}

func getColumnNameForSelectExpr(selectExpression sqlparser.SelectExpr) (string, error) {
	aliasedExpr := selectExpression.(*sqlparser.AliasedExpr)
	expr := aliasedExpr.Expr
	var colname string
	switch t := expr.(type) {
	case *sqlparser.ColName:
		colname = t.Name.Lowered()
	case *sqlparser.FuncExpr: // only in case datetime was converted using convert_tz()
		colname = aliasedExpr.As.Lowered()
	default:
		return "", fmt.Errorf("found target SelectExpr which was neither ColName nor FuncExpr: %+v", aliasedExpr)
	}
	return colname, nil
}
