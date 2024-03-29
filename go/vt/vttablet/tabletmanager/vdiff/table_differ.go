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
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

type tableDiffPhase string

const (
	initializing           = tableDiffPhase("initializing")
	pickingTablets         = tableDiffPhase("picking_streaming_tablets")
	syncingSources         = tableDiffPhase("syncing_source_streams")
	syncingTargets         = tableDiffPhase("syncing_target_streams")
	startingSources        = tableDiffPhase("starting_source_data_streams")
	startingTargets        = tableDiffPhase("starting_target_data_streams")
	restartingVreplication = tableDiffPhase("restarting_vreplication_streams")
	diffingTable           = tableDiffPhase("diffing_table")
)

// how long to wait for background operations to complete
var BackgroundOperationTimeout = topo.RemoteOperationTimeout * 4

var ErrMaxDiffDurationExceeded = vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "table diff was stopped due to exceeding the max-diff-duration time")
var ErrVDiffStoppedByUser = vterrors.Errorf(vtrpcpb.Code_CANCELED, "vdiff was stopped by user")

// compareColInfo contains the metadata for a column of the table being diffed
type compareColInfo struct {
	colIndex  int           // index of the column in the filter's select
	collation collations.ID // is the collation of the column, if any
	isPK      bool          // is this column part of the primary key
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

	// wgShardStreamers is used, with a cancellable context, to wait for all shard streamers
	// to finish after each diff is complete.
	wgShardStreamers   sync.WaitGroup
	shardStreamsCtx    context.Context
	shardStreamsCancel context.CancelFunc
}

func newTableDiffer(wd *workflowDiffer, table *tabletmanagerdatapb.TableDefinition, sourceQuery string) *tableDiffer {
	return &tableDiffer{wd: wd, table: table, sourceQuery: sourceQuery}
}

// initialize
func (td *tableDiffer) initialize(ctx context.Context) error {
	defer td.wd.ct.TableDiffPhaseTimings.Record(fmt.Sprintf("%s.%s", td.table.Name, initializing), time.Now())
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
			log.Errorf("UnlockKeyspace %s failed: %v", targetKeyspace, err)
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

	td.shardStreamsCtx, td.shardStreamsCancel = context.WithCancel(ctx)

	if err := td.selectTablets(ctx); err != nil {
		return err
	}
	if err := td.syncSourceStreams(ctx); err != nil {
		return err
	}
	if err := td.startSourceDataStreams(td.shardStreamsCtx); err != nil {
		return err
	}
	if err := td.syncTargetStreams(ctx); err != nil {
		return err
	}
	if err := td.startTargetDataStream(td.shardStreamsCtx); err != nil {
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

func (td *tableDiffer) selectTablets(ctx context.Context) error {
	defer td.wd.ct.TableDiffPhaseTimings.Record(fmt.Sprintf("%s.%s", td.table.Name, pickingTablets), time.Now())
	var (
		wg                   sync.WaitGroup
		sourceErr, targetErr error
		targetTablet         *topodatapb.Tablet
	)

	// The cells from the vdiff record are a comma separated list.
	sourceCells := strings.Split(td.wd.opts.PickerOptions.SourceCell, ",")
	targetCells := strings.Split(td.wd.opts.PickerOptions.TargetCell, ",")

	// For Mount+Migrate, the source tablets will be in a different
	// Vitess cluster with its own TopoServer.
	sourceTopoServer := td.wd.ct.ts
	if td.wd.ct.externalCluster != "" {
		extTS, err := td.wd.ct.ts.OpenExternalVitessClusterServer(ctx, td.wd.ct.externalCluster)
		if err != nil {
			return err
		}
		sourceTopoServer = extTS
	}
	tabletPickerOptions := discovery.TabletPickerOptions{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		sourceErr = td.forEachSource(func(source *migrationSource) error {
			sourceTablet, err := td.pickTablet(ctx, sourceTopoServer, sourceCells, td.wd.ct.sourceKeyspace,
				source.shard, td.wd.opts.PickerOptions.TabletTypes, tabletPickerOptions)
			if err != nil {
				return err
			}
			source.tablet = sourceTablet
			return nil
		})
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if td.wd.ct.workflowType == binlogdatapb.VReplicationWorkflowType_Reshard {
			// For resharding, the target shards could be non-serving if traffic has already been switched once.
			// When shards are created their IsPrimaryServing attribute is set to true. However, when the traffic is switched
			// it is set to false for the shards we are switching from. We don't have a way to know if we have
			// switched or not, so we just include non-serving tablets for all reshards.
			tabletPickerOptions.IncludeNonServingTablets = true
		}
		targetTablet, targetErr = td.pickTablet(ctx, td.wd.ct.ts, targetCells, td.wd.ct.vde.thisTablet.Keyspace,
			td.wd.ct.vde.thisTablet.Shard, td.wd.opts.PickerOptions.TabletTypes, tabletPickerOptions)
		if targetErr != nil {
			return
		}
		td.wd.ct.targetShardStreamer = &shardStreamer{
			tablet: targetTablet,
			shard:  targetTablet.Shard,
		}
	}()

	wg.Wait()
	if sourceErr != nil {
		return sourceErr
	}
	return targetErr
}

func (td *tableDiffer) pickTablet(ctx context.Context, ts *topo.Server, cells []string, keyspace,
	shard, tabletTypes string, options discovery.TabletPickerOptions) (*topodatapb.Tablet, error) {

	tp, err := discovery.NewTabletPicker(ctx, ts, cells, td.wd.ct.vde.thisTablet.Alias.Cell, keyspace,
		shard, tabletTypes, options)
	if err != nil {
		return nil, err
	}
	return tp.PickForStreaming(ctx)
}

func (td *tableDiffer) syncSourceStreams(ctx context.Context) error {
	defer td.wd.ct.TableDiffPhaseTimings.Record(fmt.Sprintf("%s.%s", td.table.Name, syncingSources), time.Now())
	// source can be replica, wait for them to at least reach max gtid of all target streams
	ct := td.wd.ct
	waitCtx, cancel := context.WithTimeout(ctx, time.Duration(ct.options.CoreOptions.TimeoutSeconds*int64(time.Second)))
	defer cancel()

	if err := td.forEachSource(func(source *migrationSource) error {
		log.Flush()
		if err := ct.tmc.WaitForPosition(waitCtx, source.tablet, replication.EncodePosition(source.position)); err != nil {
			return vterrors.Wrapf(err, "WaitForPosition for tablet %v", topoproto.TabletAliasString(source.tablet.Alias))
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (td *tableDiffer) syncTargetStreams(ctx context.Context) error {
	defer td.wd.ct.TableDiffPhaseTimings.Record(fmt.Sprintf("%s.%s", td.table.Name, syncingTargets), time.Now())
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
	defer td.wd.ct.TableDiffPhaseTimings.Record(fmt.Sprintf("%s.%s", td.table.Name, startingTargets), time.Now())
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
	defer td.wd.ct.TableDiffPhaseTimings.Record(fmt.Sprintf("%s.%s", td.table.Name, startingSources), time.Now())
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
	defer td.wd.ct.TableDiffPhaseTimings.Record(fmt.Sprintf("%s.%s", td.table.Name, restartingVreplication), time.Now())
	ct := td.wd.ct
	query := fmt.Sprintf("update _vt.vreplication set state='Running', message='', stop_pos='' where db_name=%s and workflow=%s",
		encodeString(ct.vde.dbName), encodeString(ct.workflow))
	log.Infof("Restarting the %q VReplication workflow using %q", ct.workflow, query)
	var err error
	// Let's retry a few times if we get a retryable error.
	for i := 1; i <= 3; i++ {
		_, err := ct.tmc.VReplicationExec(ctx, ct.vde.thisTablet, query)
		if err == nil || !sqlerror.IsEphemeralError(err) {
			break
		}
		log.Warningf("Encountered the following error while restarting the %q VReplication workflow, will retry (attempt #%d): %v",
			ct.workflow, i, err)
	}
	return err
}

func (td *tableDiffer) streamOneShard(ctx context.Context, participant *shardStreamer, query string, lastPK *querypb.QueryResult, gtidch chan string) {
	log.Infof("streamOneShard Start on %s using query: %s", participant.tablet.Alias.String(), query)
	td.wgShardStreamers.Add(1)
	defer func() {
		log.Infof("streamOneShard End on %s", participant.tablet.Alias.String())
		close(participant.result)
		close(gtidch)
		td.wgShardStreamers.Done()
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
			vsr := vsrRaw.CloneVT()
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
			if len(vsr.Fields) == 0 {
				result.Fields = nil
			}
			select {
			case participant.result <- result:
			case <-ctx.Done():
				return vterrors.Wrap(ctx.Err(), "VStreamRows")
			case <-td.wd.ct.done:
				return ErrVDiffStoppedByUser
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
	td.sourcePrimitive = newMergeSorter(sources, td.tablePlan.comparePKs, td.wd.collationEnv)

	// Create a merge sorter for the target.
	targets := make(map[string]*shardStreamer)
	targets[td.wd.ct.targetShardStreamer.shard] = td.wd.ct.targetShardStreamer
	td.targetPrimitive = newMergeSorter(targets, td.tablePlan.comparePKs, td.wd.collationEnv)

	// If there were aggregate expressions, we have to re-aggregate
	// the results, which engine.OrderedAggregate can do.
	if len(td.tablePlan.aggregates) != 0 {
		td.sourcePrimitive = &engine.OrderedAggregate{
			Aggregates:   td.tablePlan.aggregates,
			GroupByKeys:  pkColsToGroupByParams(td.tablePlan.pkCols, td.wd.collationEnv),
			Input:        td.sourcePrimitive,
			CollationEnv: td.wd.collationEnv,
		}
	}
}

func (td *tableDiffer) diff(ctx context.Context, rowsToCompare int64, debug, onlyPks bool, maxExtraRowsToCompare int64, maxReportSampleRows int64, stop <-chan time.Time) (*DiffReport, error) {
	defer td.wd.ct.TableDiffPhaseTimings.Record(fmt.Sprintf("%s.%s", td.table.Name, diffingTable), time.Now())
	dbClient := td.wd.ct.dbClientFactory()
	if err := dbClient.Connect(); err != nil {
		return nil, err
	}
	defer dbClient.Close()

	// We need to continue were we left off when appropriate. This can be an
	// auto-retry on error, or a manual retry via the resume command.
	// Otherwise the existing state will be empty and we start from scratch.
	query, err := sqlparser.ParseAndBind(sqlGetVDiffTable,
		sqltypes.Int64BindVariable(td.wd.ct.id),
		sqltypes.StringBindVariable(td.table.Name),
	)
	if err != nil {
		return nil, err
	}
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

	// Save our progress when we finish the run.
	defer func() {
		if err := td.updateTableProgress(dbClient, dr, lastProcessedRow); err != nil {
			log.Errorf("Failed to update vdiff progress on %s table: %v", td.table.Name, err)
		}
		globalStats.RowsDiffedCount.Add(dr.ProcessedRows)
	}()

	for {
		lastProcessedRow = sourceRow

		select {
		case <-ctx.Done():
			return nil, vterrors.Errorf(vtrpcpb.Code_CANCELED, "context has expired")
		case <-td.wd.ct.done:
			return nil, ErrVDiffStoppedByUser
		case <-stop:
			globalStats.RestartedTableDiffs.Add(td.table.Name, 1)
			return nil, ErrMaxDiffDurationExceeded
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
			log.Infof("Stopping vdiff, specified row limit reached")
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

			// Drain target, update count.
			count, err := targetExecutor.drain(ctx)
			if err != nil {
				return nil, err
			}
			dr.ExtraRowsTarget += 1 + count
			dr.ProcessedRows += 1 + count
			return dr, nil
		}
		if targetRow == nil {
			// No more rows from the target but we know we have more rows from
			// source, so drain them and update the counts.
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
			// We don't do a second pass to compare mismatched rows so we can cap the slice here.
			if maxReportSampleRows == 0 || dr.MismatchedRows < maxReportSampleRows {
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
		collationID = col.collation
		if collationID == collations.Unknown {
			collationID = collations.CollationBinaryID
		}
		c, err = evalengine.NullsafeCompare(sourceRow[compareIndex], targetRow[compareIndex], td.wd.collationEnv, collationID)
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

		if td.wd.opts.CoreOptions.MaxDiffSeconds > 0 {
			// Update the in-memory lastPK as well so that we can restart the table
			// diff if needed.
			lastpkpb := &querypb.QueryResult{}
			if err := prototext.Unmarshal(lastPK, lastpkpb); err != nil {
				return err
			}
			td.lastPK = lastpkpb
		}

		query, err = sqlparser.ParseAndBind(sqlUpdateTableProgress,
			sqltypes.Int64BindVariable(dr.ProcessedRows),
			sqltypes.StringBindVariable(string(lastPK)),
			sqltypes.StringBindVariable(string(rpt)),
			sqltypes.Int64BindVariable(td.wd.ct.id),
			sqltypes.StringBindVariable(td.table.Name),
		)
		if err != nil {
			return err
		}
	} else {
		query, err = sqlparser.ParseAndBind(sqlUpdateTableNoProgress,
			sqltypes.Int64BindVariable(dr.ProcessedRows),
			sqltypes.StringBindVariable(string(rpt)),
			sqltypes.Int64BindVariable(td.wd.ct.id),
			sqltypes.StringBindVariable(td.table.Name),
		)
		if err != nil {
			return err
		}
	}
	if _, err := dbClient.ExecuteFetch(query, 1); err != nil {
		return err
	}
	td.wd.ct.TableDiffRowCounts.Add([]string{td.table.Name}, dr.ProcessedRows)
	return nil
}

func (td *tableDiffer) updateTableState(ctx context.Context, dbClient binlogplayer.DBClient, state VDiffState) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateTableState,
		sqltypes.StringBindVariable(string(state)),
		sqltypes.Int64BindVariable(td.wd.ct.id),
		sqltypes.StringBindVariable(td.table.Name),
	)
	if err != nil {
		return err
	}
	if _, err = dbClient.ExecuteFetch(query, 1); err != nil {
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
	query, err := sqlparser.ParseAndBind(sqlUpdateTableStateAndReport,
		sqltypes.StringBindVariable(string(state)),
		sqltypes.Int64BindVariable(dr.ProcessedRows),
		sqltypes.StringBindVariable(report),
		sqltypes.Int64BindVariable(td.wd.ct.id),
		sqltypes.StringBindVariable(td.table.Name),
	)
	if err != nil {
		return err
	}
	if _, err = dbClient.ExecuteFetch(query, 1); err != nil {
		return err
	}
	insertVDiffLog(ctx, dbClient, td.wd.ct.id, fmt.Sprintf("%s: table %s", state, encodeString(td.table.Name)))

	return nil
}

func updateTableMismatch(dbClient binlogplayer.DBClient, vdiffID int64, table string) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateTableMismatch,
		sqltypes.Int64BindVariable(vdiffID),
		sqltypes.StringBindVariable(table),
	)
	if err != nil {
		return err
	}
	if _, err = dbClient.ExecuteFetch(query, 1); err != nil {
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
						Exprs: sqlparser.Exprs{
							selExpr.Expr,
							sqlparser.NewStrLiteral(td.wd.ct.targetTimeZone),
							sqlparser.NewStrLiteral(td.wd.ct.sourceTimeZone),
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
