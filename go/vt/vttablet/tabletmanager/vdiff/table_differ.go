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
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
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

var (
	ErrMaxDiffDurationExceeded = vterrors.Errorf(vtrpcpb.Code_DEADLINE_EXCEEDED, "table diff was stopped due to exceeding the max-diff-duration time")
	ErrVDiffStoppedByUser      = vterrors.Errorf(vtrpcpb.Code_CANCELED, "vdiff was stopped by user")
)

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
	sourceQuery  string
	table        *tabletmanagerdatapb.TableDefinition
	lastSourcePK *querypb.QueryResult
	lastTargetPK *querypb.QueryResult

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
	lockName := fmt.Sprintf("%s/%s", targetKeyspace, td.wd.ct.workflow)
	log.Info(fmt.Sprintf("Locking workflow %s for VDiff %s", lockName, td.wd.ct.uuid))
	// We attempt to get the lock until we can, using an exponential backoff.
	var (
		vctx          context.Context
		unlock        func(*error)
		lockErr       error
		retryDelay    = 100 * time.Millisecond
		maxRetryDelay = topo.LockTimeout
		backoffFactor = 1.5
	)
	for {
		vctx, unlock, lockErr = td.wd.ct.ts.LockName(ctx, lockName, "vdiff")
		if lockErr == nil {
			break
		}
		log.Warn(fmt.Sprintf("Locking workflow %s for VDiff %s initialization (stream ID: %d) failed, will wait %v before retrying: %v", lockName, td.wd.ct.uuid, td.wd.ct.id, retryDelay, lockErr))
		select {
		case <-ctx.Done():
			return vterrors.Errorf(vtrpcpb.Code_CANCELED, "engine is shutting down")
		case <-td.wd.ct.done:
			return ErrVDiffStoppedByUser
		case <-time.After(retryDelay):
			if retryDelay < maxRetryDelay {
				retryDelay = min(time.Duration(float64(retryDelay)*backoffFactor), maxRetryDelay)
			}
			// Add jitter to prevent thundering herds: ±25% of original retryDelay.
			// This means that we may wait up to maxRetryDelay * 1.25, but it prevents all of
			// the waiters from eventually waiting for the fixed maxRetryDelay period.
			jitter := time.Duration(rand.IntN(int(retryDelay) / 2))
			retryDelay = retryDelay - (retryDelay / 4) + jitter
			continue
		}
	}

	var err error
	defer func() {
		unlock(&err)
		if err != nil {
			log.Error(fmt.Sprintf("Unlocking workflow %s for vdiff %s failed: %v", lockName, td.wd.ct.uuid, err))
		}
	}()

	if err := td.stopTargetVReplicationStreams(vctx, dbClient); err != nil {
		return err
	}
	defer func() {
		// We use a new context as we want to reset the state even
		// when the parent context has timed out or been canceled.
		log.Info(fmt.Sprintf("Restarting the %q VReplication workflow for vdiff %s on target tablets in keyspace %q", td.wd.ct.workflow, td.wd.ct.uuid, targetKeyspace))
		restartCtx, restartCancel := context.WithTimeout(context.Background(), BackgroundOperationTimeout)
		defer restartCancel()
		if err := td.restartTargetVReplicationStreams(restartCtx); err != nil {
			log.Error(fmt.Sprintf("error restarting target streams for vdiff %s: %v", td.wd.ct.uuid, err))
		}
	}()

	td.shardStreamsCtx, td.shardStreamsCancel = context.WithCancel(vctx)

	if err := td.selectTablets(vctx); err != nil {
		return err
	}
	if err := td.syncSourceStreams(vctx); err != nil {
		return err
	}
	if err := td.startSourceDataStreams(td.shardStreamsCtx); err != nil {
		return err
	}
	if err := td.syncTargetStreams(vctx); err != nil {
		return err
	}
	if err := td.startTargetDataStream(td.shardStreamsCtx); err != nil {
		return err
	}
	td.setupRowSorters()
	return nil
}

func (td *tableDiffer) stopTargetVReplicationStreams(ctx context.Context, dbClient binlogplayer.DBClient) error {
	log.Info("stopTargetVReplicationStreams for vdiff " + td.wd.ct.uuid)
	ct := td.wd.ct
	query := "update _vt.vreplication set state = 'Stopped', message='for vdiff' " + ct.workflowFilter
	if _, err := ct.vde.vre.Exec(query); err != nil {
		return err
	}
	// streams are no longer running because vre.Exec would have replaced old controllers and new ones will not start

	// update position of all source streams
	query = "select id, source, pos from _vt.vreplication " + ct.workflowFilter
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

	sourceTopoServer, err := td.wd.getSourceTopoServer()
	if err != nil {
		return vterrors.Wrap(err, "failed to get source topo server")
	}
	tabletPickerOptions := discovery.TabletPickerOptions{}
	wg.Go(func() {
		sourceErr = td.forEachSource(func(source *migrationSource) error {
			sourceTablet, err := td.pickTablet(ctx, sourceTopoServer, sourceCells, td.wd.ct.sourceKeyspace,
				source.shard, td.wd.opts.PickerOptions.TabletTypes, tabletPickerOptions)
			if err != nil {
				return err
			}
			source.tablet = sourceTablet
			return nil
		})
	})

	wg.Go(func() {
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
	})

	wg.Wait()
	if sourceErr != nil {
		return sourceErr
	}
	return targetErr
}

func (td *tableDiffer) pickTablet(ctx context.Context, ts *topo.Server, cells []string, keyspace,
	shard, tabletTypes string, options discovery.TabletPickerOptions,
) (*topodatapb.Tablet, error) {
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
			log.Error(fmt.Sprintf("WaitForPosition for vdiff %s error: %d: %s", td.wd.ct.uuid, source.vrID, err))
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
	go td.streamOneShard(ctx, ct.targetShardStreamer, td.tablePlan.targetQuery, td.lastTargetPK, gtidch)
	gtid, ok := <-gtidch
	if !ok {
		log.Error(fmt.Sprintf("VDiff %s streaming error on target tablet %s: %v", td.wd.ct.uuid, topoproto.TabletAliasString(ct.targetShardStreamer.tablet.Alias), ct.targetShardStreamer.err))
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
		go td.streamOneShard(ctx, source.shardStreamer, td.tablePlan.sourceQuery, td.lastSourcePK, gtidch)

		gtid, ok := <-gtidch
		if !ok {
			log.Error(fmt.Sprintf("VDiff %s streaming error on source tablet %s: %v", td.wd.ct.uuid, topoproto.TabletAliasString(source.tablet.Alias), source.err))
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
	log.Info(fmt.Sprintf("Restarting the %q VReplication workflow for vdiff %s using %q", ct.workflow, td.wd.ct.uuid, query))
	var err error
	// Let's retry a few times if we get a retryable error.
	for i := 1; i <= 3; i++ {
		_, err := ct.tmc.VReplicationExec(ctx, ct.vde.thisTablet, query)
		if err == nil || !sqlerror.IsEphemeralError(err) {
			break
		}
		log.Warn(fmt.Sprintf("Encountered the following error while restarting the %q VReplication workflow, will retry (attempt #%d): %v", ct.workflow, i, err))
	}
	return err
}

func (td *tableDiffer) streamOneShard(ctx context.Context, participant *shardStreamer, query string, lastPK *querypb.QueryResult, gtidch chan string) {
	tabletAliasString := topoproto.TabletAliasString(participant.tablet.Alias)
	log.Info(fmt.Sprintf("streamOneShard Start for vdiff %s on %s using query: %s", td.wd.ct.uuid, tabletAliasString, query))
	td.wgShardStreamers.Add(1)
	resultch := participant.result

	defer func() {
		log.Info(fmt.Sprintf("streamOneShard for vdiff %s End on %s (err: %v)", td.wd.ct.uuid, tabletAliasString, participant.err))

		close(resultch)
		close(gtidch)

		td.wgShardStreamers.Done()
	}()

	participant.err = func() error {
		conn, err := tabletconn.GetDialer()(ctx, participant.tablet, false)
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
		req := &binlogdatapb.VStreamRowsRequest{
			// We pass the NoTimeouts options as otherwise the row streamer will add a MAX_EXECUTION_TIME
			// query hint with a value based on the --vreplication-copy-phase-duration flag.
			Target: target, Query: query, Lastpk: lastPK, Options: &binlogdatapb.VStreamOptions{NoTimeouts: true},
		}
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
			case resultch <- result:
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
			Aggregates:  td.tablePlan.aggregates,
			GroupByKeys: pkColsToGroupByParams(td.tablePlan.pkCols, td.wd.collationEnv),
			Input:       td.sourcePrimitive,
		}
	}
}

func (td *tableDiffer) diff(ctx context.Context, coreOpts *tabletmanagerdatapb.VDiffCoreOptions, reportOpts *tabletmanagerdatapb.VDiffReportOptions, stop <-chan time.Time) (*DiffReport, error) {
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
	if td.tablePlan.sourceCheckpointUnavailable {
		// This table has no resumable checkpoint and restarts from the beginning
		// on every run (see getSourcePKCols). Carrying over the persisted partial
		// report or mismatch flag would double-count rows and duplicate mismatch
		// samples across restarts, so we start fresh instead. Also clear the
		// persisted mismatch bit so a mismatch recorded by a discarded partial
		// attempt does not stick after a clean full-table pass.
		mismatch = false
		if err = setTableMismatch(dbClient, td.wd.ct.id, td.table.Name, false); err != nil {
			return nil, err
		}
	} else if rpt := curState.AsBytes("report", []byte("{}")); json.Valid(rpt) {
		if err = json.Unmarshal(rpt, dr); err != nil {
			return nil, err
		}
	}
	dr.TableName = td.table.Name

	// Scope executor goroutines to this single diff attempt, rather
	// than surviving until the controller context is canceled.
	execCtx, cancelExec := context.WithCancel(ctx)
	defer cancelExec()

	sourceExecutor := newPrimitiveExecutor(execCtx, td.sourcePrimitive, "source")
	targetExecutor := newPrimitiveExecutor(execCtx, td.targetPrimitive, "target")
	var sourceRow, lastProcessedRow, targetRow []sqltypes.Value
	advanceSource := true
	advanceTarget := true

	// Save our progress when we finish the run.
	defer func() {
		if err := td.updateTableProgress(dbClient, dr, lastProcessedRow); err != nil {
			log.Error(fmt.Sprintf("Failed to update vdiff %s progress on %s table: %v", td.wd.ct.uuid, td.table.Name, err))
		}
		globalStats.RowsDiffedCount.Add(dr.ProcessedRows)
	}()

	rowsToCompare := coreOpts.GetMaxRows()
	maxExtraRowsToCompare := coreOpts.GetMaxExtraRowsToCompare()
	maxReportSampleRows := reportOpts.GetMaxSampleRows()

	for {
		// Only advance the persisted position when the previous iteration
		// consumed the held source row (advanceSource still holds that
		// iteration's decision here). After an extra-target-row iteration the
		// held source row has not been processed yet, and recording it as
		// lastpk would make a resumed diff skip it permanently.
		if advanceSource {
			lastProcessedRow = sourceRow
		}

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
			log.Info(fmt.Sprintf("Flagging mismatch in vdiff %s for %s: %+v", td.wd.ct.uuid, td.table.Name, dr))
			if err := setTableMismatch(dbClient, td.wd.ct.id, td.table.Name, true); err != nil {
				return nil, err
			}
		}

		rowsToCompare--
		if rowsToCompare < 0 {
			log.Info(fmt.Sprintf("Stopping vdiff %s, specified row limit of %d reached", td.wd.ct.uuid, rowsToCompare))
			return dr, nil
		}
		if advanceSource {
			sourceRow, err = sourceExecutor.next()
			if err != nil {
				log.Error(fmt.Sprint(err))
				return nil, err
			}
		}
		if advanceTarget {
			targetRow, err = targetExecutor.next()
			if err != nil {
				log.Error(fmt.Sprint(err))
				return nil, err
			}
		}

		if sourceRow == nil && targetRow == nil {
			return dr, nil
		}

		advanceSource = true
		advanceTarget = true
		if sourceRow == nil {
			// No more rows from the source; drain the remaining target rows,
			// saving a sample for each one (up to maxExtraRowsToCompare) so that
			// reconcileExtraRows can match them against any extra source rows.
			// Counting drained rows without saving a sample makes them impossible
			// to reconcile, producing false positive extra rows in the report.
			// The drained rows are merged into the report only after the full
			// drain succeeds: they are beyond the persisted lastpk, so partially
			// counted rows would be counted again when a failed diff is resumed.
			drainedRows := int64(0)
			var drainedDiffs []*RowDiff
			for targetRow != nil {
				if dr.ExtraRowsTarget+drainedRows < maxExtraRowsToCompare {
					diffRow, err := td.genRowDiff(td.tablePlan.targetQuery, targetRow, reportOpts)
					if err != nil {
						return nil, vterrors.Wrap(err, "unexpected error generating diff")
					}
					drainedDiffs = append(drainedDiffs, diffRow)
				}
				drainedRows++
				targetRow, err = targetExecutor.next()
				if err != nil {
					return nil, err
				}
			}
			dr.ExtraRowsTarget += drainedRows
			dr.ProcessedRows += drainedRows
			dr.ExtraRowsTargetDiffs = append(dr.ExtraRowsTargetDiffs, drainedDiffs...)
			return dr, nil
		}
		if targetRow == nil {
			// No more rows from the target; drain the remaining source rows,
			// saving a sample for each one (up to maxExtraRowsToCompare) so that
			// reconcileExtraRows can match them against any extra target rows.
			// As above, the drained rows are merged into the report only after
			// the full drain succeeds.
			drainedRows := int64(0)
			var drainedDiffs []*RowDiff
			for sourceRow != nil {
				if dr.ExtraRowsSource+drainedRows < maxExtraRowsToCompare {
					diffRow, err := td.genRowDiff(td.tablePlan.sourceQuery, sourceRow, reportOpts)
					if err != nil {
						return nil, vterrors.Wrap(err, "unexpected error generating diff")
					}
					drainedDiffs = append(drainedDiffs, diffRow)
				}
				drainedRows++
				sourceRow, err = sourceExecutor.next()
				if err != nil {
					return nil, err
				}
			}
			dr.ExtraRowsSource += drainedRows
			dr.ProcessedRows += drainedRows
			dr.ExtraRowsSourceDiffs = append(dr.ExtraRowsSourceDiffs, drainedDiffs...)
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
				diffRow, err := td.genRowDiff(td.tablePlan.sourceQuery, sourceRow, reportOpts)
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
				diffRow, err := td.genRowDiff(td.tablePlan.targetQuery, targetRow, reportOpts)
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
				sourceDiffRow, err := td.genRowDiff(td.tablePlan.targetQuery, sourceRow, reportOpts)
				if err != nil {
					return nil, vterrors.Wrap(err, "unexpected error generating diff")
				}
				targetDiffRow, err := td.genRowDiff(td.tablePlan.targetQuery, targetRow, reportOpts)
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
		c, err = evalengine.NullsafeCompare(sourceRow[compareIndex], targetRow[compareIndex], td.wd.collationEnv, collationID, nil)
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
		return errors.New("cannot update progress with a nil diff report")
	}

	var err error
	var query string
	rpt, err := json.Marshal(dr)
	if err != nil {
		return err
	}

	switch {
	case td.tablePlan.sourceCheckpointUnavailable:
		// The source PK cannot be represented as a resumable checkpoint (see
		// getSourcePKCols). Persist progress but explicitly clear lastpk (to NULL,
		// which also discards any stale value written before this fix) and leave
		// the in-memory retry PKs unset. Any resume then restarts the whole table
		// from the beginning for both streams, which avoids the false
		// ExtraRowsSource that a source-only restart against a resumed target
		// would produce.
		query, err = sqlparser.ParseAndBind(sqlUpdateTableProgress,
			sqltypes.Int64BindVariable(dr.ProcessedRows),
			sqltypes.NullBindVariable,
			sqltypes.StringBindVariable(string(rpt)),
			sqltypes.Int64BindVariable(td.wd.ct.id),
			sqltypes.StringBindVariable(td.table.Name),
		)
		if err != nil {
			return err
		}
	case lastRow == nil:
		// No rows were processed, so there is nothing to checkpoint.
		query, err = sqlparser.ParseAndBind(sqlUpdateTableNoProgress,
			sqltypes.Int64BindVariable(dr.ProcessedRows),
			sqltypes.StringBindVariable(string(rpt)),
			sqltypes.Int64BindVariable(td.wd.ct.id),
			sqltypes.StringBindVariable(td.table.Name),
		)
		if err != nil {
			return err
		}
	default:
		lastPK := td.lastPKFromRow(lastRow)
		if td.wd.opts.CoreOptions.MaxDiffSeconds > 0 {
			// Update the in-memory lastPK as well so that we can restart the table
			// diff if needed.
			td.lastTargetPK = lastPK.Target
			if lastPK.Source == nil {
				// If the source PK is nil, we use the target value for both.
				td.lastSourcePK = lastPK.Target
			} else {
				td.lastSourcePK = lastPK.Source
			}
		}
		lastPKTxt, err := prototext.Marshal(lastPK)
		if err != nil {
			return vterrors.Wrapf(err, "failed to marshal lastpk value %+v for table %s", lastPK, td.table.Name)
		}
		query, err = sqlparser.ParseAndBind(sqlUpdateTableProgress,
			sqltypes.Int64BindVariable(dr.ProcessedRows),
			sqltypes.StringBindVariable(string(lastPKTxt)),
			sqltypes.StringBindVariable(string(rpt)),
			sqltypes.Int64BindVariable(td.wd.ct.id),
			sqltypes.StringBindVariable(td.table.Name),
		)
		if err != nil {
			return err
		}
	}
	if _, err := dbClient.ExecuteFetch(query, 1); err != nil {
		return vterrors.Wrapf(errWithoutQueryEcho(err), "failed to save diff report for table %s", td.table.Name)
	}

	td.wd.ct.TableDiffRowCounts.Add(td.table.Name, dr.ProcessedRows)
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
		return vterrors.Wrapf(errWithoutQueryEcho(err), "failed to save diff report for table %s", td.table.Name)
	}
	insertVDiffLog(ctx, dbClient, td.wd.ct.id, fmt.Sprintf("%s: table %s", state, encodeString(td.table.Name)))

	return nil
}

// setTableMismatch sets (mismatch=true) or clears (mismatch=false) the persisted
// mismatch bit for a table. It is cleared when a table with no resumable
// checkpoint restarts from the beginning, so a mismatch recorded by a discarded
// partial attempt does not stick after a clean full-table pass.
func setTableMismatch(dbClient binlogplayer.DBClient, vdiffID int64, table string, mismatch bool) error {
	query, err := sqlparser.ParseAndBind(sqlUpdateTableMismatch,
		sqltypes.BoolBindVariable(mismatch),
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

func (td *tableDiffer) lastPKFromRow(row []sqltypes.Value) *tabletmanagerdatapb.VDiffTableLastPK {
	// Resolve target PK fields by column name (not by DDL ordinal) so reordered
	// projections pair each value with its own column's field.
	targetFieldByName := make(map[string]*querypb.Field, len(td.tablePlan.table.Fields))
	for _, f := range td.tablePlan.table.Fields {
		targetFieldByName[strings.ToLower(f.Name)] = f
	}
	buildQR := func(pkCols []int, pkFields []*querypb.Field) *querypb.QueryResult {
		pkVals := make([]sqltypes.Value, len(pkCols))
		for i, colIndex := range pkCols {
			pkVals[i] = row[colIndex]
		}
		return &querypb.QueryResult{
			Fields: pkFields,
			Rows:   []*querypb.Row{sqltypes.RowToProto3(pkVals)},
		}
	}
	targetFields := make([]*querypb.Field, len(td.tablePlan.pkCols))
	for i, colIndex := range td.tablePlan.pkCols {
		targetFields[i] = targetFieldByName[strings.ToLower(td.tablePlan.compareCols[colIndex].colName)]
	}

	// The row is the source stream's row, so take source field types from the values
	// themselves; a type-changing rename then quotes the source key with the source type.
	sourceFields := make([]*querypb.Field, len(td.tablePlan.sourcePkCols))
	for i, colIndex := range td.tablePlan.sourcePkCols {
		sourceFields[i] = &querypb.Field{
			Name: td.tablePlan.compareCols[colIndex].colName,
			Type: row[colIndex].Type(),
		}
	}

	lastPK := &tabletmanagerdatapb.VDiffTableLastPK{
		Target: buildQR(td.tablePlan.pkCols, targetFields),
	}
	// A separate source lastpk is only needed when the source key differs from the
	// target by column position or by field type; otherwise the target value is reused.
	if !slices.Equal(td.tablePlan.pkCols, td.tablePlan.sourcePkCols) || !fieldTypesEqual(targetFields, sourceFields) {
		lastPK.Source = buildQR(td.tablePlan.sourcePkCols, sourceFields)
	}
	return lastPK
}

// fieldTypesEqual reports whether two field slices have the same length and
// element-wise column types.
func fieldTypesEqual(a, b []*querypb.Field) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].GetType() != b[i].GetType() {
			return false
		}
	}
	return true
}

// If SourceTimeZone is defined in the BinlogSource (_vt.vreplication.source), the
// VReplication workflow would have converted the datetime columns expecting the
// source to have been in the SourceTimeZone and target in TargetTimeZone. We need
// to do the reverse conversion in VDiff before the comparison.
func (td *tableDiffer) adjustForSourceTimeZone(targetSelectExprs []sqlparser.SelectExpr, fields map[string]querypb.Type) []sqlparser.SelectExpr {
	if td.wd.ct.sourceTimeZone == "" {
		return targetSelectExprs
	}
	log.Info(fmt.Sprintf("Source time zone specified for vdiff %s: %s", td.wd.ct.uuid, td.wd.ct.sourceTimeZone))
	var newSelectExprs []sqlparser.SelectExpr
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
					convertTZFuncExpr = sqlparser.NewFuncExpr("convert_tz",
						selExpr.Expr,
						sqlparser.NewStrLiteral(td.wd.ct.targetTimeZone),
						sqlparser.NewStrLiteral(td.wd.ct.sourceTimeZone),
					)
					log.Info(fmt.Sprintf("Converting datetime column %s using convert_tz() for vdiff %s", colName, td.wd.ct.uuid))
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
		log.Info("Found datetime columns when SourceTimeZone was set, resetting target SelectExprs after convert_tz() for vdiff " + td.wd.ct.uuid)
		return newSelectExprs
	}
	return targetSelectExprs
}

// getSourcePKCols populates the sourcePkCols field in the tablePlan.
// We need this information in order to save the lastpk value for the
// source if the PK columns differ between the source and target.
func (td *tableDiffer) getSourcePKCols() error {
	ctx, cancel := context.WithTimeout(td.wd.ct.vde.ctx, topo.RemoteOperationTimeout*3)
	defer cancel()

	// Parse the source query to resolve the physical source table (which can differ
	// from td.table.Name for cross-table filters) and to map PK columns by SELECT
	// order rather than column ordinal.
	statement, err := td.wd.ct.vde.parser.Parse(td.tablePlan.sourceQuery)
	if err != nil {
		return vterrors.Wrapf(err, "failed to parse source query for table %s", td.table.Name)
	}
	sourceSelect, ok := statement.(*sqlparser.Select)
	if !ok {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected statement type for source query of table %s", td.table.Name)
	}
	sourceTableName, err := sourceTableNameFromSelect(sourceSelect)
	if err != nil {
		return vterrors.Wrapf(err, "failed to determine source table for target table %s", td.table.Name)
	}

	// We use the first sourceShard as all of them should have the same schema.
	if len(td.wd.ct.sources) == 0 {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "no source shards found in %s keyspace",
			td.wd.ct.sourceKeyspace)
	}
	sourceShardName := maps.Keys(td.wd.ct.sources)[0]
	sourceTS, err := td.wd.getSourceTopoServer()
	if err != nil {
		return vterrors.Wrap(err, "failed to get source topo server")
	}
	sourceShard, err := sourceTS.GetShard(ctx, td.wd.ct.sourceKeyspace, sourceShardName)
	if err != nil {
		return vterrors.Wrapf(err, "failed to get source shard %s", sourceShardName)
	}
	if sourceShard.PrimaryAlias == nil {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "source shard %s has no primary", sourceShardName)
	}
	sourceTablet, err := sourceTS.GetTablet(ctx, sourceShard.PrimaryAlias)
	if err != nil {
		return vterrors.Wrapf(err, "failed to get primary tablet in source shard %s/%s",
			td.wd.ct.sourceKeyspace, sourceShardName)
	}
	sourceSchema, err := td.wd.ct.tmc.GetSchema(ctx, sourceTablet.Tablet, &tabletmanagerdatapb.GetSchemaRequest{
		Tables: []string{sourceTableName},
	})
	if err != nil {
		return vterrors.Wrapf(err, "failed to get the schema for table %s from source tablet %s",
			sourceTableName, topoproto.TabletAliasString(sourceTablet.Alias))
	}
	if len(sourceSchema.TableDefinitions) == 0 {
		// The table no longer exists on the source. Any rows that exist on the target will be
		// reported as extra rows.
		log.Warn(fmt.Sprintf("The %s table was not found on source tablet %s during VDiff for the %s workflow; any rows on the target will be reported as extra", sourceTableName, topoproto.TabletAliasString(sourceTablet.Alias), td.wd.ct.workflow))
		return nil
	}
	sourceTable := sourceSchema.TableDefinitions[0]
	if len(sourceTable.PrimaryKeyColumns) == 0 {
		// We use the columns from a PKE if there is one.
		executeFetch := func(query string, maxrows int, wantfields bool) (*sqltypes.Result, error) {
			res, err := td.wd.ct.tmc.ExecuteFetchAsApp(ctx, sourceTablet.Tablet, false, &tabletmanagerdatapb.ExecuteFetchAsAppRequest{
				Query:   []byte(query),
				MaxRows: uint64(maxrows),
			})
			if err != nil {
				return nil, vterrors.Wrapf(err, "failed to query the %s source tablet in order to get a primary key equivalent for the %s table",
					topoproto.TabletAliasString(sourceTablet.Alias), sourceTableName)
			}
			return sqltypes.Proto3ToResult(res), nil
		}
		pkeCols, _, err := mysqlctl.GetPrimaryKeyEquivalentColumns(ctx, executeFetch, sourceTablet.DbName(), sourceTableName)
		if err != nil {
			return vterrors.Wrapf(err, "failed to get a primary key equivalent for the %s table from source tablet %s",
				sourceTableName, topoproto.TabletAliasString(sourceTablet.Alias))
		}
		if len(pkeCols) > 0 {
			log.Info(fmt.Sprintf("Using primary key equivalent columns %+v for table %s in vdiff %s", pkeCols, sourceTableName, td.wd.ct.uuid))
			sourceTable.PrimaryKeyColumns = pkeCols
		} else {
			// We use every column together as a substitute PK.
			log.Info(fmt.Sprintf("Using all columns as a substitute primary key for table %s in vdiff %s", sourceTableName, td.wd.ct.uuid))
			sourceTable.PrimaryKeyColumns = append(sourceTable.PrimaryKeyColumns, sourceTable.Columns...)
		}
	}

	indices, allProjected, err := sourcePKSelectIndices(sourceSelect, sourceTable.PrimaryKeyColumns)
	if err != nil {
		return vterrors.Wrapf(err, "table %s", sourceTableName)
	}
	// Reject any plan whose comparison key is not an order-preserving prefix of the
	// physical source PK; the source stream is always ordered by that PK.
	if err := comparisonKeyIsSourcePKPrefix(sourceSelect, td.tablePlan.comparePKs, sourceTable.PrimaryKeyColumns); err != nil {
		return err
	}
	if !allProjected {
		// The full source PK is not projected, so no resumable source checkpoint is
		// possible; any resume restarts the whole table. Clear any stale checkpoint.
		td.tablePlan.sourceCheckpointUnavailable = true
		td.lastSourcePK = nil
		td.lastTargetPK = nil
		return nil
	}
	td.tablePlan.sourcePkCols = indices

	// A loaded checkpoint for a reordered/renamed-PK layout is byte-identical to a
	// pre-fix one (same field names, only value order differs), so we cannot tell
	// them apart; discard it and restart both streams. This is always correct but
	// means such layouts restart on every resume rather than resuming.
	if td.lastSourcePK != nil &&
		!slices.Equal(indices, legacySourcePkColOrder(td.table.Columns, sourceTable.PrimaryKeyColumns)) {
		log.Info(fmt.Sprintf("VDiff %s: restarting table %s instead of resuming; its source PK projection order does not match the physical column order, so a persisted source checkpoint cannot be safely reused",
			td.wd.ct.uuid, td.table.Name))
		td.tablePlan.sourceCheckpointUnavailable = true
		td.tablePlan.sourcePkCols = nil
		td.lastSourcePK = nil
		td.lastTargetPK = nil
	}

	return nil
}

// legacySourcePkColOrder reproduces the pre-fix source-PK index mapping: the
// positions of the source PK columns within the target table's columns, in column
// order. Used only to detect whether a persisted checkpoint predates the ordering
// fix and so cannot be safely reused.
func legacySourcePkColOrder(targetColumns, sourcePKColumns []string) []int {
	pkSet := make(map[string]struct{}, len(sourcePKColumns))
	for _, pk := range sourcePKColumns {
		pkSet[strings.ToLower(pk)] = struct{}{}
	}
	order := make([]int, 0, len(sourcePKColumns))
	for i, col := range targetColumns {
		if _, ok := pkSet[strings.ToLower(col)]; ok {
			order = append(order, i)
		}
	}
	return order
}

// comparisonKeyIsSourcePKPrefix returns an error unless the columns the VDiff
// merge sorter compares on (comparePKs, in comparison order) are an order-
// preserving prefix of the physical source PK. Each comparePK indexes the SELECT
// list (shared with the source query), so its underlying source column must equal
// the source PK column at the same rank. Physical PK columns that the filter's
// WHERE pins to a single value are constant across the stream and so do not affect
// its order; they are dropped before the prefix comparison (e.g. a multi-tenant
// "where tenant_id = 1" filter makes comparing on id valid even though the source
// PK is (tenant_id, id)).
func comparisonKeyIsSourcePKPrefix(sourceSelect *sqlparser.Select, comparePKs []compareColInfo, sourcePKColumns []string) error {
	pinned := equalityPinnedColumns(sourceSelect.Where)
	effectivePK := make([]string, 0, len(sourcePKColumns))
	for _, col := range sourcePKColumns {
		if _, ok := pinned[strings.ToLower(col)]; !ok {
			effectivePK = append(effectivePK, col)
		}
	}

	// unsupportedFilter wraps the rejection as MySQL ERNotSupportedYet so the
	// "(errno 1235)" suffix survives being persisted as a string and rebuilt by
	// retryVDiffs, making IsEphemeralError classify it as non-ephemeral (no infinite
	// auto-retry).
	unsupportedFilter := func(format string, args ...any) error {
		return sqlerror.NewSQLError(sqlerror.ERNotSupportedYet, sqlerror.SSClientError, fmt.Sprintf(format, args...))
	}

	// Build the comparison key's source columns in comparison order, dropping the
	// pinned ones: a constant comparison component does not affect the merge order,
	// so it must be dropped from both sides before the prefix check.
	effectiveCompare := make([]string, 0, len(comparePKs))
	for _, cpk := range comparePKs {
		if cpk.colIndex < 0 || cpk.colIndex >= len(sourceSelect.SelectExprs.Exprs) {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL,
				"comparison key index %d out of range for vdiff source query: %s", cpk.colIndex, sqlparser.String(sourceSelect))
		}
		aliasedExpr, ok := sourceSelect.SelectExprs.Exprs[cpk.colIndex].(*sqlparser.AliasedExpr)
		if !ok {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL,
				"unexpected non-aliased expression at position %d in vdiff source query: %s", cpk.colIndex, sqlparser.String(sourceSelect))
		}
		// A constant projected into the comparison key (e.g. "select 1 as
		// tenant_id, ...") is invariant across the source stream, so like a
		// WHERE-pinned column it does not affect its order; drop it.
		if _, isLiteral := aliasedExpr.Expr.(*sqlparser.Literal); isLiteral {
			continue
		}
		colName, ok := underlyingSourceColumn(aliasedExpr.Expr)
		if !ok {
			return unsupportedFilter("vdiff does not support this filter: the comparison key includes a non-physical column, so the source stream is not sorted by the compared columns: %s",
				sqlparser.String(sourceSelect))
		}
		if _, isPinned := pinned[strings.ToLower(colName)]; isPinned {
			continue
		}
		effectiveCompare = append(effectiveCompare, colName)
	}

	if len(effectiveCompare) > len(effectivePK) {
		return unsupportedFilter("vdiff does not support this filter: the comparison key has more columns (%d) than the unconstrained physical source primary key (%d): %s",
			len(effectiveCompare), len(effectivePK), sqlparser.String(sourceSelect))
	}
	for i, col := range effectiveCompare {
		if !strings.EqualFold(col, effectivePK[i]) {
			return unsupportedFilter("vdiff does not support this filter: the comparison key is not an order-preserving prefix of the physical source primary key %v, so the source stream is not sorted by the compared columns: %s",
				sourcePKColumns, sqlparser.String(sourceSelect))
		}
	}
	return nil
}

// equalityPinnedColumns returns the set of columns the WHERE clause constrains to
// a single literal value via a top-level "col = literal" conjunct (lowercased). It
// is intentionally conservative: only plain equality against a literal is treated
// as pinned; ranges, IN, OR, in_keyrange, and non-literal comparisons are ignored.
func equalityPinnedColumns(where *sqlparser.Where) map[string]struct{} {
	pinned := make(map[string]struct{})
	if where == nil {
		return pinned
	}
	for _, expr := range sqlparser.SplitAndExpression(nil, where.Expr) {
		cmp, ok := expr.(*sqlparser.ComparisonExpr)
		if !ok || cmp.Operator != sqlparser.EqualOp {
			continue
		}
		if col, ok := cmp.Left.(*sqlparser.ColName); ok {
			if _, isLiteral := cmp.Right.(*sqlparser.Literal); isLiteral {
				pinned[col.Name.Lowered()] = struct{}{}
			}
		}
		if col, ok := cmp.Right.(*sqlparser.ColName); ok {
			if _, isLiteral := cmp.Left.(*sqlparser.Literal); isLiteral {
				pinned[col.Name.Lowered()] = struct{}{}
			}
		}
	}
	return pinned
}

// sourceTableNameFromSelect returns the physical source table referenced by the
// VReplication filter's SELECT. This can differ from the VDiff target table name
// for cross-table MoveTables filters (e.g. "select ... from t2" for target t1),
// and must be used for all source schema/PK lookups.
func sourceTableNameFromSelect(sourceSelect *sqlparser.Select) (string, error) {
	if len(sourceSelect.From) != 1 {
		return "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported source query, expected a single table in the FROM clause: %s", sqlparser.String(sourceSelect))
	}
	aliased, ok := sourceSelect.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported source query, expected a simple table reference: %s", sqlparser.String(sourceSelect))
	}
	tableName := sqlparser.GetTableName(aliased.Expr)
	if tableName.IsEmpty() {
		return "", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unsupported source query, could not resolve the source table: %s", sqlparser.String(sourceSelect))
	}
	return tableName.String(), nil
}

// sourcePKSelectIndices maps each source PK column to its SELECT-list position, in
// PK definition order. allProjected is false (no error) when a PK column is not
// projected as a physical column, in which case a resumable source checkpoint
// cannot be built; merge-ordering correctness is enforced by the caller via
// comparisonKeyIsSourcePKPrefix.
func sourcePKSelectIndices(sourceSelect *sqlparser.Select, pkColumns []string) (indices []int, allProjected bool, err error) {
	indices = make([]int, 0, len(pkColumns))
	for _, pkc := range pkColumns {
		physicalIdx := -1
		for i, selExpr := range sourceSelect.SelectExprs.Exprs {
			// Invariant: buildTablePlan expands "*" into explicit columns before
			// this runs, so the SELECT list must contain only AliasedExprs. A
			// StarExpr here means a caller passed an unexpanded query; fail loud
			// rather than silently treating PK columns as not projected.
			if _, isStar := selExpr.(*sqlparser.StarExpr); isStar {
				return nil, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected '*' in vdiff source query SELECT list; expected columns to be expanded by buildTablePlan: %s", sqlparser.String(sourceSelect))
			}
			aliasedExpr, ok := selExpr.(*sqlparser.AliasedExpr)
			if !ok {
				continue
			}
			// A physical match wins over an alias that shadows the PK name
			// (e.g. "select b as a, a as b" resolves PK a to the real a).
			if colName, ok := underlyingSourceColumn(aliasedExpr.Expr); ok && strings.EqualFold(pkc, colName) {
				physicalIdx = i
				break
			}
		}
		if physicalIdx < 0 {
			// Not projected as a physical column: no resumable source checkpoint.
			return nil, false, nil
		}
		indices = append(indices, physicalIdx)
	}
	return indices, true, nil
}

// underlyingSourceColumn returns the name of the physical source column that a
// SELECT expression reads from, and whether the expression resolves to one. It
// handles plain column references and CONVERT(col USING charset) renames (used
// for charset conversions where the AS is the renamed target column). Computed
// expressions, functions, and literals do not resolve to a physical column and
// return ok == false.
func underlyingSourceColumn(expr sqlparser.Expr) (string, bool) {
	switch e := expr.(type) {
	case *sqlparser.ColName:
		return e.Name.String(), true
	case *sqlparser.ConvertUsingExpr:
		// Only a direct column rename like "convert(c1 using utf8mb4) as c2" is a
		// physical column; a computation wrapped in CONVERT is not.
		if inner, ok := e.Expr.(*sqlparser.ColName); ok {
			return inner.Name.String(), true
		}
	}
	return "", false
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
