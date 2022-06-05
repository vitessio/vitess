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

	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"

	"google.golang.org/protobuf/encoding/prototext"

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
		if err := td.restartTargetVReplicationStreams(ctx); err != nil {
			log.Errorf("error restarting target streams: %s", err)
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
	query := fmt.Sprintf("update _vt.vreplication set state = 'Stopped' %s", ct.workflowFilter)
	if _, err := ct.vde.vre.Exec(query); err != nil {
		return err
	}
	// streams are no longer running because vre.Exec would have replaced old controllers and new ones will not start

	// update position of all source streams
	query = fmt.Sprintf("select id, source, pos from _vt.vreplication %s", ct.workflowFilter)
	qr, err := withDDL.Exec(ctx, query, dbClient.ExecuteFetch, dbClient.ExecuteFetch)
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
			return fmt.Errorf("stream %d has not started", id)
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
	wg.Add(1)
	go func() {
		defer wg.Done()
		err1 = td.forEachSource(func(source *migrationSource) error {
			//todo: handle external sources for Mount/Migrate
			tablet, err := pickTablet(ctx, ct.ts, cell, ct.sourceKeyspace, source.shard, tabletTypes)
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
	log.Infof("syncSourceStreams start")
	ct := td.wd.ct
	waitCtx, cancel := context.WithTimeout(ctx, time.Duration(ct.options.CoreOptions.TimeoutSeconds)*time.Second)
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
	log.Infof("syncTargetStreams start")
	ct := td.wd.ct
	waitCtx, cancel := context.WithTimeout(ctx, time.Duration(ct.options.CoreOptions.TimeoutSeconds)*time.Second)
	defer cancel()

	if err := td.forEachSource(func(source *migrationSource) error {
		query := fmt.Sprintf("update _vt.vreplication set state='Running', stop_pos='%s', message='synchronizing for vdiff' where id=%d",
			source.snapshotPosition, source.vrID)
		if _, err := ct.tmc.VReplicationExec(waitCtx, ct.vde.thisTablet, query); err != nil {
			return err
		}
		if err := ct.vde.vre.WaitForPos(waitCtx, int(source.vrID), source.snapshotPosition); err != nil {
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
	log.Infof("startTargetDataStream")
	ct := td.wd.ct
	gtidch := make(chan string)
	ct.targetShardStreamer.result = make(chan *sqltypes.Result)
	go td.streamOneShard(ctx, ct.targetShardStreamer, td.tablePlan.targetQuery, gtidch)
	gtid, ok := <-gtidch
	if !ok {
		log.Infof("streaming error: %s", ct.targetShardStreamer.err)
		return ct.targetShardStreamer.err
	}
	ct.targetShardStreamer.snapshotPosition = gtid
	return nil
}

func (td *tableDiffer) startSourceDataStreams(ctx context.Context) error {
	log.Infof("startSourceDataStreams")
	if err := td.forEachSource(func(source *migrationSource) error {
		source.result = make(chan *sqltypes.Result)
		gtidch := make(chan string)
		go td.streamOneShard(ctx, source.shardStreamer, td.tablePlan.sourceQuery, gtidch)

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
	log.Infof("restartTargetVReplicationStreams")
	ct := td.wd.ct
	query := fmt.Sprintf("update _vt.vreplication set state='Running', message='', stop_pos='' where db_name=%s and workflow=%s", encodeString(ct.vde.dbName), encodeString(ct.workflow))
	log.Infof("restarting target replication with %s", query)
	_, err := ct.tmc.VReplicationExec(ctx, ct.vde.thisTablet, query)
	return err
}

func (td *tableDiffer) streamOneShard(ctx context.Context, participant *shardStreamer, query string, gtidch chan string) {
	log.Infof("streamOneShard Start %s", participant.tablet.Alias.String())
	defer func() {
		log.Infof("streamOneShard End %s", participant.tablet.Alias.String())
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
		return conn.VStreamRows(ctx, target, query, nil, func(vsr *binlogdatapb.VStreamRowsResponse) error {
			log.Infof("VStreamRows start %s:%d", participant.tablet.Alias.String(), len(vsr.Rows))

			if len(fields) == 0 {
				if len(vsr.Fields) == 0 {
					return fmt.Errorf("did not received expected fields in response %+v", vsr)
				}
				//log.Infof("received fields from %s", participant.tablet.Alias)
				fields = vsr.Fields
				//log.Infof("fields received were %+v, %s", fields, participant.tablet.Alias)
				gtidch <- vsr.Gtid
			}
			if len(vsr.Rows) == 0 && len(vsr.Fields) == 0 {
				//log.Infof("no rows, no fields %+v, %s", fields, participant.tablet.Alias)
				return nil
			}
			log.Infof(">>>>>>>>>> workflow %s: received %d rows, %d fields, from %s, rows %+v",
				td.wd.ct.workflow, len(vsr.Rows), len(vsr.Fields), participant.tablet.Alias, vsr.Rows)
			p3qr := &querypb.QueryResult{
				Fields: fields,
				Rows:   vsr.Rows,
			}
			result := sqltypes.Proto3ToResult(p3qr)

			// Fields should be received only once, and sent only once.
			if vsr.Fields == nil {
				result.Fields = nil
			}
			//log.Infof("%s pushing1 %d fields, %d rows", participant.tablet.Alias.String(), len(result.Fields),len(result.Rows))//, result.Rows)
			select {
			case participant.result <- result:
			case <-ctx.Done():
				return vterrors.Wrap(ctx.Err(), "VStreamRows")
			}
			//log.Infof("%s pushing2 %d rows", participant.tablet.Alias.String(), len(result.Rows))//, result.Rows)
			return nil
		})
	}()
}

func (td *tableDiffer) setupRowSorters() {
	// combine all sources into a slice and create a merge sorter for it
	sources := make(map[string]*shardStreamer)
	for shard, source := range td.wd.ct.sources {
		sources[shard] = source.shardStreamer
	}
	td.sourcePrimitive = newMergeSorter(sources, td.tablePlan.comparePKs)

	// create a merge sorter for the target
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

func (td *tableDiffer) diff(ctx context.Context, rowsToCompare *int64, debug, onlyPks bool, maxExtraRowsToCompare int64) (*DiffReport, error) {
	dbClient := td.wd.ct.dbClientFactory()
	if err := dbClient.Connect(); err != nil {
		return nil, err
	}
	defer dbClient.Close()

	sourceExecutor := newPrimitiveExecutor(ctx, td.sourcePrimitive, "source")
	targetExecutor := newPrimitiveExecutor(ctx, td.targetPrimitive, "target")
	dr := &DiffReport{TableName: td.table.Name}
	var sourceRow, targetRow []sqltypes.Value
	var err error
	advanceSource := true
	advanceTarget := true
	mismatch := false

	for {
		if dr.ProcessedRows%1e7 == 0 { // log progress every 10 million rows
			log.Infof("VDiff progress:: table %s: %d rows", td.table.Name, dr.ProcessedRows)
			if err := td.updateRowsCompared(dbClient, dr.ProcessedRows); err != nil {
				return nil, err
			}
		}
		if !mismatch && dr.MismatchedRows > 0 {
			mismatch = true
			log.Infof("Flagging mismatch for %s: %+v", td.table.Name, dr)
			if err := updateTableMismatch(dbClient, td.wd.ct.id, td.table.Name); err != nil {
				return nil, err
			}
		}
		*rowsToCompare--
		//log.Infof("VDiff progress:: table %s: %d rows", td.table.Name, dr.ProcessedRows)
		if *rowsToCompare < 0 {
			log.Infof("Stopping vdiff, specified limit reached")
			return dr, nil
		}
		if advanceSource {
			//log.Infof("waiting for source row")
			sourceRow, err = sourceExecutor.next()
			if err != nil {
				log.Error(err)
				return nil, err
			}
			//log.Infof("got source row")// %+v", sourceRow)
		}
		if advanceTarget {
			//log.Infof("waiting for target row")
			targetRow, err = targetExecutor.next()
			if err != nil {
				log.Error(err)
				return nil, err
			}
			//log.Infof("got target row")// %+v", targetRow)
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
	}
}

func (td *tableDiffer) compare(sourceRow, targetRow []sqltypes.Value, cols []compareColInfo, compareOnlyNonPKs bool) (int, error) {
	for _, col := range cols {
		if col.isPK && compareOnlyNonPKs {
			continue
		}
		compareIndex := col.colIndex
		var c int
		var err error
		var collationID collations.ID
		// if the collation is nil or unknown, use binary collation to compare as bytes
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

func (td *tableDiffer) updateRowsCompared(dbClient binlogplayer.DBClient, numRows int64) error {
	query := fmt.Sprintf(sqlUpdateRowsCompared, numRows, td.wd.ct.id, encodeString(td.table.Name))
	if _, err := dbClient.ExecuteFetch(query, 1); err != nil {
		return err
	}
	return nil
}

func (td *tableDiffer) updateTableState(ctx context.Context, dbClient binlogplayer.DBClient, tableName, state string, dr *DiffReport) error {
	state = strings.ToLower(state)
	reportJSON := "{}"
	if dr != nil {
		reportJSONBytes, err := json.Marshal(dr)
		if err != nil {
			return err
		}
		reportJSON = string(reportJSONBytes)
	}
	query := fmt.Sprintf(sqlUpdateTableState, encodeString(state), encodeString(reportJSON), td.wd.ct.id, encodeString(tableName))
	if _, err := withDDL.Exec(ctx, query, dbClient.ExecuteFetch, dbClient.ExecuteFetch); err != nil {
		return err
	}
	insertVDiffLog(ctx, dbClient, td.wd.ct.id, fmt.Sprintf("%s: table %s", state, encodeString(tableName)))

	return nil
}

func updateTableMismatch(dbClient binlogplayer.DBClient, vdiffID int64, table string) error {
	query := fmt.Sprintf(sqlUpdateTableMismatch, vdiffID, encodeString(table))
	if _, err := dbClient.ExecuteFetch(query, 1); err != nil {
		return err
	}
	return nil
}
