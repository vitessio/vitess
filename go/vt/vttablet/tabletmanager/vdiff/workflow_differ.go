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
	"fmt"
	"reflect"
	"strings"

	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/schema"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtctl/schematools"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
)

// workflowDiffer has metadata and state for the vdiff of a single workflow on this tablet
// only one vdiff can be running for a workflow at any time.
type workflowDiffer struct {
	ct *controller

	tableDiffers map[string]*tableDiffer // key is table name
	opts         *tabletmanagerdatapb.VDiffOptions
}

func newWorkflowDiffer(ct *controller, opts *tabletmanagerdatapb.VDiffOptions) (*workflowDiffer, error) {
	wd := &workflowDiffer{
		ct:           ct,
		opts:         opts,
		tableDiffers: make(map[string]*tableDiffer, 1),
	}
	return wd, nil
}

// If the only difference is the order in which the rows were returned
// by MySQL on each side then we'll have the same number of extras on
// both sides. If that's the case, then let's see if the extra rows on
// both sides are actually different.
func (wd *workflowDiffer) reconcileExtraRows(dr *DiffReport, maxExtraRowsToCompare int64) error {
	if dr.MismatchedRows == 0 {
		// Get the VSchema on the target and source keyspaces. We can then use this
		// for handling additional edge cases, such as adjusting results for reference
		// tables when the shard count is different between the source and target as
		// then there will be a extra rows reported on the side with more shards.
		srcvschema, err := wd.ct.ts.GetVSchema(wd.ct.vde.ctx, wd.ct.sourceKeyspace)
		if err != nil {
			return err
		}
		tgtvschema, err := wd.ct.ts.GetVSchema(wd.ct.vde.ctx, wd.ct.vde.thisTablet.Keyspace)
		if err != nil {
			return err
		}
		svt, sok := srcvschema.Tables[dr.TableName]
		tvt, tok := tgtvschema.Tables[dr.TableName]
		if dr.ExtraRowsSource > 0 && sok && svt.Type == vindexes.TypeReference && dr.ExtraRowsSource%dr.MatchingRows == 0 {
			// We have a reference table with no mismatched rows and the number of
			// extra rows on the source is a multiple of the matching rows. This
			// means that there's no actual diff.
			dr.ExtraRowsSource = 0
			dr.ExtraRowsSourceDiffs = nil
		}
		if dr.ExtraRowsTarget > 0 && tok && tvt.Type == vindexes.TypeReference && dr.ExtraRowsTarget%dr.MatchingRows == 0 {
			// We have a reference table with no mismatched rows and the number of
			// extra rows on the target is a multiple of the matching rows. This
			// means that there's no actual diff.
			dr.ExtraRowsTarget = 0
			dr.ExtraRowsTargetDiffs = nil
		}
	}

	if (dr.ExtraRowsSource == dr.ExtraRowsTarget) && (dr.ExtraRowsSource <= maxExtraRowsToCompare) {
		for i := 0; i < len(dr.ExtraRowsSourceDiffs); i++ {
			foundMatch := false
			for j := 0; j < len(dr.ExtraRowsTargetDiffs); j++ {
				if reflect.DeepEqual(dr.ExtraRowsSourceDiffs[i], dr.ExtraRowsTargetDiffs[j]) {
					dr.ExtraRowsSourceDiffs = append(dr.ExtraRowsSourceDiffs[:i], dr.ExtraRowsSourceDiffs[i+1:]...)
					dr.ExtraRowsTargetDiffs = append(dr.ExtraRowsTargetDiffs[:j], dr.ExtraRowsTargetDiffs[j+1:]...)
					dr.ExtraRowsSource--
					dr.ExtraRowsTarget--
					dr.ProcessedRows--
					dr.MatchingRows++
					// We've removed an element from both slices at the current index
					// so we need to shift the counters back as well to process the
					// new elements at the index and avoid using an index out of range.
					i--
					j--
					foundMatch = true
					break
				}
			}
			// If we didn't find a match then the tables are in fact different and we can short circuit the second pass
			if !foundMatch {
				break
			}
		}
	}
	// We can now trim the extra rows diffs on both sides to the maxVDiffReportSampleRows value
	if len(dr.ExtraRowsSourceDiffs) > maxVDiffReportSampleRows {
		dr.ExtraRowsSourceDiffs = dr.ExtraRowsSourceDiffs[:maxVDiffReportSampleRows-1]
	}
	if len(dr.ExtraRowsTargetDiffs) > maxVDiffReportSampleRows {
		dr.ExtraRowsTargetDiffs = dr.ExtraRowsTargetDiffs[:maxVDiffReportSampleRows-1]
	}

	return nil
}

func (wd *workflowDiffer) diffTable(ctx context.Context, dbClient binlogplayer.DBClient, td *tableDiffer) error {
	select {
	case <-ctx.Done():
		return vterrors.Errorf(vtrpcpb.Code_CANCELED, "context has expired")
	default:
	}

	log.Infof("Starting differ on table %s for vdiff %s", td.table.Name, wd.ct.uuid)
	if err := td.updateTableState(ctx, dbClient, StartedState); err != nil {
		return err
	}
	if err := td.initialize(ctx); err != nil {
		return err
	}
	log.Infof("Table initialization done on table %s for vdiff %s", td.table.Name, wd.ct.uuid)
	dr, err := td.diff(ctx, wd.opts.CoreOptions.MaxRows, wd.opts.ReportOptions.DebugQuery, wd.opts.ReportOptions.OnlyPks, wd.opts.CoreOptions.MaxExtraRowsToCompare)
	if err != nil {
		log.Errorf("Encountered an error diffing table %s for vdiff %s: %v", td.table.Name, wd.ct.uuid, err)
		return err
	}
	log.Infof("Table diff done on table %s for vdiff %s with report: %+v", td.table.Name, wd.ct.uuid, dr)
	if dr.ExtraRowsSource > 0 || dr.ExtraRowsTarget > 0 {
		if err := wd.reconcileExtraRows(dr, wd.opts.CoreOptions.MaxExtraRowsToCompare); err != nil {
			log.Errorf("Encountered an error reconciling extra rows found for table %s for vdiff %s: %v", td.table.Name, wd.ct.uuid, err)
			return vterrors.Wrap(err, "failed to reconcile extra rows")
		}
	}

	if dr.MismatchedRows > 0 || dr.ExtraRowsTarget > 0 || dr.ExtraRowsSource > 0 {
		if err := updateTableMismatch(dbClient, wd.ct.id, td.table.Name); err != nil {
			return err
		}
	}

	log.Infof("Completed reconciliation on table %s for vdiff %s with updated report: %+v", td.table.Name, wd.ct.uuid, dr)
	if err := td.updateTableStateAndReport(ctx, dbClient, CompletedState, dr); err != nil {
		return err
	}
	return nil
}

func (wd *workflowDiffer) diff(ctx context.Context) error {
	dbClient := wd.ct.dbClientFactory()
	if err := dbClient.Connect(); err != nil {
		return err
	}
	defer dbClient.Close()

	select {
	case <-ctx.Done():
		return vterrors.Errorf(vtrpcpb.Code_CANCELED, "context has expired")
	default:
	}

	filter := wd.ct.filter
	req := &tabletmanagerdatapb.GetSchemaRequest{}
	schm, err := schematools.GetSchema(ctx, wd.ct.ts, wd.ct.tmc, wd.ct.vde.thisTablet.Alias, req)
	if err != nil {
		return vterrors.Wrap(err, "GetSchema")
	}
	if err = wd.buildPlan(dbClient, filter, schm); err != nil {
		return vterrors.Wrap(err, "buildPlan")
	}
	if err := wd.initVDiffTables(dbClient); err != nil {
		return err
	}
	for _, td := range wd.tableDiffers {
		select {
		case <-ctx.Done():
			return vterrors.Errorf(vtrpcpb.Code_CANCELED, "context has expired")
		default:
		}
		query, err := sqlparser.ParseAndBind(sqlGetVDiffTable,
			sqltypes.Int64BindVariable(wd.ct.id),
			sqltypes.StringBindVariable(td.table.Name),
		)
		if err != nil {
			return err
		}
		qr, err := dbClient.ExecuteFetch(query, 1)
		if err != nil {
			return err
		}
		if len(qr.Rows) == 0 {
			return fmt.Errorf("no vdiff table found for %s on tablet %v",
				td.table.Name, wd.ct.vde.thisTablet.Alias)
		}

		log.Infof("Starting diff of table %s for vdiff %s", td.table.Name, wd.ct.uuid)
		if err := wd.diffTable(ctx, dbClient, td); err != nil {
			if err := td.updateTableState(ctx, dbClient, ErrorState); err != nil {
				return err
			}
			insertVDiffLog(ctx, dbClient, wd.ct.id, fmt.Sprintf("Table %s Error: %s", td.table.Name, err))
			return err
		}
		if err := td.updateTableState(ctx, dbClient, CompletedState); err != nil {
			return err
		}
		log.Infof("Completed diff of table %s for vdiff %s", td.table.Name, wd.ct.uuid)
	}
	if err := wd.markIfCompleted(ctx, dbClient); err != nil {
		return err
	}
	return nil
}

func (wd *workflowDiffer) markIfCompleted(ctx context.Context, dbClient binlogplayer.DBClient) error {
	query, err := sqlparser.ParseAndBind(sqlGetIncompleteTables, sqltypes.Int64BindVariable(wd.ct.id))
	if err != nil {
		return err
	}
	qr, err := dbClient.ExecuteFetch(query, -1)
	if err != nil {
		return err
	}

	// Double check to be sure all of the individual table diffs completed without error
	// before marking the vdiff as completed.
	if len(qr.Rows) == 0 {
		if err := wd.ct.updateState(dbClient, CompletedState, nil); err != nil {
			return err
		}
	}
	return nil
}

func (wd *workflowDiffer) buildPlan(dbClient binlogplayer.DBClient, filter *binlogdatapb.Filter, schm *tabletmanagerdatapb.SchemaDefinition) error {
	var specifiedTables []string
	optTables := strings.TrimSpace(wd.opts.CoreOptions.Tables)
	if optTables != "" {
		specifiedTables = strings.Split(optTables, ",")
	}

	for _, table := range schm.TableDefinitions {
		// if user specified tables explicitly only use those, otherwise diff all tables in workflow
		if len(specifiedTables) != 0 && !stringListContains(specifiedTables, table.Name) {
			continue
		}
		if schema.IsInternalOperationTableName(table.Name) {
			continue
		}
		rule, err := vreplication.MatchTable(table.Name, filter)
		if err != nil {
			return err
		}
		if rule == nil || rule.Filter == "exclude" {
			continue
		}
		sourceQuery := rule.Filter
		switch {
		case rule.Filter == "":
			buf := sqlparser.NewTrackedBuffer(nil)
			buf.Myprintf("select * from %v", sqlparser.NewIdentifierCS(table.Name))
			sourceQuery = buf.String()
		case key.IsValidKeyRange(rule.Filter):
			buf := sqlparser.NewTrackedBuffer(nil)
			buf.Myprintf("select * from %v where in_keyrange(%v)", sqlparser.NewIdentifierCS(table.Name), sqlparser.NewStrLiteral(rule.Filter))
			sourceQuery = buf.String()
		}

		td := newTableDiffer(wd, table, sourceQuery)
		lastpkpb, err := wd.getTableLastPK(dbClient, table.Name)
		if err != nil {
			return err
		}
		td.lastPK = lastpkpb
		wd.tableDiffers[table.Name] = td
		if _, err := td.buildTablePlan(dbClient, wd.ct.vde.dbName); err != nil {
			return err
		}
	}
	if len(wd.tableDiffers) == 0 {
		return fmt.Errorf("no tables found to diff, %s:%s, on tablet %v",
			optTables, specifiedTables, wd.ct.vde.thisTablet.Alias)
	}
	return nil
}

// getTableLastPK gets the lastPK protobuf message for a given vdiff table.
func (wd *workflowDiffer) getTableLastPK(dbClient binlogplayer.DBClient, tableName string) (*querypb.QueryResult, error) {
	query, err := sqlparser.ParseAndBind(sqlGetVDiffTable,
		sqltypes.Int64BindVariable(wd.ct.id),
		sqltypes.StringBindVariable(tableName),
	)
	if err != nil {
		return nil, err
	}
	qr, err := dbClient.ExecuteFetch(query, 1)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) == 1 {
		var lastpk []byte
		if lastpk, err = qr.Named().Row().ToBytes("lastpk"); err != nil {
			return nil, err
		}
		if len(lastpk) != 0 {
			var lastpkpb querypb.QueryResult
			if err := prototext.Unmarshal(lastpk, &lastpkpb); err != nil {
				return nil, err
			}
			return &lastpkpb, nil
		}
	}
	return nil, nil
}

func (wd *workflowDiffer) initVDiffTables(dbClient binlogplayer.DBClient) error {
	tableIn := strings.Builder{}
	n := 0
	for tableName := range wd.tableDiffers {
		// Update the table statistics for each table if requested.
		if wd.opts.CoreOptions.UpdateTableStats {
			stmt := sqlparser.BuildParsedQuery(sqlAnalyzeTable,
				wd.ct.vde.dbName,
				tableName,
			)
			log.Infof("Updating the table stats for %s.%s using: %q", wd.ct.vde.dbName, tableName, stmt.Query)
			if _, err := dbClient.ExecuteFetch(stmt.Query, -1); err != nil {
				return err
			}
			log.Infof("Finished updating the table stats for %s.%s", wd.ct.vde.dbName, tableName)
		}
		tableIn.WriteString(encodeString(tableName))
		if n++; n < len(wd.tableDiffers) {
			tableIn.WriteByte(',')
		}
	}
	query := sqlparser.BuildParsedQuery(sqlGetAllTableRows,
		encodeString(wd.ct.vde.dbName),
		tableIn.String(),
	)
	isqr, err := dbClient.ExecuteFetch(query.Query, -1)
	if err != nil {
		return err
	}
	for _, row := range isqr.Named().Rows {
		tableName, _ := row.ToString("table_name")
		tableRows, _ := row.ToInt64("table_rows")

		query, err := sqlparser.ParseAndBind(sqlGetVDiffTable,
			sqltypes.Int64BindVariable(wd.ct.id),
			sqltypes.StringBindVariable(tableName),
		)
		if err != nil {
			return err
		}
		qr, err := dbClient.ExecuteFetch(query, -1)
		if err != nil {
			return err
		}
		if len(qr.Rows) == 0 {
			query, err = sqlparser.ParseAndBind(sqlNewVDiffTable,
				sqltypes.Int64BindVariable(wd.ct.id),
				sqltypes.StringBindVariable(tableName),
				sqltypes.Int64BindVariable(tableRows),
			)
			if err != nil {
				return err
			}
		} else if len(qr.Rows) == 1 {
			query, err = sqlparser.ParseAndBind(sqlUpdateTableRows,
				sqltypes.Int64BindVariable(tableRows),
				sqltypes.Int64BindVariable(wd.ct.id),
				sqltypes.StringBindVariable(tableName),
			)
			if err != nil {
				return err
			}
		} else {
			return fmt.Errorf("invalid state found for vdiff table %s for vdiff_id %d on tablet %s",
				tableName, wd.ct.id, wd.ct.vde.thisTablet.Alias)
		}
		if _, err := dbClient.ExecuteFetch(query, 1); err != nil {
			return err
		}
	}
	return nil
}
