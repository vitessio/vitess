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

	"vitess.io/vitess/go/vt/binlog/binlogplayer"

	"vitess.io/vitess/go/vt/schema"

	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtctl/schematools"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
)

// workflowDiffer has metadata and state for the vdiff of a single workflow on this tablet
// only one vdiff can be running for a workflow at any time
type workflowDiffer struct {
	ct *controller

	tableDiffers map[string]*tableDiffer // key is table name
	tableSizes   map[string]int64        // approx. size of tables when vdiff started
	opts         *tabletmanagerdatapb.VDiffOptions
}

func newWorkflowDiffer(ct *controller, opts *tabletmanagerdatapb.VDiffOptions) (*workflowDiffer, error) {
	wd := &workflowDiffer{
		ct:           ct,
		opts:         opts,
		tableSizes:   make(map[string]int64),
		tableDiffers: make(map[string]*tableDiffer, 1),
	}
	return wd, nil
}

// If the only difference is the order in which the rows were returned
// by MySQL on each side then we'll have the same number of extras on
// both sides. If that's the case, then let's see if the extra rows on
// both sides are actually different.
func (wd *workflowDiffer) reconcileExtraRows(dr *DiffReport, maxExtraRowsToCompare int64) {
	if (dr.ExtraRowsSource == dr.ExtraRowsTarget) && (dr.ExtraRowsSource <= maxExtraRowsToCompare) {
		for i := range dr.ExtraRowsSourceDiffs {
			foundMatch := false
			for j := range dr.ExtraRowsTargetDiffs {
				if reflect.DeepEqual(dr.ExtraRowsSourceDiffs[i], dr.ExtraRowsTargetDiffs[j]) {
					dr.ExtraRowsSourceDiffs = append(dr.ExtraRowsSourceDiffs[:i], dr.ExtraRowsSourceDiffs[i+1:]...)
					dr.ExtraRowsSource--
					dr.ExtraRowsTargetDiffs = append(dr.ExtraRowsTargetDiffs[:j], dr.ExtraRowsTargetDiffs[j+1:]...)
					dr.ExtraRowsTarget--
					dr.ProcessedRows--
					dr.MatchingRows++
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
}

func (wd *workflowDiffer) diffTable(ctx context.Context, dbClient binlogplayer.DBClient, td *tableDiffer) error {
	tableName := td.table.Name
	log.Infof("Starting differ on table %s", tableName)
	if err := td.updateTableState(ctx, dbClient, tableName, StartedState, nil); err != nil {
		return err
	}
	if err := td.initialize(ctx); err != nil {
		return err
	}
	log.Infof("initialize done")
	dr, err := td.diff(ctx, &wd.opts.CoreOptions.MaxRows, wd.opts.ReportOptions.DebugQuery, false, wd.opts.CoreOptions.MaxExtraRowsToCompare)
	if err != nil {
		log.Errorf("td.diff error %s", err.Error())
		return err
	}
	log.Infof("td.diff done for %s, with dr %+v", tableName, dr)
	if dr.ExtraRowsSource > 0 || dr.ExtraRowsTarget > 0 {
		wd.reconcileExtraRows(dr, wd.opts.CoreOptions.MaxExtraRowsToCompare)
	}

	if dr.MismatchedRows > 0 || dr.ExtraRowsTarget > 0 || dr.ExtraRowsSource > 0 {
		if err := updateTableMismatch(dbClient, wd.ct.id, tableName); err != nil {
			return err
		}
	}

	log.Infof("td.diff after reconciliation for %s, with dr %+v", tableName, dr)
	if err := td.updateTableState(ctx, dbClient, tableName, CompletedState, dr); err != nil {
		return err
	}
	return nil
}

func (wd *workflowDiffer) getTotalRowsEstimate(dbClient binlogplayer.DBClient) error {
	query := "select db_name as db_name from _vt.vreplication where workflow = %s limit 1"
	query = fmt.Sprintf(query, encodeString(wd.ct.workflow))
	qr, err := dbClient.ExecuteFetch(query, 1)
	if err != nil {
		return err
	}
	dbName, _ := qr.Named().Row().ToString("db_name")
	query = "select table_name as table_name, table_rows as table_rows from information_schema.tables where table_schema = %s"
	query = fmt.Sprintf(query, encodeString(dbName))
	qr, err = dbClient.ExecuteFetch(query, -1)
	if err != nil {
		return err
	}
	for _, row := range qr.Named().Rows {
		tableName, _ := row.ToString("table_name")
		tableRows, _ := row.ToInt64("table_rows")
		wd.tableSizes[tableName] = tableRows
	}
	return nil
}

func (wd *workflowDiffer) diff(ctx context.Context) error {
	dbClient := wd.ct.dbClientFactory()
	if err := dbClient.Connect(); err != nil {
		return err
	}
	defer dbClient.Close()

	filter := wd.ct.filter
	req := &tabletmanagerdatapb.GetSchemaRequest{}
	schm, err := schematools.GetSchema(ctx, wd.ct.ts, wd.ct.tmc, wd.ct.vde.thisTablet.Alias, req)
	if err != nil {
		return vterrors.Wrap(err, "GetSchema")
	}
	if err = wd.buildPlan(dbClient, filter, schm); err != nil {
		return vterrors.Wrap(err, "buildPlan")
	}
	if err := wd.getTotalRowsEstimate(dbClient); err != nil {
		return err
	}
	for _, td := range wd.tableDiffers {
		tableRows, ok := wd.tableSizes[td.table.Name]
		if !ok {
			tableRows = 0
		}
		var query string
		query = fmt.Sprintf(sqlGetVDiffTable, wd.ct.id, encodeString(td.table.Name))
		qr, err := withDDL.Exec(ctx, query, dbClient.ExecuteFetch, dbClient.ExecuteFetch)
		if err != nil {
			return err
		}
		if len(qr.Rows) == 0 {
			query = fmt.Sprintf(sqlNewVDiffTable, wd.ct.id, encodeString(td.table.Name), tableRows)
		} else {
			// Update the table rows estimate when resuming
			query = fmt.Sprintf(sqlUpdateTableRows, tableRows, wd.ct.id, encodeString(td.table.Name))
		}
		if _, err := withDDL.Exec(ctx, query, dbClient.ExecuteFetch, dbClient.ExecuteFetch); err != nil {
			return err
		}

		log.Infof("starting table %s", td.table.Name)
		if err := wd.diffTable(ctx, dbClient, td); err != nil {
			if err := td.updateTableState(ctx, dbClient, td.table.Name, ErrorState, nil); err != nil {
				return err
			}
			insertVDiffLog(ctx, dbClient, wd.ct.id, fmt.Sprintf("Table %s Error: %s", td.table.Name, err))
			return err
		}
		log.Infof("done table %s", td.table.Name)
	}
	if err := wd.markIfCompleted(ctx, dbClient); err != nil {
		return err
	}
	return nil
}

func (wd *workflowDiffer) markIfCompleted(ctx context.Context, dbClient binlogplayer.DBClient) error {
	query := fmt.Sprintf(sqlGetIncompleteTables, wd.ct.id)
	qr, err := withDDL.Exec(ctx, query, dbClient.ExecuteFetch, dbClient.ExecuteFetch)
	if err != nil {
		return err
	}
	if len(qr.Rows) == 0 {
		if err := wd.ct.updateState(dbClient, CompletedState); err != nil {
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
			buf.Myprintf("select * from %v", sqlparser.NewTableIdent(table.Name))
			sourceQuery = buf.String()
		case key.IsKeyRange(rule.Filter):
			buf := sqlparser.NewTrackedBuffer(nil)
			buf.Myprintf("select * from %v where in_keyrange(%v)", sqlparser.NewTableIdent(table.Name), sqlparser.NewStrLiteral(rule.Filter))
			sourceQuery = buf.String()
		}

		td := newTableDiffer(wd, table, sourceQuery)
		lastpkpb, err := wd.getTableLastPK(dbClient, table.Name)
		if err != nil {
			return err
		}
		td.lastPK = lastpkpb
		wd.tableDiffers[table.Name] = td
		if _, err := td.buildTablePlan(); err != nil {
			return err
		}
	}
	if len(wd.tableDiffers) == 0 {
		return fmt.Errorf("no tables found to diff, %s:%s", optTables, specifiedTables)
	}
	return nil
}

// getTableLastPK gets the lastPK protobuf message for a given vdiff table
func (wd *workflowDiffer) getTableLastPK(dbClient binlogplayer.DBClient, tableName string) (*querypb.QueryResult, error) {
	query := fmt.Sprintf(sqlGetVDiffTable, wd.ct.id, encodeString(tableName))
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
