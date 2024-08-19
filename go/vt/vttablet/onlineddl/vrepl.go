/*
	Original copyright by GitHub as follows. Additions by the Vitess authors as follows.
*/
/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/
/*
Copyright 2021 The Vitess Authors.

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

package onlineddl

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/schemadiff"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// VReplStream represents a row in _vt.vreplication table
type VReplStream struct {
	id                   int32
	workflow             string
	source               string
	pos                  string
	timeUpdated          int64
	timeHeartbeat        int64
	timeThrottled        int64
	componentThrottled   string
	reasonThrottled      string
	transactionTimestamp int64
	state                binlogdatapb.VReplicationWorkflowState
	message              string
	rowsCopied           int64
	bls                  *binlogdatapb.BinlogSource
}

// livenessTimeIndicator returns a time indicator for last known healthy state.
// vreplication uses three indicators:
// - transaction_timestamp
// - time_heartbeat
// - time_throttled.
// Updating any of them, also updates time_updated, indicating liveness.
func (v *VReplStream) livenessTimeIndicator() int64 {
	return v.timeUpdated
}

// isRunning() returns true when the workflow is actively running
func (v *VReplStream) isRunning() bool {
	switch v.state {
	case binlogdatapb.VReplicationWorkflowState_Init, binlogdatapb.VReplicationWorkflowState_Copying, binlogdatapb.VReplicationWorkflowState_Running:
		return true
	}
	return false
}

// hasError() returns true when the workflow has failed and will not retry
func (v *VReplStream) hasError() (isTerminal bool, vreplError error) {
	switch {
	case v.state == binlogdatapb.VReplicationWorkflowState_Error:
		return true, errors.New(v.message)
	case strings.Contains(strings.ToLower(v.message), "error"):
		return false, errors.New(v.message)
	}
	return false, nil
}

// VRepl is an online DDL helper for VReplication based migrations (ddl_strategy="online")
type VRepl struct {
	workflow  string
	keyspace  string
	shard     string
	dbName    string
	pos       string
	tableRows int64

	sourceCreateTableEntity *schemadiff.CreateTableEntity
	targetCreateTableEntity *schemadiff.CreateTableEntity
	analysis                *schemadiff.MigrationTablesAnalysis

	analyzeTable bool

	filterQuery string
	bls         *binlogdatapb.BinlogSource

	alterTableAnalysis *schemadiff.AlterTableAnalysis

	convertCharset map[string](*binlogdatapb.CharsetConversion)

	env *vtenv.Environment
}

// NewVRepl creates a VReplication handler for Online DDL
func NewVRepl(
	env *vtenv.Environment,
	workflow string,
	keyspace string,
	shard string,
	dbName string,
	sourceCreateTable *sqlparser.CreateTable,
	targetCreateTable *sqlparser.CreateTable,
	alterQuery *sqlparser.AlterTable,
	analyzeTable bool,
) (*VRepl, error) {
	v := &VRepl{
		env:                env,
		workflow:           workflow,
		keyspace:           keyspace,
		shard:              shard,
		dbName:             dbName,
		alterTableAnalysis: schemadiff.OnlineDDLAlterTableAnalysis(alterQuery),
		analyzeTable:       analyzeTable,
		convertCharset:     map[string](*binlogdatapb.CharsetConversion){},
	}
	senv := schemadiff.NewEnv(v.env, v.env.CollationEnv().DefaultConnectionCharset())
	var err error
	v.sourceCreateTableEntity, err = schemadiff.NewCreateTableEntity(senv, sourceCreateTable)
	if err != nil {
		return nil, err
	}
	v.targetCreateTableEntity, err = schemadiff.NewCreateTableEntity(senv, targetCreateTable)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (v *VRepl) sourceTableName() string {
	return v.sourceCreateTableEntity.Name()
}

func (v *VRepl) targetTableName() string {
	return v.targetCreateTableEntity.Name()
}

// isFastAnalyzeTableSupported checks if the underlying MySQL server supports 'fast_analyze_table',
// introduced by a fork of MySQL: https://github.com/planetscale/mysql-server/commit/c8a9d93686358dabfba8f3dc5cc0621e3149fe78
// When `fast_analyze_table=1`, an `ANALYZE TABLE` command only analyzes the clustering index (normally the `PRIMARY KEY`).
// This is useful when you want to get a better estimate of the number of table rows, as fast as possible.
func (v *VRepl) isFastAnalyzeTableSupported(ctx context.Context, conn *dbconnpool.DBConnection) (isSupported bool, err error) {
	rs, err := conn.ExecuteFetch(sqlShowVariablesLikeFastAnalyzeTable, -1, true)
	if err != nil {
		return false, err
	}
	return len(rs.Rows) > 0, nil
}

// executeAnalyzeTable runs an ANALYZE TABLE command
func (v *VRepl) executeAnalyzeTable(ctx context.Context, conn *dbconnpool.DBConnection, tableName string) error {
	fastAnalyzeTableSupported, err := v.isFastAnalyzeTableSupported(ctx, conn)
	if err != nil {
		return err
	}
	if fastAnalyzeTableSupported {
		// This code is only applicable when MySQL supports the 'fast_analyze_table' variable. This variable
		// does not exist in vanilla MySQL.
		// See  https://github.com/planetscale/mysql-server/commit/c8a9d93686358dabfba8f3dc5cc0621e3149fe78
		// as part of https://github.com/planetscale/mysql-server/releases/tag/8.0.34-ps1.
		if _, err := conn.ExecuteFetch(sqlEnableFastAnalyzeTable, 1, false); err != nil {
			return err
		}
		log.Infof("@@fast_analyze_table enabled")
		defer conn.ExecuteFetch(sqlDisableFastAnalyzeTable, 1, false)
	}

	parsed := sqlparser.BuildParsedQuery(sqlAnalyzeTable, tableName)
	if _, err := conn.ExecuteFetch(parsed.Query, 1, false); err != nil {
		return err
	}
	return nil
}

// readTableStatus reads table status information
func (v *VRepl) readTableStatus(ctx context.Context, conn *dbconnpool.DBConnection, tableName string) (tableRows int64, err error) {
	parsed := sqlparser.BuildParsedQuery(sqlShowTableStatus, tableName)
	rs, err := conn.ExecuteFetch(parsed.Query, -1, true)
	if err != nil {
		return 0, err
	}
	row := rs.Named().Row()
	if row == nil {
		return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Cannot SHOW TABLE STATUS LIKE '%s'", tableName)
	}
	tableRows, err = row.ToInt64("Rows")
	return tableRows, err
}

// formalizeColumns
func formalizeColumns(columnsLists ...*schemadiff.ColumnDefinitionEntityList) error {
	for _, colList := range columnsLists {
		for _, col := range colList.Entities {
			col.SetExplicitDefaultAndNull()
			if err := col.SetExplicitCharsetCollate(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (v *VRepl) analyzeAlter() error {
	if v.alterTableAnalysis.IsRenameTable {
		return fmt.Errorf("renaming the table is not supported in ALTER TABLE")
	}
	return nil
}

func (v *VRepl) analyzeTables() (err error) {
	analysis, err := schemadiff.OnlineDDLMigrationTablesAnalysis(v.sourceCreateTableEntity, v.targetCreateTableEntity, v.alterTableAnalysis)
	if err != nil {
		return err
	}
	v.analysis = analysis

	return nil
}

// analyzeTableStatus reads information from SHOW TABLE STATUS
func (v *VRepl) analyzeTableStatus(ctx context.Context, conn *dbconnpool.DBConnection) (err error) {
	if v.analyzeTable {
		if err := v.executeAnalyzeTable(ctx, conn, v.sourceTableName()); err != nil {
			return err
		}
	}
	v.tableRows, err = v.readTableStatus(ctx, conn, v.sourceTableName())
	if err != nil {
		return err
	}
	return nil
}

// generateFilterQuery creates a SELECT query used by vreplication as a filter. It SELECTs all
// non-generated columns between source & target tables, and takes care of column renames.
func (v *VRepl) generateFilterQuery() error {
	if v.analysis.SourceSharedColumns.Len() == 0 {
		return fmt.Errorf("empty column list")
	}
	var sb strings.Builder
	sb.WriteString("select ")

	for i, sourceCol := range v.analysis.SourceSharedColumns.Entities {
		name := sourceCol.Name()
		targetName := v.analysis.SharedColumnsMap[name]

		targetCol := v.analysis.TargetSharedColumns.GetColumn(targetName)
		if targetCol == nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Cannot find target column %s", targetName)
		}

		if i > 0 {
			sb.WriteString(", ")
		}
		switch {
		case sourceCol.HasEnumValues():
			// Source is `enum` or `set`. We always take the textual represenation rather than the numeric one.
			sb.WriteString(fmt.Sprintf("CONCAT(%s)", escapeName(name)))
		case v.analysis.IntToEnumMap[name]:
			sb.WriteString(fmt.Sprintf("CONCAT(%s)", escapeName(name)))
		case sourceCol.Type() == "json":
			sb.WriteString(fmt.Sprintf("convert(%s using utf8mb4)", escapeName(name)))
		case targetCol.Type() == "json": // we already know the source col is not JSON, per the above `case` condition
			// Convert any type to JSON: encode the type as utf8mb4 text
			sb.WriteString(fmt.Sprintf("convert(%s using utf8mb4)", escapeName(name)))
		case sourceCol.IsTextual():
			// Check source and target charset/encoding. If needed, create
			// a binlogdatapb.CharsetConversion entry (later written to vreplication)
			fromCollation := v.env.CollationEnv().DefaultCollationForCharset(sourceCol.Charset())
			if fromCollation == collations.Unknown {
				return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Character set %s not supported for column %s", sourceCol.Charset(), sourceCol.Name())
			}
			toCollation := v.env.CollationEnv().DefaultCollationForCharset(targetCol.Charset())
			// Let's see if target col is at all textual
			if targetCol.IsTextual() && toCollation == collations.Unknown {
				return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Character set %s not supported for column %s", targetCol.Charset(), targetCol.Name())
			}
			if trivialCharset(fromCollation) && trivialCharset(toCollation) {
				sb.WriteString(escapeName(name))
			} else if fromCollation == toCollation {
				// No need for charset conversions as both have the same collation.
				sb.WriteString(escapeName(name))
			} else {
				// Charset conversion required:
				v.convertCharset[targetName] = &binlogdatapb.CharsetConversion{
					FromCharset: sourceCol.Charset(),
					ToCharset:   targetCol.Charset(),
				}
				sb.WriteString(escapeName(name))
			}
		default:
			sb.WriteString(escapeName(name))
		}
		sb.WriteString(" as ")
		sb.WriteString(escapeName(targetName))
	}
	sb.WriteString(" from ")
	sb.WriteString(escapeName(v.sourceTableName()))

	v.filterQuery = sb.String()
	return nil
}

func trivialCharset(c collations.ID) bool {
	if c == collations.Unknown {
		return true
	}
	utf8mb4Charset := charset.Charset_utf8mb4{}
	return utf8mb4Charset.IsSuperset(colldata.Lookup(c).Charset()) || c == collations.CollationBinaryID
}

func (v *VRepl) analyzeBinlogSource(ctx context.Context) {
	bls := &binlogdatapb.BinlogSource{
		Keyspace:      v.keyspace,
		Shard:         v.shard,
		Filter:        &binlogdatapb.Filter{},
		StopAfterCopy: false,
	}

	encodeColumns := func(names []string) string {
		return textutil.EscapeJoin(names, ",")
	}
	rule := &binlogdatapb.Rule{
		Match:                        v.targetTableName(),
		Filter:                       v.filterQuery,
		SourceUniqueKeyColumns:       encodeColumns(v.analysis.ChosenSourceUniqueKey.ColumnList.Names()),
		TargetUniqueKeyColumns:       encodeColumns(v.analysis.ChosenTargetUniqueKey.ColumnList.Names()),
		SourceUniqueKeyTargetColumns: encodeColumns(schemadiff.MappedColumnNames(v.analysis.ChosenSourceUniqueKey.ColumnList, v.analysis.SharedColumnsMap)),
		ForceUniqueKey:               url.QueryEscape(v.analysis.ChosenSourceUniqueKey.Name()),
	}
	if len(v.convertCharset) > 0 {
		rule.ConvertCharset = v.convertCharset
	}
	if len(v.analysis.IntToEnumMap) > 0 {
		rule.ConvertIntToEnum = v.analysis.IntToEnumMap
	}

	bls.Filter.Rules = append(bls.Filter.Rules, rule)
	v.bls = bls
}

func (v *VRepl) analyze(ctx context.Context, conn *dbconnpool.DBConnection) error {
	if err := v.analyzeAlter(); err != nil {
		return err
	}
	if err := v.analyzeTables(); err != nil {
		return err
	}
	if err := v.generateFilterQuery(); err != nil {
		return err
	}
	if err := v.analyzeTableStatus(ctx, conn); err != nil {
		return err
	}
	v.analyzeBinlogSource(ctx)
	return nil
}

// generateInsertStatement generates the INSERT INTO _vt.replication statement that creates the vreplication workflow
func (v *VRepl) generateInsertStatement() (string, error) {
	ig := vreplication.NewInsertGenerator(binlogdatapb.VReplicationWorkflowState_Stopped, v.dbName)
	ig.AddRow(v.workflow, v.bls, v.pos, "", "in_order:REPLICA,PRIMARY",
		binlogdatapb.VReplicationWorkflowType_OnlineDDL, binlogdatapb.VReplicationWorkflowSubType_None, false)

	return ig.String(), nil
}

// generateStartStatement Generates the statement to start VReplication running on the workflow
func (v *VRepl) generateStartStatement() (string, error) {
	return sqlparser.ParseAndBind(sqlStartVReplStream,
		sqltypes.StringBindVariable(v.dbName),
		sqltypes.StringBindVariable(v.workflow),
	)
}

func getVreplTable(s *VReplStream) (string, error) {
	// sanity checks:
	if s == nil {
		return "", vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "No vreplication stream migration")
	}
	if s.bls.Filter == nil {
		return "", vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "No binlog source filter for migration %s", s.workflow)
	}
	if len(s.bls.Filter.Rules) != 1 {
		return "", vterrors.Errorf(vtrpcpb.Code_UNKNOWN, "Cannot detect filter rules for migration/vreplication %s", s.workflow)
	}
	vreplTable := s.bls.Filter.Rules[0].Match
	return vreplTable, nil
}

// escapeName will escape a db/table/column/... name by wrapping with backticks.
// It is not fool proof. I'm just trying to do the right thing here, not solving
// SQL injection issues, which should be irrelevant for this tool.
func escapeName(name string) string {
	if unquoted, err := strconv.Unquote(name); err == nil {
		name = unquoted
	}
	return fmt.Sprintf("`%s`", name)
}
