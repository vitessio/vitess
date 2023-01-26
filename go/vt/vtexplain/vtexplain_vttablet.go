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

package vtexplain

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"vitess.io/vitess/go/vt/sidecardb"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

type tabletEnv struct {
	// map of schema introspection queries to their expected results
	schemaQueries map[string]*sqltypes.Result

	// map for each table from the column name to its type
	tableColumns map[string]map[string]querypb.Type
}

func newTabletEnv() *tabletEnv {
	return &tabletEnv{
		schemaQueries: make(map[string]*sqltypes.Result),
		tableColumns:  make(map[string]map[string]querypb.Type),
	}
}

func (te *tabletEnv) addResult(query string, result *sqltypes.Result) {
	te.schemaQueries[query] = result
}

func (te *tabletEnv) getResult(query string) *sqltypes.Result {
	result, ok := te.schemaQueries[query]
	if !ok {
		return nil
	}
	return result
}

func (vte *VTExplain) setGlobalTabletEnv(env *tabletEnv) {
	vte.globalTabletEnv = env
}

func (vte *VTExplain) getGlobalTabletEnv() *tabletEnv {
	return vte.globalTabletEnv
}

// explainTablet is the query service that simulates a tablet.
//
// To avoid needing to boilerplate implement the unneeded portions of the
// QueryService interface, it overrides only the necessary methods and embeds
// a wrapped QueryService that throws an error if an unimplemented method is
// called.
type explainTablet struct {
	queryservice.QueryService

	db  *fakesqldb.DB
	tsv *tabletserver.TabletServer

	mu            sync.Mutex
	tabletQueries []*TabletQuery
	mysqlQueries  []*MysqlQuery
	currentTime   int
	vte           *VTExplain
}

var _ queryservice.QueryService = (*explainTablet)(nil)

func (vte *VTExplain) newTablet(opts *Options, t *topodatapb.Tablet) *explainTablet {
	db := fakesqldb.New(nil)
	sidecardb.AddSchemaInitQueries(db, true)

	config := tabletenv.NewCurrentConfig()
	config.TrackSchemaVersions = false
	if opts.ExecutionMode == ModeTwoPC {
		config.TwoPCCoordinatorAddress = "XXX"
		config.TwoPCAbandonAge = 1.0
		config.TwoPCEnable = true
	}
	config.EnableOnlineDDL = false
	config.EnableTableGC = false

	// XXX much of this is cloned from the tabletserver tests
	tsv := tabletserver.NewTabletServer(topoproto.TabletAliasString(t.Alias), config, memorytopo.NewServer(""), t.Alias)

	tablet := explainTablet{db: db, tsv: tsv, vte: vte}
	db.Handler = &tablet

	tablet.QueryService = queryservice.Wrap(
		nil,
		func(ctx context.Context, target *querypb.Target, conn queryservice.QueryService, name string, inTransaction bool, inner func(context.Context, *querypb.Target, queryservice.QueryService) (bool, error)) error {
			return fmt.Errorf("explainTablet does not implement %s", name)
		},
	)

	params, _ := db.ConnParams().MysqlParams()
	cp := *params
	dbcfgs := dbconfigs.NewTestDBConfigs(cp, cp, "")
	cnf := mysqlctl.NewMycnf(22222, 6802)
	cnf.ServerID = 33333

	target := querypb.Target{
		Keyspace:   t.Keyspace,
		Shard:      t.Shard,
		TabletType: topodatapb.TabletType_PRIMARY,
	}
	tsv.StartService(&target, dbcfgs, nil /* mysqld */)

	// clear all the schema initialization queries out of the tablet
	// to avoid clutttering the output
	tablet.mysqlQueries = nil

	return &tablet
}

var _ queryservice.QueryService = (*explainTablet)(nil) // compile-time interface check

// Begin is part of the QueryService interface.
func (t *explainTablet) Begin(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions) (queryservice.TransactionState, error) {
	t.mu.Lock()
	t.currentTime = t.vte.batchTime.Wait()
	t.tabletQueries = append(t.tabletQueries, &TabletQuery{
		Time: t.currentTime,
		SQL:  "begin",
	})

	t.mu.Unlock()

	return t.tsv.Begin(ctx, target, options)
}

// Commit is part of the QueryService interface.
func (t *explainTablet) Commit(ctx context.Context, target *querypb.Target, transactionID int64) (int64, error) {
	t.mu.Lock()
	t.currentTime = t.vte.batchTime.Wait()
	t.tabletQueries = append(t.tabletQueries, &TabletQuery{
		Time: t.currentTime,
		SQL:  "commit",
	})
	t.mu.Unlock()

	return t.tsv.Commit(ctx, target, transactionID)
}

// Rollback is part of the QueryService interface.
func (t *explainTablet) Rollback(ctx context.Context, target *querypb.Target, transactionID int64) (int64, error) {
	t.mu.Lock()
	t.currentTime = t.vte.batchTime.Wait()
	t.mu.Unlock()
	return t.tsv.Rollback(ctx, target, transactionID)
}

// Execute is part of the QueryService interface.
func (t *explainTablet) Execute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID, reservedID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	t.mu.Lock()
	t.currentTime = t.vte.batchTime.Wait()

	// Since the query is simulated being "sent" over the wire we need to
	// copy the bindVars into the executor to avoid a data race.
	bindVariables = sqltypes.CopyBindVariables(bindVariables)
	t.tabletQueries = append(t.tabletQueries, &TabletQuery{
		Time:     t.currentTime,
		SQL:      sql,
		BindVars: bindVariables,
	})
	t.mu.Unlock()

	return t.tsv.Execute(ctx, target, sql, bindVariables, transactionID, reservedID, options)
}

// Prepare is part of the QueryService interface.
func (t *explainTablet) Prepare(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	t.mu.Lock()
	t.currentTime = t.vte.batchTime.Wait()
	t.mu.Unlock()
	return t.tsv.Prepare(ctx, target, transactionID, dtid)
}

// CommitPrepared commits the prepared transaction.
func (t *explainTablet) CommitPrepared(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	t.mu.Lock()
	t.currentTime = t.vte.batchTime.Wait()
	t.mu.Unlock()
	return t.tsv.CommitPrepared(ctx, target, dtid)
}

// CreateTransaction is part of the QueryService interface.
func (t *explainTablet) CreateTransaction(ctx context.Context, target *querypb.Target, dtid string, participants []*querypb.Target) (err error) {
	t.mu.Lock()
	t.currentTime = t.vte.batchTime.Wait()
	t.mu.Unlock()
	return t.tsv.CreateTransaction(ctx, target, dtid, participants)
}

// StartCommit is part of the QueryService interface.
func (t *explainTablet) StartCommit(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	t.mu.Lock()
	t.currentTime = t.vte.batchTime.Wait()
	t.mu.Unlock()
	return t.tsv.StartCommit(ctx, target, transactionID, dtid)
}

// SetRollback is part of the QueryService interface.
func (t *explainTablet) SetRollback(ctx context.Context, target *querypb.Target, dtid string, transactionID int64) (err error) {
	t.mu.Lock()
	t.currentTime = t.vte.batchTime.Wait()
	t.mu.Unlock()
	return t.tsv.SetRollback(ctx, target, dtid, transactionID)
}

// ConcludeTransaction is part of the QueryService interface.
func (t *explainTablet) ConcludeTransaction(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	t.mu.Lock()
	t.currentTime = t.vte.batchTime.Wait()
	t.mu.Unlock()
	return t.tsv.ConcludeTransaction(ctx, target, dtid)
}

// ReadTransaction is part of the QueryService interface.
func (t *explainTablet) ReadTransaction(ctx context.Context, target *querypb.Target, dtid string) (metadata *querypb.TransactionMetadata, err error) {
	t.mu.Lock()
	t.currentTime = t.vte.batchTime.Wait()
	t.mu.Unlock()
	return t.tsv.ReadTransaction(ctx, target, dtid)
}

// BeginExecute is part of the QueryService interface.
func (t *explainTablet) BeginExecute(ctx context.Context, target *querypb.Target, preQueries []string, sql string, bindVariables map[string]*querypb.BindVariable, reservedID int64, options *querypb.ExecuteOptions) (queryservice.TransactionState, *sqltypes.Result, error) {
	t.mu.Lock()
	t.currentTime = t.vte.batchTime.Wait()
	bindVariables = sqltypes.CopyBindVariables(bindVariables)
	t.tabletQueries = append(t.tabletQueries, &TabletQuery{
		Time:     t.currentTime,
		SQL:      sql,
		BindVars: bindVariables,
	})
	t.mu.Unlock()

	return t.tsv.BeginExecute(ctx, target, preQueries, sql, bindVariables, reservedID, options)
}

// Close is part of the QueryService interface.
func (t *explainTablet) Close(ctx context.Context) error {
	return t.tsv.Close(ctx)
}

func newTabletEnvironment(ddls []sqlparser.DDLStatement, opts *Options) (*tabletEnv, error) {
	tEnv := newTabletEnv()
	schemaQueries := map[string]*sqltypes.Result{
		"select unix_timestamp()": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			Rows: [][]sqltypes.Value{
				{sqltypes.NewInt32(1427325875)},
			},
		},
		"select @@global.sql_mode": {
			Fields: []*querypb.Field{{
				Type: sqltypes.VarChar,
			}},
			Rows: [][]sqltypes.Value{
				{sqltypes.NewVarBinary("STRICT_TRANS_TABLES")},
			},
		},
		"select @@autocommit": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			Rows: [][]sqltypes.Value{
				{sqltypes.NewVarBinary("1")},
			},
		},
		"select @@sql_auto_is_null": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			Rows: [][]sqltypes.Value{
				{sqltypes.NewVarBinary("0")},
			},
		},
		"set @@session.sql_log_bin = 0": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			Rows: [][]sqltypes.Value{},
		},
		"create database if not exists `_vt`": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			Rows: [][]sqltypes.Value{},
		},
		"drop table if exists `_vt`.redo_log_transaction": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			Rows: [][]sqltypes.Value{},
		},
		"drop table if exists `_vt`.redo_log_statement": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			Rows: [][]sqltypes.Value{},
		},
		"drop table if exists `_vt`.transaction": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			Rows: [][]sqltypes.Value{},
		},
		"drop table if exists `_vt`.participant": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			Rows: [][]sqltypes.Value{},
		},
		"create table if not exists `_vt`.redo_state(\n  dtid varbinary(512),\n  state bigint,\n  time_created bigint,\n  primary key(dtid)\n\t) engine=InnoDB": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			Rows: [][]sqltypes.Value{},
		},
		"create table if not exists `_vt`.redo_statement(\n  dtid varbinary(512),\n  id bigint,\n  statement mediumblob,\n  primary key(dtid, id)\n\t) engine=InnoDB": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			Rows: [][]sqltypes.Value{},
		},
		"create table if not exists `_vt`.dt_state(\n  dtid varbinary(512),\n  state bigint,\n  time_created bigint,\n  primary key(dtid)\n\t) engine=InnoDB": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			Rows: [][]sqltypes.Value{},
		},
		"create table if not exists `_vt`.dt_participant(\n  dtid varbinary(512),\n\tid bigint,\n\tkeyspace varchar(256),\n\tshard varchar(256),\n  primary key(dtid, id)\n\t) engine=InnoDB": {

			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			Rows: [][]sqltypes.Value{},
		},
		mysql.ShowRowsRead: sqltypes.MakeTestResult(
			sqltypes.MakeTestFields(
				"Variable_name|value",
				"varchar|uint64",
			),
			"Innodb_rows|0",
		),
	}

	for query, result := range schemaQueries {
		tEnv.addResult(query, result)
	}

	showTableRows := make([][]sqltypes.Value, 0, 4)
	for _, ddl := range ddls {
		table := ddl.GetTable().Name.String()
		options := ""
		spec := ddl.GetTableSpec()
		if spec != nil {
			for _, option := range spec.Options {
				if option.Name == "comment" && string(option.Value.Val) == "vitess_sequence" {
					options = "vitess_sequence"
				}
			}
		}
		showTableRows = append(showTableRows, mysql.BaseShowTablesRow(table, false, options))
	}
	tEnv.addResult(mysql.TablesWithSize57, &sqltypes.Result{
		Fields: mysql.BaseShowTablesFields,
		Rows:   showTableRows,
	})

	indexRows := make([][]sqltypes.Value, 0, 4)
	for _, ddl := range ddls {
		table := sqlparser.String(ddl.GetTable().Name)
		backtickedTable := sqlescape.EscapeID(sqlescape.UnescapeID(table))
		if ddl.GetOptLike() != nil {
			likeTable := ddl.GetOptLike().LikeTable.Name.String()
			backtickedLikeTable := sqlescape.EscapeID(sqlescape.UnescapeID(likeTable))

			likeQuery := "SELECT * FROM " + backtickedLikeTable + " WHERE 1 != 1"
			query := "SELECT * FROM " + backtickedTable + " WHERE 1 != 1"
			if tEnv.getResult(likeQuery) == nil {
				return nil, fmt.Errorf("check your schema, table[%s] doesn't exist", likeTable)
			}
			tEnv.addResult(query, tEnv.getResult(likeQuery))

			likeQuery = fmt.Sprintf(mysqlctl.GetColumnNamesQuery, "database()", sqlescape.UnescapeID(likeTable))
			query = fmt.Sprintf(mysqlctl.GetColumnNamesQuery, "database()", sqlescape.UnescapeID(table))
			if tEnv.getResult(likeQuery) == nil {
				return nil, fmt.Errorf("check your schema, table[%s] doesn't exist", likeTable)
			}
			tEnv.addResult(query, tEnv.getResult(likeQuery))
			continue
		}
		for _, idx := range ddl.GetTableSpec().Indexes {
			if !idx.Info.Primary {
				continue
			}
			for _, col := range idx.Columns {
				row := mysql.ShowPrimaryRow(table, col.Column.String())
				indexRows = append(indexRows, row)
			}
		}

		tEnv.tableColumns[table] = make(map[string]querypb.Type)
		var rowTypes []*querypb.Field
		var colTypes []*querypb.Field
		var colValues [][]sqltypes.Value
		colType := &querypb.Field{
			Name: "column_type",
			Type: sqltypes.VarChar,
		}
		colTypes = append(colTypes, colType)
		for _, col := range ddl.GetTableSpec().Columns {
			colName := strings.ToLower(col.Name.String())
			rowType := &querypb.Field{
				Name: colName,
				Type: col.Type.SQLType(),
			}
			rowTypes = append(rowTypes, rowType)
			tEnv.tableColumns[table][colName] = col.Type.SQLType()
			colValues = append(colValues, []sqltypes.Value{sqltypes.NewVarChar(colName)})
		}
		tEnv.addResult("SELECT * FROM "+backtickedTable+" WHERE 1 != 1", &sqltypes.Result{
			Fields: rowTypes,
		})
		query := fmt.Sprintf(mysqlctl.GetColumnNamesQuery, "database()", sqlescape.UnescapeID(table))
		tEnv.addResult(query, &sqltypes.Result{
			Fields: colTypes,
			Rows:   colValues,
		})
	}

	tEnv.addResult(mysql.BaseShowPrimary, &sqltypes.Result{
		Fields: mysql.ShowPrimaryFields,
		Rows:   indexRows,
	})

	return tEnv, nil
}

// HandleQuery implements the fakesqldb query handler interface
func (t *explainTablet) HandleQuery(c *mysql.Conn, query string, callback func(*sqltypes.Result) error) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !strings.Contains(query, "1 != 1") {
		t.mysqlQueries = append(t.mysqlQueries, &MysqlQuery{
			Time: t.currentTime,
			SQL:  query,
		})
	}

	// return the pre-computed results for any schema introspection queries
	tEnv := t.vte.getGlobalTabletEnv()
	result := tEnv.getResult(query)
	emptyResult := &sqltypes.Result{}
	if sidecardb.MatchesInitQuery(query) {
		return callback(emptyResult)
	}
	if result != nil {
		return callback(result)
	}

	if err := t.db.GetRejectedQueryResult(query); err != nil {
		return err
	}

	if result := t.db.GetQueryResult(query); result != nil {
		if f := result.BeforeFunc; f != nil {
			f()
		}
		return callback(result.Result)
	}

	if pat, ok := t.db.GetQueryPatternResult(query); ok {
		userCallback, ok := t.db.GetQueryPatternUserCallBack(pat.Expr)
		if ok {
			userCallback(query)
		}
		if pat.Err != "" {
			return fmt.Errorf(pat.Err)
		}
		return callback(pat.Result)
	}

	switch sqlparser.Preview(query) {
	case sqlparser.StmtSelect:
		// Parse the select statement to figure out the table and columns
		// that were referenced so that the synthetic response has the
		// expected field names and types.
		stmt, err := sqlparser.Parse(query)
		if err != nil {
			return err
		}

		var selStmt *sqlparser.Select
		switch stmt := stmt.(type) {
		case *sqlparser.Select:
			selStmt = stmt
		case *sqlparser.Union:
			selStmt = sqlparser.GetFirstSelect(stmt)
		default:
			return fmt.Errorf("vtexplain: unsupported statement type +%v", reflect.TypeOf(stmt))
		}

		// Gen4 supports more complex queries so we now need to
		// handle multiple FROM clauses
		tables := make([]*sqlparser.AliasedTableExpr, len(selStmt.From))
		for _, from := range selStmt.From {
			tables = append(tables, getTables(from)...)
		}

		tableColumnMap := map[sqlparser.IdentifierCS]map[string]querypb.Type{}
		for _, table := range tables {
			if table == nil {
				continue
			}

			tableName := sqlparser.String(sqlparser.GetTableName(table.Expr))
			columns, exists := t.vte.getGlobalTabletEnv().tableColumns[tableName]
			if !exists && tableName != "" && tableName != "dual" {
				return fmt.Errorf("unable to resolve table name %s", tableName)
			}

			colTypeMap := map[string]querypb.Type{}

			if table.As.IsEmpty() {
				tableColumnMap[sqlparser.GetTableName(table.Expr)] = colTypeMap
			} else {
				tableColumnMap[table.As] = colTypeMap
			}

			for k, v := range columns {
				if colType, exists := colTypeMap[k]; exists {
					if colType != v {
						return fmt.Errorf("column type mismatch for column : %s, types: %d vs %d", k, colType, v)
					}
					continue
				}
				colTypeMap[k] = v
			}

		}

		colNames := make([]string, 0, 4)
		colTypes := make([]querypb.Type, 0, 4)
		for _, node := range selStmt.SelectExprs {
			switch node := node.(type) {
			case *sqlparser.AliasedExpr:
				colNames, colTypes = inferColTypeFromExpr(node.Expr, tableColumnMap, colNames, colTypes)
			case *sqlparser.StarExpr:
				if node.TableName.Name.IsEmpty() {
					// SELECT *
					for _, colTypeMap := range tableColumnMap {
						for col, colType := range colTypeMap {
							colNames = append(colNames, col)
							colTypes = append(colTypes, colType)
						}
					}
				} else {
					// SELECT tableName.*
					colTypeMap := tableColumnMap[node.TableName.Name]
					for col, colType := range colTypeMap {
						colNames = append(colNames, col)
						colTypes = append(colTypes, colType)
					}
				}
			}
		}

		// the query against lookup table is in-query, handle it specifically
		var inColName string
		inVal := make([]sqltypes.Value, 0, 10)

		rowCount := 1
		if selStmt.Where != nil {
			switch v := selStmt.Where.Expr.(type) {
			case *sqlparser.ComparisonExpr:
				if v.Operator == sqlparser.InOp {
					switch c := v.Left.(type) {
					case *sqlparser.ColName:
						colName := strings.ToLower(c.Name.String())
						colType := tableColumnMap[sqlparser.GetTableName(selStmt.From[0].(*sqlparser.AliasedTableExpr).Expr)][colName]

						switch values := v.Right.(type) {
						case sqlparser.ValTuple:
							for _, val := range values {
								switch v := val.(type) {
								case *sqlparser.Literal:
									value, err := evalengine.LiteralToValue(v)
									if err != nil {
										return err
									}

									// Cast the value in the tuple to the expected value of the column
									castedValue, err := evalengine.Cast(value, colType)
									if err != nil {
										return err
									}

									// Check if we have a duplicate value
									isNewValue := true
									for _, v := range inVal {
										result, err := evalengine.NullsafeCompare(v, value, collations.Default())
										if err != nil {
											return err
										}

										if result == 0 {
											isNewValue = false
											break
										}
									}

									if isNewValue {
										inVal = append(inVal, castedValue)
									}
								}
							}
							rowCount = len(inVal)
						}
						inColName = strings.ToLower(c.Name.String())
					}
				}
			}
		}

		fields := make([]*querypb.Field, len(colNames))
		rows := make([][]sqltypes.Value, 0, rowCount)
		for i, col := range colNames {
			colType := colTypes[i]
			fields[i] = &querypb.Field{
				Name: col,
				Type: colType,
			}
		}

		for j := 0; j < rowCount; j++ {
			values := make([]sqltypes.Value, len(colNames))
			for i, col := range colNames {
				// Generate a fake value for the given column. For the column in the IN clause,
				// use the provided values in the query, For numeric types,
				// use the column index. For all other types, just shortcut to using
				// a string type that encodes the column name + index.
				colType := colTypes[i]
				if len(inVal) > j && col == inColName {
					values[i], _ = sqltypes.NewValue(querypb.Type_VARBINARY, inVal[j].Raw())
				} else if sqltypes.IsIntegral(colType) {
					values[i] = sqltypes.NewInt32(int32(i + 1))
				} else if sqltypes.IsFloat(colType) {
					values[i] = sqltypes.NewFloat64(1.0 + float64(i))
				} else {
					values[i] = sqltypes.NewVarChar(fmt.Sprintf("%s_val_%d", col, i+1))
				}
			}
			rows = append(rows, values)
		}
		result = &sqltypes.Result{
			Fields:   fields,
			InsertID: 0,
			Rows:     rows,
		}

		resultJSON, _ := json.MarshalIndent(result, "", "    ")
		log.V(100).Infof("query %s result %s\n", query, string(resultJSON))

	case sqlparser.StmtBegin, sqlparser.StmtCommit, sqlparser.StmtSet,
		sqlparser.StmtSavepoint, sqlparser.StmtSRollback, sqlparser.StmtRelease:
		result = &sqltypes.Result{}
	case sqlparser.StmtShow:
		result = &sqltypes.Result{Fields: sqltypes.MakeTestFields("", "")}
	case sqlparser.StmtInsert, sqlparser.StmtReplace, sqlparser.StmtUpdate, sqlparser.StmtDelete:
		result = &sqltypes.Result{
			RowsAffected: 1,
		}
	case sqlparser.StmtUse:
		result = &sqltypes.Result{}
	case sqlparser.StmtDDL:
		result = &sqltypes.Result{}
	default:
		return fmt.Errorf("unsupported query %s", query)
	}

	return callback(result)
}

func getTables(node sqlparser.SQLNode) []*sqlparser.AliasedTableExpr {
	var tables []*sqlparser.AliasedTableExpr
	switch expr := node.(type) {
	case *sqlparser.AliasedTableExpr:
		tables = append(tables, expr)
	case *sqlparser.JoinTableExpr:
		tables = append(tables, getTables(expr.LeftExpr)...)
		tables = append(tables, getTables(expr.RightExpr)...)
	}
	return tables
}

func inferColTypeFromExpr(node sqlparser.Expr, tableColumnMap map[sqlparser.IdentifierCS]map[string]querypb.Type, colNames []string, colTypes []querypb.Type) ([]string, []querypb.Type) {
	switch node := node.(type) {
	case *sqlparser.ColName:
		if node.Qualifier.Name.IsEmpty() {
			// Unqualified column name, try to search for it across all tables
			col := strings.ToLower(node.Name.String())

			var colType querypb.Type

			for _, colTypeMap := range tableColumnMap {
				if colTypeMap[col] != querypb.Type_NULL_TYPE {
					if colType != querypb.Type_NULL_TYPE {
						log.Errorf("vtexplain: ambiguous column %s", col)
						return colNames, colTypes
					}

					colType = colTypeMap[col]
				}
			}

			if colType == querypb.Type_NULL_TYPE {
				log.Errorf("vtexplain: invalid column %s.%s, tableColumnMap +%v", node.Qualifier.Name, col, tableColumnMap)
			}

			colNames = append(colNames, col)
			colTypes = append(colTypes, colType)
		} else {
			// Qualified column name, try to look it up
			colTypeMap := tableColumnMap[node.Qualifier.Name]
			col := strings.ToLower(node.Name.String())
			colType := colTypeMap[col]

			if colType == querypb.Type_NULL_TYPE {
				log.Errorf("vtexplain: invalid column %s.%s, tableColumnMap +%v", node.Qualifier.Name, col, tableColumnMap)
			}

			colNames = append(colNames, col)
			colTypes = append(colTypes, colType)
		}
	case sqlparser.Callable:
		// As a shortcut, functions are integral types
		colNames = append(colNames, sqlparser.String(node))
		colTypes = append(colTypes, querypb.Type_INT32)
	case *sqlparser.Literal:
		colNames = append(colNames, sqlparser.String(node))
		switch node.Type {
		case sqlparser.IntVal:
			fallthrough
		case sqlparser.HexNum:
			fallthrough
		case sqlparser.HexVal:
			fallthrough
		case sqlparser.BitVal:
			colTypes = append(colTypes, querypb.Type_INT32)
		case sqlparser.StrVal:
			colTypes = append(colTypes, querypb.Type_VARCHAR)
		case sqlparser.FloatVal:
			colTypes = append(colTypes, querypb.Type_FLOAT64)
		case sqlparser.DecimalVal:
			colTypes = append(colTypes, querypb.Type_DECIMAL)
		default:
			log.Errorf("vtexplain: unsupported sql value %s", sqlparser.String(node))
		}
	case *sqlparser.CaseExpr:
		colNames, colTypes = inferColTypeFromExpr(node.Whens[0].Val, tableColumnMap, colNames, colTypes)
	case *sqlparser.NullVal:
		colNames = append(colNames, sqlparser.String(node))
		colTypes = append(colTypes, querypb.Type_NULL_TYPE)
	case *sqlparser.ComparisonExpr:
		colNames = append(colNames, sqlparser.String(node))
		colTypes = append(colTypes, querypb.Type_INT64)
	default:
		log.Errorf("vtexplain: unsupported select expression type +%v node %s", reflect.TypeOf(node), sqlparser.String(node))
	}

	return colNames, colTypes
}
