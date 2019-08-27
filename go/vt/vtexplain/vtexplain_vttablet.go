/*
Copyright 2017 Google Inc.

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
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	// map of schema introspection queries to their expected results
	schemaQueries map[string]*sqltypes.Result

	// map for each table from the column name to its type
	tableColumns map[string]map[string]querypb.Type

	// time simulator
	batchTime *sync2.Batcher
)

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
}

func newTablet(opts *Options, t *topodatapb.Tablet) *explainTablet {
	db := fakesqldb.New(nil)

	config := tabletenv.Config
	if opts.ExecutionMode == ModeTwoPC {
		config.TwoPCCoordinatorAddress = "XXX"
		config.TwoPCAbandonAge = 1.0
		config.TwoPCEnable = true
	}

	// XXX much of this is cloned from the tabletserver tests
	tsv := tabletserver.NewTabletServerWithNilTopoServer(config)

	tablet := explainTablet{db: db, tsv: tsv}
	db.Handler = &tablet

	tablet.QueryService = queryservice.Wrap(
		nil,
		func(ctx context.Context, target *querypb.Target, conn queryservice.QueryService, name string, inTransaction bool, inner func(context.Context, *querypb.Target, queryservice.QueryService) (bool, error)) error {
			return fmt.Errorf("explainTablet does not implement %s", name)
		},
	)

	dbcfgs := dbconfigs.NewTestDBConfigs(*db.ConnParams(), *db.ConnParams(), "")
	cnf := mysqlctl.NewMycnf(22222, 6802)
	cnf.ServerID = 33333

	target := querypb.Target{
		Keyspace:   t.Keyspace,
		Shard:      t.Shard,
		TabletType: topodatapb.TabletType_MASTER,
	}
	tsv.StartService(target, dbcfgs)

	// clear all the schema initialization queries out of the tablet
	// to avoid clutttering the output
	tablet.mysqlQueries = nil

	return &tablet
}

var _ queryservice.QueryService = (*explainTablet)(nil) // compile-time interface check

// Begin is part of the QueryService interface.
func (t *explainTablet) Begin(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions) (int64, error) {
	t.mu.Lock()
	t.currentTime = batchTime.Wait()
	t.tabletQueries = append(t.tabletQueries, &TabletQuery{
		Time: t.currentTime,
		SQL:  "begin",
	})

	t.mu.Unlock()

	return t.tsv.Begin(ctx, target, options)
}

// Commit is part of the QueryService interface.
func (t *explainTablet) Commit(ctx context.Context, target *querypb.Target, transactionID int64) error {
	t.mu.Lock()
	t.currentTime = batchTime.Wait()
	t.tabletQueries = append(t.tabletQueries, &TabletQuery{
		Time: t.currentTime,
		SQL:  "commit",
	})
	t.mu.Unlock()

	return t.tsv.Commit(ctx, target, transactionID)
}

// Rollback is part of the QueryService interface.
func (t *explainTablet) Rollback(ctx context.Context, target *querypb.Target, transactionID int64) error {
	t.mu.Lock()
	t.currentTime = batchTime.Wait()
	t.mu.Unlock()
	return t.tsv.Rollback(ctx, target, transactionID)
}

// Execute is part of the QueryService interface.
func (t *explainTablet) Execute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	t.mu.Lock()
	t.currentTime = batchTime.Wait()

	// Since the query is simulated being "sent" over the wire we need to
	// copy the bindVars into the executor to avoid a data race.
	bindVariables = sqltypes.CopyBindVariables(bindVariables)
	t.tabletQueries = append(t.tabletQueries, &TabletQuery{
		Time:     t.currentTime,
		SQL:      sql,
		BindVars: bindVariables,
	})
	t.mu.Unlock()

	return t.tsv.Execute(ctx, target, sql, bindVariables, transactionID, options)
}

// Prepare is part of the QueryService interface.
func (t *explainTablet) Prepare(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	t.mu.Lock()
	t.currentTime = batchTime.Wait()
	t.mu.Unlock()
	return t.tsv.Prepare(ctx, target, transactionID, dtid)
}

// CommitPrepared commits the prepared transaction.
func (t *explainTablet) CommitPrepared(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	t.mu.Lock()
	t.currentTime = batchTime.Wait()
	t.mu.Unlock()
	return t.tsv.CommitPrepared(ctx, target, dtid)
}

// CreateTransaction is part of the QueryService interface.
func (t *explainTablet) CreateTransaction(ctx context.Context, target *querypb.Target, dtid string, participants []*querypb.Target) (err error) {
	t.mu.Lock()
	t.currentTime = batchTime.Wait()
	t.mu.Unlock()
	return t.tsv.CreateTransaction(ctx, target, dtid, participants)
}

// StartCommit is part of the QueryService interface.
func (t *explainTablet) StartCommit(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	t.mu.Lock()
	t.currentTime = batchTime.Wait()
	t.mu.Unlock()
	return t.tsv.StartCommit(ctx, target, transactionID, dtid)
}

// SetRollback is part of the QueryService interface.
func (t *explainTablet) SetRollback(ctx context.Context, target *querypb.Target, dtid string, transactionID int64) (err error) {
	t.mu.Lock()
	t.currentTime = batchTime.Wait()
	t.mu.Unlock()
	return t.tsv.SetRollback(ctx, target, dtid, transactionID)
}

// ConcludeTransaction is part of the QueryService interface.
func (t *explainTablet) ConcludeTransaction(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	t.mu.Lock()
	t.currentTime = batchTime.Wait()
	t.mu.Unlock()
	return t.tsv.ConcludeTransaction(ctx, target, dtid)
}

// ReadTransaction is part of the QueryService interface.
func (t *explainTablet) ReadTransaction(ctx context.Context, target *querypb.Target, dtid string) (metadata *querypb.TransactionMetadata, err error) {
	t.mu.Lock()
	t.currentTime = batchTime.Wait()
	t.mu.Unlock()
	return t.tsv.ReadTransaction(ctx, target, dtid)
}

// ExecuteBatch is part of the QueryService interface.
func (t *explainTablet) ExecuteBatch(ctx context.Context, target *querypb.Target, queries []*querypb.BoundQuery, asTransaction bool, transactionID int64, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	t.mu.Lock()
	t.currentTime = batchTime.Wait()

	// Since the query is simulated being "sent" over the wire we need to
	// copy the bindVars into the executor to avoid a data race.
	for _, query := range queries {
		query.BindVariables = sqltypes.CopyBindVariables(query.BindVariables)
		t.tabletQueries = append(t.tabletQueries, &TabletQuery{
			Time:     t.currentTime,
			SQL:      query.Sql,
			BindVars: query.BindVariables,
		})
	}
	t.mu.Unlock()

	return t.tsv.ExecuteBatch(ctx, target, queries, asTransaction, transactionID, options)
}

// BeginExecute is part of the QueryService interface.
func (t *explainTablet) BeginExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, error) {
	t.mu.Lock()
	t.currentTime = batchTime.Wait()
	bindVariables = sqltypes.CopyBindVariables(bindVariables)
	t.tabletQueries = append(t.tabletQueries, &TabletQuery{
		Time:     t.currentTime,
		SQL:      sql,
		BindVars: bindVariables,
	})
	t.mu.Unlock()

	return t.tsv.BeginExecute(ctx, target, sql, bindVariables, options)
}

// Close is part of the QueryService interface.
func (t *explainTablet) Close(ctx context.Context) error {
	return t.tsv.Close(ctx)
}

func initTabletEnvironment(ddls []*sqlparser.DDL, opts *Options) error {
	tableColumns = make(map[string]map[string]querypb.Type)
	schemaQueries = map[string]*sqltypes.Result{
		"select unix_timestamp()": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.NewInt32(1427325875)},
			},
		},
		"select @@global.sql_mode": {
			Fields: []*querypb.Field{{
				Type: sqltypes.VarChar,
			}},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.NewVarBinary("STRICT_TRANS_TABLES")},
			},
		},
		"select @@autocommit": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.NewVarBinary("1")},
			},
		},
		"select @@sql_auto_is_null": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.NewVarBinary("0")},
			},
		},
		"show variables like 'binlog_format'": {
			Fields: []*querypb.Field{{
				Type: sqltypes.VarChar,
			}, {
				Type: sqltypes.VarChar,
			}},
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{{
				sqltypes.NewVarBinary("binlog_format"),
				sqltypes.NewVarBinary(opts.ReplicationMode),
			}},
		},
		"set @@session.sql_log_bin = 0": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			RowsAffected: 0,
			Rows:         [][]sqltypes.Value{},
		},
		"create database if not exists `_vt`": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			RowsAffected: 0,
			Rows:         [][]sqltypes.Value{},
		},
		"drop table if exists `_vt`.redo_log_transaction": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			RowsAffected: 0,
			Rows:         [][]sqltypes.Value{},
		},
		"drop table if exists `_vt`.redo_log_statement": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			RowsAffected: 0,
			Rows:         [][]sqltypes.Value{},
		},
		"drop table if exists `_vt`.transaction": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			RowsAffected: 0,
			Rows:         [][]sqltypes.Value{},
		},
		"drop table if exists `_vt`.participant": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			RowsAffected: 0,
			Rows:         [][]sqltypes.Value{},
		},
		"create table if not exists `_vt`.redo_state(\n  dtid varbinary(512),\n  state bigint,\n  time_created bigint,\n  primary key(dtid)\n\t) engine=InnoDB": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			RowsAffected: 0,
			Rows:         [][]sqltypes.Value{},
		},
		"create table if not exists `_vt`.redo_statement(\n  dtid varbinary(512),\n  id bigint,\n  statement mediumblob,\n  primary key(dtid, id)\n\t) engine=InnoDB": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			RowsAffected: 0,
			Rows:         [][]sqltypes.Value{},
		},
		"create table if not exists `_vt`.dt_state(\n  dtid varbinary(512),\n  state bigint,\n  time_created bigint,\n  primary key(dtid)\n\t) engine=InnoDB": {
			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			RowsAffected: 0,
			Rows:         [][]sqltypes.Value{},
		},
		"create table if not exists `_vt`.dt_participant(\n  dtid varbinary(512),\n\tid bigint,\n\tkeyspace varchar(256),\n\tshard varchar(256),\n  primary key(dtid, id)\n\t) engine=InnoDB": {

			Fields: []*querypb.Field{{
				Type: sqltypes.Uint64,
			}},
			RowsAffected: 0,
			Rows:         [][]sqltypes.Value{},
		},
	}

	showTableRows := make([][]sqltypes.Value, 0, 4)
	for _, ddl := range ddls {
		table := ddl.Table.Name.String()
		showTableRows = append(showTableRows, mysql.BaseShowTablesRow(table, false, ""))
	}
	schemaQueries[mysql.BaseShowTables] = &sqltypes.Result{
		Fields:       mysql.BaseShowTablesFields,
		RowsAffected: uint64(len(showTableRows)),
		Rows:         showTableRows,
	}

	for i, ddl := range ddls {
		table := sqlparser.String(ddl.Table.Name)
		schemaQueries[mysql.BaseShowTablesForTable(table)] = &sqltypes.Result{
			Fields:       mysql.BaseShowTablesFields,
			RowsAffected: 1,
			Rows:         [][]sqltypes.Value{showTableRows[i]},
		}

		if ddl.OptLike != nil {
			likeTable := ddl.OptLike.LikeTable.Name.String()
			if _, ok := schemaQueries["describe "+likeTable]; !ok {
				return fmt.Errorf("check your schema, table[%s] doesnt exist", likeTable)
			}
			schemaQueries["show index from "+table] = schemaQueries["show index from "+likeTable]
			schemaQueries["describe "+table] = schemaQueries["describe "+likeTable]
			schemaQueries["select * from "+table+" where 1 != 1"] = schemaQueries["select * from "+likeTable+" where 1 != 1"]
			continue
		}
		pkColumns := make(map[string]bool)
		indexRows := make([][]sqltypes.Value, 0, 4)
		for _, idx := range ddl.TableSpec.Indexes {
			for i, col := range idx.Columns {
				row := mysql.ShowIndexFromTableRow(table, idx.Info.Unique, idx.Info.Name.String(), i+1, col.Column.String(), false)
				indexRows = append(indexRows, row)
				if idx.Info.Primary {
					pkColumns[col.Column.String()] = true
				}
			}
		}

		schemaQueries["show index from "+table] = &sqltypes.Result{
			Fields:       mysql.ShowIndexFromTableFields,
			RowsAffected: uint64(len(indexRows)),
			Rows:         indexRows,
		}

		describeTableRows := make([][]sqltypes.Value, 0, 4)
		rowTypes := make([]*querypb.Field, 0, 4)
		tableColumns[table] = make(map[string]querypb.Type)

		for _, col := range ddl.TableSpec.Columns {
			colName := strings.ToLower(col.Name.String())
			defaultVal := ""
			if col.Type.Default != nil {
				defaultVal = sqlparser.String(col.Type.Default)
			}
			idxVal := ""
			if pkColumns[colName] {
				idxVal = "PRI"
			}
			row := mysql.DescribeTableRow(colName, col.Type.DescribeType(), !bool(col.Type.NotNull), idxVal, defaultVal)
			describeTableRows = append(describeTableRows, row)

			rowType := &querypb.Field{
				Name: colName,
				Type: col.Type.SQLType(),
			}
			rowTypes = append(rowTypes, rowType)

			tableColumns[table][colName] = col.Type.SQLType()
		}

		schemaQueries["describe "+table] = &sqltypes.Result{
			Fields:       mysql.DescribeTableFields,
			RowsAffected: uint64(len(describeTableRows)),
			Rows:         describeTableRows,
		}

		schemaQueries["select * from "+table+" where 1 != 1"] = &sqltypes.Result{
			Fields: rowTypes,
		}
	}

	return nil
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
	result, ok := schemaQueries[query]
	if ok {
		return callback(result)
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

		selStmt := stmt.(*sqlparser.Select)

		if len(selStmt.From) != 1 {
			return fmt.Errorf("unsupported select with multiple from clauses")
		}

		var table sqlparser.TableIdent
		switch node := selStmt.From[0].(type) {
		case *sqlparser.AliasedTableExpr:
			table = sqlparser.GetTableName(node.Expr)
		}

		// For complex select queries just return an empty result
		// since it's too hard to figure out the real columns
		if table.IsEmpty() {
			log.V(100).Infof("query %s result {}\n", query)
			return callback(&sqltypes.Result{})
		}

		tableName := sqlparser.String(table)
		colTypeMap := tableColumns[tableName]
		if colTypeMap == nil && tableName != "dual" {
			return fmt.Errorf("unable to resolve table name %s", tableName)
		}

		colNames := make([]string, 0, 4)
		colTypes := make([]querypb.Type, 0, 4)
		for _, node := range selStmt.SelectExprs {
			switch node := node.(type) {
			case *sqlparser.AliasedExpr:
				colNames, colTypes = inferColTypeFromExpr(node.Expr, colTypeMap, colNames, colTypes)
			case *sqlparser.StarExpr:
				for col, colType := range colTypeMap {
					colNames = append(colNames, col)
					colTypes = append(colTypes, colType)
				}
			}
		}

		fields := make([]*querypb.Field, len(colNames))
		values := make([]sqltypes.Value, len(colNames))
		for i, col := range colNames {
			colType := colTypes[i]
			fields[i] = &querypb.Field{
				Name: col,
				Type: colType,
			}

			// Generate a fake value for the given column. For numeric types,
			// use the column index. For all other types, just shortcut to using
			// a string type that encodes the column name + index.
			if sqltypes.IsIntegral(colType) {
				values[i] = sqltypes.NewInt32(int32(i + 1))
			} else if sqltypes.IsFloat(colType) {
				values[i] = sqltypes.NewFloat64(1.0 + float64(i))
			} else {
				values[i] = sqltypes.NewVarChar(fmt.Sprintf("%s_val_%d", col, i+1))
			}
		}
		result = &sqltypes.Result{
			Fields:       fields,
			RowsAffected: 1,
			InsertID:     0,
			Rows:         [][]sqltypes.Value{values},
		}

		resultJSON, _ := json.MarshalIndent(result, "", "    ")
		log.V(100).Infof("query %s result %s\n", query, string(resultJSON))

	case sqlparser.StmtBegin, sqlparser.StmtCommit, sqlparser.StmtSet, sqlparser.StmtShow:
		result = &sqltypes.Result{}
	case sqlparser.StmtInsert, sqlparser.StmtReplace, sqlparser.StmtUpdate, sqlparser.StmtDelete:
		result = &sqltypes.Result{
			RowsAffected: 1,
		}
	default:
		return fmt.Errorf("unsupported query %s", query)
	}

	return callback(result)
}

func inferColTypeFromExpr(node sqlparser.Expr, colTypeMap map[string]querypb.Type, colNames []string, colTypes []querypb.Type) ([]string, []querypb.Type) {
	switch node := node.(type) {
	case *sqlparser.ColName:
		col := strings.ToLower(node.Name.String())
		colType := colTypeMap[col]
		if colType == querypb.Type_NULL_TYPE {
			log.Errorf("vtexplain: invalid column %s, typeMap +%v", col, colTypeMap)
		}
		colNames = append(colNames, col)
		colTypes = append(colTypes, colType)
	case *sqlparser.FuncExpr:
		// As a shortcut, functions are integral types
		colNames = append(colNames, sqlparser.String(node))
		colTypes = append(colTypes, querypb.Type_INT32)
	case *sqlparser.SQLVal:
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
		default:
			log.Errorf("vtexplain: unsupported sql value %s", sqlparser.String(node))
		}
	case *sqlparser.ParenExpr:
		colNames, colTypes = inferColTypeFromExpr(node.Expr, colTypeMap, colNames, colTypes)
	case *sqlparser.CaseExpr:
		colNames, colTypes = inferColTypeFromExpr(node.Whens[0].Val, colTypeMap, colNames, colTypes)
	default:
		log.Errorf("vtexplain: unsupported select expression type +%v node %s", reflect.TypeOf(node), sqlparser.String(node))
	}

	return colNames, colTypes
}
