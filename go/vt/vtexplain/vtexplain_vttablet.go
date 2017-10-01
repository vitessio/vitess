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
	"strings"

	"golang.org/x/net/context"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/mysql/fakesqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/sqlparser"

	"github.com/youtube/vitess/go/vt/vttablet/queryservice"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

var (
	// map of schema introspection queries to their expected results
	schemaQueries map[string]*sqltypes.Result

	// map for each table from the column name to its type
	tableColumns map[string]map[string]querypb.Type

	// time simulator
	batchTime *sync2.Batcher
)

type fakeTablet struct {
	db            *fakesqldb.DB
	tsv           *tabletserver.TabletServer
	tabletQueries []*TabletQuery
	mysqlQueries  []*MysqlQuery
	currentTime   int32
}

func newFakeTablet(t *topodatapb.Tablet) *fakeTablet {
	db := fakesqldb.New(nil)

	// XXX much of this is cloned from the tabletserver tests
	tsv := tabletserver.NewTabletServerWithNilTopoServer(tabletenv.DefaultQsConfig)

	tablet := fakeTablet{db: db, tsv: tsv}
	db.Handler = &tablet

	dbcfgs := dbconfigs.DBConfigs{
		App:           *db.ConnParams(),
		SidecarDBName: "_vt",
	}
	cnf := mysqlctl.NewMycnf(22222, 6802)
	cnf.ServerID = 33333
	mysqld := mysqlctl.NewMysqld(
		cnf,
		&dbcfgs,
		dbconfigs.AppConfig, // These tests only use the app pool.
	)

	target := querypb.Target{
		Keyspace:   t.Keyspace,
		Shard:      t.Shard,
		TabletType: topodatapb.TabletType_MASTER,
	}
	tsv.StartService(target, dbcfgs, mysqld)

	// clear all the schema initialization queries out of the tablet
	// to avoid clutttering the output
	tablet.mysqlQueries = nil

	return &tablet
}

var _ queryservice.QueryService = (*fakeTablet)(nil) // compile-time interface check

// Begin is part of the QueryService interface.
func (tablet *fakeTablet) Begin(ctx context.Context, target *querypb.Target, options *querypb.ExecuteOptions) (int64, error) {
	tablet.currentTime = batchTime.Wait()
	return tablet.tsv.Begin(ctx, target, options)
}

// Commit is part of the QueryService interface.
func (tablet *fakeTablet) Commit(ctx context.Context, target *querypb.Target, transactionID int64) error {
	tablet.currentTime = batchTime.Wait()
	return tablet.tsv.Commit(ctx, target, transactionID)
}

// Rollback is part of the QueryService interface.
func (tablet *fakeTablet) Rollback(ctx context.Context, target *querypb.Target, transactionID int64) error {
	tablet.currentTime = batchTime.Wait()
	return tablet.tsv.Rollback(ctx, target, transactionID)
}

// Prepare is part of the QueryService interface.
func (tablet *fakeTablet) Prepare(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	panic("not implemented")
}

// CommitPrepared is part of the QueryService interface.
func (tablet *fakeTablet) CommitPrepared(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	panic("not implemented")
}

// RollbackPrepared is part of the QueryService interface.
func (tablet *fakeTablet) RollbackPrepared(ctx context.Context, target *querypb.Target, dtid string, originalID int64) (err error) {
	panic("not implemented")
}

// CreateTransaction is part of the QueryService interface.
func (tablet *fakeTablet) CreateTransaction(ctx context.Context, target *querypb.Target, dtid string, participants []*querypb.Target) (err error) {
	panic("not implemented")
}

// StartCommit is part of the QueryService interface.
func (tablet *fakeTablet) StartCommit(ctx context.Context, target *querypb.Target, transactionID int64, dtid string) (err error) {
	panic("not implemented")
}

// SetRollback is part of the QueryService interface.
func (tablet *fakeTablet) SetRollback(ctx context.Context, target *querypb.Target, dtid string, transactionID int64) (err error) {
	panic("not implemented")
}

// ConcludeTransaction is part of the QueryService interface.
func (tablet *fakeTablet) ConcludeTransaction(ctx context.Context, target *querypb.Target, dtid string) (err error) {
	panic("not implemented")
}

// ReadTransaction is part of the QueryService interface.
func (tablet *fakeTablet) ReadTransaction(ctx context.Context, target *querypb.Target, dtid string) (metadata *querypb.TransactionMetadata, err error) {
	panic("not implemented")
}

// Execute is part of the QueryService interface.
func (tablet *fakeTablet) Execute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {

	tablet.currentTime = batchTime.Wait()

	// Since the query is simulated being "sent" over the wire we need to
	// copy the bindVars into the executor to avoid a data race.
	bindVariables = sqltypes.CopyBindVariables(bindVariables)
	tablet.tabletQueries = append(tablet.tabletQueries, &TabletQuery{
		Time:     tablet.currentTime,
		SQL:      sql,
		BindVars: bindVariables,
	})
	return tablet.tsv.Execute(ctx, target, sql, bindVariables, transactionID, options)
}

// StreamExecute is part of the QueryService interface.
func (tablet *fakeTablet) StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions, callback func(*sqltypes.Result) error) error {
	panic("not implemented")
}

// ExecuteBatch is part of the QueryService interface.
func (tablet *fakeTablet) ExecuteBatch(ctx context.Context, target *querypb.Target, queries []*querypb.BoundQuery, asTransaction bool, transactionID int64, options *querypb.ExecuteOptions) ([]sqltypes.Result, error) {
	panic("not implemented")
}

// BeginExecute is part of the QueryService interface.
func (tablet *fakeTablet) BeginExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, options *querypb.ExecuteOptions) (*sqltypes.Result, int64, error) {
	tablet.currentTime = batchTime.Wait()
	bindVariables = sqltypes.CopyBindVariables(bindVariables)
	tablet.tabletQueries = append(tablet.tabletQueries, &TabletQuery{
		Time:     tablet.currentTime,
		SQL:      sql,
		BindVars: bindVariables,
	})
	return tablet.tsv.BeginExecute(ctx, target, sql, bindVariables, options)
}

// BeginExecuteBatch is part of the QueryService interface.
func (tablet *fakeTablet) BeginExecuteBatch(ctx context.Context, target *querypb.Target, queries []*querypb.BoundQuery, asTransaction bool, options *querypb.ExecuteOptions) ([]sqltypes.Result, int64, error) {
	panic("not implemented")
}

// MessageStream is part of the QueryService interface.
func (tablet *fakeTablet) MessageStream(ctx context.Context, target *querypb.Target, name string, callback func(*sqltypes.Result) error) error {
	panic("not implemented")
}

// MessageAck is part of the QueryService interface.
func (tablet *fakeTablet) MessageAck(ctx context.Context, target *querypb.Target, name string, ids []*querypb.Value) (count int64, err error) {
	panic("not implemented")
}

// SplitQuery is part of the QueryService interface.
func (tablet *fakeTablet) SplitQuery(ctx context.Context, target *querypb.Target, query *querypb.BoundQuery, splitColumns []string, splitCount int64, numRowsPerQueryPart int64, algorithm querypb.SplitQueryRequest_Algorithm) ([]*querypb.QuerySplit, error) {
	panic("not implemented")
}

// UpdateStream is part of the QueryService interface.
func (tablet *fakeTablet) UpdateStream(ctx context.Context, target *querypb.Target, position string, timestamp int64, callback func(*querypb.StreamEvent) error) error {
	panic("not implemented")
}

// StreamHealth is part of the QueryService interface.
func (tablet *fakeTablet) StreamHealth(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error {
	panic("not implemented")
}

// HandlePanic is part of the QueryService interface.
func (tablet *fakeTablet) HandlePanic(err *error) {
	panic("not implemented")
}

// Close is part of the QueryService interface.
func (tablet *fakeTablet) Close(ctx context.Context) error {
	return tablet.tsv.Close(ctx)
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
	}

	showTableRows := make([][]sqltypes.Value, 0, 4)
	for _, ddl := range ddls {
		table := ddl.NewName.Name.String()
		showTableRows = append(showTableRows, mysql.BaseShowTablesRow(table, false, ""))
	}
	schemaQueries[mysql.BaseShowTables] = &sqltypes.Result{
		Fields:       mysql.BaseShowTablesFields,
		RowsAffected: uint64(len(showTableRows)),
		Rows:         showTableRows,
	}

	for i, ddl := range ddls {
		table := ddl.NewName.Name.String()
		schemaQueries[mysql.BaseShowTablesForTable(table)] = &sqltypes.Result{
			Fields:       mysql.BaseShowTablesFields,
			RowsAffected: 1,
			Rows:         [][]sqltypes.Value{showTableRows[i]},
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
			colName := col.Name.String()
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
func (tablet *fakeTablet) HandleQuery(c *mysql.Conn, q []byte, callback func(*sqltypes.Result) error) error {
	query := string(q)
	if !strings.Contains(query, "1 != 1") {
		tablet.mysqlQueries = append(tablet.mysqlQueries, &MysqlQuery{
			Time: tablet.currentTime,
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
			break
		}

		// For complex select queries just return an empty result
		// since it's too hard to figure out the real columns
		if table.IsEmpty() {
			log.V(100).Infof("query %s result {}\n", query)
			return callback(&sqltypes.Result{})
		}

		colTypeMap := tableColumns[table.String()]
		if colTypeMap == nil {
			return fmt.Errorf("unable to resolve table name %s", table.String())
		}

		colNames := make([]string, 0, 4)
		colTypes := make([]querypb.Type, 0, 4)
		for _, node := range selStmt.SelectExprs {
			switch node := node.(type) {
			case *sqlparser.AliasedExpr:
				switch node := node.Expr.(type) {
				case *sqlparser.ColName:
					col := node.Name.String()
					colType := colTypeMap[col]
					if colType == querypb.Type_NULL_TYPE {
						return fmt.Errorf("invalid column %s", col)
					}
					colNames = append(colNames, col)
					colTypes = append(colTypes, colType)
					break
				case *sqlparser.FuncExpr:
					// As a shortcut, functions are integral types
					colNames = append(colNames, sqlparser.String(node))
					colTypes = append(colTypes, querypb.Type_INT32)
					break
				case *sqlparser.SQLVal:
					colNames = append(colNames, sqlparser.String(node))
					switch node.Type {
					case sqlparser.IntVal:
						colTypes = append(colTypes, querypb.Type_INT32)
						break
					case sqlparser.StrVal:
						colTypes = append(colTypes, querypb.Type_VARCHAR)
						break
					case sqlparser.FloatVal:
						colTypes = append(colTypes, querypb.Type_VARCHAR)
						break
					default:
						return fmt.Errorf("unsupported sql value %s", sqlparser.String(node))
					}
					break
				default:
					return fmt.Errorf("unsupported select expression %s", sqlparser.String(node))
				}
				break
			case *sqlparser.StarExpr:
				for col, colType := range colTypeMap {
					colNames = append(colNames, col)
					colTypes = append(colTypes, colType)
				}
			}
		}

		// Generate a fake value for the given column. For numeric types,
		// use the column index. For strings, use the column name + index.
		fields := make([]*querypb.Field, len(colNames))
		values := make([]sqltypes.Value, len(colNames))
		for i, col := range colNames {
			colType := colTypes[i]
			fields[i] = &querypb.Field{
				Name: col,
				Type: colType,
			}

			if sqltypes.IsIntegral(colType) {
				values[i] = sqltypes.NewInt32(int32(i + 1))
			} else if sqltypes.IsFloat(colType) {
				values[i] = sqltypes.NewFloat64(1.0 + float64(i))
			} else if sqltypes.IsBinary(colType) || sqltypes.IsText(colType) {
				values[i] = sqltypes.NewVarChar(fmt.Sprintf("%s_val_%d", col, i+1))
			} else {
				return fmt.Errorf("unhandled type %d for col %s", colType, col)
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

		break
	case sqlparser.StmtBegin, sqlparser.StmtCommit:
		result = &sqltypes.Result{}
		break
	case sqlparser.StmtInsert, sqlparser.StmtReplace, sqlparser.StmtUpdate, sqlparser.StmtDelete:
		result = &sqltypes.Result{
			RowsAffected: 1,
		}
		break
	default:
		return fmt.Errorf("unsupported query %s", query)
	}

	return callback(result)
}
