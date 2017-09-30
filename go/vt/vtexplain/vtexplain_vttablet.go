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

	"golang.org/x/net/context"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/mysql/fakesqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/sqlparser"

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
)

type fakeTablet struct {
	db      *fakesqldb.DB
	tsv     *tabletserver.TabletServer
	queries []string
}

func newFakeTablet() *fakeTablet {
	db := fakesqldb.New(nil)

	// XXX much of this is cloned from the tabletserver tests
	config := tabletenv.DefaultQsConfig
	config.EnableAutoCommit = true
	tsv := tabletserver.NewTabletServerWithNilTopoServer(config)

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

	target := querypb.Target{TabletType: topodatapb.TabletType_MASTER}
	tsv.StartService(target, dbcfgs, mysqld)

	// clear all the schema initialization queries out of the tablet
	// to avoid clutttering the output
	tablet.queries = nil

	return &tablet
}

// Execute hook called by SandboxConn as part of running the query.
func (tablet *fakeTablet) Execute(ctx context.Context, target *querypb.Target, query string, bindVars map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions) (*sqltypes.Result, error) {
	logStats := tabletenv.NewLogStats(ctx, "FakeQueryExecutor")

	plan, err := tablet.tsv.GetPlan(ctx, logStats, query)
	if err != nil {
		return nil, err
	}

	txID := int64(0)

	// Since the query is simulated being "sent" over the wire we need to
	// copy the bindVars into the executor to avoid a data race.
	qre := tabletserver.NewQueryExecutor(ctx, query, sqltypes.CopyBindVariables(bindVars), txID, nil, plan, logStats, tablet.tsv)
	return qre.Execute()
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
	tablet.queries = append(tablet.queries, query)

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
