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
	"golang.org/x/net/context"

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
	schemaQueries map[string]*sqltypes.Result
)

func startFakeTablet() (*fakesqldb.DB, *tabletserver.TabletServer) {
	db := newFakeDB()

	// XXX much of this is cloned from the tabletserver tests
	config := tabletenv.DefaultQsConfig
	config.EnableAutoCommit = true
	tsv := tabletserver.NewTabletServerWithNilTopoServer(config)

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

	return db, tsv
}

func fakeTabletExecute(sql string, bindVars map[string]*querypb.BindVariable) ([]string, error) {
	db, tsv := startFakeTablet()
	defer db.Close()
	defer tsv.StopService()

	ctx := context.Background()
	logStats := tabletenv.NewLogStats(ctx, "FakeQueryExecutor")
	plan, err := tsv.GetPlan(ctx, logStats, sql)
	if err != nil {
		return nil, err
	}
	txID := int64(0)
	qre := tabletserver.NewQueryExecutor(ctx, sql, bindVars, txID, nil, plan, logStats, tsv)

	queries := make([]string, 0, 4)

	db.QueryLogger = func(query string, result *sqltypes.Result, err error) {
		queries = append(queries, query)
	}

	_, err = qre.Execute()
	if err != nil {
		return nil, err
	}

	return queries, nil
}

func initTabletEnvironment(ddls []*sqlparser.DDL, opts *Options) error {
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

// Set up the fakesqldb with queries needed to resolve the schema and accept
// all other queries
func newFakeDB() *fakesqldb.DB {
	// XXX passing nil for testing.t?
	db := fakesqldb.New(nil)

	for q, r := range schemaQueries {
		//		log.Infof("adding query %s %v", q, r)
		db.AddQuery(q, r)
	}

	db.AddQueryPattern(".*", &sqltypes.Result{})

	return db
}
