/*
Copyright 2026 The Vitess Authors.

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

// Package columnnames checks that vtgate returns the same result-set column
// names as MySQL, for every case in the column-name corpus
// (go/vt/sqlparser/testdata/column_names.json).
package columnnames

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/sqlparser"
)

const (
	corpusFile = "../../../../../vt/sqlparser/testdata/column_names.json"
	schemaFile = "../../../../../vt/sqlparser/testdata/column_names_schema.sql"

	// corpusDB is the database name the corpus queries use.
	corpusDB = "colnames"

	shardedKs   = "colnames_sharded"
	unshardedKs = "colnames_unsharded"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams

	// mysqlVersionKey selects the expected names in the corpus: "mysql84" or
	// "mysql80", depending on the MySQL version the tablets run.
	mysqlVersionKey string

	updateKnown = flag.Bool("update-known", false, "rewrite known_divergences.txt with the divergences found")
	showKnown   = flag.Bool("show-known", false, "log what MySQL and vtgate return for the known divergences")
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		ddl, dml, vschema, uvschema, err := loadSchema()
		if err != nil {
			fmt.Println(err)
			return 1
		}

		clusterInstance = cluster.NewCluster("zone1", "localhost")
		defer clusterInstance.Teardown()

		if err := clusterInstance.StartTopo(); err != nil {
			fmt.Println(err)
			return 1
		}

		unsharded := &cluster.Keyspace{Name: unshardedKs, SchemaSQL: ddl, VSchema: uvschema}
		if err := clusterInstance.StartUnshardedKeyspace(*unsharded, 0, false, clusterInstance.Cell); err != nil {
			fmt.Println(err)
			return 1
		}
		sharded := &cluster.Keyspace{Name: shardedKs, SchemaSQL: ddl, VSchema: vschema}
		if err := clusterInstance.StartKeyspace(*sharded, []string{"-80", "80-"}, 0, false, clusterInstance.Cell); err != nil {
			fmt.Println(err)
			return 1
		}
		if err := clusterInstance.StartVtgate(); err != nil {
			fmt.Println(err)
			return 1
		}
		vtParams = clusterInstance.GetVTParams(shardedKs)

		if err := loadData(dml); err != nil {
			fmt.Println(err)
			return 1
		}
		mysqlVersionKey, err = tabletMySQLVersionKey()
		if err != nil {
			fmt.Println(err)
			return 1
		}
		return m.Run()
	}()
	os.Exit(exitCode)
}

// loadSchema splits the corpus schema into DDL and INSERT statements, and
// builds the vschemas. The sharded vschema has a hash vindex on the first
// column of every table; views get a hash vindex on "id", which only affects
// routing. The unsharded vschema lists the same tables, so that unqualified
// table names resolve to the keyspace the connection uses.
func loadSchema() (ddl string, dml []string, vschema, uvschema string, err error) {
	data, err := os.ReadFile(schemaFile)
	if err != nil {
		return "", nil, "", "", err
	}
	parser := sqlparser.NewTestParser()
	pieces, err := parser.SplitStatementToPieces(string(data))
	if err != nil {
		return "", nil, "", "", err
	}
	type columnVindex struct {
		Column string `json:"column"`
		Name   string `json:"name"`
	}
	type table struct {
		ColumnVindexes []columnVindex `json:"column_vindexes"`
	}
	tables := map[string]table{}
	var ddlStmts []string
	for _, piece := range pieces {
		stmt, err := parser.Parse(piece)
		if err != nil {
			return "", nil, "", "", fmt.Errorf("parsing %q: %w", piece, err)
		}
		switch stmt := stmt.(type) {
		case *sqlparser.DropDatabase, *sqlparser.CreateDatabase, *sqlparser.Use:
			continue
		case *sqlparser.Insert:
			dml = append(dml, piece)
			continue
		case *sqlparser.CreateTable:
			column := stmt.TableSpec.Columns[0].Name.String()
			tables[stmt.Table.Name.String()] = table{ColumnVindexes: []columnVindex{{Column: column, Name: "hash"}}}
		case *sqlparser.CreateView:
			tables[stmt.ViewName.Name.String()] = table{ColumnVindexes: []columnVindex{{Column: "id", Name: "hash"}}}
		}
		ddlStmts = append(ddlStmts, piece)
	}
	vs, err := json.Marshal(map[string]any{
		"sharded":  true,
		"vindexes": map[string]any{"hash": map[string]string{"type": "hash"}},
		"tables":   tables,
	})
	if err != nil {
		return "", nil, "", "", err
	}
	utables := map[string]any{}
	for name := range tables {
		utables[name] = map[string]any{}
	}
	uvs, err := json.Marshal(map[string]any{"tables": utables})
	if err != nil {
		return "", nil, "", "", err
	}
	return strings.Join(ddlStmts, ";\n") + ";\n", dml, string(vs), string(uvs), nil
}

func loadData(dml []string) error {
	for _, ks := range []string{shardedKs, unshardedKs} {
		params := clusterInstance.GetVTParams(ks)
		params.DbName = ks
		conn, err := mysql.Connect(context.Background(), &params)
		if err != nil {
			return err
		}
		for _, stmt := range dml {
			if _, err := conn.ExecuteFetch(stmt, 0, false); err != nil {
				conn.Close()
				return fmt.Errorf("%s: %q: %w", ks, stmt, err)
			}
		}
		conn.Close()
	}
	return nil
}

func tabletMySQLVersionKey() (string, error) {
	tablet := clusterInstance.Keyspaces[0].Shards[0].Vttablets[0]
	qr, err := tablet.VttabletProcess.QueryTablet("select @@global.version", tablet.VttabletProcess.Keyspace, false)
	if err != nil {
		return "", err
	}
	version := qr.Rows[0][0].ToString()
	if strings.HasPrefix(version, "8.0.") {
		return "mysql80", nil
	}
	return "mysql84", nil
}
