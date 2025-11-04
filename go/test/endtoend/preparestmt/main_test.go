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

package preparestmt

import (
	"database/sql"
	_ "embed"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/utils"
)

// tableData is a temporary structure to hold selected data.
type tableData struct {
	Msg            string
	Data           string
	TextCol        string
	DateTime       time.Time
	DateTimeMicros time.Time
}

// DBInfo information about the database.
type DBInfo struct {
	Username     string
	Password     string
	Host         string
	Port         uint
	KeyspaceName string
	Params       []string
}

func init() {
	dbInfo.KeyspaceName = uks
	dbInfo.Username = "testuser1"
	dbInfo.Password = "testpassword1"
	dbInfo.Params = []string{
		"charset=utf8",
		"parseTime=True",
		"loc=Local",
	}
}

var (
	clusterInstance       *cluster.LocalProcessCluster
	dbInfo                DBInfo
	hostname              = "localhost"
	uks                   = "uks"
	sks                   = "sks"
	testingID             = 1
	cell                  = "zone1"
	mysqlAuthServerStatic = "mysql_auth_server_static.json"
	jsonExample           = `{
		"quiz": {
			"sport": {
				"q1": {
					"question": "Which one is correct team name in NBA?",
					"options": [
						"New York Bulls",
						"Los Angeles Kings",
						"Golden State Warriors",
						"Huston Rocket"
					],
					"answer": "Huston Rocket"
				}
			},
			"maths": {
				"q1": {
					"question": "5 + 7 = ?",
					"options": [
						"10",
						"11",
						"12",
						"13"
					],
					"answer": "12"
				},
				"q2": {
					"question": "12 - 8 = ?",
					"options": [
						"1",
						"2",
						"3",
						"4"
					],
					"answer": "4"
				}
			}
		}
	}`

	//go:embed uSchema.sql
	uSQLSchema string

	//go:embed uVschema.json
	uVschema string

	//go:embed sSchema.sql
	sSQLSchema string

	//go:embed sVschema.json
	sVschema string
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitcode, err := func() (int, error) {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1, err
		}

		// create auth server config
		SQLConfig := `{
			"testuser1": {
				"Password": "testpassword1",
				"UserData": "vtgate client 1"
			}
		}`
		if err := createConfig(mysqlAuthServerStatic, SQLConfig); err != nil {
			return 1, err
		}

		// Start keyspace
		ks := cluster.Keyspace{Name: uks, SchemaSQL: uSQLSchema, VSchema: uVschema}
		if err := clusterInstance.StartUnshardedKeyspace(ks, 1, false); err != nil {
			return 1, err
		}

		ks = cluster.Keyspace{Name: sks, SchemaSQL: sSQLSchema, VSchema: sVschema}
		if err := clusterInstance.StartKeyspace(ks, []string{"-"}, 0, false); err != nil {
			return 1, err
		}

		vtgateInstance := clusterInstance.NewVtgateInstance()
		vtgateInstance.MySQLAuthServerImpl = "static"
		// add extra arguments
		vtgateInstance.ExtraArgs = []string{
			utils.GetFlagVariantForTests("--mysql-server-query-timeout"), "1s",
			"--mysql-auth-server-static-file", clusterInstance.TmpDirectory + "/" + mysqlAuthServerStatic,
			"--pprof-http",
			utils.GetFlagVariantForTests("--schema-change-signal") + "=false",
		}

		// Start vtgate
		if err := vtgateInstance.Setup(); err != nil {
			return 1, err
		}
		// ensure it is torn down during cluster TearDown
		clusterInstance.VtgateProcess = *vtgateInstance

		dbInfo.Host = clusterInstance.Hostname
		dbInfo.Port = uint(clusterInstance.VtgateMySQLPort)

		return m.Run(), nil
	}()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}
}

// ConnectionString generates the connection string using dbinfo.
func (db DBInfo) ConnectionString(params ...string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s", db.Username, db.Password, db.Host,
		db.Port, db.KeyspaceName, strings.Join(append(db.Params, params...), "&"))
}

// createConfig creates a config file in TmpDir in vtdataroot and writes the given data.
func createConfig(name, data string) error {
	// creating new file
	f, err := os.Create(clusterInstance.TmpDirectory + "/" + name)
	if err != nil {
		return err
	}

	if data == "" {
		return nil
	}

	// write the given data
	_, err = fmt.Fprint(f, data)
	return err
}

// Connect will connect the vtgate through mysql protocol.
func Connect(t testing.TB, params ...string) *sql.DB {
	dbo, err := sql.Open("mysql", dbInfo.ConnectionString(params...))
	require.Nil(t, err)
	return dbo
}

// execWithError executes the prepared query, and validates the error_code.
func execWithError(t *testing.T, dbo *sql.DB, errorCodes []uint16, stmt string, params ...any) {
	_, err := dbo.Exec(stmt, params...)
	require.NotNilf(t, err, "error expected, got nil")
	mysqlErr, ok := err.(*mysql.MySQLError)
	require.Truef(t, ok, "invalid error type")
	require.Contains(t, errorCodes, mysqlErr.Number)
}

// exec executes the query using the params.
func exec(t *testing.T, dbo *sql.DB, stmt string, params ...any) {
	require.Nil(t, execErr(dbo, stmt, params...))
}

// execErr executes the query and returns an error if one occurs.
func execErr(dbo *sql.DB, stmt string, params ...any) *mysql.MySQLError {
	if _, err := dbo.Exec(stmt, params...); err != nil {
		// TODO : need to handle
		mysqlErr, _ := err.(*mysql.MySQLError)
		return mysqlErr
	}
	return nil
}

// selectWhere select the row corresponding to the where condition.
func selectWhere(t *testing.T, dbo *sql.DB, where string, params ...any) []tableData {
	var out []tableData
	// prepare query
	qry := "SELECT msg, data, text_col, t_datetime, t_datetime_micros FROM vt_prepare_stmt_test"
	if where != "" {
		qry += " WHERE (" + where + ")"
	}

	// execute query
	r, err := dbo.Query(qry, params...)
	require.Nil(t, err)

	// prepare result
	for r.Next() {
		var t tableData
		r.Scan(&t.Msg, &t.Data, &t.TextCol, &t.DateTime, &t.DateTimeMicros)
		out = append(out, t)
	}
	return out
}

// selectWhereWithTx select the row corresponding to the where condition.
func selectWhereWithTx(t *testing.T, tx *sql.Tx, where string, params ...any) []tableData {
	var out []tableData
	// prepare query
	qry := "SELECT msg, data, text_col, t_datetime, t_datetime_micros FROM vt_prepare_stmt_test"
	if where != "" {
		qry += " WHERE (" + where + ")"
	}

	// execute query
	r, err := tx.Query(qry, params...)
	require.Nil(t, err)

	// prepare result
	for r.Next() {
		var t tableData
		r.Scan(&t.Msg, &t.Data, &t.TextCol, &t.DateTime, &t.DateTimeMicros)
		out = append(out, t)
	}
	return out
}
