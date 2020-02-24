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
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"

	"vitess.io/vitess/go/test/endtoend/cluster"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// tableData is a temporary structure to hold selected data.
type tableData struct {
	Msg     string
	Data    string
	TextCol string
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
	dbInfo.KeyspaceName = keyspaceName
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
	keyspaceName          = "test_keyspace"
	testingID             = 1
	tableName             = "vt_prepare_stmt_test"
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
	sqlSchema = `create table ` + tableName + ` (
		id bigint auto_increment,
		msg varchar(64),
		keyspace_id bigint(20) unsigned NOT NULL,
		tinyint_unsigned TINYINT,
		bool_signed BOOL,
		smallint_unsigned SMALLINT,
		mediumint_unsigned MEDIUMINT,
		int_unsigned INT,
		float_unsigned FLOAT(10,2),
		double_unsigned DOUBLE(16,2),
		decimal_unsigned DECIMAL,
		t_date DATE,
		t_datetime DATETIME,
		t_time TIME,
		t_timestamp TIMESTAMP,
		c8 bit(8) DEFAULT NULL,
		c16 bit(16) DEFAULT NULL,
		c24 bit(24) DEFAULT NULL,
		c32 bit(32) DEFAULT NULL,
		c40 bit(40) DEFAULT NULL,
		c48 bit(48) DEFAULT NULL,
		c56 bit(56) DEFAULT NULL,
		c63 bit(63) DEFAULT NULL,
		c64 bit(64) DEFAULT NULL,
		json_col JSON,
		text_col TEXT,
		data longblob,
		primary key (id)
		) Engine=InnoDB`
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

		// add extra arguments
		clusterInstance.VtGateExtraArgs = []string{
			"-mysql_auth_server_impl", "static",
			"-mysql_server_query_timeout", "1s",
			"-mysql_auth_server_static_file", clusterInstance.TmpDirectory + "/" + mysqlAuthServerStatic,
			"-mysql_server_version", "8.0.16-7",
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
		}
		if err := clusterInstance.StartUnshardedKeyspace(*keyspace, 1, false); err != nil {
			return 1, err
		}

		// Start vtgate
		if err := clusterInstance.StartVtgate(); err != nil {
			return 1, err
		}

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
func Connect(t *testing.T, params ...string) *sql.DB {
	dbo, err := sql.Open("mysql", dbInfo.ConnectionString(params...))
	require.Nil(t, err)
	return dbo
}

// execWithError executes the prepared query, and validates the error_code.
func execWithError(t *testing.T, dbo *sql.DB, errorCodes []uint16, stmt string, params ...interface{}) {
	_, err := dbo.Exec(stmt, params...)
	require.NotNilf(t, err, "error expected, got nil")
	mysqlErr, ok := err.(*mysql.MySQLError)
	require.Truef(t, ok, "invalid error type")
	require.Contains(t, errorCodes, mysqlErr.Number)
}

// exec executes the query using the params.
func exec(t *testing.T, dbo *sql.DB, stmt string, params ...interface{}) {
	require.Nil(t, execErr(dbo, stmt, params...))
}

// execErr executes the query and returns an error if one occurs.
func execErr(dbo *sql.DB, stmt string, params ...interface{}) *mysql.MySQLError {
	if _, err := dbo.Exec(stmt, params...); err != nil {
		// TODO : need to handle
		mysqlErr, _ := err.(*mysql.MySQLError)
		return mysqlErr
	}
	return nil
}

// selectWhere select the row corresponding to the where condition.
func selectWhere(t *testing.T, dbo *sql.DB, where string, params ...interface{}) []tableData {
	var out []tableData
	// prepare query
	qry := "SELECT msg, data, text_col FROM " + tableName
	if where != "" {
		qry += " WHERE (" + where + ")"
	}

	// execute query
	r, err := dbo.Query(qry, params...)
	require.Nil(t, err)

	// prepare result
	for r.Next() {
		var t tableData
		r.Scan(&t.Msg, &t.Data, &t.TextCol)
		out = append(out, t)
	}
	return out
}
