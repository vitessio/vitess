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

package wordpress

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/test/endtoend/apps"
)

func TestInstallation(t *testing.T) {
	t.Skip("not successful yet. run manually until this test reliably passes")
	queryLog, err := apps.ReadLogFile("wordpres_install_querylog.txt")
	require.NoError(t, err)

	mySQLDb, err := sql.Open("mysql", fmt.Sprintf("root@unix(%s)/", socketFile))
	require.NoError(t, err)

	vitessDb, err := sql.Open("mysql", fmt.Sprintf("root@tcp(127.0.0.1:%d)/", vtParams.Port))
	require.NoError(t, err)

	sessions := make(map[int]*QueryRunner)
	for i, line := range queryLog {
		t.Run(fmt.Sprintf("%d %s", i, line.Text), func(t *testing.T) {
			runner, found := sessions[line.ConnectionID]
			if !found {
				runner = &QueryRunner{
					mysqlF: func() (*sql.Conn, error) {
						return mySQLDb.Conn(ctx)
					},
					vitessF: func() (*sql.Conn, error) {
						return vitessDb.Conn(ctx)
					},
				}
				sessions[line.ConnectionID] = runner
			}
			switch line.Typ {
			case apps.Connect:
				runner.Reconnect()
			case apps.InitDb:
				runner.Query(t, "use "+line.Text)
			case apps.Query:
				runner.Query(t, line.Text)
			case apps.Quit:
				runner.mysql.Close()
				runner.vitess.Close()
				delete(sessions, line.ConnectionID)
			}
		})
	}
}

type conCreator func() (*sql.Conn, error)

type QueryRunner struct {
	mysqlF, vitessF conCreator
	mysql, vitess   *sql.Conn
}

func (qr *QueryRunner) Reconnect() error {
	if qr.mysql != nil {
		qr.mysql.Close()
	}
	if qr.vitess != nil {
		qr.vitess.Close()
	}
	db, err := qr.vitessF()
	if err != nil {
		return err
	}
	qr.vitess = db

	db, err = qr.mysqlF()
	if err != nil {
		return err
	}
	qr.mysql = db

	return nil
}

var ctx = context.Background()

func (qr *QueryRunner) Query(t *testing.T, q string) {
	resultM, errM := qr.mysql.QueryContext(ctx, q)
	resultV, errV := qr.vitess.QueryContext(ctx, q)
	if errM == nil {
		defer resultM.Close()
	}
	if errV == nil {
		defer resultV.Close()
	}

	checkErrors := func(mysql, vitess error) {
		if mysql == nil && vitess != nil {
			t.Errorf("Vitess returned an error but mysql did not. Query: %s\nError: %s", q, errV.Error())
		}
		if mysql != nil && vitess == nil {
			t.Errorf("Mysql returned an error but Vitess did not. Query: %s\nError: %s", q, errM.Error())
		}
	}

	checkErrors(errM, errV)

	if errV == nil && errM == nil {
		_, errM := resultM.Columns()
		_, errV := resultV.Columns()
		checkErrors(errM, errV)

		// TODO check that the column names are equal
		//if diff := cmp.Diff(mysqlColumns, vitessColumns); diff != "" {
		//	t.Error(diff)
		//}

		m := count(resultM)
		v := count(resultV)
		if m != v {
			t.Errorf("Query worked against both, but returned different number of rows. Query:%s\nmysql: %d vitess: %d", q, m, v)
		}
	}
}

func count(in *sql.Rows) int {
	i := 0
	for in.Next() {
		i++
	}
	return i
}
