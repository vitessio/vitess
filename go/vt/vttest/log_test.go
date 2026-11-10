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

package vttest

import (
	"log/slog"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/log/logtest"
)

// fakeMySQL is a MySQLManager that runs nothing.
type fakeMySQL struct {
	tabletDir string
}

func (m *fakeMySQL) Setup() error           { return nil }
func (m *fakeMySQL) Start() error           { return nil }
func (m *fakeMySQL) TearDown() error        { return nil }
func (m *fakeMySQL) Auth() (string, string) { return "vt_dba", "params-secret" }
func (m *fakeMySQL) Address() (string, int) { return "", 0 }
func (m *fakeMySQL) UnixSocket() string     { return "" }
func (m *fakeMySQL) TabletDir() string      { return m.tabletDir }
func (m *fakeMySQL) Params(dbname string) mysql.ConnParams {
	return mysql.ConnParams{Uname: "vt_dba", Pass: "params-secret", DbName: dbname}
}

type fakeEnv struct {
	*LocalTestEnv
	mysql *fakeMySQL
}

func (e *fakeEnv) MySQLManager([]string, string) (MySQLManager, error) { return e.mysql, nil }

// TestStartupLogsKeepCredentialsOut checks that the MySQL password, the
// vtcombo password flags and the inherited environment stay out of the startup
// lines, and that vtcombo still runs with the real passwords.
func TestStartupLogsKeepCredentialsOut(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)

	mysqld := &fakeMySQL{tabletDir: t.TempDir()}
	db := &LocalCluster{Env: &fakeEnv{LocalTestEnv: &LocalTestEnv{}, mysql: mysqld}}
	db.PersistentMode = true
	db.OnlyMySQL = true

	require.NoError(t, db.Setup())
	record := logs.Record(t, "MySQL up")
	assert.Contains(t, record["mysql_params"], `"Uname":"vt_dba","Pass":"****"`)

	t.Setenv("VTTEST_LOG_SECRET", "env-secret")
	ctl := &Mysqlctl{
		Binary: path.Join(t.TempDir(), "no-such-mysqlctl"),
		MyCnf:  []string{"a.cnf", "b.cnf"},
		Env:    []string{"VTDATAROOT=/vtdataroot"},
	}
	require.Error(t, ctl.Start())
	record = logs.Record(t, "Starting MySQL using environment")
	assert.Equal(t, []any{"VTDATAROOT=/vtdataroot", "EXTRA_MY_CNF=a.cnf:b.cnf"}, record["env"])

	vtp := &VtProcess{
		Binary:      path.Join(t.TempDir(), "no-such-vtcombo"),
		Port:        1,
		BindAddress: "localhost",
		ExtraArgs:   []string{"--db-app-user", "vt_app", "--db-app-password", "args-secret", "--db-dba-password=args-secret-password", "--db-dba-user", "vt_dba", "--db_repl_password", "args-secret", "--db-allprivs-password=args-secret", "--db-filtered-password=", "--grpc-auth-static-password-file", "/etc/auth.json"},
	}
	require.Error(t, vtp.WaitStart())
	record = logs.Record(t, "Starting Vitess process")
	assert.Equal(t, vtp.Binary+" --port 1 --bind-address localhost --db-app-user vt_app --db-app-password **** --db-dba-password=**** --db-dba-user vt_dba --db_repl_password **** --db-allprivs-password=**** --db-filtered-password= --grpc-auth-static-password-file /etc/auth.json", record["exec_cmd"])
	assert.Equal(t, append([]string{vtp.Binary, "--port", "1", "--bind-address", "localhost"}, vtp.ExtraArgs...), vtp.proc.Args)

	for _, secret := range []string{"params-secret", "env-secret", "args-secret"} {
		assert.NotContains(t, logs.String(), secret)
	}
}
