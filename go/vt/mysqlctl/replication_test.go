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

package mysqlctl

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/vt/dbconfigs"
)

func testRedacted(t *testing.T, source, expected string) {
	assert.Equal(t, expected, redactPassword(source))
}

func TestRedactMasterPassword(t *testing.T) {

	// regular test case
	testRedacted(t, `CHANGE MASTER TO
  MASTER_PASSWORD = 'AAA',
  MASTER_CONNECT_RETRY = 1
`,
		`CHANGE MASTER TO
  MASTER_PASSWORD = '****',
  MASTER_CONNECT_RETRY = 1
`)

	// empty password
	testRedacted(t, `CHANGE MASTER TO
  MASTER_PASSWORD = '',
  MASTER_CONNECT_RETRY = 1
`,
		`CHANGE MASTER TO
  MASTER_PASSWORD = '****',
  MASTER_CONNECT_RETRY = 1
`)

	// no beginning match
	testRedacted(t, "aaaaaaaaaaaaaa", "aaaaaaaaaaaaaa")

	// no end match
	testRedacted(t, `CHANGE MASTER TO
  MASTER_PASSWORD = 'AAA`, `CHANGE MASTER TO
  MASTER_PASSWORD = 'AAA`)
}

func TestRedactPassword(t *testing.T) {
	// regular case
	testRedacted(t, `START xxx USER = 'vt_repl', PASSWORD = 'AAA'`,
		`START xxx USER = 'vt_repl', PASSWORD = '****'`)

	// empty password
	testRedacted(t, `START xxx USER = 'vt_repl', PASSWORD = ''`,
		`START xxx USER = 'vt_repl', PASSWORD = '****'`)

	// no end match
	testRedacted(t, `START xxx USER = 'vt_repl', PASSWORD = 'AAA`,
		`START xxx USER = 'vt_repl', PASSWORD = 'AAA`)

	// both primary password and password
	testRedacted(t, `START xxx
  MASTER_PASSWORD = 'AAA',
  PASSWORD = 'BBB'
`,
		`START xxx
  MASTER_PASSWORD = '****',
  PASSWORD = '****'
`)
}

func TestWaitForReplicationStart(t *testing.T) {
	// TODO: Needs more tests
	db := fakesqldb.New(t)
	fakemysqld := NewFakeMysqlDaemon(db)

	defer func() {
		db.Close()
		fakemysqld.Close()
	}()

	err := WaitForReplicationStart(fakemysqld, 2)
	assert.NoError(t, err)
}

func TestStartReplication(t *testing.T) {
	db := fakesqldb.New(t)
	params := db.ConnParams()
	cp := *params
	dbc := dbconfigs.NewTestDBConfigs(cp, cp, "test_db_name")
	// uid := uint32(11111)
	// cnf := NewMycnf(uid, 6802)
	// // Assigning ServerID to be different from tablet UID to make sure that there are no
	// // assumptions in the code that those IDs are the same.
	// cnf.ServerID = 22222

	// dbconfigs.GlobalDBConfigs.InitWithSocket(cnf.SocketFile, collations.MySQL8())
	mysqld := NewMysqld(dbc)
	defer func() {
		db.Close()
		mysqld.Close()
	}()
	// servenv.OnClose(mysqld.Close)

	err := mysqld.StartReplication(map[string]string{})
	fmt.Println("Error: ", err)
}
