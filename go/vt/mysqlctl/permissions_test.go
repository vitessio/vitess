/*
Copyright 2024 The Vitess Authors.

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
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
)

func TestGetPermissions(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()

	testMysqld := NewFakeMysqlDaemon(db)
	defer testMysqld.Close()

	testMysqld.FetchSuperQueryMap = map[string]*sqltypes.Result{
		"SELECT * FROM mysql.user ORDER BY host, user":   sqltypes.MakeTestResult(sqltypes.MakeTestFields("host|user", "varchar|varchar"), "test_host1|test_user1", "test_host2|test_user2"),
		"SELECT * FROM mysql.db ORDER BY host, db, user": sqltypes.MakeTestResult(sqltypes.MakeTestFields("host|user|db", "varchar|varchar|varchar"), "test_host1|test_user1|test_db1", "test_host2|test_user2|test_db2"),
	}

	per, err := GetPermissions(testMysqld)
	assert.NoError(t, err)
	assert.Len(t, per.DbPermissions, 2)
	assert.Len(t, per.UserPermissions, 2)
}
