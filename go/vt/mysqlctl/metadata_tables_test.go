/*
Copyright 2019 Google Inc.

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

package mysqlctl_test

import (
	"errors"
	"testing"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/fakemysqldaemon"
)

func TestInitMetadataTable_NoTable(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQueryPattern(`(SHOW|SET|CREATE|BEGIN|INSERT|COMMIT)\b.*`, &sqltypes.Result{})
	mysqlDaemon := fakemysqldaemon.NewFakeMysqlDaemon(db)
	db.AddQuery("SHOW TABLES FROM _vt LIKE '%_metadata'", &sqltypes.Result{
		Fields: mysql.BaseShowTablesFields,
		Rows:   [][]sqltypes.Value{},
	})
	if err := mysqlctl.PopulateMetadataTables(mysqlDaemon, map[string]string{}); err != nil {
		t.Fatalf("PopulateMetadataTables failed: %v", err)
	}
}

func TestInitMetadataTable_NoDb(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	db.AddQueryPattern(`(SHOW|SET|CREATE|BEGIN|INSERT|COMMIT)\b.*`, &sqltypes.Result{})
	mysqlDaemon := fakemysqldaemon.NewFakeMysqlDaemon(db)
	db.AddRejectedQuery("SHOW TABLES FROM _vt LIKE '%_metadata'", errors.New("rejected"))
	if err := mysqlctl.PopulateMetadataTables(mysqlDaemon, map[string]string{}); err != nil {
		t.Fatalf("PopulateMetadataTables failed: %v", err)
	}
}

func TestInitMetadataTable_AlreadyExists(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	mysqlDaemon := fakemysqldaemon.NewFakeMysqlDaemon(db)
	db.AddQuery("SHOW TABLES FROM _vt LIKE '%_metadata'", &sqltypes.Result{
		Fields: mysql.BaseShowTablesFields,
		Rows: [][]sqltypes.Value{
			mysql.BaseShowTablesRow("local_metadata", false, ""),
			mysql.BaseShowTablesRow("shard_metadata", false, ""),
		},
	})
	if err := mysqlctl.PopulateMetadataTables(mysqlDaemon, map[string]string{}); err != nil {
		t.Fatalf("PopulateMetadataTables failed: %v", err)
	}
}
