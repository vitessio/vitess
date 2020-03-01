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

This adds sharded keyspace dynamically in this test only and test sql insert, select
*/

package clustertest

import (
	"context"
	"fmt"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	testKeyspace = &cluster.Keyspace{
		Name: "kstest",
		SchemaSQL: `create table vt_user (
id bigint,
name varchar(64),
primary key (id)
) Engine=InnoDB`,
		VSchema: `{
 "sharded": true,
 "vindexes": {
   "hash_index": {
     "type": "hash"
   }
 },
 "tables": {
   "vt_user": {
     "column_vindexes": [
       {
         "column": "id",
         "name": "hash_index"
       }
     ]
   }
 }
}`,
	}
)

func TestAddKeyspace(t *testing.T) {
	if err := clusterInstance.StartKeyspace(*testKeyspace, []string{"-80", "80-"}, 1, true); err != nil {
		println(err.Error())
		t.Fatal(err)
	}
	// Restart vtgate process
	_ = clusterInstance.VtgateProcess.TearDown()
	_ = clusterInstance.VtgateProcess.Setup()

	if err := clusterInstance.WaitForTabletsToHealthyInVtgate(); err != nil {
		t.Error(err)
	}

	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: clusterInstance.Hostname,
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	exec(t, conn, "insert into vt_user(id, name) values(1,'name1')")

	qr := exec(t, conn, "select id, name from vt_user")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1) VARCHAR("name1")]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
}
