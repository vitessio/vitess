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

package vschema

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	vtParams        mysql.ConnParams
	hostname        = "localhost"
	keyspaceName    = "ks"
	cell            = "zone1"
	sqlSchema       = `
		create table vt_user (
			id bigint,
			name varchar(64),
			primary key (id)
		) Engine=InnoDB;
			
		create table main (
			id bigint,
			val varchar(128),
			primary key(id)
		) Engine=InnoDB;
`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitcode, err := func() (int, error) {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1, err
		}

		// List of users authorized to execute vschema ddl operations
		clusterInstance.VtGateExtraArgs = []string{"-vschema_ddl_authorized_users=%"}

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
		vtParams = mysql.ConnParams{
			Host: clusterInstance.Hostname,
			Port: clusterInstance.VtgateMySQLPort,
		}
		return m.Run(), nil
	}()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	} else {
		os.Exit(exitcode)
	}

}

func TestVSchema(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Test the empty database with no vschema
	exec(t, conn, "insert into vt_user (id,name) values(1,'test1'), (2,'test2'), (3,'test3'), (4,'test4')")

	qr := exec(t, conn, "select id, name from vt_user order by id")
	got := fmt.Sprintf("%v", qr.Rows)
	want := `[[INT64(1) VARCHAR("test1")] [INT64(2) VARCHAR("test2")] [INT64(3) VARCHAR("test3")] [INT64(4) VARCHAR("test4")]]`
	assert.Equal(t, want, got)

	qr = exec(t, conn, "delete from vt_user")
	got = fmt.Sprintf("%v", qr.Rows)
	want = `[]`
	assert.Equal(t, want, got)

	// Test empty vschema
	qr = exec(t, conn, "SHOW VSCHEMA TABLES")
	got = fmt.Sprintf("%v", qr.Rows)
	want = `[[VARCHAR("dual")]]`
	assert.Equal(t, want, got)

	// Use the DDL to create an unsharded vschema and test again

	// Create VSchema and do a Select to force update VSCHEMA
	exec(t, conn, "begin")
	exec(t, conn, "ALTER VSCHEMA ADD TABLE vt_user")
	exec(t, conn, "select * from  vt_user")
	exec(t, conn, "commit")

	exec(t, conn, "begin")
	exec(t, conn, "ALTER VSCHEMA ADD TABLE main")
	exec(t, conn, "select * from  main")
	exec(t, conn, "commit")

	// Test Showing Tables
	qr = exec(t, conn, "SHOW VSCHEMA TABLES")
	got = fmt.Sprintf("%v", qr.Rows)
	want = `[[VARCHAR("dual")] [VARCHAR("main")] [VARCHAR("vt_user")]]`
	assert.Equal(t, want, got)

	// Test Showing Vindexes
	qr = exec(t, conn, "SHOW VSCHEMA VINDEXES")
	got = fmt.Sprintf("%v", qr.Rows)
	want = `[]`
	assert.Equal(t, want, got)

	// Test DML operations
	exec(t, conn, "insert into vt_user (id,name) values(1,'test1'), (2,'test2'), (3,'test3'), (4,'test4')")
	qr = exec(t, conn, "select id, name from vt_user order by id")
	got = fmt.Sprintf("%v", qr.Rows)
	want = `[[INT64(1) VARCHAR("test1")] [INT64(2) VARCHAR("test2")] [INT64(3) VARCHAR("test3")] [INT64(4) VARCHAR("test4")]]`
	assert.Equal(t, want, got)

	qr = exec(t, conn, "delete from vt_user")
	got = fmt.Sprintf("%v", qr.Rows)
	want = `[]`
	assert.Equal(t, want, got)

}

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	if err != nil {
		t.Fatal(err)
	}
	return qr
}
