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

package endtoend

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
	"vitess.io/vitess/go/vt/vttest"
)

var (
	cluster        *vttest.LocalCluster
	vtParams       mysql.ConnParams
	mysqlParams    mysql.ConnParams
	grpcAddress    string
	tabletHostName = flag.String("tablet_hostname", "", "the tablet hostname")

	schema = `
create table t1(
		id1 bigint,
		id2 bigint,
		primary key(id1)
	) Engine=InnoDB;
	
create table t1_id2_idx(
		id2 bigint,
		keyspace_id varbinary(10),
		primary key(id2)
	) Engine=InnoDB;

create table del_test_a(
	id bigint,
	val1 varbinary(16),
	val2 bigint,
	primary key(id)
) Engine=InnoDB;

create table del_test_b(
	id bigint,
	val1 varbinary(16),
	val2 bigint,
	primary key(id)
) Engine=InnoDB;
`

	vschema = &vschemapb.Keyspace{
		Sharded: false,
		Vindexes: map[string]*vschemapb.Vindex{
			"hash": {
				Type: "hash",
			},
			"t1_id2_vdx": {
				Type: "consistent_lookup_unique",
				Params: map[string]string{
					"table": "t1_id2_idx",
					"from":  "id2",
					"to":    "keyspace_id",
				},
				Owner: "t1",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "id1",
					Name:   "hash",
				}, {
					Column: "id2",
					Name:   "t1_id2_vdx",
				}},
			},
			"t1_id2_idx": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "id2",
					Name:   "hash",
				}},
			},
			"del_test_a": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "id",
					Name:   "hash",
				}},
			},
			"del_test_b": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "id",
					Name:   "hash",
				}},
			},
		},
	}
)

func TestMain(m *testing.M) {
	flag.Parse()

	exitCode := func() int {
		var cfg vttest.Config
		cfg.Topology = &vttestpb.VTTestTopology{
			Keyspaces: []*vttestpb.Keyspace{{
				Name: "ks",
				Shards: []*vttestpb.Shard{{
					Name: "80",
				}},
			}},
		}
		cfg.ExtraMyCnf = []string{path.Join(os.Getenv("VTTOP"), "config/mycnf/testsuite-rbr.cnf")}
		if err := cfg.InitSchemas("ks", schema, vschema); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.RemoveAll(cfg.SchemaDir)
			return 1
		}
		defer os.RemoveAll(cfg.SchemaDir)

		cfg.TabletHostName = *tabletHostName

		cluster = &vttest.LocalCluster{
			Config: cfg,
		}
		if err := cluster.Setup(); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			cluster.TearDown()
			return 1
		}
		defer cluster.TearDown()

		vtParams = mysql.ConnParams{
			Host: "localhost",
			Port: cluster.Env.PortForProtocol("vtcombo_mysql_port", ""),
		}
		mysqlParams = cluster.MySQLConnParams()
		grpcAddress = fmt.Sprintf("localhost:%d", cluster.Env.PortForProtocol("vtcombo", "grpc"))

		return m.Run()
	}()
	os.Exit(exitCode)
}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	exec(t, conn, "insert into del_test_a(id, val1, val2) values(1,'a',1), (2,'a',1), (3,'b',1), (4,'c',3), (5,'c',4)")
	exec(t, conn, "insert into del_test_b(id, val1, val2) values(1,'a',1), (2,'a',1), (3,'a',1), (4,'d',3), (5,'f',4)")

	qr := exec(t, conn, "delete a.*, b.* from del_test_a a, del_test_b b where a.id = b.id and b.val1 = 'a' and a.val1 = 'a'")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	qr = exec(t, conn, "select * from del_test_a")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(3) VARBINARY("b") INT64(1)] [INT64(4) VARBINARY("c") INT64(3)] [INT64(5) VARBINARY("c") INT64(4)]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
}

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	if err != nil {
		t.Fatal(err)
	}
	return qr
}
