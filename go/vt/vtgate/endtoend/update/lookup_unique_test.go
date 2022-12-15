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
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/internal/flag"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vttest"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

var (
	cluster     *vttest.LocalCluster
	vtParams    mysql.ConnParams
	mysqlParams mysql.ConnParams
	grpcAddress string

	schema = `
create table t1(
	id bigint,
	sharding_key bigint,
	primary key(id)
) Engine=InnoDB;

create table t1_id_idx(
	id bigint,
	keyspace_id varbinary(10),
	primary key(id)
) Engine=InnoDB;

create table t2(
	id bigint,
	t1_id bigint,
	sharding_key bigint,
	primary key(id)
) Engine=InnoDB;

create table t2_id_idx(
	id bigint,
	keyspace_id varbinary(10),
	primary key(id)
) Engine=InnoDB;
`

	vschema = &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"hash": {
				Type: "hash",
			},
			"t1_id_idx": {
				Type: "consistent_lookup_unique",
				Params: map[string]string{
					"table": "t1_id_idx",
					"from":  "id",
					"to":    "keyspace_id",
				},
				Owner: "t1",
			},
			"t2_id_idx": {
				Type: "consistent_lookup_unique",
				Params: map[string]string{
					"table": "t2_id_idx",
					"from":  "id",
					"to":    "keyspace_id",
				},
				Owner: "t2",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "sharding_key",
					Name:   "hash",
				}, {
					Column: "id",
					Name:   "t1_id_idx",
				}},
			},
			"t1_id_idx": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "id",
					Name:   "hash",
				}},
			},
			"t2": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "sharding_key",
					Name:   "hash",
				}, {
					Column: "t1_id",
					Name:   "t1_id_idx",
				}, {
					Column: "id",
					Name:   "t2_id_idx",
				}},
			},
			"t2_id_idx": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "id",
					Name:   "hash",
				}},
			},
		},
	}
)

func TestMain(m *testing.M) {
	flag.ParseFlagsForTest()

	exitCode := func() int {
		var cfg vttest.Config
		cfg.Topology = &vttestpb.VTTestTopology{
			Keyspaces: []*vttestpb.Keyspace{{
				Name: "ks",
				Shards: []*vttestpb.Shard{{
					Name: "-80",
				}, {
					Name: "80-",
				}},
			}},
		}
		if err := cfg.InitSchemas("ks", schema, vschema); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.RemoveAll(cfg.SchemaDir)
			return 1
		}
		defer os.RemoveAll(cfg.SchemaDir)

		cluster = &vttest.LocalCluster{
			Config: cfg,
		}
		if err := cluster.Setup(); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			//log error
			if err := cluster.TearDown(); err != nil {
				log.Errorf("cluster.TearDown() did not work: ", err)
			}
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

func TestUpdateUnownedLookupVindexValidValue(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)

	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	utils.Exec(t, conn, "insert into t1(id, sharding_key) values(1,1), (2,1), (3,2), (4,2)")
	t.Cleanup(func() { utils.Exec(t, conn, "delete from t1") })

	utils.Exec(t, conn, "insert into t2(id, sharding_key, t1_id) values (1,1,1), (2,1,2), (3,2,3), (4,2,4)")
	t.Cleanup(func() { utils.Exec(t, conn, "delete from t2") })

	utils.Exec(t, conn, "UPDATE t2 SET t1_id = 1 WHERE id = 2")

	qr := utils.Exec(t, conn, "select id, sharding_key, t1_id from t2 WHERE id = 2")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(2) INT64(1) INT64(1)]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
}

func TestUpdateUnownedLookupVindexInvalidValue(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)

	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	utils.Exec(t, conn, "insert into t1(id, sharding_key) values(1,1), (2,1), (3,2), (4,2)")
	t.Cleanup(func() { utils.Exec(t, conn, "delete from t1") })

	utils.Exec(t, conn, "insert into t2(id, sharding_key, t1_id) values (1,1,1), (2,1,2), (3,2,3), (4,2,4)")
	t.Cleanup(func() { utils.Exec(t, conn, "delete from t2") })

	_, err = conn.ExecuteFetch("UPDATE t2 SET t1_id = 5 WHERE id = 2", 1000, true)
	require.EqualError(t, err, `values [INT64(5)] for column [t1_id] does not map to keyspace ids (errno 1105) (sqlstate HY000) during query: UPDATE t2 SET t1_id = 5 WHERE id = 2`)

	qr := utils.Exec(t, conn, "select id, sharding_key, t1_id from t2 WHERE id = 2")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(2) INT64(1) INT64(2)]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
}

func TestUpdateUnownedLookupVindexToNull(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	utils.Exec(t, conn, "insert into t1(id, sharding_key) values(1,1), (2,1), (3,2), (4,2)")
	t.Cleanup(func() { utils.Exec(t, conn, "delete from t1") })

	utils.Exec(t, conn, "insert into t2(id, sharding_key, t1_id) values (1,1,1), (2,1,2), (3,2,3), (4,2,4)")
	t.Cleanup(func() { utils.Exec(t, conn, "delete from t2") })

	utils.Exec(t, conn, "UPDATE t2 SET t1_id = NULL WHERE id = 2")

	qr := utils.Exec(t, conn, "select id, sharding_key, t1_id from t2 WHERE id = 2")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(2) INT64(1) NULL]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
}
