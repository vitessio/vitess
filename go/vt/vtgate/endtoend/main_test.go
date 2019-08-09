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
	"flag"
	"fmt"
	"os"
	"path"
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
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
	id1 bigint,
	id2 bigint,
	primary key(id1)
) Engine=InnoDB;

create table t1_id2_idx(
	id2 bigint,
	keyspace_id varbinary(10),
	primary key(id2)
) Engine=InnoDB;

create table vstream_test(
	id bigint,
	val bigint,
	primary key(id)
) Engine=InnoDB;

create table aggr_test(
	id bigint,
	val1 varchar(16),
	val2 bigint,
	primary key(id)
) Engine=InnoDB;

create table t2(
	id3 bigint,
	id4 bigint,
	primary key(id3)
) Engine=InnoDB;

create table t2_id4_idx(
	id bigint not null auto_increment,
	id4 bigint,
	id3 bigint,
	primary key(id),
	key idx_id4(id4)
) Engine=InnoDB;
`

	vschema = &vschemapb.Keyspace{
		Sharded: true,
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
			"t2_id4_idx": {
				Type: "lookup_hash",
				Params: map[string]string{
					"table":      "t2_id4_idx",
					"from":       "id4",
					"to":         "id3",
					"autocommit": "true",
				},
				Owner: "t2",
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
			"t2": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "id3",
					Name:   "hash",
				}, {
					Column: "id4",
					Name:   "t2_id4_idx",
				}},
			},
			"t2_id4_idx": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "id4",
					Name:   "hash",
				}},
			},
			"vstream_test": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "id",
					Name:   "hash",
				}},
			},
			"aggr_test": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "id",
					Name:   "hash",
				}},
				Columns: []*vschemapb.Column{{
					Name: "val1",
					Type: sqltypes.VarChar,
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
					Name: "-80",
				}, {
					Name: "80-",
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
