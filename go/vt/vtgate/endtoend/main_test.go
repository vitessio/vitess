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
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/vttest"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
)

var (
	cluster     *vttest.LocalCluster
	vtParams    mysql.ConnParams
	mysqlParams mysql.ConnParams

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

		return m.Run()
	}()
	os.Exit(exitCode)
}
