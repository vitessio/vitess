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
	_ "embed"
	"fmt"
	"os"
	"testing"

	_flag "vitess.io/vitess/go/internal/flag"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vttestpb "vitess.io/vitess/go/vt/proto/vttest"
	"vitess.io/vitess/go/vt/vttest"
)

var (
	cluster     *vttest.LocalCluster
	vtParams    mysql.ConnParams
	mysqlParams mysql.ConnParams
	grpcAddress string

	//go:embed schema.sql
	Schema string

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
			"t1_copy_basic": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "id1",
					Name:   "hash",
				}},
			},
			"t1_copy_all": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "id1",
					Name:   "hash",
				}},
			},
			"t1_copy_resume": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "id1",
					Name:   "hash",
				}},
			},
			"t1_sharded": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "id1",
					Name:   "hash",
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
			"t1_last_insert_id": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "id1",
					Name:   "hash",
				}},
				Columns: []*vschemapb.Column{{
					Name: "id1",
					Type: sqltypes.Int64,
				}},
			},
			"t1_row_count": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "id",
					Name:   "hash",
				}},
			},
		},
	}

	schema2 = `
create table t1_copy_all_ks2(
	id1 bigint,
	id2 bigint,
	primary key(id1)
) Engine=InnoDB;
`

	vschema2 = &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"hash": {
				Type: "hash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"t1_copy_all_ks2": {
				ColumnVindexes: []*vschemapb.ColumnVindex{{
					Column: "id1",
					Name:   "hash",
				}},
			},
		},
	}
)

func TestMain(m *testing.M) {
	_flag.ParseFlagsForTest()

	exitCode := func() int {
		var cfg vttest.Config
		cfg.Topology = &vttestpb.VTTestTopology{
			Keyspaces: []*vttestpb.Keyspace{
				{
					Name: "ks",
					Shards: []*vttestpb.Shard{{
						Name: "-80",
					}, {
						Name: "80-",
					}},
				},
				{
					Name: "ks2",
					Shards: []*vttestpb.Shard{{
						Name: "-80",
					}, {
						Name: "80-",
					}},
				},
			},
		}
		if err := cfg.InitSchemas("ks", Schema, vschema); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.RemoveAll(cfg.SchemaDir)
			return 1
		}
		defer os.RemoveAll(cfg.SchemaDir)
		if err := cfg.InitSchemas("ks2", schema2, vschema2); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.RemoveAll(cfg.SchemaDir)
			return 1
		}

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

		insertStartValue()

		return m.Run()
	}()
	os.Exit(exitCode)
}

func insertStartValue() {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// lets insert a single starting value for tests
	_, err = conn.ExecuteFetch("insert into t1_last_insert_id(id1) values(42)", 1000, true)
	if err != nil {
		panic(err)
	}
}
