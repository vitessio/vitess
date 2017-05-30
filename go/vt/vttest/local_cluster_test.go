/*
Copyright 2017 Google Inc.

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

package vttest

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
	vttestpb "github.com/youtube/vitess/go/vt/proto/vttest"
)

func TestVitess(t *testing.T) {
	topology := &vttestpb.VTTestTopology{
		Keyspaces: []*vttestpb.Keyspace{
			{
				Name: "test_keyspace",
				Shards: []*vttestpb.Shard{
					{
						Name: "0",
					},
				},
			},
		},
	}
	schema := `CREATE TABLE messages (
	  page BIGINT(20) UNSIGNED,
	  time_created_ns BIGINT(20) UNSIGNED,
	  message VARCHAR(10000),
	  PRIMARY KEY (page, time_created_ns)
	) ENGINE=InnoDB`
	vschema := &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"hash": {
				Type: "hash",
			},
		},
		Tables: map[string]*vschemapb.Table{
			"messages": {
				ColumnVindexes: []*vschemapb.ColumnVindex{
					{
						Column: "page",
						Name:   "hash",
					},
				},
			},
		},
	}

	hdl, err := LaunchVitess(ProtoTopo(topology), Schema(schema), VSchema(vschema))
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = hdl.TearDown()
		if err != nil {
			t.Error(err)
			return
		}
	}()
	ctx := context.Background()
	vtgateAddr, err := hdl.VtgateAddress()
	if err != nil {
		t.Error(err)
		return
	}
	conn, err := vtgateconn.DialProtocol(ctx, vtgateProtocol(), vtgateAddr, 5*time.Second)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = conn.ExecuteShards(ctx, "select 1 from dual", "test_keyspace", []string{"0"}, nil, topodatapb.TabletType_MASTER, nil)
	if err != nil {
		t.Error(err)
		return
	}
	// Test that vtgate can use the VSchema to route the query to the keyspace.
	// TODO(mberlin): This also works without a vschema for the table. How to fix?
	_, err = conn.Session("", nil).Execute(ctx, "select * from messages", nil)
	if err != nil {
		t.Error(err)
		return
	}
}

func TestMySQL(t *testing.T) {
	// Load schema from file to verify the SchemaDirectory() option.
	dbName := "vttest"
	schema := "create table a(id int, name varchar(128), primary key(id))"
	schemaDir, err := ioutil.TempDir("", "vt")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(schemaDir)
	ksDir := filepath.Join(schemaDir, dbName)
	if err := os.Mkdir(ksDir, os.ModeDir|0775); err != nil {
		t.Fatal(err)
	}
	schemaFile := filepath.Join(ksDir, "table.sql")
	if err := ioutil.WriteFile(schemaFile, []byte(schema), 0666); err != nil {
		t.Fatal(err)
	}

	hdl, err := LaunchVitess(MySQLOnly(dbName), SchemaDirectory(schemaDir))
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err = hdl.TearDown()
		if err != nil {
			t.Error(err)
			return
		}
	}()
	params, err := hdl.MySQLConnParams()
	if err != nil {
		t.Error(err)
	}
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &params)
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.ExecuteFetch("insert into a values(1, 'name')", 10, false)
	if err != nil {
		t.Error(err)
	}
	qr, err := conn.ExecuteFetch("select * from a", 10, false)
	if err != nil {
		t.Error(err)
	}
	if qr.RowsAffected != 1 {
		t.Errorf("Rows affected: %d, want 1", qr.RowsAffected)
	}
}
