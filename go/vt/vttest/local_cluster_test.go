// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vttest

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"

	// FIXME(alainjobart) remove this when it's the only option.
	// Registers our implementation.
	_ "github.com/youtube/vitess/go/mysql"

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
	conn, err := vtgateconn.DialProtocol(ctx, vtgateProtocol(), vtgateAddr, 5*time.Second, "")
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
	_, err = conn.Execute(ctx, "select * from messages", nil, topodatapb.TabletType_MASTER, nil)
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
	conn, err := sqldb.Connect(params)
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
