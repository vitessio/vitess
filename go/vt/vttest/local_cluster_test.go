// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vttest

import (
	"fmt"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"

	// FIXME(alainjobart) remove this when it's the only option.
	// Registers our implementation.
	_ "github.com/youtube/vitess/go/mysql"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
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

	hdl, err := LaunchVitess(ProtoTopo(topology))
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
	if hdl.Data == nil {
		t.Error("map is nil")
		return
	}
	portName := "port"
	if vtgateProtocol() == "grpc" {
		portName = "grpc_port"
	}
	fport, ok := hdl.Data[portName]
	if !ok {
		t.Errorf("port %v not found in map", portName)
		return
	}
	port := int(fport.(float64))
	ctx := context.Background()
	conn, err := vtgateconn.DialProtocol(ctx, vtgateProtocol(), fmt.Sprintf("localhost:%d", port), 5*time.Second, "")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = conn.ExecuteShards(ctx, "select 1 from dual", "test_keyspace", []string{"0"}, nil, topodatapb.TabletType_MASTER, nil)
	if err != nil {
		t.Error(err)
		return
	}
}

func TestMySQL(t *testing.T) {
	hdl, err := LaunchVitess(MySQLOnly("vttest"), Schema("create table a(id int, name varchar(128), primary key(id))"))
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
