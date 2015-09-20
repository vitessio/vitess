// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vttest

import (
	"fmt"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/vt/proto/topodata"
	_ "github.com/youtube/vitess/go/vt/vtgate/grpcvtgateconn"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
)

func TestVitess(t *testing.T) {
	// TODO(sougou/alainjobart): Fix this test.
	t.Skip()
	hdl, err := LaunchVitess("test_keyspace/0:test_keyspace", "", false)
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
	fport, ok := hdl.Data["port"]
	if !ok {
		t.Error("port not found in map")
		return
	}
	port := int(fport.(float64))
	ctx := context.Background()
	conn, err := vtgateconn.DialProtocol(ctx, "grpc", fmt.Sprintf("localhost:%d", port), 5*time.Second)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = conn.ExecuteShards(ctx, "select * from test_table", "test_keyspace", []string{"0"}, nil, topodata.TabletType_MASTER)
	if err != nil {
		t.Error(err)
		return
	}
}

func TestMySQL(t *testing.T) {
	hdl, err := LauncMySQL("test", "create table a(id int, name varchar(128), primary key(id))", false)
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
	params, err := hdl.MySQLConnParams()
	if err != nil {
		t.Error(err)
	}
	conn, err := mysql.Connect(params)
	if err != nil {
		t.Error(err)
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
