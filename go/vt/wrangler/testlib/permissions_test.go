// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package testlib

import (
	"strings"
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/zktopo"
	"golang.org/x/net/context"
)

func TestPermissions(t *testing.T) {
	// Initialize our environment
	ctx := context.Background()
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient(), time.Second)
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	master := NewFakeTablet(t, wr, "cell1", 0, topo.TYPE_MASTER)
	replica := NewFakeTablet(t, wr, "cell1", 1, topo.TYPE_REPLICA)

	// mark the master inside the shard
	si, err := ts.GetShard(ctx, master.Tablet.Keyspace, master.Tablet.Shard)
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}
	si.MasterAlias = topo.TabletAliasToProto(master.Tablet.Alias)
	if err := topo.UpdateShard(ctx, ts, si); err != nil {
		t.Fatalf("UpdateShard failed: %v", err)
	}

	// master will be asked for permissions
	master.FakeMysqlDaemon.FetchSuperQueryMap = map[string]*mproto.QueryResult{
		"SELECT * FROM mysql.user": &mproto.QueryResult{
			Fields: []mproto.Field{
				mproto.Field{
					Name:  "Host",
					Type:  254,
					Flags: 16515},
				mproto.Field{
					Name:  "User",
					Type:  254,
					Flags: 16515},
				mproto.Field{
					Name:  "Password",
					Type:  254,
					Flags: 129},
				mproto.Field{
					Name:  "Select_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Insert_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Update_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Delete_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Create_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Drop_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Reload_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Shutdown_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Process_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "File_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Grant_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "References_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Index_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Alter_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Show_db_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Super_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Create_tmp_table_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Lock_tables_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Execute_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Repl_slave_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Repl_client_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Create_view_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Show_view_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Create_routine_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Alter_routine_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Create_user_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Event_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Trigger_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Create_tablespace_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "ssl_type",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "ssl_cipher",
					Type:  252,
					Flags: 4241},
				mproto.Field{
					Name:  "x509_issuer",
					Type:  252,
					Flags: 4241},
				mproto.Field{
					Name:  "x509_subject",
					Type:  252,
					Flags: 4241},
				mproto.Field{
					Name:  "max_questions",
					Type:  3,
					Flags: 32801},
				mproto.Field{
					Name:  "max_updates",
					Type:  3,
					Flags: 32801},
				mproto.Field{
					Name:  "max_connections",
					Type:  3,
					Flags: 32801},
				mproto.Field{
					Name:  "max_user_connections",
					Type:  3,
					Flags: 32769},
				mproto.Field{
					Name:  "plugin",
					Type:  254,
					Flags: 1},
				mproto.Field{
					Name:  "authentication_string",
					Type:  252,
					Flags: 4241},
				mproto.Field{
					Name:  "password_expired",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "is_role",
					Type:  254,
					Flags: 257}},
			RowsAffected: 0x6,
			InsertId:     0x0,
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
					sqltypes.MakeString([]byte("test_host1")),
					sqltypes.MakeString([]byte("test_user1")),
					sqltypes.MakeString([]byte("test_password1")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeString([]byte("0")),
					sqltypes.MakeString([]byte("0")),
					sqltypes.MakeString([]byte("0")),
					sqltypes.MakeString([]byte("0")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N"))},
				[]sqltypes.Value{
					sqltypes.MakeString([]byte("test_host2")),
					sqltypes.MakeString([]byte("test_user2")),
					sqltypes.MakeString([]byte("test_password2")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeString([]byte("0")),
					sqltypes.MakeString([]byte("0")),
					sqltypes.MakeString([]byte("0")),
					sqltypes.MakeString([]byte("0")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N"))},
				[]sqltypes.Value{
					sqltypes.MakeString([]byte("test_host3")),
					sqltypes.MakeString([]byte("test_user3")),
					sqltypes.MakeString([]byte("test_password3")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeString([]byte("0")),
					sqltypes.MakeString([]byte("0")),
					sqltypes.MakeString([]byte("0")),
					sqltypes.MakeString([]byte("0")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N"))},
				[]sqltypes.Value{
					sqltypes.MakeString([]byte("test_host4")),
					sqltypes.MakeString([]byte("test_user4")),
					sqltypes.MakeString([]byte("test_password4")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeString([]byte("0")),
					sqltypes.MakeString([]byte("0")),
					sqltypes.MakeString([]byte("0")),
					sqltypes.MakeString([]byte("0")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeString([]byte("")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("N")),
				},
			},
		},
		"SELECT * FROM mysql.db": &mproto.QueryResult{
			Fields: []mproto.Field{
				mproto.Field{
					Name:  "Host",
					Type:  mproto.VT_STRING,
					Flags: 16515},
				mproto.Field{
					Name:  "Db",
					Type:  mproto.VT_STRING,
					Flags: 16515},
				mproto.Field{
					Name:  "User",
					Type:  mproto.VT_STRING,
					Flags: 16515},
				mproto.Field{
					Name:  "Select_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Insert_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Update_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Delete_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Create_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Drop_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Grant_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "References_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Index_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Alter_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Create_tmp_table_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Lock_tables_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Create_view_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Show_view_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Create_routine_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Alter_routine_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Execute_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Event_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Trigger_priv",
					Type:  254,
					Flags: 257},
			},
			RowsAffected: 0,
			InsertId:     0,
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
					sqltypes.MakeString([]byte("test_host")),
					sqltypes.MakeString([]byte("test_db")),
					sqltypes.MakeString([]byte("test_user")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
				},
			},
		},
		"SELECT * FROM mysql.host": &mproto.QueryResult{
			Fields: []mproto.Field{
				mproto.Field{
					Name:  "Host",
					Type:  mproto.VT_STRING,
					Flags: 16515},
				mproto.Field{
					Name:  "Db",
					Type:  254,
					Flags: 16515},
				mproto.Field{
					Name:  "Select_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Insert_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Update_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Delete_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Create_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Drop_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Grant_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "References_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Index_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Alter_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Create_tmp_table_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Lock_tables_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Create_view_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Show_view_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Create_routine_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Alter_routine_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Execute_priv",
					Type:  254,
					Flags: 257},
				mproto.Field{
					Name:  "Trigger_priv",
					Type:  254,
					Flags: 257},
			},
			RowsAffected: 0,
			InsertId:     0,
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
					sqltypes.MakeString([]byte("test_host")),
					sqltypes.MakeString([]byte("test_db")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("N")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
					sqltypes.MakeString([]byte("Y")),
				},
			},
		},
	}
	master.StartActionLoop(t, wr)
	defer master.StopActionLoop(t)

	// replica will be asked for permissions
	replica.FakeMysqlDaemon.FetchSuperQueryMap = map[string]*mproto.QueryResult{
		"SELECT * FROM mysql.user": master.FakeMysqlDaemon.FetchSuperQueryMap["SELECT * FROM mysql.user"],
		"SELECT * FROM mysql.db":   master.FakeMysqlDaemon.FetchSuperQueryMap["SELECT * FROM mysql.db"],
		"SELECT * FROM mysql.host": master.FakeMysqlDaemon.FetchSuperQueryMap["SELECT * FROM mysql.host"],
	}
	replica.StartActionLoop(t, wr)
	defer replica.StopActionLoop(t)

	// run ValidatePermissionsKeyspace, this should work
	if err := vp.Run([]string{"ValidatePermissionsKeyspace", master.Tablet.Keyspace}); err != nil {
		t.Fatalf("ValidatePermissionsKeyspace failed: %v", err)
	}

	// modify one field, this should fail
	replica.FakeMysqlDaemon.FetchSuperQueryMap["SELECT * FROM mysql.host"] = &mproto.QueryResult{
		Fields: []mproto.Field{
			mproto.Field{
				Name:  "Host",
				Type:  mproto.VT_STRING,
				Flags: 16515},
			mproto.Field{
				Name:  "Db",
				Type:  254,
				Flags: 16515},
			mproto.Field{
				Name:  "Select_priv",
				Type:  254,
				Flags: 257},
			mproto.Field{
				Name:  "Insert_priv",
				Type:  254,
				Flags: 257},
			mproto.Field{
				Name:  "Update_priv",
				Type:  254,
				Flags: 257},
			mproto.Field{
				Name:  "Delete_priv",
				Type:  254,
				Flags: 257},
			mproto.Field{
				Name:  "Create_priv",
				Type:  254,
				Flags: 257},
			mproto.Field{
				Name:  "Drop_priv",
				Type:  254,
				Flags: 257},
			mproto.Field{
				Name:  "Grant_priv",
				Type:  254,
				Flags: 257},
			mproto.Field{
				Name:  "References_priv",
				Type:  254,
				Flags: 257},
			mproto.Field{
				Name:  "Index_priv",
				Type:  254,
				Flags: 257},
			mproto.Field{
				Name:  "Alter_priv",
				Type:  254,
				Flags: 257},
			mproto.Field{
				Name:  "Create_tmp_table_priv",
				Type:  254,
				Flags: 257},
			mproto.Field{
				Name:  "Lock_tables_priv",
				Type:  254,
				Flags: 257},
			mproto.Field{
				Name:  "Create_view_priv",
				Type:  254,
				Flags: 257},
			mproto.Field{
				Name:  "Show_view_priv",
				Type:  254,
				Flags: 257},
			mproto.Field{
				Name:  "Create_routine_priv",
				Type:  254,
				Flags: 257},
			mproto.Field{
				Name:  "Alter_routine_priv",
				Type:  254,
				Flags: 257},
			mproto.Field{
				Name:  "Execute_priv",
				Type:  254,
				Flags: 257},
			mproto.Field{
				Name:  "Trigger_priv",
				Type:  254,
				Flags: 257},
		},
		RowsAffected: 0,
		InsertId:     0,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{
				sqltypes.MakeString([]byte("test_host")),
				sqltypes.MakeString([]byte("test_db")),
				sqltypes.MakeString([]byte("Y")),
				sqltypes.MakeString([]byte("Y")),
				sqltypes.MakeString([]byte("Y")),
				sqltypes.MakeString([]byte("Y")),
				sqltypes.MakeString([]byte("Y")),
				sqltypes.MakeString([]byte("Y")),
				sqltypes.MakeString([]byte("N")),
				sqltypes.MakeString([]byte("Y")),
				sqltypes.MakeString([]byte("Y")),
				sqltypes.MakeString([]byte("Y")),
				sqltypes.MakeString([]byte("Y")),
				sqltypes.MakeString([]byte("Y")),
				sqltypes.MakeString([]byte("N")),
				sqltypes.MakeString([]byte("Y")),
				sqltypes.MakeString([]byte("Y")),
				sqltypes.MakeString([]byte("Y")),
				sqltypes.MakeString([]byte("Y")),
				sqltypes.MakeString([]byte("N")), // different
			},
		},
	}

	// run ValidatePermissionsKeyspace again, this should now fail
	if err := vp.Run([]string{"ValidatePermissionsKeyspace", master.Tablet.Keyspace}); err == nil || !strings.Contains(err.Error(), "disagree on host test_host:test_db") {
		t.Fatalf("ValidatePermissionsKeyspace has unexpected err: %v", err)
	}

}
