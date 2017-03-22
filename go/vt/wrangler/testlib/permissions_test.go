// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package testlib

import (
	"strings"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"github.com/youtube/vitess/go/vt/vttablet/tmclient"
	"github.com/youtube/vitess/go/vt/wrangler"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestPermissions(t *testing.T) {
	// Initialize our environment
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	master := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, nil)
	replica := NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)

	// mark the master inside the shard
	_, err := ts.UpdateShardFields(ctx, master.Tablet.Keyspace, master.Tablet.Shard, func(si *topo.ShardInfo) error {
		si.MasterAlias = master.Tablet.Alias
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateShardFields failed: %v", err)
	}

	// master will be asked for permissions
	master.FakeMysqlDaemon.FetchSuperQueryMap = map[string]*sqltypes.Result{
		"SELECT * FROM mysql.user": {
			Fields: []*querypb.Field{
				{
					Name: "Host",
					Type: sqltypes.Char,
				},
				{
					Name: "User",
					Type: sqltypes.Char,
				},
				{
					Name: "Password",
					Type: sqltypes.Char,
				},
				{
					Name: "Select_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Insert_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Update_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Delete_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Create_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Drop_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Reload_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Shutdown_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Process_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "File_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Grant_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "References_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Index_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Alter_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Show_db_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Super_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Create_tmp_table_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Lock_tables_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Execute_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Repl_slave_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Repl_client_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Create_view_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Show_view_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Create_routine_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Alter_routine_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Create_user_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Event_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Trigger_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Create_tablespace_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "ssl_type",
					Type: sqltypes.Char,
				},
				{
					Name: "ssl_cipher",
					Type: 252,
				},
				{
					Name: "x509_issuer",
					Type: 252,
				},
				{
					Name: "x509_subject",
					Type: 252,
				},
				{
					Name: "max_questions",
					Type: 3,
				},
				{
					Name: "max_updates",
					Type: 3,
				},
				{
					Name: "max_connections",
					Type: 3,
				},
				{
					Name: "max_user_connections",
					Type: 3,
				},
				{
					Name: "plugin",
					Type: sqltypes.Char,
				},
				{
					Name: "authentication_string",
					Type: 252,
				},
				{
					Name: "password_expired",
					Type: sqltypes.Char,
				},
				{
					Name: "is_role",
					Type: sqltypes.Char,
				}},
			RowsAffected: 0x6,
			InsertID:     0x0,
			Rows: [][]sqltypes.Value{
				{
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
				{
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
				{
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
				{
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
		"SELECT * FROM mysql.db": {
			Fields: []*querypb.Field{
				{
					Name: "Host",
					Type: sqltypes.Char,
				},
				{
					Name: "Db",
					Type: sqltypes.Char,
				},
				{
					Name: "User",
					Type: sqltypes.Char,
				},
				{
					Name: "Select_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Insert_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Update_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Delete_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Create_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Drop_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Grant_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "References_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Index_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Alter_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Create_tmp_table_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Lock_tables_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Create_view_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Show_view_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Create_routine_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Alter_routine_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Execute_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Event_priv",
					Type: sqltypes.Char,
				},
				{
					Name: "Trigger_priv",
					Type: sqltypes.Char,
				},
			},
			RowsAffected: 0,
			InsertID:     0,
			Rows: [][]sqltypes.Value{
				{
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
	}
	master.StartActionLoop(t, wr)
	defer master.StopActionLoop(t)

	// Make a two-level-deep copy, so we can make them diverge later.
	user := *master.FakeMysqlDaemon.FetchSuperQueryMap["SELECT * FROM mysql.user"]
	user.Fields = append([]*querypb.Field{}, user.Fields...)

	// replica will be asked for permissions
	replica.FakeMysqlDaemon.FetchSuperQueryMap = map[string]*sqltypes.Result{
		"SELECT * FROM mysql.user": &user,
		"SELECT * FROM mysql.db":   master.FakeMysqlDaemon.FetchSuperQueryMap["SELECT * FROM mysql.db"],
	}
	replica.StartActionLoop(t, wr)
	defer replica.StopActionLoop(t)

	// Overwrite with the correct value to make sure it passes.
	replica.FakeMysqlDaemon.FetchSuperQueryMap["SELECT * FROM mysql.user"].Fields[0] = &querypb.Field{
		Name: "Host",
		Type: sqltypes.Char,
	}

	// run ValidatePermissionsKeyspace, this should work
	if err := vp.Run([]string{"ValidatePermissionsKeyspace", master.Tablet.Keyspace}); err != nil {
		t.Fatalf("ValidatePermissionsKeyspace failed: %v", err)
	}

	// modify one field, this should fail
	replica.FakeMysqlDaemon.FetchSuperQueryMap["SELECT * FROM mysql.user"].Fields[0] = &querypb.Field{
		Name: "Wrong",
		Type: sqltypes.Char,
	}

	// run ValidatePermissionsKeyspace again, this should now fail
	if err := vp.Run([]string{"ValidatePermissionsKeyspace", master.Tablet.Keyspace}); err == nil || !strings.Contains(err.Error(), "has an extra user") {
		t.Fatalf("ValidatePermissionsKeyspace has unexpected err: %v", err)
	}

}
