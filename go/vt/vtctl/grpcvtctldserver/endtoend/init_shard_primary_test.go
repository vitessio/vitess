/*
Copyright 2020 The Vitess Authors.

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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"
	"vitess.io/vitess/go/vt/wrangler/testlib"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

func TestInitShardPrimary(t *testing.T) {
	ts := memorytopo.NewServer("cell1")
	tmc := tmclient.NewTabletManagerClient()
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmc)

	primaryDb := fakesqldb.New(t)
	primaryDb.AddQuery("create database if not exists `vt_test_keyspace`", &sqltypes.Result{InsertID: 0, RowsAffected: 0})

	tablet1 := testlib.NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_MASTER, primaryDb)
	tablet2 := testlib.NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	tablet3 := testlib.NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	tablet1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE RESET ALL REPLICATION",
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, master_alias, replication_position) VALUES",
	}

	tablet2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE RESET ALL REPLICATION",
		"FAKE RESET ALL REPLICATION",
		"FAKE SET SLAVE POSITION",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	tablet2.FakeMysqlDaemon.SetMasterInput = fmt.Sprintf("%v:%v", tablet1.Tablet.Hostname, tablet1.Tablet.MysqlPort)

	tablet3.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE RESET ALL REPLICATION",
		"FAKE RESET ALL REPLICATION",
		"FAKE SET SLAVE POSITION",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	tablet3.FakeMysqlDaemon.SetMasterInput = fmt.Sprintf("%v:%v", tablet1.Tablet.Hostname, tablet1.Tablet.MysqlPort)

	for _, tablet := range []*testlib.FakeTablet{tablet1, tablet2, tablet3} {
		tablet.StartActionLoop(t, wr)
		defer tablet.StopActionLoop(t)

		tablet.TM.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)
	}

	vtctld := grpcvtctldserver.NewVtctldServer(ts)
	resp, err := vtctld.InitShardPrimary(context.Background(), &vtctldatapb.InitShardPrimaryRequest{
		Keyspace:                tablet1.Tablet.Keyspace,
		Shard:                   tablet1.Tablet.Shard,
		PrimaryElectTabletAlias: tablet1.Tablet.Alias,
	})

	assert.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestInitShardPrimaryNoFormerPrimary(t *testing.T) {
	ts := memorytopo.NewServer("cell1")
	tmc := tmclient.NewTabletManagerClient()
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmc)

	primaryDb := fakesqldb.New(t)
	primaryDb.AddQuery("create database if not exists `vt_test_keyspace`", &sqltypes.Result{InsertID: 0, RowsAffected: 0})

	tablet1 := testlib.NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_REPLICA, primaryDb)
	tablet2 := testlib.NewFakeTablet(t, wr, "cell1", 1, topodatapb.TabletType_REPLICA, nil)
	tablet3 := testlib.NewFakeTablet(t, wr, "cell1", 2, topodatapb.TabletType_REPLICA, nil)

	tablet1.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE RESET ALL REPLICATION",
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"CREATE DATABASE IF NOT EXISTS _vt",
		"SUBCREATE TABLE IF NOT EXISTS _vt.reparent_journal",
		"SUBINSERT INTO _vt.reparent_journal (time_created_ns, action_name, master_alias, replication_position) VALUES",
	}

	tablet2.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE RESET ALL REPLICATION",
		"FAKE SET SLAVE POSITION",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	tablet2.FakeMysqlDaemon.SetMasterInput = fmt.Sprintf("%v:%v", tablet1.Tablet.Hostname, tablet1.Tablet.MysqlPort)

	tablet3.FakeMysqlDaemon.ExpectedExecuteSuperQueryList = []string{
		"FAKE RESET ALL REPLICATION",
		"FAKE SET SLAVE POSITION",
		"FAKE SET MASTER",
		"START SLAVE",
	}
	tablet3.FakeMysqlDaemon.SetMasterInput = fmt.Sprintf("%v:%v", tablet1.Tablet.Hostname, tablet1.Tablet.MysqlPort)

	for _, tablet := range []*testlib.FakeTablet{tablet1, tablet2, tablet3} {
		tablet.StartActionLoop(t, wr)
		defer tablet.StopActionLoop(t)

		tablet.TM.QueryServiceControl.(*tabletservermock.Controller).SetQueryServiceEnabledForTests(true)
	}

	vtctld := grpcvtctldserver.NewVtctldServer(ts)
	_, err := vtctld.InitShardPrimary(context.Background(), &vtctldatapb.InitShardPrimaryRequest{
		Keyspace:                tablet1.Tablet.Keyspace,
		Shard:                   tablet1.Tablet.Shard,
		PrimaryElectTabletAlias: tablet1.Tablet.Alias,
	})

	assert.Error(t, err)

	resp, err := vtctld.InitShardPrimary(context.Background(), &vtctldatapb.InitShardPrimaryRequest{
		Keyspace:                tablet1.Tablet.Keyspace,
		Shard:                   tablet1.Tablet.Shard,
		PrimaryElectTabletAlias: tablet1.Tablet.Alias,
		Force:                   true,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	tablet1PostInit, err := ts.GetTablet(context.Background(), tablet1.Tablet.Alias)
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_MASTER, tablet1PostInit.Type)
}
