/*
Copyright 2021 The Vitess Authors.

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

package controller

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/orchestrator/inst"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
	"vitess.io/vitess/go/vt/vtgr/config"
	"vitess.io/vitess/go/vt/vtgr/db"
)

const diagnoseGroupSize = 2

var (
	testHost, _ = os.Hostname()
	alias0      = "test_cell-0000000000"
	alias1      = "test_cell-0000000001"
	alias2      = "test_cell-0000000002"
	testPort0   = 17000
	testPort1   = 17001
	testPort2   = 17002
)

type testGroupInput struct {
	groupName  string
	readOnly   bool
	groupState []db.TestGroupState
	gtid       mysql.GTIDSet
}

func TestShardIsHealthy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	ts := memorytopo.NewServer("test_cell")
	defer ts.Close()
	ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{})
	ts.CreateShard(ctx, "ks", "0")
	tmc := NewMockGRTmcClient(ctrl)
	dbAgent := db.NewMockAgent(ctrl)
	tablet1 := buildTabletInfo(uint32(testPort0), testHost, testPort0, topodatapb.TabletType_MASTER, time.Now())
	tablet2 := buildTabletInfo(uint32(testPort1), testHost, testPort1, topodatapb.TabletType_SPARE, time.Time{})
	tablet3 := buildTabletInfo(uint32(testPort2), testHost, testPort2, topodatapb.TabletType_REPLICA, time.Time{})
	testutil.AddTablet(ctx, t, ts, tablet1.Tablet, nil)
	testutil.AddTablet(ctx, t, ts, tablet2.Tablet, nil)
	testutil.AddTablet(ctx, t, ts, tablet3.Tablet, nil)
	dbAgent.
		EXPECT().
		FetchGroupView(gomock.Any(), gomock.Any()).
		DoAndReturn(func(alias string, target *inst.InstanceKey) (*db.GroupView, error) {
			return db.BuildGroupView(alias, "group", testHost, testPort0, false, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}), nil
		}).
		AnyTimes()
	tmc.EXPECT().Ping(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	cfg := &config.VTGRConfig{GroupSize: 3, MinNumReplica: 2, BackoffErrorWaitTimeSeconds: 1, BootstrapWaitTimeSeconds: 1}
	shard := NewGRShard("ks", "0", nil, tmc, ts, dbAgent, cfg, testPort0)
	shard.refreshTabletsInShardLocked(ctx)
	diagnose, _ := shard.Diagnose(ctx)
	assert.Equal(t, DiagnoseTypeHealthy, string(diagnose))
}

func TestTabletIssueDiagnoses(t *testing.T) {
	type data struct {
		pingable bool
		ttype    topodatapb.TabletType
	}
	var tablettests = []struct {
		name       string
		expected   DiagnoseType
		errMessage string
		inputs     []data
	}{
		{name: "healthy shard", expected: DiagnoseTypeHealthy, errMessage: "", inputs: []data{
			{true, topodatapb.TabletType_MASTER},
			{true, topodatapb.TabletType_REPLICA},
			{true, topodatapb.TabletType_REPLICA},
		}},
		{name: "non primary tablet is not pingable", expected: DiagnoseTypeHealthy, errMessage: "", inputs: []data{ // vtgr should do nothing
			{true, topodatapb.TabletType_MASTER},
			{false, topodatapb.TabletType_REPLICA},
			{false, topodatapb.TabletType_REPLICA},
		}},
		{name: "primary tablet is not pingable", expected: DiagnoseTypeUnreachablePrimary, errMessage: "", inputs: []data{ // vtgr should trigger a failover
			{false, topodatapb.TabletType_MASTER},
			{true, topodatapb.TabletType_REPLICA},
			{true, topodatapb.TabletType_REPLICA},
		}},
		{name: "no primary tablet", expected: DiagnoseTypeWrongPrimaryTablet, errMessage: "", inputs: []data{ // vtgr should create one based on mysql
			{true, topodatapb.TabletType_REPLICA},
			{true, topodatapb.TabletType_REPLICA},
			{true, topodatapb.TabletType_REPLICA},
		}},
		{name: "mysql and vttablet has different primary", expected: DiagnoseTypeWrongPrimaryTablet, errMessage: "", inputs: []data{ // vtgr should fix vttablet
			{true, topodatapb.TabletType_REPLICA},
			{true, topodatapb.TabletType_MASTER},
			{true, topodatapb.TabletType_REPLICA},
		}},
		{name: "unreachable wrong vttablet primary", expected: DiagnoseTypeWrongPrimaryTablet, errMessage: "", inputs: []data{ // vtgr should fix vttablet
			{true, topodatapb.TabletType_REPLICA},
			{false, topodatapb.TabletType_MASTER},
			{true, topodatapb.TabletType_REPLICA},
		}},
		{name: "unreachable uninitialized primary vttablet", expected: DiagnoseTypeUnreachablePrimary, errMessage: "", inputs: []data{ // vtgr should failover
			{false, topodatapb.TabletType_REPLICA},
			{true, topodatapb.TabletType_REPLICA},
			{true, topodatapb.TabletType_REPLICA},
		}},
	}
	for _, tt := range tablettests {
		t.Run(tt.name, func(t *testing.T) {
			expected := tt.expected
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			ts := NewMockGRTopo(ctrl)
			tmc := NewMockGRTmcClient(ctrl)
			dbAgent := db.NewMockAgent(ctrl)
			tablets := make(map[string]*topo.TabletInfo)
			for i, input := range tt.inputs {
				id := uint32(testPort0 + i)
				tablet := buildTabletInfo(id, testHost, testPort0+i, input.ttype, time.Now())
				tablets[fmt.Sprintf("cell-%d", id)] = tablet
				var response = struct {
					pingable bool
				}{input.pingable}
				dbAgent.
					EXPECT().
					FetchGroupView(gomock.Any(), gomock.Any()).
					DoAndReturn(func(alias string, target *inst.InstanceKey) (*db.GroupView, error) {
						if target.Hostname == "" || target.Port == 0 {
							return nil, errors.New("invalid mysql instance key")
						}
						return db.BuildGroupView(alias, "group", testHost, testPort0, false, []db.TestGroupState{
							{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
							{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
							{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
						}), nil
					}).
					AnyTimes()
				tmc.
					EXPECT().
					Ping(gomock.Any(), &topodatapb.Tablet{
						Alias:               tablet.Alias,
						Hostname:            tablet.Hostname,
						Keyspace:            tablet.Keyspace,
						Shard:               tablet.Shard,
						Type:                tablet.Type,
						Tags:                tablet.Tags,
						MysqlHostname:       tablet.MysqlHostname,
						MysqlPort:           tablet.MysqlPort,
						MasterTermStartTime: tablet.MasterTermStartTime,
					}).
					DoAndReturn(func(_ context.Context, t *topodatapb.Tablet) error {
						if !response.pingable {
							return errors.New("unreachable")
						}
						return nil
					}).
					AnyTimes()
			}
			ts.
				EXPECT().
				GetTabletMapForShardByCell(gomock.Any(), gomock.Eq("ks"), gomock.Eq("0"), gomock.Any()).
				Return(tablets, nil)

			ctx := context.Background()
			cfg := &config.VTGRConfig{GroupSize: diagnoseGroupSize, MinNumReplica: 2, BackoffErrorWaitTimeSeconds: 1, BootstrapWaitTimeSeconds: 1}
			shard := NewGRShard("ks", "0", nil, tmc, ts, dbAgent, cfg, testPort0)
			shard.refreshTabletsInShardLocked(ctx)
			diagnose, err := shard.Diagnose(ctx)
			assert.Equal(t, expected, diagnose)
			if tt.errMessage == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.True(t, strings.Contains(err.Error(), tt.errMessage), err.Error())
			}
		})
	}
}

func TestMysqlIssueDiagnoses(t *testing.T) {
	cfg := &config.VTGRConfig{GroupSize: diagnoseGroupSize, MinNumReplica: 2, BackoffErrorWaitTimeSeconds: 1, BootstrapWaitTimeSeconds: 1}
	disableProtectionCfg := &config.VTGRConfig{GroupSize: diagnoseGroupSize, MinNumReplica: 2, DisableReadOnlyProtection: true, BackoffErrorWaitTimeSeconds: 1, BootstrapWaitTimeSeconds: 1}
	type data struct {
		alias      string
		groupName  string
		readOnly   bool
		groupInput []db.TestGroupState
		ttype      topodatapb.TabletType
	}
	var sqltests = []struct {
		name          string
		expected      DiagnoseType
		errMessage    string
		config        *config.VTGRConfig
		inputs        []data
		removeTablets []string // to simulate missing tablet in topology
	}{
		{name: "healthy shard", expected: DiagnoseTypeHealthy, errMessage: "", inputs: []data{
			{alias0, "group", false, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_MASTER},
			{alias1, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias2, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
		}},
		{name: "recovering primary shard", expected: DiagnoseTypeBackoffError, errMessage: "", inputs: []data{
			{alias0, "group", false, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "RECOVERING", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_MASTER},
			{alias1, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "RECOVERING", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias2, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "RECOVERING", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
		}},
		{name: "no group in shard", expected: DiagnoseTypeShardHasNoGroup, errMessage: "", inputs: []data{
			{alias0, "", true, []db.TestGroupState{
				{MemberHost: "", MemberPort: "", MemberState: "OFFLINE", MemberRole: ""},
			}, topodatapb.TabletType_REPLICA},
			{alias1, "", true, []db.TestGroupState{
				{MemberHost: "", MemberPort: "", MemberState: "OFFLINE", MemberRole: ""},
			}, topodatapb.TabletType_REPLICA},
			{alias2, "", true, []db.TestGroupState{
				{MemberHost: "", MemberPort: "", MemberState: "OFFLINE", MemberRole: ""},
			}, topodatapb.TabletType_REPLICA},
		}},
		{name: "unreachable node", expected: DiagnoseTypeBackoffError, errMessage: "", inputs: []data{
			{alias0, "group", false, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "UNREACHABLE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_MASTER},
			{alias1, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "UNREACHABLE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias2, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "UNREACHABLE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
		}},
		{name: "mysql and tablet has different primary", expected: DiagnoseTypeWrongPrimaryTablet, errMessage: "", inputs: []data{ // vtgr should failover vttablet
			{alias0, "group", false, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias1, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_MASTER},
			{alias2, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
		}},
		{name: "mysql primary out of topology", expected: DiagnoseTypeUnreachablePrimary, errMessage: "", inputs: []data{ // vtgr should failover mysql
			{alias0, "group", false, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias1, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_MASTER},
			{alias2, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
		}, removeTablets: []string{alias0}},
		{name: "one error node", expected: DiagnoseTypeUnconnectedReplica, errMessage: "", inputs: []data{
			{alias0, "group", false, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ERROR", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_MASTER},
			{alias1, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ERROR", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias2, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ERROR", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
		}},
		{name: "two error node", expected: DiagnoseTypeInsufficientGroupSize, errMessage: "", inputs: []data{
			{alias0, "group", false, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ERROR", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ERROR", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_MASTER},
			{alias1, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ERROR", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias2, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ERROR", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
		}},
		{name: "insufficient group member", expected: DiagnoseTypeInsufficientGroupSize, errMessage: "", inputs: []data{
			{alias0, "group", false, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_MASTER},
			{alias1, "group", true, []db.TestGroupState{}, topodatapb.TabletType_REPLICA},
			{alias2, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
		}},
		{name: "unconnected node", expected: DiagnoseTypeBackoffError, errMessage: "", inputs: []data{
			{alias0, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "UNREACHABLE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_MASTER},
			{alias1, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "UNREACHABLE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias2, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "UNREACHABLE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
		}},
		{name: "unreachable primary", expected: DiagnoseTypeBackoffError, errMessage: "", inputs: []data{
			{alias0, "group", false, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "UNREACHABLE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_MASTER},
			{alias1, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "UNREACHABLE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias2, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "UNREACHABLE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
		}},
		{name: "more than one group name", expected: DiagnoseTypeError, errMessage: "fail to refreshSQLGroup: group has split brain", inputs: []data{ // vtgr should raise error
			{alias0, "group", false, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_MASTER},
			{alias1, "group_xxx", false, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "PRIMARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias2, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
		}},
		{name: "different primary", expected: DiagnoseTypeError, errMessage: "fail to refreshSQLGroup: group has split brain", inputs: []data{ // vtgr should raise error
			{alias0, "group", false, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_MASTER},
			{alias1, "group", false, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias2, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
		}},
		{name: "insufficient members in group", expected: DiagnoseTypeInsufficientGroupSize, errMessage: "", inputs: []data{
			{alias0, "group", false, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_MASTER},
			{alias1, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias2, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ERROR", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
		}},
		// the shard has insufficient member, but the primary is already read_only
		// we should try to connect the replica node
		{name: "insufficient members in read only shard", expected: DiagnoseTypeUnconnectedReplica, errMessage: "", inputs: []data{
			{alias0, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_MASTER},
			{alias1, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias2, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ERROR", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
		}},
		{name: "insufficient members in group with disable read only protection", expected: DiagnoseTypeUnconnectedReplica, errMessage: "", config: disableProtectionCfg, inputs: []data{
			{alias0, "group", false, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_MASTER},
			{alias1, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias2, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ERROR", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
		}},
		{name: "read only with disable read only protection", expected: DiagnoseTypeReadOnlyShard, errMessage: "", config: disableProtectionCfg, inputs: []data{
			{alias0, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_MASTER},
			{alias1, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias2, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ERROR", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
		}},
		{name: "read only healthy shard", expected: DiagnoseTypeReadOnlyShard, errMessage: "", inputs: []data{
			{alias0, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_MASTER},
			{alias1, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias2, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "ONLINE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "ONLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
		}},
		{name: "network partition", expected: DiagnoseTypeBackoffError, errMessage: "", inputs: []data{
			{alias0, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort0), MemberState: "ONLINE", MemberRole: "PRIMARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "UNREACHABLE", MemberRole: "SECONDARY"},
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "UNREACHABLE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_MASTER},
			{alias1, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "OFFLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias2, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort2), MemberState: "OFFLINE", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
		}},
		{name: "start bootstrap in progress", expected: DiagnoseTypeBootstrapBackoff, errMessage: "", inputs: []data{
			{alias0, "group", true, []db.TestGroupState{
				{MemberHost: testHost, MemberPort: strconv.Itoa(testPort1), MemberState: "RECOVERING", MemberRole: "SECONDARY"},
			}, topodatapb.TabletType_REPLICA},
			{alias1, "", true, []db.TestGroupState{}, topodatapb.TabletType_REPLICA},
			{alias2, "", true, []db.TestGroupState{
				{MemberHost: "", MemberPort: "", MemberState: "OFFLINE", MemberRole: ""},
			}, topodatapb.TabletType_REPLICA},
		}},
	}
	for _, tt := range sqltests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			ts := NewMockGRTopo(ctrl)
			tmc := NewMockGRTmcClient(ctrl)
			dbAgent := db.NewMockAgent(ctrl)
			tablets := make(map[string]*topo.TabletInfo)
			expected := tt.expected
			inputMap := make(map[string]testGroupInput)
			if tt.config == nil {
				tt.config = cfg
			}
			conf := tt.config
			for i, input := range tt.inputs {
				id := uint32(testPort0 + i)
				tablet := buildTabletInfo(id, testHost, testPort0+i, input.ttype, time.Now())
				tablets[input.alias] = tablet
				inputMap[input.alias] = testGroupInput{
					input.groupName,
					input.readOnly,
					input.groupInput,
					nil,
				}
				dbAgent.
					EXPECT().
					FetchGroupView(gomock.Any(), gomock.Any()).
					DoAndReturn(func(alias string, target *inst.InstanceKey) (*db.GroupView, error) {
						if target.Hostname == "" || target.Port == 0 {
							return nil, errors.New("invalid mysql instance key")
						}
						s := inputMap[alias]
						view := db.BuildGroupView(alias, s.groupName, target.Hostname, target.Port, s.readOnly, s.groupState)
						return view, nil
					}).
					AnyTimes()
			}
			for _, tid := range tt.removeTablets {
				delete(tablets, tid)
			}
			ts.
				EXPECT().
				GetTabletMapForShardByCell(gomock.Any(), gomock.Eq("ks"), gomock.Eq("0"), gomock.Any()).
				Return(tablets, nil)
			tmc.EXPECT().Ping(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

			ctx := context.Background()
			shard := NewGRShard("ks", "0", nil, tmc, ts, dbAgent, conf, testPort0)
			shard.refreshTabletsInShardLocked(ctx)
			diagnose, err := shard.Diagnose(ctx)
			assert.Equal(t, expected, diagnose)
			if tt.errMessage == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.True(t, strings.Contains(err.Error(), tt.errMessage), err.Error())
			}
		})
	}
}

func TestGroupStatusRecorder(t *testing.T) {
	r := &groupGTIDRecorder{}

	err := r.recordGroupStatus("group1", true)
	assert.NoError(t, err)
	assert.Equal(t, r.name, "group1")
	assert.Equal(t, r.hasActive, true)

	err = r.recordGroupStatus("group2", false)
	assert.Error(t, err, "group has more than one group name")
	assert.Equal(t, r.name, "group1")

	err = r.recordGroupStatus("group1", false)
	assert.NoError(t, err)
	assert.Equal(t, r.name, "group1")
	assert.Equal(t, r.hasActive, true)

	pos1, err := mysql.ParsePosition(mysql.Mysql56FlavorID, "264a8230-67d2-11eb-acdd-0a8d91f24125:1-22:1000019-1000021")
	assert.NoError(t, err)
	inst1 := &grInstance{alias: "alias1"}
	r.recordGroupGTIDs(pos1.GTIDSet, inst1)
	pos2, err := mysql.ParsePosition(mysql.Mysql56FlavorID, "264a8230-67d2-11eb-acdd-0a8d91f24125:1-1000021")
	assert.NoError(t, err)
	inst2 := &grInstance{alias: "alias2"}
	r.recordGroupGTIDs(pos2.GTIDSet, inst2)
	assert.Equal(t, len(r.gtidWithInstances), 2)
	assert.Equal(t, r.gtidWithInstances[0].instance, inst1)
	assert.Equal(t, pos1.GTIDSet.Equal(r.gtidWithInstances[0].gtids), true)
	assert.Equal(t, r.gtidWithInstances[1].instance, inst2)
	assert.Equal(t, pos2.GTIDSet.Equal(r.gtidWithInstances[1].gtids), true)
}
