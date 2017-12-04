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

package tabletmanager

import (
	"errors"
	"flag"
	"fmt"
	"html/template"
	"reflect"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/binlog/binlogplayer"
	"github.com/youtube/vitess/go/vt/health"
	"github.com/youtube/vitess/go/vt/mysqlctl/fakemysqldaemon"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver"
	"github.com/youtube/vitess/go/vt/vttablet/tabletservermock"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestHealthRecordDeduplication(t *testing.T) {
	now := time.Now()
	later := now.Add(5 * time.Minute)
	cases := []struct {
		left, right *HealthRecord
		duplicate   bool
	}{
		{
			left:      &HealthRecord{Time: now},
			right:     &HealthRecord{Time: later},
			duplicate: true,
		},
		{
			left:      &HealthRecord{Time: now, Error: errors.New("foo")},
			right:     &HealthRecord{Time: now, Error: errors.New("foo")},
			duplicate: true,
		},
		{
			left:      &HealthRecord{Time: now, ReplicationDelay: defaultDegradedThreshold / 2},
			right:     &HealthRecord{Time: later, ReplicationDelay: defaultDegradedThreshold / 3},
			duplicate: true,
		},
		{
			left:      &HealthRecord{Time: now, ReplicationDelay: defaultDegradedThreshold / 2},
			right:     &HealthRecord{Time: later, ReplicationDelay: defaultDegradedThreshold * 2},
			duplicate: false,
		},
		{
			left:      &HealthRecord{Time: now, Error: errors.New("foo"), ReplicationDelay: defaultDegradedThreshold * 2},
			right:     &HealthRecord{Time: later, ReplicationDelay: defaultDegradedThreshold * 2},
			duplicate: false,
		},
	}

	for _, c := range cases {
		if got := c.left.IsDuplicate(c.right); got != c.duplicate {
			t.Errorf("IsDuplicate %v and %v: got %v, want %v", c.left, c.right, got, c.duplicate)
		}
	}
}

func TestHealthRecordClass(t *testing.T) {
	cases := []struct {
		r     *HealthRecord
		state string
	}{
		{
			r:     &HealthRecord{},
			state: "healthy",
		},
		{
			r:     &HealthRecord{Error: errors.New("foo")},
			state: "unhealthy",
		},
		{
			r:     &HealthRecord{ReplicationDelay: defaultDegradedThreshold * 2},
			state: "unhappy",
		},
		{
			r:     &HealthRecord{ReplicationDelay: defaultDegradedThreshold / 2},
			state: "healthy",
		},
	}

	for _, c := range cases {
		if got := c.r.Class(); got != c.state {
			t.Errorf("class of %v: got %v, want %v", c.r, got, c.state)
		}
	}
}

var tabletAlias = &topodatapb.TabletAlias{Cell: "cell1", Uid: 42}

// fakeHealthCheck implements health.Reporter interface
type fakeHealthCheck struct {
	reportReplicationDelay time.Duration
	reportError            error
}

func (fhc *fakeHealthCheck) Report(isSlaveType, shouldQueryServiceBeRunning bool) (replicationDelay time.Duration, err error) {
	return fhc.reportReplicationDelay, fhc.reportError
}

func (fhc *fakeHealthCheck) HTMLName() template.HTML {
	return template.HTML("fakeHealthCheck")
}

func createTestAgent(ctx context.Context, t *testing.T, preStart func(*ActionAgent)) (*ActionAgent, chan<- *binlogplayer.VtClientMock) {
	ts := memorytopo.NewServer("cell1")

	if err := ts.CreateKeyspace(ctx, "test_keyspace", &topodatapb.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}

	if err := ts.CreateShard(ctx, "test_keyspace", "0"); err != nil {
		t.Fatalf("CreateShard failed: %v", err)
	}

	port := int32(1234)
	tablet := &topodatapb.Tablet{
		Alias:    tabletAlias,
		Hostname: "host",
		PortMap: map[string]int32{
			"vt": port,
		},
		Keyspace: "test_keyspace",
		Shard:    "0",
		Type:     topodatapb.TabletType_REPLICA,
	}
	if err := ts.CreateTablet(ctx, tablet); err != nil {
		t.Fatalf("CreateTablet failed: %v", err)
	}

	mysqlDaemon := &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: 3306}
	agent := NewTestActionAgent(ctx, ts, tabletAlias, port, 0, mysqlDaemon, preStart)

	vtClientMocksChannel := make(chan *binlogplayer.VtClientMock, 1)
	agent.BinlogPlayerMap = NewBinlogPlayerMap(ts, mysqlDaemon, func() binlogplayer.VtClient {
		return <-vtClientMocksChannel
	})

	agent.HealthReporter = &fakeHealthCheck{}

	return agent, vtClientMocksChannel
}

// TestHealthCheckControlsQueryService verifies that a tablet going healthy
// starts the query service, and going unhealthy stops it.
func TestHealthCheckControlsQueryService(t *testing.T) {
	// we need an actual grace period set, so lameduck is enabled
	*gracePeriod = 10 * time.Millisecond
	defer func() {
		*gracePeriod = 0
	}()

	ctx := context.Background()
	agent, _ := createTestAgent(ctx, t, nil)

	/// Consume the first health broadcast triggered by ActionAgent.Start():
	//  (REPLICA, NOT_SERVING) goes to (REPLICA, SERVING). And we
	//  should be serving.
	if _, err := expectBroadcastData(agent.QueryServiceControl, true, "healthcheck not run yet", 0); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChange(agent.QueryServiceControl, true, topodatapb.TabletType_REPLICA); err != nil {
		t.Fatal(err)
	}
	if !agent.QueryServiceControl.IsServing() {
		t.Errorf("Query service should be running")
	}
	if !agent.UpdateStream.IsEnabled() {
		t.Errorf("UpdateStream should be running")
	}

	// first health check, should keep us as replica and serving,
	// and update the mysql port to 3306
	before := time.Now()
	agent.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 12 * time.Second
	agent.runHealthCheck()
	ti, err := agent.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_REPLICA {
		t.Errorf("First health check failed to go to replica: %v", ti.Type)
	}
	if port := topoproto.MysqlPort(ti.Tablet); port != 3306 {
		t.Errorf("First health check failed to update mysql port: %v", port)
	}
	if !agent.gotMysqlPort {
		t.Errorf("Healthcheck didn't record it updated the MySQL port.")
	}
	if !agent.QueryServiceControl.IsServing() {
		t.Errorf("Query service should be running")
	}
	if !agent.UpdateStream.IsEnabled() {
		t.Errorf("UpdateStream should be running")
	}
	if agent._healthyTime.Sub(before) < 0 {
		t.Errorf("runHealthCheck did not update agent._healthyTime")
	}
	if agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType != topodatapb.TabletType_REPLICA {
		t.Errorf("invalid tabletserver target: %v", agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType)
	}
	if _, err := expectBroadcastData(agent.QueryServiceControl, true, "", 12); err != nil {
		t.Fatal(err)
	}

	// now make the tablet unhealthy
	agent.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 13 * time.Second
	agent.HealthReporter.(*fakeHealthCheck).reportError = fmt.Errorf("tablet is unhealthy")
	before = time.Now()
	agent.runHealthCheck()
	ti, err = agent.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_REPLICA {
		t.Errorf("Unhappy health check failed to stay as replica: %v", ti.Type)
	}
	if agent.QueryServiceControl.IsServing() {
		t.Errorf("Query service should not be running")
	}
	if agent.UpdateStream.IsEnabled() {
		t.Errorf("UpdateStream should not be running")
	}
	if agent._healthyTime.Sub(before) < 0 {
		t.Errorf("runHealthCheck did not update agent._healthyTime")
	}
	if got := agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType; got != topodatapb.TabletType_REPLICA {
		t.Errorf("invalid tabletserver target: got = %v, want = %v", got, topodatapb.TabletType_REPLICA)
	}

	// first we get the lameduck broadcast, with no error and old
	// replication delay
	if _, err := expectBroadcastData(agent.QueryServiceControl, false, "", 12); err != nil {
		t.Fatal(err)
	}

	// then query service is disabled since we are unhealthy now.
	if err := expectStateChange(agent.QueryServiceControl, false, topodatapb.TabletType_REPLICA); err != nil {
		t.Fatal(err)
	}

	// and the associated broadcast
	if _, err := expectBroadcastData(agent.QueryServiceControl, false, "tablet is unhealthy", 13); err != nil {
		t.Fatal(err)
	}

	// and nothing more.
	if err := expectBroadcastDataEmpty(agent.QueryServiceControl); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChangesEmpty(agent.QueryServiceControl); err != nil {
		t.Fatal(err)
	}
}

// TestErrSlaveNotRunningIsHealthy verifies that a tablet whose
// healthcheck reports health.ErrSlaveNotRunning is still considered
// healthy with high replication lag.
func TestErrSlaveNotRunningIsHealthy(t *testing.T) {
	*unhealthyThreshold = 10 * time.Minute
	ctx := context.Background()
	agent, _ := createTestAgent(ctx, t, nil)

	/// Consume the first health broadcast triggered by ActionAgent.Start():
	//  (REPLICA, NOT_SERVING) goes to (REPLICA, SERVING). And we
	//  should be serving.
	if _, err := expectBroadcastData(agent.QueryServiceControl, true, "healthcheck not run yet", 0); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChange(agent.QueryServiceControl, true, topodatapb.TabletType_REPLICA); err != nil {
		t.Fatal(err)
	}
	if !agent.QueryServiceControl.IsServing() {
		t.Errorf("Query service should be running")
	}
	if !agent.UpdateStream.IsEnabled() {
		t.Errorf("UpdateStream should be running")
	}

	// health check returning health.ErrSlaveNotRunning, should
	// keep us as replica and serving
	before := time.Now()
	agent.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 12 * time.Second
	agent.HealthReporter.(*fakeHealthCheck).reportError = health.ErrSlaveNotRunning
	agent.runHealthCheck()
	if !agent.QueryServiceControl.IsServing() {
		t.Errorf("Query service should be running")
	}
	if !agent.UpdateStream.IsEnabled() {
		t.Errorf("UpdateStream should be running")
	}
	if agent._healthyTime.Sub(before) < 0 {
		t.Errorf("runHealthCheck did not update agent._healthyTime")
	}
	if agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType != topodatapb.TabletType_REPLICA {
		t.Errorf("invalid tabletserver target: %v", agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType)
	}
	if _, err := expectBroadcastData(agent.QueryServiceControl, true, "", 10*60); err != nil {
		t.Fatal(err)
	}

	// and nothing more.
	if err := expectBroadcastDataEmpty(agent.QueryServiceControl); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChangesEmpty(agent.QueryServiceControl); err != nil {
		t.Fatal(err)
	}
}

// TestQueryServiceNotStarting verifies that if a tablet cannot start the
// query service, it should not go healthy.
func TestQueryServiceNotStarting(t *testing.T) {
	ctx := context.Background()
	agent, _ := createTestAgent(ctx, t, func(a *ActionAgent) {
		// The SetServingType that will fail is part of Start()
		// so we have to do this here.
		a.QueryServiceControl.(*tabletservermock.Controller).SetServingTypeError = fmt.Errorf("test cannot start query service")
	})

	// we should not be serving.
	if agent.QueryServiceControl.IsServing() {
		t.Errorf("Query service should not be running")
	}
	if agent.UpdateStream.IsEnabled() {
		t.Errorf("UpdateStream should not be running")
	}

	// There is no broadcast data to consume, we're just not
	// healthy from startup

	// Now we can run another health check, it will stay unhealthy forever.
	before := time.Now()
	agent.runHealthCheck()
	ti, err := agent.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_REPLICA {
		t.Errorf("Happy health check which cannot start query service should stay replica: %v", ti.Type)
	}
	if agent.QueryServiceControl.IsServing() {
		t.Errorf("Query service should not be running")
	}
	if agent.UpdateStream.IsEnabled() {
		t.Errorf("UpdateStream should not be running")
	}
	if agent._healthyTime.Sub(before) < 0 {
		t.Errorf("runHealthCheck did not update agent._healthyTime")
	}
	bd := <-agent.QueryServiceControl.(*tabletservermock.Controller).BroadcastData
	if bd.RealtimeStats.HealthError != "test cannot start query service" {
		t.Errorf("unexpected HealthError: %v", *bd)
	}
	if agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType != topodatapb.TabletType_REPLICA {
		t.Errorf("invalid tabletserver target: %v", agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType)
	}

	if err := expectBroadcastDataEmpty(agent.QueryServiceControl); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChangesEmpty(agent.QueryServiceControl); err != nil {
		t.Fatal(err)
	}
}

// TestQueryServiceStopped verifies that if a healthy tablet's query
// service is shut down, the tablet goes unhealthy
func TestQueryServiceStopped(t *testing.T) {
	ctx := context.Background()
	agent, _ := createTestAgent(ctx, t, nil)

	/// Consume the first health broadcast triggered by ActionAgent.Start():
	//  (REPLICA, NOT_SERVING) goes to (REPLICA, SERVING). And we
	//  should be serving.
	if _, err := expectBroadcastData(agent.QueryServiceControl, true, "healthcheck not run yet", 0); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChange(agent.QueryServiceControl, true, topodatapb.TabletType_REPLICA); err != nil {
		t.Fatal(err)
	}
	if !agent.QueryServiceControl.IsServing() {
		t.Errorf("Query service should be running")
	}
	if !agent.UpdateStream.IsEnabled() {
		t.Errorf("UpdateStream should be running")
	}

	// first health check, should keep us in replica / healthy
	before := time.Now()
	agent.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 14 * time.Second
	agent.runHealthCheck()
	ti, err := agent.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_REPLICA {
		t.Errorf("First health check failed to stay in replica: %v", ti.Type)
	}
	if !agent.QueryServiceControl.IsServing() {
		t.Errorf("Query service should be running")
	}
	if !agent.UpdateStream.IsEnabled() {
		t.Errorf("UpdateStream should be running")
	}
	if agent._healthyTime.Sub(before) < 0 {
		t.Errorf("runHealthCheck did not update agent._healthyTime")
	}
	want := topodatapb.TabletType_REPLICA
	if got := agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType; got != want {
		t.Errorf("invalid tabletserver target: got = %v, want = %v", got, want)
	}

	if _, err := expectBroadcastData(agent.QueryServiceControl, true, "", 14); err != nil {
		t.Fatal(err)
	}

	// shut down query service and prevent it from starting again
	// (this is to simulate mysql going away, tablet server detecting it
	// and shutting itself down). Intercept the message
	agent.QueryServiceControl.SetServingType(topodatapb.TabletType_REPLICA, false, nil)
	agent.QueryServiceControl.(*tabletservermock.Controller).SetServingTypeError = fmt.Errorf("test cannot start query service")
	if err := expectStateChange(agent.QueryServiceControl, false, topodatapb.TabletType_REPLICA); err != nil {
		t.Fatal(err)
	}

	// health check should now fail
	before = time.Now()
	agent.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 15 * time.Second
	agent.runHealthCheck()
	ti, err = agent.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_REPLICA {
		t.Errorf("Happy health check which cannot start query service should stay replica: %v", ti.Type)
	}
	if agent.QueryServiceControl.IsServing() {
		t.Errorf("Query service should not be running")
	}
	if agent.UpdateStream.IsEnabled() {
		t.Errorf("UpdateStream should not be running")
	}
	if agent._healthyTime.Sub(before) < 0 {
		t.Errorf("runHealthCheck did not update agent._healthyTime")
	}
	want = topodatapb.TabletType_REPLICA
	if got := agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType; got != want {
		t.Errorf("invalid tabletserver target: got = %v, want = %v", got, want)
	}
	if _, err := expectBroadcastData(agent.QueryServiceControl, false, "test cannot start query service", 15); err != nil {
		t.Fatal(err)
	}
	// NOTE: No more broadcasts or state changes since SetServingTypeError is set
	// on the mocked controller and this disables its SetServingType().

	if err := expectBroadcastDataEmpty(agent.QueryServiceControl); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChangesEmpty(agent.QueryServiceControl); err != nil {
		t.Fatal(err)
	}
}

// TestTabletControl verifies the shard's TabletControl record can disable
// query service in a tablet.
func TestTabletControl(t *testing.T) {
	ctx := context.Background()
	agent, _ := createTestAgent(ctx, t, nil)

	/// Consume the first health broadcast triggered by ActionAgent.Start():
	//  (REPLICA, NOT_SERVING) goes to (REPLICA, SERVING). And we
	//  should be serving.
	if _, err := expectBroadcastData(agent.QueryServiceControl, true, "healthcheck not run yet", 0); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChange(agent.QueryServiceControl, true, topodatapb.TabletType_REPLICA); err != nil {
		t.Fatal(err)
	}

	// first health check, should keep us in replica, just broadcast
	before := time.Now()
	agent.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 16 * time.Second
	agent.runHealthCheck()
	ti, err := agent.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_REPLICA {
		t.Errorf("First health check failed to go to replica: %v", ti.Type)
	}
	if !agent.QueryServiceControl.IsServing() {
		t.Errorf("Query service should be running")
	}
	if !agent.UpdateStream.IsEnabled() {
		t.Errorf("UpdateStream should be running")
	}
	if agent._healthyTime.Sub(before) < 0 {
		t.Errorf("runHealthCheck did not update agent._healthyTime")
	}
	if got := agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType; got != topodatapb.TabletType_REPLICA {
		t.Errorf("invalid tabletserver target: got = %v, want = %v", got, topodatapb.TabletType_REPLICA)
	}
	if _, err := expectBroadcastData(agent.QueryServiceControl, true, "", 16); err != nil {
		t.Fatal(err)
	}

	// now update the shard
	_, err = agent.TopoServer.UpdateShardFields(ctx, "test_keyspace", "0", func(si *topo.ShardInfo) error {
		si.TabletControls = []*topodatapb.Shard_TabletControl{
			{
				TabletType:          topodatapb.TabletType_REPLICA,
				DisableQueryService: true,
			},
		}
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateShardFields failed: %v", err)
	}

	// now refresh the tablet state, as the resharding process would do
	agent.RefreshState(ctx)

	// check we shutdown query service
	if agent.QueryServiceControl.IsServing() {
		t.Errorf("Query service should not be running")
	}

	// check UpdateStream is still running
	if !agent.UpdateStream.IsEnabled() {
		t.Errorf("UpdateStream should be running")
	}

	// Consume the health broadcast which was triggered due to the QueryService
	// state change from SERVING to NOT_SERVING.
	if _, err := expectBroadcastData(agent.QueryServiceControl, false, "", 16); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChange(agent.QueryServiceControl, false, topodatapb.TabletType_REPLICA); err != nil {
		t.Fatal(err)
	}

	// check running a health check will not start it again
	before = time.Now()
	agent.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 17 * time.Second
	agent.runHealthCheck()
	ti, err = agent.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_REPLICA {
		t.Errorf("Health check failed to go to replica: %v", ti.Type)
	}
	if agent.QueryServiceControl.IsServing() {
		t.Errorf("Query service should not be running")
	}
	if !agent.UpdateStream.IsEnabled() {
		t.Errorf("UpdateStream should be running")
	}
	if agent._healthyTime.Sub(before) < 0 {
		t.Errorf("runHealthCheck did not update agent._healthyTime")
	}
	if got := agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType; got != topodatapb.TabletType_REPLICA {
		t.Errorf("invalid tabletserver target: got = %v, want = %v", got, topodatapb.TabletType_REPLICA)
	}
	if _, err := expectBroadcastData(agent.QueryServiceControl, false, "", 17); err != nil {
		t.Fatal(err)
	}
	// NOTE: No state change here since nothing has changed.

	// go unhealthy, check we go to error state and QS is not running
	agent.HealthReporter.(*fakeHealthCheck).reportError = fmt.Errorf("tablet is unhealthy")
	agent.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 18 * time.Second
	before = time.Now()
	agent.runHealthCheck()
	ti, err = agent.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_REPLICA {
		t.Errorf("Unhealthy health check should stay replica: %v", ti.Type)
	}
	if agent.QueryServiceControl.IsServing() {
		t.Errorf("Query service should not be running")
	}
	if agent.UpdateStream.IsEnabled() {
		t.Errorf("UpdateStream should not be running")
	}
	if agent._healthyTime.Sub(before) < 0 {
		t.Errorf("runHealthCheck did not update agent._healthyTime")
	}
	if _, err := expectBroadcastData(agent.QueryServiceControl, false, "tablet is unhealthy", 18); err != nil {
		t.Fatal(err)
	}
	// NOTE: No state change here since QueryService is already NOT_SERVING.
	want := topodatapb.TabletType_REPLICA
	if got := agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType; got != want {
		t.Errorf("invalid tabletserver target: got = %v, want = %v", got, want)
	}

	// go back healthy, check QS is still not running
	agent.HealthReporter.(*fakeHealthCheck).reportError = nil
	agent.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 19 * time.Second
	before = time.Now()
	agent.runHealthCheck()
	ti, err = agent.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_REPLICA {
		t.Errorf("Healthy health check should go to replica: %v", ti.Type)
	}
	if agent.QueryServiceControl.IsServing() {
		t.Errorf("Query service should not be running")
	}
	if !agent.UpdateStream.IsEnabled() {
		t.Errorf("UpdateStream should be running")
	}
	if agent._healthyTime.Sub(before) < 0 {
		t.Errorf("runHealthCheck did not update agent._healthyTime")
	}
	if _, err := expectBroadcastData(agent.QueryServiceControl, false, "", 19); err != nil {
		t.Fatal(err)
	}
	if got := agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType; got != topodatapb.TabletType_REPLICA {
		t.Errorf("invalid tabletserver target: got = %v, want = %v", got, topodatapb.TabletType_REPLICA)
	}

	// now clear TabletControl, run health check, make sure we go
	// back healthy and serving.
	_, err = agent.TopoServer.UpdateShardFields(ctx, "test_keyspace", "0", func(si *topo.ShardInfo) error {
		si.TabletControls = nil
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateShardFields failed: %v", err)
	}

	// now refresh the tablet state, as the resharding process would do
	agent.RefreshState(ctx)

	// QueryService changed back from SERVING to NOT_SERVING since refreshTablet()
	// re-read the topology and saw that REPLICA is still not allowed to serve.
	if _, err := expectBroadcastData(agent.QueryServiceControl, true, "", 19); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChange(agent.QueryServiceControl, true, topodatapb.TabletType_REPLICA); err != nil {
		t.Fatal(err)
	}

	if err := expectBroadcastDataEmpty(agent.QueryServiceControl); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChangesEmpty(agent.QueryServiceControl); err != nil {
		t.Fatal(err)
	}
}

// TestQueryServiceChangeImmediateHealthcheckResponse verifies that a change
// of the QueryService state or the tablet type will result into a broadcast
// of a StreamHealthResponse message.
func TestStateChangeImmediateHealthBroadcast(t *testing.T) {
	// BinlogPlayer will fail in the second retry because we don't fully mock
	// it. Retry faster to make it fail faster.
	flag.Set("binlog_player_retry_delay", "100ms")

	ctx := context.Background()
	agent, vtClientMocksChannel := createTestAgent(ctx, t, nil)

	/// Consume the first health broadcast triggered by ActionAgent.Start():
	//  (REPLICA, NOT_SERVING) goes to (REPLICA, SERVING). And we
	//  should be serving.
	if _, err := expectBroadcastData(agent.QueryServiceControl, true, "healthcheck not run yet", 0); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChange(agent.QueryServiceControl, true, topodatapb.TabletType_REPLICA); err != nil {
		t.Fatal(err)
	}

	// Run health check to turn into a healthy replica
	agent.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 12 * time.Second
	agent.runHealthCheck()
	if !agent.QueryServiceControl.IsServing() {
		t.Errorf("Query service should be running")
	}
	if got := agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType; got != topodatapb.TabletType_REPLICA {
		t.Errorf("invalid tabletserver target: got = %v, want = %v", got, topodatapb.TabletType_REPLICA)
	}
	if _, err := expectBroadcastData(agent.QueryServiceControl, true, "", 12); err != nil {
		t.Fatal(err)
	}

	// Run TER to turn us into a proper master, wait for it to finish.
	agent.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 19 * time.Second
	if err := agent.TabletExternallyReparented(ctx, "unused_id"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-agent.finalizeReparentCtx.Done():
	}
	ti, err := agent.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_MASTER {
		t.Errorf("TER failed to go to master: %v", ti.Type)
	}
	if !agent.QueryServiceControl.IsServing() {
		t.Errorf("Query service should be running")
	}
	if got := agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType; got != topodatapb.TabletType_MASTER {
		t.Errorf("invalid tabletserver target: got = %v, want = %v", got, topodatapb.TabletType_MASTER)
	}

	// Consume the health broadcast (no replication delay as we are master)
	if _, err := expectBroadcastData(agent.QueryServiceControl, true, "", 0); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChange(agent.QueryServiceControl, true, topodatapb.TabletType_MASTER); err != nil {
		t.Fatal(err)
	}

	// Run health check to make sure we stay good
	agent.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 20 * time.Second
	agent.runHealthCheck()
	ti, err = agent.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_MASTER {
		t.Errorf("First health check failed to go to master: %v", ti.Type)
	}
	if !agent.QueryServiceControl.IsServing() {
		t.Errorf("Query service should be running")
	}
	if got := agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType; got != topodatapb.TabletType_MASTER {
		t.Errorf("invalid tabletserver target: got = %v, want = %v", got, topodatapb.TabletType_MASTER)
	}
	if _, err := expectBroadcastData(agent.QueryServiceControl, true, "", 20); err != nil {
		t.Fatal(err)
	}

	// Simulate a vertical split resharding where we set
	// SourceShards in the topo and enable filtered replication.
	_, err = agent.TopoServer.UpdateShardFields(ctx, "test_keyspace", "0", func(si *topo.ShardInfo) error {
		si.SourceShards = []*topodatapb.Shard_SourceShard{
			{
				Uid:      1,
				Keyspace: "source_keyspace",
				Shard:    "0",
				Tables: []string{
					"table1",
				},
			},
		}
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateShardFields failed: %v", err)
	}

	// Mock out the BinlogPlayer client. Tell the BinlogPlayer not to start.
	vtClientMock := binlogplayer.NewVtClientMock()
	vtClientMock.AddResult(&sqltypes.Result{
		Fields:       nil,
		RowsAffected: 1,
		InsertID:     0,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.NewVarBinary("MariaDB/0-1-1234"),
				sqltypes.NewVarBinary("DontStart"),
			},
		},
	})
	vtClientMocksChannel <- vtClientMock

	// Refresh the tablet state, as vtworker would do.
	// Since we change the QueryService state, we'll also trigger a health broadcast.
	agent.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 21 * time.Second
	agent.RefreshState(ctx)

	// (Destination) MASTER with enabled filtered replication mustn't serve anymore.
	if agent.QueryServiceControl.IsServing() {
		t.Errorf("Query service should not be running")
	}
	// Consume health broadcast sent out due to QueryService state change from
	// (MASTER, SERVING) to (MASTER, NOT_SERVING).
	// Since we didn't run healthcheck again yet, the broadcast data contains the
	// cached replication lag of 20 instead of 21.
	if bd, err := expectBroadcastData(agent.QueryServiceControl, false, "", 20); err == nil {
		if bd.RealtimeStats.BinlogPlayersCount != 1 {
			t.Fatalf("filtered replication must be enabled: %v", bd)
		}
	} else {
		t.Fatal(err)
	}
	if err := expectStateChange(agent.QueryServiceControl, false, topodatapb.TabletType_MASTER); err != nil {
		t.Fatal(err)
	}

	// Running a healthcheck won't put the QueryService back to SERVING.
	agent.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 22 * time.Second
	agent.runHealthCheck()
	ti, err = agent.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_MASTER {
		t.Errorf("Health check failed to go to replica: %v", ti.Type)
	}
	if agent.QueryServiceControl.IsServing() {
		t.Errorf("Query service should not be running")
	}
	if got := agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType; got != topodatapb.TabletType_MASTER {
		t.Errorf("invalid tabletserver target: got = %v, want = %v", got, topodatapb.TabletType_MASTER)
	}
	if bd, err := expectBroadcastData(agent.QueryServiceControl, false, "", 22); err == nil {
		if bd.RealtimeStats.BinlogPlayersCount != 1 {
			t.Fatalf("filtered replication must be still running: %v", bd)
		}
	} else {
		t.Fatal(err)
	}
	// NOTE: No state change here since nothing has changed.

	// Simulate migration to destination master i.e. remove SourceShards.
	_, err = agent.TopoServer.UpdateShardFields(ctx, "test_keyspace", "0", func(si *topo.ShardInfo) error {
		si.SourceShards = nil
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateShardFields failed: %v", err)
	}

	// Refresh the tablet state, as vtctl MigrateServedFrom would do.
	// This should also trigger a health broadcast since the QueryService state
	// changes from NOT_SERVING to SERVING.
	agent.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 23 * time.Second
	agent.RefreshState(ctx)

	// QueryService changed from NOT_SERVING to SERVING.
	if !agent.QueryServiceControl.IsServing() {
		t.Errorf("Query service should not be running")
	}
	// Since we didn't run healthcheck again yet, the broadcast data contains the
	// cached replication lag of 22 instead of 23.
	if bd, err := expectBroadcastData(agent.QueryServiceControl, true, "", 22); err == nil {
		if bd.RealtimeStats.BinlogPlayersCount != 0 {
			t.Fatalf("filtered replication must be disabled now: %v", bd)
		}
	} else {
		t.Fatal(err)
	}
	if err := expectStateChange(agent.QueryServiceControl, true, topodatapb.TabletType_MASTER); err != nil {
		t.Fatal(err)
	}

	if err := expectBroadcastDataEmpty(agent.QueryServiceControl); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChangesEmpty(agent.QueryServiceControl); err != nil {
		t.Fatal(err)
	}
}

// TestOldHealthCheck verifies that a healthcheck that is too old will
// return an error
func TestOldHealthCheck(t *testing.T) {
	ctx := context.Background()
	agent, _ := createTestAgent(ctx, t, nil)
	*healthCheckInterval = 20 * time.Second
	agent._healthy = nil

	// last health check time is now, we're good
	agent._healthyTime = time.Now()
	if _, healthy := agent.Healthy(); healthy != nil {
		t.Errorf("Healthy returned unexpected error: %v", healthy)
	}

	// last health check time is 2x interval ago, we're good
	agent._healthyTime = time.Now().Add(-2 * *healthCheckInterval)
	if _, healthy := agent.Healthy(); healthy != nil {
		t.Errorf("Healthy returned unexpected error: %v", healthy)
	}

	// last health check time is 4x interval ago, we're not good
	agent._healthyTime = time.Now().Add(-4 * *healthCheckInterval)
	if _, healthy := agent.Healthy(); healthy == nil || !strings.Contains(healthy.Error(), "last health check is too old") {
		t.Errorf("Healthy returned wrong error: %v", healthy)
	}
}

// expectBroadcastData checks that runHealthCheck() broadcasted the expected
// stats (going the value for secondsBehindMaster).
func expectBroadcastData(qsc tabletserver.Controller, serving bool, healthError string, secondsBehindMaster uint32) (*tabletservermock.BroadcastData, error) {
	bd := <-qsc.(*tabletservermock.Controller).BroadcastData
	if got := bd.Serving; got != serving {
		return nil, fmt.Errorf("unexpected BroadcastData.Serving, got: %v want: %v with bd: %+v", got, serving, bd)
	}
	if got := bd.RealtimeStats.HealthError; got != healthError {
		return nil, fmt.Errorf("unexpected BroadcastData.HealthError, got: %v want: %v with bd: %+v", got, healthError, bd)
	}
	if got := bd.RealtimeStats.SecondsBehindMaster; got != secondsBehindMaster {
		return nil, fmt.Errorf("unexpected BroadcastData.SecondsBehindMaster, got: %v want: %v with bd: %+v", got, secondsBehindMaster, bd)
	}
	return bd, nil
}

// expectBroadcastDataEmpty closes the health broadcast channel and verifies
// that all broadcasted messages were consumed by expectBroadcastData().
func expectBroadcastDataEmpty(qsc tabletserver.Controller) error {
	c := qsc.(*tabletservermock.Controller).BroadcastData
	close(c)
	bd, ok := <-c
	if ok {
		return fmt.Errorf("BroadcastData channel should have been consumed, but was not: %v", bd)
	}
	return nil
}

// expectStateChange verifies that the test changed the QueryService state
// to the expected state (serving or not, specific tablet type).
func expectStateChange(qsc tabletserver.Controller, serving bool, tabletType topodatapb.TabletType) error {
	want := &tabletservermock.StateChange{
		Serving:    serving,
		TabletType: tabletType,
	}
	got := <-qsc.(*tabletservermock.Controller).StateChanges
	if !reflect.DeepEqual(got, want) {
		return fmt.Errorf("unexpected state change. got: %v want: %v got", got, want)
	}
	return nil
}

// expectStateChangesEmpty closes the StateChange channel and verifies
// that all sent state changes were consumed by expectStateChange().
func expectStateChangesEmpty(qsc tabletserver.Controller) error {
	c := qsc.(*tabletservermock.Controller).StateChanges
	close(c)
	sc, ok := <-c
	if ok {
		return fmt.Errorf("StateChanges channel should have been consumed, but was not: %v", sc)
	}
	return nil
}
