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

package tabletmanager

import (
	"errors"
	"fmt"
	"html/template"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"

	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/binlog"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/health"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/fakemysqldaemon"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/topotools"
	"vitess.io/vitess/go/vt/vttablet/tabletserver"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	// needed so that grpc client is registered
	_ "vitess.io/vitess/go/vt/vttablet/grpctmclient"
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
			left:      &HealthRecord{Time: now, ReplicationDelay: degradedThreshold / 2},
			right:     &HealthRecord{Time: later, ReplicationDelay: degradedThreshold / 3},
			duplicate: true,
		},
		{
			left:      &HealthRecord{Time: now, ReplicationDelay: degradedThreshold / 2},
			right:     &HealthRecord{Time: later, ReplicationDelay: degradedThreshold * 2},
			duplicate: false,
		},
		{
			left:      &HealthRecord{Time: now, Error: errors.New("foo"), ReplicationDelay: degradedThreshold * 2},
			right:     &HealthRecord{Time: later, ReplicationDelay: degradedThreshold * 2},
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
			r:     &HealthRecord{ReplicationDelay: degradedThreshold * 2},
			state: "unhappy",
		},
		{
			r:     &HealthRecord{ReplicationDelay: degradedThreshold / 2},
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

func (fhc *fakeHealthCheck) Report(isReplicaType, shouldQueryServiceBeRunning bool) (replicationDelay time.Duration, err error) {
	return fhc.reportReplicationDelay, fhc.reportError
}

func (fhc *fakeHealthCheck) HTMLName() template.HTML {
	return template.HTML("fakeHealthCheck")
}

func createTestTM(ctx context.Context, t *testing.T, preStart func(*TabletManager)) *TabletManager {
	ts := memorytopo.NewServer("cell1")
	tablet := &topodatapb.Tablet{
		Alias:    tabletAlias,
		Hostname: "host",
		PortMap: map[string]int32{
			"vt": int32(1234),
		},
		Keyspace: "test_keyspace",
		Shard:    "0",
		Type:     topodatapb.TabletType_REPLICA,
	}

	tm := &TabletManager{
		BatchCtx:            ctx,
		TopoServer:          ts,
		MysqlDaemon:         &fakemysqldaemon.FakeMysqlDaemon{MysqlPort: sync2.NewAtomicInt32(-1)},
		DBConfigs:           &dbconfigs.DBConfigs{},
		QueryServiceControl: tabletservermock.NewController(),
		UpdateStream:        binlog.NewUpdateStreamControlMock(),
	}
	if preStart != nil {
		preStart(tm)
	}
	err := tm.Start(tablet)
	require.NoError(t, err)

	tm.HealthReporter = &fakeHealthCheck{}

	return tm
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
	tm := createTestTM(ctx, t, nil)

	// Consume the first health broadcast triggered by TabletManager.Start():
	//  (REPLICA, NOT_SERVING) goes to (REPLICA, SERVING). And we
	//  should be serving.
	if _, err := expectBroadcastData(tm.QueryServiceControl, true, "healthcheck not run yet", 0); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChange(tm.QueryServiceControl, true, topodatapb.TabletType_REPLICA); err != nil {
		t.Fatal(err)
	}
	if !tm.QueryServiceControl.IsServing() {
		t.Errorf("Query service should be running")
	}
	if !tm.UpdateStream.IsEnabled() {
		t.Errorf("UpdateStream should be running")
	}

	// First health check, should keep us as replica and serving.
	before := time.Now()
	tm.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 12 * time.Second
	tm.runHealthCheck()
	ti, err := tm.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_REPLICA {
		t.Errorf("First health check failed to go to replica: %v", ti.Type)
	}
	if !tm.QueryServiceControl.IsServing() {
		t.Errorf("Query service should be running")
	}
	if !tm.UpdateStream.IsEnabled() {
		t.Errorf("UpdateStream should be running")
	}
	if tm._healthyTime.Sub(before) < 0 {
		t.Errorf("runHealthCheck did not update tm._healthyTime")
	}
	if tm.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType != topodatapb.TabletType_REPLICA {
		t.Errorf("invalid tabletserver target: %v", tm.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType)
	}
	if _, err := expectBroadcastData(tm.QueryServiceControl, true, "", 12); err != nil {
		t.Fatal(err)
	}

	// now make the tablet unhealthy
	tm.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 13 * time.Second
	tm.HealthReporter.(*fakeHealthCheck).reportError = fmt.Errorf("tablet is unhealthy")
	before = time.Now()
	tm.runHealthCheck()
	ti, err = tm.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_REPLICA {
		t.Errorf("Unhappy health check failed to stay as replica: %v", ti.Type)
	}
	if tm.QueryServiceControl.IsServing() {
		t.Errorf("Query service should not be running")
	}
	if tm._healthyTime.Sub(before) < 0 {
		t.Errorf("runHealthCheck did not update tm._healthyTime")
	}
	if got := tm.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType; got != topodatapb.TabletType_REPLICA {
		t.Errorf("invalid tabletserver target: got = %v, want = %v", got, topodatapb.TabletType_REPLICA)
	}

	// first we get the lameduck broadcast, with no error and old
	// replication delay
	if _, err := expectBroadcastData(tm.QueryServiceControl, false, "", 12); err != nil {
		t.Fatal(err)
	}

	// then query service is disabled since we are unhealthy now.
	if err := expectStateChange(tm.QueryServiceControl, false, topodatapb.TabletType_REPLICA); err != nil {
		t.Fatal(err)
	}

	// and the associated broadcast
	if _, err := expectBroadcastData(tm.QueryServiceControl, false, "tablet is unhealthy", 13); err != nil {
		t.Fatal(err)
	}

	// and nothing more.
	if err := expectBroadcastDataEmpty(tm.QueryServiceControl); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChangesEmpty(tm.QueryServiceControl); err != nil {
		t.Fatal(err)
	}
}

// TestErrReplicationNotRunningIsHealthy verifies that a tablet whose
// healthcheck reports health.ErrReplicationNotRunning is still considered
// healthy with high replication lag.
func TestErrReplicationNotRunningIsHealthy(t *testing.T) {
	unhealthyThreshold = 10 * time.Minute
	ctx := context.Background()
	tm := createTestTM(ctx, t, nil)

	// Consume the first health broadcast triggered by TabletManager.Start():
	//  (REPLICA, NOT_SERVING) goes to (REPLICA, SERVING). And we
	//  should be serving.
	if _, err := expectBroadcastData(tm.QueryServiceControl, true, "healthcheck not run yet", 0); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChange(tm.QueryServiceControl, true, topodatapb.TabletType_REPLICA); err != nil {
		t.Fatal(err)
	}
	if !tm.QueryServiceControl.IsServing() {
		t.Errorf("Query service should be running")
	}
	if !tm.UpdateStream.IsEnabled() {
		t.Errorf("UpdateStream should be running")
	}

	// health check returning health.ErrReplicationNotRunning, should
	// keep us as replica and serving
	before := time.Now()
	tm.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 12 * time.Second
	tm.HealthReporter.(*fakeHealthCheck).reportError = health.ErrReplicationNotRunning
	tm.runHealthCheck()
	if !tm.QueryServiceControl.IsServing() {
		t.Errorf("Query service should be running")
	}
	if !tm.UpdateStream.IsEnabled() {
		t.Errorf("UpdateStream should be running")
	}
	if tm._healthyTime.Sub(before) < 0 {
		t.Errorf("runHealthCheck did not update tm._healthyTime")
	}
	if tm.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType != topodatapb.TabletType_REPLICA {
		t.Errorf("invalid tabletserver target: %v", tm.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType)
	}
	if _, err := expectBroadcastData(tm.QueryServiceControl, true, "", 10*60); err != nil {
		t.Fatal(err)
	}

	// and nothing more.
	if err := expectBroadcastDataEmpty(tm.QueryServiceControl); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChangesEmpty(tm.QueryServiceControl); err != nil {
		t.Fatal(err)
	}
}

// TestQueryServiceNotStarting verifies that if a tablet cannot start the
// query service, it should not go healthy.
func TestQueryServiceNotStarting(t *testing.T) {
	ctx := context.Background()
	tm := createTestTM(ctx, t, func(a *TabletManager) {
		// The SetServingType that will fail is part of Start()
		// so we have to do this here.
		a.QueryServiceControl.(*tabletservermock.Controller).SetServingTypeError = fmt.Errorf("test cannot start query service")
	})

	// we should not be serving.
	if tm.QueryServiceControl.IsServing() {
		t.Errorf("Query service should not be running")
	}
	if !tm.UpdateStream.IsEnabled() {
		t.Errorf("UpdateStream should be running")
	}

	// There is no broadcast data to consume, we're just not
	// healthy from startup

	// Now we can run another health check, it will stay unhealthy forever.
	before := time.Now()
	tm.runHealthCheck()
	ti, err := tm.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_REPLICA {
		t.Errorf("Happy health check which cannot start query service should stay replica: %v", ti.Type)
	}
	if tm.QueryServiceControl.IsServing() {
		t.Errorf("Query service should not be running")
	}
	if tm._healthyTime.Sub(before) < 0 {
		t.Errorf("runHealthCheck did not update tm._healthyTime")
	}
	bd := <-tm.QueryServiceControl.(*tabletservermock.Controller).BroadcastData
	if bd.RealtimeStats.HealthError != "test cannot start query service" {
		t.Errorf("unexpected HealthError: %v", *bd)
	}
	if tm.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType != topodatapb.TabletType_REPLICA {
		t.Errorf("invalid tabletserver target: %v", tm.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType)
	}

	if err := expectBroadcastDataEmpty(tm.QueryServiceControl); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChangesEmpty(tm.QueryServiceControl); err != nil {
		t.Fatal(err)
	}
}

// TestQueryServiceStopped verifies that if a healthy tablet's query
// service is shut down, the tablet goes unhealthy
func TestQueryServiceStopped(t *testing.T) {
	ctx := context.Background()
	tm := createTestTM(ctx, t, nil)

	// Consume the first health broadcast triggered by TabletManager.Start():
	//  (REPLICA, NOT_SERVING) goes to (REPLICA, SERVING). And we
	//  should be serving.
	if _, err := expectBroadcastData(tm.QueryServiceControl, true, "healthcheck not run yet", 0); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChange(tm.QueryServiceControl, true, topodatapb.TabletType_REPLICA); err != nil {
		t.Fatal(err)
	}
	if !tm.QueryServiceControl.IsServing() {
		t.Errorf("Query service should be running")
	}
	if !tm.UpdateStream.IsEnabled() {
		t.Errorf("UpdateStream should be running")
	}

	// first health check, should keep us in replica / healthy
	before := time.Now()
	tm.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 14 * time.Second
	tm.runHealthCheck()
	ti, err := tm.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_REPLICA {
		t.Errorf("First health check failed to stay in replica: %v", ti.Type)
	}
	if !tm.QueryServiceControl.IsServing() {
		t.Errorf("Query service should be running")
	}
	if !tm.UpdateStream.IsEnabled() {
		t.Errorf("UpdateStream should be running")
	}
	if tm._healthyTime.Sub(before) < 0 {
		t.Errorf("runHealthCheck did not update tm._healthyTime")
	}
	want := topodatapb.TabletType_REPLICA
	if got := tm.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType; got != want {
		t.Errorf("invalid tabletserver target: got = %v, want = %v", got, want)
	}

	if _, err := expectBroadcastData(tm.QueryServiceControl, true, "", 14); err != nil {
		t.Fatal(err)
	}

	// shut down query service and prevent it from starting again
	// (this is to simulate mysql going away, tablet server detecting it
	// and shutting itself down). Intercept the message
	tm.QueryServiceControl.SetServingType(topodatapb.TabletType_REPLICA, false, nil)
	tm.QueryServiceControl.(*tabletservermock.Controller).SetServingTypeError = fmt.Errorf("test cannot start query service")
	if err := expectStateChange(tm.QueryServiceControl, false, topodatapb.TabletType_REPLICA); err != nil {
		t.Fatal(err)
	}

	// health check should now fail
	before = time.Now()
	tm.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 15 * time.Second
	tm.runHealthCheck()
	ti, err = tm.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_REPLICA {
		t.Errorf("Happy health check which cannot start query service should stay replica: %v", ti.Type)
	}
	if tm.QueryServiceControl.IsServing() {
		t.Errorf("Query service should not be running")
	}
	if tm._healthyTime.Sub(before) < 0 {
		t.Errorf("runHealthCheck did not update tm._healthyTime")
	}
	want = topodatapb.TabletType_REPLICA
	if got := tm.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType; got != want {
		t.Errorf("invalid tabletserver target: got = %v, want = %v", got, want)
	}
	if _, err := expectBroadcastData(tm.QueryServiceControl, false, "test cannot start query service", 15); err != nil {
		t.Fatal(err)
	}
	// NOTE: No more broadcasts or state changes since SetServingTypeError is set
	// on the mocked controller and this disables its SetServingType().

	if err := expectBroadcastDataEmpty(tm.QueryServiceControl); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChangesEmpty(tm.QueryServiceControl); err != nil {
		t.Fatal(err)
	}
}

// TestTabletControl verifies the shard's TabletControl record can disable
// query service in a tablet.
func TestTabletControl(t *testing.T) {
	ctx := context.Background()
	tm := createTestTM(ctx, t, nil)

	// Consume the first health broadcast triggered by TabletManager.Start():
	//  (REPLICA, NOT_SERVING) goes to (REPLICA, SERVING). And we
	//  should be serving.
	if _, err := expectBroadcastData(tm.QueryServiceControl, true, "healthcheck not run yet", 0); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChange(tm.QueryServiceControl, true, topodatapb.TabletType_REPLICA); err != nil {
		t.Fatal(err)
	}

	// first health check, should keep us in replica, just broadcast
	before := time.Now()
	tm.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 16 * time.Second
	tm.runHealthCheck()
	ti, err := tm.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_REPLICA {
		t.Errorf("First health check failed to go to replica: %v", ti.Type)
	}
	if !tm.QueryServiceControl.IsServing() {
		t.Errorf("Query service should be running")
	}
	if !tm.UpdateStream.IsEnabled() {
		t.Errorf("UpdateStream should be running")
	}
	if tm._healthyTime.Sub(before) < 0 {
		t.Errorf("runHealthCheck did not update tm._healthyTime")
	}
	if got := tm.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType; got != topodatapb.TabletType_REPLICA {
		t.Errorf("invalid tabletserver target: got = %v, want = %v", got, topodatapb.TabletType_REPLICA)
	}
	if _, err := expectBroadcastData(tm.QueryServiceControl, true, "", 16); err != nil {
		t.Fatal(err)
	}

	// now update the shard

	si, err := tm.TopoServer.GetShard(ctx, "test_keyspace", "0")
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}

	ctx, unlock, lockErr := tm.TopoServer.LockKeyspace(ctx, "test_keyspace", "UpdateDisableQueryService")
	if lockErr != nil {
		t.Fatalf("Couldn't lock keyspace for test")
	}
	defer unlock(&err)

	// Let's generate the keyspace graph we have partition information for this cell
	err = topotools.RebuildKeyspaceLocked(ctx, logutil.NewConsoleLogger(), tm.TopoServer, "test_keyspace", []string{tm.tabletAlias.GetCell()})
	if err != nil {
		t.Fatalf("RebuildKeyspaceLocked failed: %v", err)
	}

	err = tm.TopoServer.UpdateDisableQueryService(ctx, "test_keyspace", []*topo.ShardInfo{si}, topodatapb.TabletType_REPLICA, nil, true)
	if err != nil {
		t.Fatalf("UpdateShardFields failed: %v", err)
	}

	// now refresh the tablet state, as the resharding process would do
	tm.RefreshState(ctx)

	// check we shutdown query service
	if tm.QueryServiceControl.IsServing() {
		t.Errorf("Query service should not be running")
	}

	// check UpdateStream is still running
	if !tm.UpdateStream.IsEnabled() {
		t.Errorf("UpdateStream should be running")
	}

	// Consume the health broadcast which was triggered due to the QueryService
	// state change from SERVING to NOT_SERVING.
	if _, err := expectBroadcastData(tm.QueryServiceControl, false, "", 16); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChange(tm.QueryServiceControl, false, topodatapb.TabletType_REPLICA); err != nil {
		t.Fatal(err)
	}

	// check running a health check will not start it again
	before = time.Now()
	tm.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 17 * time.Second
	tm.runHealthCheck()
	ti, err = tm.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_REPLICA {
		t.Errorf("Health check failed to go to replica: %v", ti.Type)
	}
	if tm.QueryServiceControl.IsServing() {
		t.Errorf("Query service should not be running")
	}
	if !tm.UpdateStream.IsEnabled() {
		t.Errorf("UpdateStream should be running")
	}
	if tm._healthyTime.Sub(before) < 0 {
		t.Errorf("runHealthCheck did not update tm._healthyTime")
	}
	if got := tm.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType; got != topodatapb.TabletType_REPLICA {
		t.Errorf("invalid tabletserver target: got = %v, want = %v", got, topodatapb.TabletType_REPLICA)
	}
	if _, err := expectBroadcastData(tm.QueryServiceControl, false, "", 17); err != nil {
		t.Fatal(err)
	}
	// NOTE: No state change here since nothing has changed.

	// go unhealthy, check we go to error state and QS is not running
	tm.HealthReporter.(*fakeHealthCheck).reportError = fmt.Errorf("tablet is unhealthy")
	tm.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 18 * time.Second
	before = time.Now()
	tm.runHealthCheck()
	ti, err = tm.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_REPLICA {
		t.Errorf("Unhealthy health check should stay replica: %v", ti.Type)
	}
	if tm.QueryServiceControl.IsServing() {
		t.Errorf("Query service should not be running")
	}
	if tm._healthyTime.Sub(before) < 0 {
		t.Errorf("runHealthCheck did not update tm._healthyTime")
	}
	if _, err := expectBroadcastData(tm.QueryServiceControl, false, "tablet is unhealthy", 18); err != nil {
		t.Fatal(err)
	}
	// NOTE: No state change here since QueryService is already NOT_SERVING.
	want := topodatapb.TabletType_REPLICA
	if got := tm.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType; got != want {
		t.Errorf("invalid tabletserver target: got = %v, want = %v", got, want)
	}

	// go back healthy, check QS is still not running
	tm.HealthReporter.(*fakeHealthCheck).reportError = nil
	tm.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 19 * time.Second
	before = time.Now()
	tm.runHealthCheck()
	ti, err = tm.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_REPLICA {
		t.Errorf("Healthy health check should go to replica: %v", ti.Type)
	}
	if tm.QueryServiceControl.IsServing() {
		t.Errorf("Query service should not be running")
	}
	if !tm.UpdateStream.IsEnabled() {
		t.Errorf("UpdateStream should be running")
	}
	if tm._healthyTime.Sub(before) < 0 {
		t.Errorf("runHealthCheck did not update tm._healthyTime")
	}
	if _, err := expectBroadcastData(tm.QueryServiceControl, false, "", 19); err != nil {
		t.Fatal(err)
	}
	if got := tm.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType; got != topodatapb.TabletType_REPLICA {
		t.Errorf("invalid tabletserver target: got = %v, want = %v", got, topodatapb.TabletType_REPLICA)
	}

	// now clear TabletControl, run health check, make sure we go
	// back healthy and serving.
	err = tm.TopoServer.UpdateDisableQueryService(ctx, "test_keyspace", []*topo.ShardInfo{si}, topodatapb.TabletType_REPLICA, nil, false)
	if err != nil {
		t.Fatalf("UpdateDisableQueryService failed: %v", err)
	}

	// now refresh the tablet state, as the resharding process would do
	tm.RefreshState(ctx)

	// QueryService changed back from SERVING to NOT_SERVING since RefreshState()
	// re-read the topology and saw that REPLICA is still not allowed to serve.
	if _, err := expectBroadcastData(tm.QueryServiceControl, true, "", 19); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChange(tm.QueryServiceControl, true, topodatapb.TabletType_REPLICA); err != nil {
		t.Fatal(err)
	}

	if err := expectBroadcastDataEmpty(tm.QueryServiceControl); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChangesEmpty(tm.QueryServiceControl); err != nil {
		t.Fatal(err)
	}
}

// TestQueryServiceChangeImmediateHealthcheckResponse verifies that a change
// of the QueryService state or the tablet type will result into a broadcast
// of a StreamHealthResponse message.
func TestStateChangeImmediateHealthBroadcast(t *testing.T) {
	ctx := context.Background()
	tm := createTestTM(ctx, t, nil)

	// Consume the first health broadcast triggered by TabletManager.Start():
	//  (REPLICA, NOT_SERVING) goes to (REPLICA, SERVING). And we
	//  should be serving.
	if _, err := expectBroadcastData(tm.QueryServiceControl, true, "healthcheck not run yet", 0); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChange(tm.QueryServiceControl, true, topodatapb.TabletType_REPLICA); err != nil {
		t.Fatal(err)
	}

	// Run health check to turn into a healthy replica
	tm.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 12 * time.Second
	tm.runHealthCheck()
	if !tm.QueryServiceControl.IsServing() {
		t.Errorf("Query service should be running")
	}
	if got := tm.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType; got != topodatapb.TabletType_REPLICA {
		t.Errorf("invalid tabletserver target: got = %v, want = %v", got, topodatapb.TabletType_REPLICA)
	}
	if _, err := expectBroadcastData(tm.QueryServiceControl, true, "", 12); err != nil {
		t.Fatal(err)
	}

	// Change to master.
	tm.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 19 * time.Second
	if err := tm.ChangeType(ctx, topodatapb.TabletType_MASTER); err != nil {
		t.Fatalf("TabletExternallyReparented failed: %v", err)
	}
	// Wait for shard_sync to finish
	startTime := time.Now()
	for {
		if time.Since(startTime) > 10*time.Second /* timeout */ {
			si, err := tm.TopoServer.GetShard(ctx, tm.Tablet().Keyspace, tm.Tablet().Shard)
			if err != nil {
				t.Fatalf("GetShard(%v, %v) failed: %v", tm.Tablet().Keyspace, tm.Tablet().Shard, err)
			}
			if !topoproto.TabletAliasEqual(si.MasterAlias, tm.Tablet().Alias) {
				t.Fatalf("ShardInfo should have MasterAlias %v but has %v", topoproto.TabletAliasString(tm.Tablet().Alias), topoproto.TabletAliasString(si.MasterAlias))
			}
		}
		si, err := tm.TopoServer.GetShard(ctx, tm.Tablet().Keyspace, tm.Tablet().Shard)
		if err != nil {
			t.Fatalf("GetShard(%v, %v) failed: %v", tm.Tablet().Keyspace, tm.Tablet().Shard, err)
		}
		if topoproto.TabletAliasEqual(si.MasterAlias, tm.Tablet().Alias) {
			break
		} else {
			time.Sleep(100 * time.Millisecond /* interval at which to re-check the shard record */)
		}
	}

	ti, err := tm.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_MASTER {
		t.Errorf("TER failed to go to master: %v", ti.Type)
	}
	if !tm.QueryServiceControl.IsServing() {
		t.Errorf("Query service should be running")
	}
	if got := tm.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType; got != topodatapb.TabletType_MASTER {
		t.Errorf("invalid tabletserver target: got = %v, want = %v", got, topodatapb.TabletType_MASTER)
	}

	// Consume the health broadcast (no replication delay as we are master)
	if _, err := expectBroadcastData(tm.QueryServiceControl, true, "", 12); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChange(tm.QueryServiceControl, true, topodatapb.TabletType_MASTER); err != nil {
		t.Fatal(err)
	}

	// Run health check to make sure we stay good
	tm.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 20 * time.Second
	tm.runHealthCheck()
	ti, err = tm.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_MASTER {
		t.Errorf("First health check failed to go to master: %v", ti.Type)
	}
	if !tm.QueryServiceControl.IsServing() {
		t.Errorf("Query service should be running")
	}
	if got := tm.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType; got != topodatapb.TabletType_MASTER {
		t.Errorf("invalid tabletserver target: got = %v, want = %v", got, topodatapb.TabletType_MASTER)
	}
	if _, err := expectBroadcastData(tm.QueryServiceControl, true, "", 20); err != nil {
		t.Fatal(err)
	}

	// Simulate a vertical split resharding where we set
	// SourceShards in the topo and enable filtered replication.
	_, err = tm.TopoServer.UpdateShardFields(ctx, "test_keyspace", "0", func(si *topo.ShardInfo) error {
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

	// Refresh the tablet state, as vtworker would do.
	// Since we change the QueryService state, we'll also trigger a health broadcast.
	tm.RefreshState(ctx)

	// (Destination) MASTER with enabled filtered replication mustn't serve anymore.
	if tm.QueryServiceControl.IsServing() {
		t.Errorf("Query service should not be running")
	}
	// Consume health broadcast sent out due to QueryService state change from
	// (MASTER, SERVING) to (MASTER, NOT_SERVING).
	// TODO(sougou); this test case does not reflect reality. Need to fix.
	if _, err := expectBroadcastData(tm.QueryServiceControl, false, "", 20); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChange(tm.QueryServiceControl, false, topodatapb.TabletType_MASTER); err != nil {
		t.Fatal(err)
	}

	// Running a healthcheck won't put the QueryService back to SERVING.
	tm.runHealthCheck()
	ti, err = tm.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_MASTER {
		t.Errorf("Health check failed to go to replica: %v", ti.Type)
	}
	if tm.QueryServiceControl.IsServing() {
		t.Errorf("Query service should not be running")
	}
	if got := tm.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType; got != topodatapb.TabletType_MASTER {
		t.Errorf("invalid tabletserver target: got = %v, want = %v", got, topodatapb.TabletType_MASTER)
	}
	if _, err := expectBroadcastData(tm.QueryServiceControl, false, "", 20); err != nil {
		t.Fatal(err)
	}
	// NOTE: No state change here since nothing has changed.

	// Simulate migration to destination master i.e. remove SourceShards.
	_, err = tm.TopoServer.UpdateShardFields(ctx, "test_keyspace", "0", func(si *topo.ShardInfo) error {
		si.SourceShards = nil
		return nil
	})
	if err != nil {
		t.Fatalf("UpdateShardFields failed: %v", err)
	}

	// Refresh the tablet state, as vtctl MigrateServedFrom would do.
	// This should also trigger a health broadcast since the QueryService state
	// changes from NOT_SERVING to SERVING.
	tm.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 23 * time.Second
	tm.RefreshState(ctx)

	// QueryService changed from NOT_SERVING to SERVING.
	if !tm.QueryServiceControl.IsServing() {
		t.Errorf("Query service should not be running")
	}
	// Since we didn't run healthcheck again yet, the broadcast data contains the
	// cached replication lag of 0. This is because
	// RefreshState on MASTER always sets the replicationDelay to 0
	if _, err := expectBroadcastData(tm.QueryServiceControl, true, "", 20); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChange(tm.QueryServiceControl, true, topodatapb.TabletType_MASTER); err != nil {
		t.Fatal(err)
	}

	if err := expectBroadcastDataEmpty(tm.QueryServiceControl); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChangesEmpty(tm.QueryServiceControl); err != nil {
		t.Fatal(err)
	}
}

// TestOldHealthCheck verifies that a healthcheck that is too old will
// return an error
func TestOldHealthCheck(t *testing.T) {
	ctx := context.Background()
	tm := createTestTM(ctx, t, nil)
	healthCheckInterval = 20 * time.Second
	tm._healthy = nil

	// last health check time is now, we're good
	tm._healthyTime = time.Now()
	if _, healthy := tm.Healthy(); healthy != nil {
		t.Errorf("Healthy returned unexpected error: %v", healthy)
	}

	// last health check time is 2x interval ago, we're good
	tm._healthyTime = time.Now().Add(-2 * healthCheckInterval)
	if _, healthy := tm.Healthy(); healthy != nil {
		t.Errorf("Healthy returned unexpected error: %v", healthy)
	}

	// last health check time is 4x interval ago, we're not good
	tm._healthyTime = time.Now().Add(-4 * healthCheckInterval)
	if _, healthy := tm.Healthy(); healthy == nil || !strings.Contains(healthy.Error(), "last health check is too old") {
		t.Errorf("Healthy returned wrong error: %v", healthy)
	}
}

// TestBackupStateChange verifies that after backup we check
// the replication delay before setting REPLICA tablet to SERVING
func TestBackupStateChange(t *testing.T) {
	ctx := context.Background()
	tm := createTestTM(ctx, t, nil)

	degradedThreshold = 7 * time.Second
	unhealthyThreshold = 15 * time.Second

	if _, err := expectBroadcastData(tm.QueryServiceControl, true, "healthcheck not run yet", 0); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChange(tm.QueryServiceControl, true, topodatapb.TabletType_REPLICA); err != nil {
		t.Fatal(err)
	}

	tm.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 16 * time.Second

	// change to BACKUP, query service will turn off
	if err := tm.ChangeType(ctx, topodatapb.TabletType_BACKUP); err != nil {
		t.Fatal(err)
	}
	if tm.QueryServiceControl.IsServing() {
		t.Errorf("Query service should NOT be running")
	}
	if err := expectStateChange(tm.QueryServiceControl, false, topodatapb.TabletType_BACKUP); err != nil {
		t.Fatal(err)
	}
	// change back to REPLICA, query service should not start
	// because replication delay > unhealthyThreshold
	if err := tm.ChangeType(ctx, topodatapb.TabletType_REPLICA); err != nil {
		t.Fatal(err)
	}
	if tm.QueryServiceControl.IsServing() {
		t.Errorf("Query service should NOT be running")
	}
	if err := expectStateChange(tm.QueryServiceControl, false, topodatapb.TabletType_REPLICA); err != nil {
		t.Fatal(err)
	}

	// run healthcheck
	// now query service should still be OFF
	tm.runHealthCheck()
	if tm.QueryServiceControl.IsServing() {
		t.Errorf("Query service should NOT be running")
	}
}

// TestRestoreStateChange verifies that after restore we check
// the replication delay before setting REPLICA tablet to SERVING
func TestRestoreStateChange(t *testing.T) {
	ctx := context.Background()
	tm := createTestTM(ctx, t, nil)

	degradedThreshold = 7 * time.Second
	unhealthyThreshold = 15 * time.Second

	if _, err := expectBroadcastData(tm.QueryServiceControl, true, "healthcheck not run yet", 0); err != nil {
		t.Fatal(err)
	}
	if err := expectStateChange(tm.QueryServiceControl, true, topodatapb.TabletType_REPLICA); err != nil {
		t.Fatal(err)
	}

	tm.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 16 * time.Second

	// change to RESTORE, query service will turn off
	if err := tm.ChangeType(ctx, topodatapb.TabletType_RESTORE); err != nil {
		t.Fatal(err)
	}
	if tm.QueryServiceControl.IsServing() {
		t.Errorf("Query service should NOT be running")
	}
	if err := expectStateChange(tm.QueryServiceControl, false, topodatapb.TabletType_RESTORE); err != nil {
		t.Fatal(err)
	}
	// change back to REPLICA, query service should not start
	// because replication delay > unhealthyThreshold
	if err := tm.ChangeType(ctx, topodatapb.TabletType_REPLICA); err != nil {
		t.Fatal(err)
	}
	if tm.QueryServiceControl.IsServing() {
		t.Errorf("Query service should NOT be running")
	}
	if err := expectStateChange(tm.QueryServiceControl, false, topodatapb.TabletType_REPLICA); err != nil {
		t.Fatal(err)
	}

	// run healthcheck
	// now query service should still be OFF
	tm.runHealthCheck()
	if tm.QueryServiceControl.IsServing() {
		t.Errorf("Query service should NOT be running")
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
		return fmt.Errorf("unexpected state change. got: %v want: %v", got, want)
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
