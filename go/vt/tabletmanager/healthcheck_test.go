package tabletmanager

import (
	"errors"
	"fmt"
	"html/template"
	"strings"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletservermock"
	"github.com/youtube/vitess/go/vt/zktopo"
	"golang.org/x/net/context"

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

func createTestAgent(ctx context.Context, t *testing.T) *ActionAgent {
	ts := zktopo.NewTestServer(t, []string{"cell1"})

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
		Ip:       "1.0.0.1",
		Keyspace: "test_keyspace",
		Shard:    "0",
		Type:     topodatapb.TabletType_SPARE,
	}
	if err := ts.CreateTablet(ctx, tablet); err != nil {
		t.Fatalf("CreateTablet failed: %v", err)
	}

	mysqlDaemon := &mysqlctl.FakeMysqlDaemon{MysqlPort: 3306}
	agent := NewTestActionAgent(ctx, ts, tabletAlias, port, 0, mysqlDaemon)
	agent.BinlogPlayerMap = NewBinlogPlayerMap(ts, nil, nil)
	agent.HealthReporter = &fakeHealthCheck{}

	return agent
}

// TestHealthCheckControlsQueryService verifies that a tablet going healthy
// starts the query service, and going unhealthy stops it.
func TestHealthCheckControlsQueryService(t *testing.T) {
	ctx := context.Background()
	agent := createTestAgent(ctx, t)
	targetTabletType := topodatapb.TabletType_REPLICA

	// first health check, should change us to replica, and update the
	// mysql port to 3306
	before := time.Now()
	agent.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 12 * time.Second
	agent.runHealthCheck(targetTabletType)
	ti, err := agent.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != targetTabletType {
		t.Errorf("First health check failed to go to replica: %v", ti.Type)
	}
	if ti.PortMap["mysql"] != 3306 {
		t.Errorf("First health check failed to update mysql port: %v", ti.PortMap["mysql"])
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
	bd := <-agent.QueryServiceControl.(*tabletservermock.Controller).BroadcastData
	if bd.RealtimeStats.SecondsBehindMaster != 12 {
		t.Errorf("unexpected replicaton delay: %v", *bd)
	}
	if agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType != topodatapb.TabletType_REPLICA {
		t.Errorf("invalid tabletserver target: %v", agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType)
	}

	// now make the tablet unhealthy
	agent.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 13 * time.Second
	agent.HealthReporter.(*fakeHealthCheck).reportError = fmt.Errorf("tablet is unhealthy")
	before = time.Now()
	agent.runHealthCheck(targetTabletType)
	ti, err = agent.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_SPARE {
		t.Errorf("Unhappy health check failed to go to spare: %v", ti.Type)
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
	bd = <-agent.QueryServiceControl.(*tabletservermock.Controller).BroadcastData
	if bd.RealtimeStats.SecondsBehindMaster != 13 {
		t.Errorf("unexpected replicaton delay: %v", *bd)
	}
	if agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType != topodatapb.TabletType_SPARE {
		t.Errorf("invalid tabletserver target: %v", agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType)
	}
}

// TestQueryServiceNotStarting verifies that if a tablet cannot start the
// query service, it should not go healthy
func TestQueryServiceNotStarting(t *testing.T) {
	ctx := context.Background()
	agent := createTestAgent(ctx, t)
	targetTabletType := topodatapb.TabletType_REPLICA
	agent.QueryServiceControl.(*tabletservermock.Controller).SetServingTypeError = fmt.Errorf("test cannot start query service")

	before := time.Now()
	agent.runHealthCheck(targetTabletType)
	ti, err := agent.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_SPARE {
		t.Errorf("Happy health check which cannot start query service should stay spare: %v", ti.Type)
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
	if agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType != topodatapb.TabletType_SPARE {
		t.Errorf("invalid tabletserver target: %v", agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType)
	}
}

// TestQueryServiceStopped verifies that if a healthy tablet's query
// service is shut down, the tablet does unhealthy
func TestQueryServiceStopped(t *testing.T) {
	ctx := context.Background()
	agent := createTestAgent(ctx, t)
	targetTabletType := topodatapb.TabletType_REPLICA

	// first health check, should change us to replica
	before := time.Now()
	agent.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 14 * time.Second
	agent.runHealthCheck(targetTabletType)
	ti, err := agent.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != targetTabletType {
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
	bd := <-agent.QueryServiceControl.(*tabletservermock.Controller).BroadcastData
	if bd.RealtimeStats.SecondsBehindMaster != 14 {
		t.Errorf("unexpected replicaton delay: %v", *bd)
	}
	if agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType != topodatapb.TabletType_REPLICA {
		t.Errorf("invalid tabletserver target: %v", agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType)
	}

	// shut down query service and prevent it from starting again
	// (this is to simulate mysql going away, tablet server detecting it
	// and shutting itself down)
	agent.QueryServiceControl.SetServingType(topodatapb.TabletType_REPLICA, false, nil)
	agent.QueryServiceControl.(*tabletservermock.Controller).SetServingTypeError = fmt.Errorf("test cannot start query service")

	// health check should now fail
	before = time.Now()
	agent.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 15 * time.Second
	agent.runHealthCheck(targetTabletType)
	ti, err = agent.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_SPARE {
		t.Errorf("Happy health check which cannot start query service should stay spare: %v", ti.Type)
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
	bd = <-agent.QueryServiceControl.(*tabletservermock.Controller).BroadcastData
	if bd.RealtimeStats.SecondsBehindMaster != 15 {
		t.Errorf("unexpected replicaton delay: %v", *bd)
	}
	if bd.RealtimeStats.HealthError != "test cannot start query service" {
		t.Errorf("unexpected HealthError: %v", *bd)
	}
	if agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType != topodatapb.TabletType_REPLICA {
		t.Errorf("invalid tabletserver target: %v", agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType)
	}
}

// TestTabletControl verifies the shard's TabletControl record can disable
// query service in a tablet.
func TestTabletControl(t *testing.T) {
	ctx := context.Background()
	agent := createTestAgent(ctx, t)
	targetTabletType := topodatapb.TabletType_REPLICA

	// first health check, should change us to replica
	before := time.Now()
	agent.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 16 * time.Second
	agent.runHealthCheck(targetTabletType)
	ti, err := agent.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != targetTabletType {
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
	bd := <-agent.QueryServiceControl.(*tabletservermock.Controller).BroadcastData
	if bd.RealtimeStats.SecondsBehindMaster != 16 {
		t.Errorf("unexpected replicaton delay: %v", *bd)
	}
	if agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType != topodatapb.TabletType_REPLICA {
		t.Errorf("invalid tabletserver target: %v", agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType)
	}

	// now update the shard
	si, err := agent.TopoServer.GetShard(ctx, "test_keyspace", "0")
	if err != nil {
		t.Fatalf("GetShard failed: %v", err)
	}
	si.TabletControls = []*topodatapb.Shard_TabletControl{
		&topodatapb.Shard_TabletControl{
			TabletType:          targetTabletType,
			DisableQueryService: true,
		},
	}
	if err := agent.TopoServer.UpdateShard(ctx, si); err != nil {
		t.Fatalf("UpdateShard failed: %v", err)
	}

	// now refresh the tablet state, as the resharding process would do
	agent.RPCWrapLockAction(ctx, actionnode.TabletActionRefreshState, "", "", true, func() error {
		agent.RefreshState(ctx)
		return nil
	})

	// check we shutdown query service
	if agent.QueryServiceControl.IsServing() {
		t.Errorf("Query service should not be running")
	}

	// check UpdateStream is still running
	if !agent.UpdateStream.IsEnabled() {
		t.Errorf("UpdateStream should be running")
	}

	// check running a health check will not start it again
	before = time.Now()
	agent.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 17 * time.Second
	agent.runHealthCheck(targetTabletType)
	ti, err = agent.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != targetTabletType {
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
	bd = <-agent.QueryServiceControl.(*tabletservermock.Controller).BroadcastData
	if bd.RealtimeStats.SecondsBehindMaster != 17 {
		t.Errorf("unexpected replicaton delay: %v", *bd)
	}
	if agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType != topodatapb.TabletType_REPLICA {
		t.Errorf("invalid tabletserver target: %v", agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType)
	}

	// go unhealthy, check we go to spare and QS is not running
	agent.HealthReporter.(*fakeHealthCheck).reportError = fmt.Errorf("tablet is unhealthy")
	agent.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 18 * time.Second
	before = time.Now()
	agent.runHealthCheck(targetTabletType)
	ti, err = agent.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topodatapb.TabletType_SPARE {
		t.Errorf("Unhealthy health check should go to spare: %v", ti.Type)
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
	bd = <-agent.QueryServiceControl.(*tabletservermock.Controller).BroadcastData
	if bd.RealtimeStats.SecondsBehindMaster != 18 {
		t.Errorf("unexpected replicaton delay: %v", *bd)
	}
	if agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType != topodatapb.TabletType_SPARE {
		t.Errorf("invalid tabletserver target: %v", agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType)
	}

	// go back healthy, check QS is still not running
	agent.HealthReporter.(*fakeHealthCheck).reportError = nil
	agent.HealthReporter.(*fakeHealthCheck).reportReplicationDelay = 19 * time.Second
	before = time.Now()
	agent.runHealthCheck(targetTabletType)
	ti, err = agent.TopoServer.GetTablet(ctx, tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != targetTabletType {
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
	bd = <-agent.QueryServiceControl.(*tabletservermock.Controller).BroadcastData
	if bd.RealtimeStats.SecondsBehindMaster != 19 {
		t.Errorf("unexpected replicaton delay: %v", *bd)
	}
	if agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType != topodatapb.TabletType_REPLICA {
		t.Errorf("invalid tabletserver target: %v", agent.QueryServiceControl.(*tabletservermock.Controller).CurrentTarget.TabletType)
	}
}

// TestOldHealthCheck verifies that a healthcheck that is too old will
// return an error
func TestOldHealthCheck(t *testing.T) {
	ctx := context.Background()
	agent := createTestAgent(ctx, t)
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
