package tabletmanager

import (
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/health"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/zktopo"
	"golang.org/x/net/context"
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

// fakeHealthCheck implements health.Reporter interface
type fakeHealthCheck struct {
	reportReplicationDelay time.Duration
	reportError            error
}

func (fhc *fakeHealthCheck) Report(tabletType topo.TabletType, shouldQueryServiceBeRunning bool) (replicationDelay time.Duration, err error) {
	return fhc.reportReplicationDelay, fhc.reportError
}

func (fhc *fakeHealthCheck) HTMLName() template.HTML {
	return template.HTML("fakeHealthCheck")
}

func TestHealthCheck(t *testing.T) {
	// register a fake reporter for our tests
	fhc := &fakeHealthCheck{}
	health.Register("fakeHealthCheck", fhc)

	keyspace := "test_keyspace"
	shard := "0"
	cell := "cell1"
	var uid uint32 = 42
	ts := zktopo.NewTestServer(t, []string{cell})

	if err := ts.CreateKeyspace(keyspace, &topo.Keyspace{}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}

	if err := topo.CreateShard(ts, keyspace, shard); err != nil {
		t.Fatalf("CreateShard failed: %v", err)
	}

	tabletAlias := topo.TabletAlias{Cell: cell, Uid: uid}
	port := 1234
	tablet := &topo.Tablet{
		Alias:    tabletAlias,
		Hostname: "host",
		Portmap: map[string]int{
			"vt": port,
		},
		IPAddr:   "1.0.0.1",
		Keyspace: keyspace,
		Shard:    shard,
		Type:     topo.TYPE_SPARE,
		State:    topo.STATE_READ_ONLY,
	}
	if err := topo.CreateTablet(ts, tablet); err != nil {
		t.Fatalf("CreateTablet failed: %v", err)
	}

	mysqlDaemon := &mysqlctl.FakeMysqlDaemon{MysqlPort: 3306}
	agent := NewTestActionAgent(context.Background(), ts, tabletAlias, port, mysqlDaemon)
	agent.BinlogPlayerMap = NewBinlogPlayerMap(ts, nil, nil)
	targetTabletType := topo.TYPE_REPLICA

	// first health check, should change us to replica, and update the
	// mysql port to 3306
	agent.runHealthCheck(targetTabletType)
	ti, err := ts.GetTablet(tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	if ti.Type != topo.TYPE_REPLICA {
		t.Errorf("First health check failed to go to replica: %v", ti.Type)
	}
	if ti.Portmap["mysql"] != 3306 {
		t.Errorf("First health check failed to update mysql port: %v", ti.Portmap["mysql"])
	}
	if !agent.QueryServiceControl.IsServing() {
		t.Errorf("Query service should be running")
	}

	// now make the tablet unhealthy
	fhc.reportError = fmt.Errorf("tablet is unhealthy")
	agent.runHealthCheck(targetTabletType)
	ti, err = ts.GetTablet(tabletAlias)
	if err != nil {
		t.Fatalf("GetTablet failed: %v", err)
	}
	data, err := json.MarshalIndent(ti, "", "  ")
	println(string(data))
	if ti.Type != topo.TYPE_SPARE {
		t.Errorf("Unhappy health check failed to go to spare: %v", ti.Type)
	}
	if agent.QueryServiceControl.IsServing() {
		// FIXME(alainjobart) the query service should be stopped there, but it's not.
		// See b/19309685 for the tracking bug.
		// t.Errorf("Query service should not be running")
	}

}
