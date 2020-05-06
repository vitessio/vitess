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

package discovery

import (
	"bytes"
	"flag"
	"fmt"
	"html/template"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/status"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func init() {
	tabletconn.RegisterDialer("fake_gateway", tabletDialer)
	flag.Set("tablet_protocol", "fake_gateway")
}

func TestBasicHealthCheck(t *testing.T) {
	ts := memorytopo.NewServer("cell")
	hc := createTestHc(ts)
	tablet := topo.NewTablet(0, "cell", "a")
	tablet.Keyspace = "k"
	tablet.Shard = "s"
	tablet.PortMap["vt"] = 1
	tablet.Type = topodatapb.TabletType_REPLICA
	tabletAlias := topoproto.TabletAliasString(tablet.Alias)
	input := make(chan *querypb.StreamHealthResponse)
	conn := createFakeConn(tablet, input)
	t.Logf(`createFakeConn({Host: "a", PortMap: {"vt": 1}}, c)`)
	// close healthcheck
	defer hc.Close()

	testChecksum(t, 0, hc.stateChecksum())
	hc.AddTablet(tablet)
	t.Logf(`hc = HealthCheck(); hc.AddTablet({Host: "a", PortMap: {"vt": 1}}, "")`)
	//	testChecksum(t, 2829991735, hc.stateChecksum())

	// Immediately after AddTablet() there will be the first notification.
	want := &TabletHealth{
		Tablet:              tablet,
		Target:              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:             false,
		Stats:               nil,
		MasterTermStartTime: 0,
	}
	startTime := time.Now()
	timeout := 2 * time.Second
	for {
		if hc.healthByAlias[tabletAlias] != nil {
			break
		}
		if time.Since(startTime) > timeout {
			t.Fatal("Timed out waiting for initial health")
		}
		time.Sleep(10 * time.Microsecond)
	}
	assert.True(t, want.DeepEqual(hc.healthByAlias[tabletAlias].SimpleCopy()), "Wrong tabletHealth data:\n expected: %v\n got:      %v", want, hc.healthByAlias[tabletAlias])

	shr := &querypb.StreamHealthResponse{
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 0,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.5},
	}
	input <- shr
	t.Logf(`input <- {{Keyspace: "k", Shard: "s", TabletType: REPLICA}, Serving: true, TabletExternallyReparentedTimestamp: 0, {SecondsBehindMaster: 1, CpuUsage: 0.5}}`)
	want = &TabletHealth{
		Tablet:              tablet,
		Target:              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:             true,
		Stats:               &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.5},
		MasterTermStartTime: 0,
	}
	startTime = time.Now()
	lastResponseTime := hc.healthByAlias[tabletAlias].lastResponseTime()
	for {
		// Health has been updated
		if hc.healthByAlias[tabletAlias].lastResponseTime() != lastResponseTime || time.Since(startTime) > timeout {
			break
		}
		if time.Since(startTime) > timeout {
			t.Fatal("Timed out waiting for health update")
		}
		time.Sleep(10 * time.Microsecond)
	}
	assert.True(t, want.DeepEqual(hc.healthByAlias[tabletAlias].SimpleCopy()), "Wrong tabletHealth data:\n Expected: %v\n Actual:   %v", want, hc.healthByAlias[tabletAlias])
	targetKey := hc.keyFromTarget(want.Target)
	ths := hc.healthData[targetKey]
	assert.NotEmpty(t, ths, "healthData is empty")
	assert.True(t, want.DeepEqual(ths[tabletAlias].SimpleCopy()), "healthData contains wrong tabletHealth")

	tcsl := hc.CacheStatus()
	tcslWant := TabletsCacheStatusList{{
		Cell:   "cell",
		Target: want.Target,
		TabletsStats: TabletStatsList{{
			Tablet:              tablet,
			Target:              want.Target,
			Serving:             true,
			Stats:               &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.5},
			MasterTermStartTime: 0,
		}},
	}}
	assert.True(t, tcslWant.deepEqual(tcsl), "Incorrect cache status:\n Expected: %+v\n Actual:   %+v", tcslWant[0], tcsl[0])
	//	testChecksum(t, 3487343103, hc.stateChecksum())

	// TabletType changed, should get both old and new event
	shr = &querypb.StreamHealthResponse{
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 10,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}
	want = &TabletHealth{
		Tablet: tablet,
		Target: &querypb.Target{
			Keyspace:   "k",
			Shard:      "s",
			TabletType: topodatapb.TabletType_MASTER,
		},
		Serving:             true,
		Conn:                conn,
		Stats:               &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
		MasterTermStartTime: 10,
	}
	input <- shr
	startTime = time.Now()
	lastResponseTime = hc.healthByAlias[tabletAlias].lastResponseTime()
	for {
		// Health has been updated
		if hc.healthByAlias[tabletAlias].lastResponseTime() != lastResponseTime || time.Since(startTime) > timeout {
			break
		}
		if time.Since(startTime) > timeout {
			t.Fatal("Timed out waiting for health update")
		}
		time.Sleep(10 * time.Microsecond)
	}
	assert.True(t, want.DeepEqual(hc.healthByAlias[tabletAlias].SimpleCopy()), "Wrong health data:\n Expected: %v\n Actual:   %v", want, hc.healthByAlias[tabletAlias])
	//	testChecksum(t, 2773351292, hc.stateChecksum())

	err := checkErrorCounter("k", "s", topodatapb.TabletType_MASTER, 0)
	require.NoError(t, err, "error checking error counter")

	// Serving & RealtimeStats changed
	shr = &querypb.StreamHealthResponse{
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:                             false,
		TabletExternallyReparentedTimestamp: 0,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.3},
	}
	want = &TabletHealth{
		Tablet:              tablet,
		Target:              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:             false,
		Stats:               &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.3},
		MasterTermStartTime: 0,
	}
	input <- shr
	t.Logf(`input <- {{Keyspace: "k", Shard: "s", TabletType: REPLICA}, TabletExternallyReparentedTimestamp: 0, {SecondsBehindMaster: 1, CpuUsage: 0.3}}`)
	startTime = time.Now()
	lastResponseTime = hc.healthByAlias[tabletAlias].lastResponseTime()
	for {
		// Health has been updated
		if hc.healthByAlias[tabletAlias].lastResponseTime() != lastResponseTime || time.Since(startTime) > timeout {
			break
		}
		if time.Since(startTime) > timeout {
			t.Fatal("Timed out waiting for health update")
		}
		time.Sleep(10 * time.Microsecond)
	}
	assert.True(t, want.DeepEqual(hc.healthByAlias[tabletAlias].SimpleCopy()), "Wrong health data:\n Expected: %v\n Actual:   %v", want, hc.healthByAlias[tabletAlias])
	//	testChecksum(t, 2829991735, hc.stateChecksum())

	// HealthError
	shr = &querypb.StreamHealthResponse{
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 0,
		RealtimeStats:                       &querypb.RealtimeStats{HealthError: "some error", SecondsBehindMaster: 1, CpuUsage: 0.3},
	}
	want = &TabletHealth{
		Tablet:              tablet,
		Target:              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:             false,
		Stats:               &querypb.RealtimeStats{HealthError: "some error", SecondsBehindMaster: 1, CpuUsage: 0.3},
		MasterTermStartTime: 0,
		LastError:           fmt.Errorf("vttablet error: some error"),
	}
	input <- shr
	t.Logf(`input <- {{Keyspace: "k", Shard: "s", TabletType: REPLICA}, Serving: true, TabletExternallyReparentedTimestamp: 0, {HealthError: "some error", SecondsBehindMaster: 1, CpuUsage: 0.3}}`)
	startTime = time.Now()
	lastResponseTime = hc.healthByAlias[tabletAlias].lastResponseTime()
	for {
		// Health has been updated
		if hc.healthByAlias[tabletAlias].lastResponseTime() != lastResponseTime || time.Since(startTime) > timeout {
			break
		}
		if time.Since(startTime) > timeout {
			t.Fatal("Timed out waiting for health update")
		}
		time.Sleep(10 * time.Microsecond)
	}
	assert.True(t, want.DeepEqual(hc.healthByAlias[tabletAlias].SimpleCopy()), "Wrong health data:\n Expected: %v\n Actual:   %v", want, hc.healthByAlias[tabletAlias])

	testChecksum(t, 1027934207, hc.stateChecksum()) // unchanged

	// remove tablet
	hc.deleteConn(tablet)
	t.Logf(`hc.RemoveTablet({Host: "a", PortMap: {"vt": 1}})`)
	assert.Nil(t, hc.healthByAlias[tabletAlias], "Wrong tablet health")
	testChecksum(t, 0, hc.stateChecksum())

}

func TestHealthCheckStreamError(t *testing.T) {
	ts := memorytopo.NewServer("cell")
	hc := createTestHc(ts)
	tablet := topo.NewTablet(0, "cell", "a")
	tablet.PortMap["vt"] = 1
	tabletAlias := topoproto.TabletAliasString(tablet.Alias)
	input := make(chan *querypb.StreamHealthResponse)
	fc := createFakeConn(tablet, input)
	fc.errCh = make(chan error)
	t.Logf(`createFakeConn({Host: "a", PortMap: {"vt": 1}}, c)`)
	hc.AddTablet(tablet)
	t.Logf(`hc = HealthCheck(); hc.AddTablet({Host: "a", PortMap: {"vt": 1}}, "")`)

	// Immediately after AddTablet() there will be the first notification.
	want := &tabletHealthCheck{
		Tablet:              tablet,
		Target:              &querypb.Target{},
		Serving:             false,
		MasterTermStartTime: 0,
	}
	startTime := time.Now()
	timeout := 2 * time.Second
	for {
		if hc.healthByAlias[tabletAlias] != nil {
			break
		}
		if time.Since(startTime) > timeout {
			t.Fatal("Timed out waiting for initial health")
		}
		time.Sleep(10 * time.Microsecond)
	}
	assert.True(t, want.DeepEqual(hc.healthByAlias[tabletAlias]), "Wrong tabletHealth data:\n expected: %v\n got:      %v", want, hc.healthByAlias[tabletAlias])

	// one tablet after receiving a StreamHealthResponse
	shr := &querypb.StreamHealthResponse{
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 0,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}
	want = &tabletHealthCheck{
		Tablet:              tablet,
		Target:              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:             true,
		Stats:               &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
		MasterTermStartTime: 0,
	}
	input <- shr
	t.Logf(`input <- {{Keyspace: "k", Shard: "s", TabletType: MASTER}, Serving: true, TabletExternallyReparentedTimestamp: 10, {SecondsBehindMaster: 1, CpuUsage: 0.2}}`)
	startTime = time.Now()
	lastResponseTime := hc.healthByAlias[tabletAlias].lastResponseTime()
	for {
		// Health has been updated
		if hc.healthByAlias[tabletAlias].lastResponseTime() != lastResponseTime || time.Since(startTime) > timeout {
			break
		}
		if time.Since(startTime) > timeout {
			t.Fatal("Timed out waiting for health update")
		}
		time.Sleep(10 * time.Microsecond)
	}
	assert.True(t, want.DeepEqual(hc.healthByAlias[tabletAlias]), "Wrong tabletHealth data:\n Expected: %v\n Actual:   %v", want, hc.healthByAlias[tabletAlias])

	// Stream error
	fc.errCh <- fmt.Errorf("some stream error")
	want = &tabletHealthCheck{
		Tablet:              tablet,
		Target:              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:             false,
		Stats:               &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
		MasterTermStartTime: 0,
		LastError:           fmt.Errorf("some stream error"),
	}
	startTime = time.Now()
	lastResponseTime = hc.healthByAlias[tabletAlias].lastResponseTime()
	for {
		// Health has been updated
		if hc.healthByAlias[tabletAlias].lastResponseTime() != lastResponseTime || time.Since(startTime) > timeout {
			break
		}
		if time.Since(startTime) > timeout {
			t.Fatal("Timed out waiting for health update")
		}
		time.Sleep(10 * time.Microsecond)
	}
	assert.True(t, want.DeepEqual(hc.healthByAlias[tabletAlias]), "Wrong tabletHealth data:\n Expected: %v\n Actual:   %v", want, hc.healthByAlias[tabletAlias])

	// close healthcheck
	hc.Close()
}

func TestHealthCheckVerifiesTabletAlias(t *testing.T) {
	ts := memorytopo.NewServer("cell")
	t.Logf("starting")
	tablet := topo.NewTablet(1, "cell", "a")
	tablet.PortMap["vt"] = 1
	tabletAlias := topoproto.TabletAliasString(tablet.Alias)
	input := make(chan *querypb.StreamHealthResponse, 1)
	fc := createFakeConn(tablet, input)
	t.Logf(`createFakeConn({Host: "a", PortMap: {"vt": 1}}, c)`)
	hc := createTestHc(ts)
	hc.AddTablet(tablet)
	t.Logf(`hc = HealthCheck(); hc.AddTablet({Host: "a", PortMap: {"vt": 1}}, "")`)

	// Immediately after AddTablet() there will be the first notification.
	want := &tabletHealthCheck{
		Tablet:              tablet,
		Target:              &querypb.Target{},
		Serving:             false,
		MasterTermStartTime: 0,
	}
	startTime := time.Now()
	timeout := 2 * time.Second
	for {
		if hc.healthByAlias[tabletAlias] != nil {
			break
		}
		if time.Since(startTime) > timeout {
			t.Fatal("Timed out waiting for initial health")
		}
		time.Sleep(10 * time.Microsecond)
	}
	assert.True(t, want.DeepEqual(hc.healthByAlias[tabletAlias]), "Wrong tabletHealth data:\n expected: %v\n got:      %v", want, hc.healthByAlias[tabletAlias])

	input <- &querypb.StreamHealthResponse{
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		TabletAlias:                         &topodatapb.TabletAlias{Uid: 20, Cell: "cellb"},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 10,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}

	ticker := time.NewTicker(2 * time.Second)
	select {
	case err := <-fc.cbErrCh:
		t.Logf("<-fc.cbErrCh: %v", err)
		if prefix := "health stats mismatch"; !strings.HasPrefix(err.Error(), prefix) {
			t.Fatalf("wrong error, got %v; want prefix %v", err, prefix)
		}
	case <-ticker.C:
		t.Fatalf("Timed out waiting for StreamHealth to return a health stats mismatch error")
	}

	input <- &querypb.StreamHealthResponse{
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		TabletAlias:                         &topodatapb.TabletAlias{Uid: 1, Cell: "cell"},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 10,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}

	startTime = time.Now()
	lastResponseTime := hc.healthByAlias[tabletAlias].lastResponseTime()
	for {
		// Health has been updated
		if hc.healthByAlias[tabletAlias].lastResponseTime() != lastResponseTime || time.Since(startTime) > timeout {
			break
		}
		if time.Since(startTime) > timeout {
			t.Fatal("Timed out waiting for health update")
		}
		time.Sleep(10 * time.Microsecond)
	}
	assert.True(t, want.DeepEqual(hc.healthByAlias[tabletAlias]), "Wrong tabletHealth data:\n Expected: %v\n Actual:   %v", want, hc.healthByAlias[tabletAlias])

	// close healthcheck
	hc.Close()
}

// TestHealthCheckCloseWaitsForGoRoutines tests that Close() waits for all Go
// routines to finish and the listener won't be called anymore.
func TestHealthCheckCloseWaitsForGoRoutines(t *testing.T) {
	ts := memorytopo.NewServer("cell")
	hc := createTestHc(ts)
	tablet := topo.NewTablet(0, "cell", "a")
	tablet.PortMap["vt"] = 1
	tabletAlias := topoproto.TabletAliasString(tablet.Alias)
	input := make(chan *querypb.StreamHealthResponse, 1)
	createFakeConn(tablet, input)
	t.Logf(`createFakeConn({Host: "a", PortMap: {"vt": 1}}, c)`)
	hc.AddTablet(tablet)
	t.Logf(`hc = HealthCheck(); hc.AddTablet({Host: "a", PortMap: {"vt": 1}}, "")`)

	// Immediately after AddTablet() there will be the first notification.
	want := &tabletHealthCheck{
		Tablet:              tablet,
		Target:              &querypb.Target{},
		Serving:             false,
		MasterTermStartTime: 0,
	}
	startTime := time.Now()
	timeout := 2 * time.Second
	for {
		if hc.healthByAlias[tabletAlias] != nil {
			break
		}
		if time.Since(startTime) > timeout {
			t.Fatal("Timed out waiting for initial health")
		}
		time.Sleep(10 * time.Microsecond)
	}
	assert.True(t, want.DeepEqual(hc.healthByAlias[tabletAlias]), "Wrong tabletHealth data:\n expected: %v\n got:      %v", want, hc.healthByAlias[tabletAlias])

	// one tablet after receiving a StreamHealthResponse
	shr := &querypb.StreamHealthResponse{
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 0,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}
	want = &tabletHealthCheck{
		Tablet:              tablet,
		Target:              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:             true,
		Stats:               &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
		MasterTermStartTime: 0,
	}
	input <- shr
	t.Logf(`input <- {{Keyspace: "k", Shard: "s", TabletType: MASTER}, Serving: true, TabletExternallyReparentedTimestamp: 10, {SecondsBehindMaster: 1, CpuUsage: 0.2}}`)
	startTime = time.Now()
	lastResponseTime := hc.healthByAlias[tabletAlias].lastResponseTime()
	for {
		// Health has been updated
		if hc.healthByAlias[tabletAlias].lastResponseTime() != lastResponseTime || time.Since(startTime) > timeout {
			break
		}
		if time.Since(startTime) > timeout {
			t.Fatal("Timed out waiting for health update")
		}
		time.Sleep(10 * time.Microsecond)
	}
	assert.True(t, want.DeepEqual(hc.healthByAlias[tabletAlias]), "Wrong tabletHealth data:\n Expected: %v\n Actual:   %v", want, hc.healthByAlias[tabletAlias])

	// Change input to distinguish between stats sent before and after Close().
	shr.TabletExternallyReparentedTimestamp = 11
	// Close the healthcheck. Tablet connections are closed asynchronously and
	// Close() will block until all Go routines (one per connection) are done.
	hc.Close()
	// Try to send more updates. They should be ignored and nothing should change
	// Note that this code is racy by nature. If there is a regression, it should
	// fail in some cases.
	input <- shr
	t.Logf(`input <- %v`, shr)
	assert.Nil(t, hc.healthByAlias, "health data should be nil")
}

func TestHealthCheckTimeout(t *testing.T) {
	ts := memorytopo.NewServer("cell")
	hc := createTestHc(ts)
	timeout := 500 * time.Millisecond
	hc.healthCheckTimeout = 2 * timeout
	tablet := topo.NewTablet(0, "cell", "a")
	tablet.PortMap["vt"] = 1
	tabletAlias := topoproto.TabletAliasString(tablet.Alias)
	input := make(chan *querypb.StreamHealthResponse)
	fc := createFakeConn(tablet, input)
	t.Logf(`createFakeConn({Host: "a", PortMap: {"vt": 1}}, c)`)
	hc.AddTablet(tablet)
	t.Logf(`hc = HealthCheck(); hc.AddTablet({Host: "a", PortMap: {"vt": 1}}, "")`)
	// Immediately after AddTablet() there will be the first notification.
	want := &TabletHealth{
		Tablet:              tablet,
		Target:              &querypb.Target{},
		Serving:             false,
		MasterTermStartTime: 0,
	}
	startTime := time.Now()
	for {
		if hc.healthByAlias[tabletAlias] != nil {
			break
		}
		if time.Since(startTime) > timeout {
			t.Fatal("Timed out waiting for initial health")
		}
		time.Sleep(10 * time.Microsecond)
	}
	assert.True(t, want.DeepEqual(hc.healthByAlias[tabletAlias].SimpleCopy()), "Wrong tabletHealth data:\n expected: %v\n got:      %v", want, hc.healthByAlias[tabletAlias])

	// one tablet after receiving a StreamHealthResponse
	shr := &querypb.StreamHealthResponse{
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 0,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}
	want = &TabletHealth{
		Tablet:              tablet,
		Target:              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:             true,
		Stats:               &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
		MasterTermStartTime: 0,
	}
	input <- shr
	t.Logf(`input <- {{Keyspace: "k", Shard: "s", TabletType: MASTER}, Serving: true, TabletExternallyReparentedTimestamp: 10, {SecondsBehindMaster: 1, CpuUsage: 0.2}}`)
	startTime = time.Now()
	lastResponseTime := hc.healthByAlias[tabletAlias].lastResponseTime()
	for {
		// Health has been updated
		if hc.healthByAlias[tabletAlias].lastResponseTime() != lastResponseTime || time.Since(startTime) > timeout {
			break
		}
		if time.Since(startTime) > timeout {
			t.Fatal("Timed out waiting for health update")
		}
		time.Sleep(10 * time.Microsecond)
	}
	assert.True(t, want.DeepEqual(hc.healthByAlias[tabletAlias].SimpleCopy()), "Wrong tabletHealth data:\n Expected: %v\n Actual:   %v", want, hc.healthByAlias[tabletAlias])

	if err := checkErrorCounter("k", "s", topodatapb.TabletType_REPLICA, 0); err != nil {
		t.Errorf("%v", err)
	}

	// wait for timeout period
	time.Sleep(hc.healthCheckTimeout + 100*time.Millisecond)
	t.Logf(`Sleep(1.1 * timeout)`)
	res := hc.healthByAlias[tabletAlias]
	if res.Serving {
		t.Errorf(`tabletHealthCheck: %+v; want not serving`, res)
	}

	if err := checkErrorCounter("k", "s", topodatapb.TabletType_REPLICA, 1); err != nil {
		t.Errorf("%v", err)
	}

	if !fc.isCanceled() {
		t.Errorf("StreamHealth should be canceled after timeout, but is not")
	}

	// repeat the wait. It will timeout one more time trying to get the connection.
	fc.resetCanceledFlag()
	time.Sleep(hc.healthCheckTimeout)
	t.Logf(`Sleep(timeout)`)

	res = hc.healthByAlias[tabletAlias]
	if res.Serving {
		t.Errorf(`tabletHealthCheck: %+v; want not serving`, res)
	}

	if err := checkErrorCounter("k", "s", topodatapb.TabletType_REPLICA, 2); err != nil {
		t.Errorf("%v", err)
	}

	if !fc.isCanceled() {
		t.Errorf("StreamHealth should be canceled again after timeout")
	}

	// send a healthcheck response, it should be serving again
	fc.resetCanceledFlag()
	input <- shr
	t.Logf(`input <- {{Keyspace: "k", Shard: "s", TabletType: MASTER}, Serving: true, TabletExternallyReparentedTimestamp: 10, {SecondsBehindMaster: 1, CpuUsage: 0.2}}`)

	// wait for the exponential backoff to wear off and health monitoring to resume.
	startTime = time.Now()
	lastResponseTime = hc.healthByAlias[tabletAlias].lastResponseTime()
	for {
		// Health has been updated
		if hc.healthByAlias[tabletAlias].lastResponseTime() != lastResponseTime || time.Since(startTime) > timeout {
			break
		}
		if time.Since(startTime) > timeout {
			t.Fatal("Timed out waiting for health update")
		}
		time.Sleep(10 * time.Microsecond)
	}
	res = hc.healthByAlias[tabletAlias]
	assert.True(t, want.DeepEqual(res.SimpleCopy()), "Wrong tabletHealth data:\n Expected: %v\n Actual:   %v", want, hc.healthByAlias[tabletAlias])
	// close healthcheck
	hc.Close()
}

func TestTemplate(t *testing.T) {
	tablet := topo.NewTablet(0, "cell", "a")
	ts := []*TabletHealth{
		{
			Tablet:              tablet,
			Target:              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
			Serving:             false,
			Stats:               &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.3},
			MasterTermStartTime: 0,
		},
	}
	tcs := &TabletsCacheStatus{
		Cell:         "cell",
		Target:       &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		TabletsStats: ts,
	}
	templ := template.New("").Funcs(status.StatusFuncs)
	templ, err := templ.Parse(HealthCheckTemplate)
	require.Nil(t, err, "error parsing template")
	wr := &bytes.Buffer{}
	err = templ.Execute(wr, []*TabletsCacheStatus{tcs})
	require.Nil(t, err, "error executing template")
}

func TestDebugURLFormatting(t *testing.T) {
	flag.Set("tablet_url_template", "https://{{.GetHostNameLevel 0}}.bastion.{{.Tablet.Alias.Cell}}.corp")
	ParseTabletURLTemplateFromFlag()

	tablet := topo.NewTablet(0, "cell", "host.dc.domain")
	ts := []*TabletHealth{
		{
			Tablet:              tablet,
			Target:              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
			Serving:             false,
			Stats:               &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.3},
			MasterTermStartTime: 0,
		},
	}
	tcs := &TabletsCacheStatus{
		Cell:         "cell",
		Target:       &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		TabletsStats: ts,
	}
	templ := template.New("").Funcs(status.StatusFuncs)
	templ, err := templ.Parse(HealthCheckTemplate)
	require.Nil(t, err, "error parsing template")
	wr := &bytes.Buffer{}
	err = templ.Execute(wr, []*TabletsCacheStatus{tcs})
	require.Nil(t, err, "error executing template")
	expectedURL := `"https://host.bastion.cell.corp"`
	require.Contains(t, wr.String(), expectedURL, "output missing formatted URL")
}

func tabletDialer(tablet *topodatapb.Tablet, failFast grpcclient.FailFast) (queryservice.QueryService, error) {
	key := TabletToMapKey(tablet)
	if qs, ok := connMap[key]; ok {
		return qs, nil
	}
	return nil, fmt.Errorf("tablet %v not found", key)
}

func createTestHc(ts *topo.Server) *HealthCheckImpl {
	return NewHealthCheck(context.Background(), 1*time.Millisecond, time.Hour, ts, "cell", nil).(*HealthCheckImpl)
}
