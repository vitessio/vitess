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
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/vttablet/queryservice/fakes"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	"context"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/status"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	"vitess.io/vitess/go/vt/proto/query"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var connMap map[string]*fakeConn
var connMapMu sync.Mutex

func init() {
	tabletconn.RegisterDialer("fake_gateway", tabletDialer)

	//log error
	if err := flag.Set("tablet_protocol", "fake_gateway"); err != nil {
		log.Errorf("failed to set flag \"tablet_protocol\" to \"fake_gateway\":%v", err)
	}
	connMap = make(map[string]*fakeConn)
}

func TestHealthCheck(t *testing.T) {
	// reset error counters
	hcErrorCounters.ResetAll()
	ts := memorytopo.NewServer("cell")
	hc := createTestHc(ts)
	// close healthcheck
	defer hc.Close()
	tablet := createTestTablet(0, "cell", "a")
	tablet.Type = topodatapb.TabletType_REPLICA
	input := make(chan *querypb.StreamHealthResponse)
	conn := createFakeConn(tablet, input)

	// create a channel and subscribe to healthcheck
	resultChan := hc.Subscribe()
	testChecksum(t, 0, hc.stateChecksum())
	hc.AddTablet(tablet)
	testChecksum(t, 1027934207, hc.stateChecksum())

	// Immediately after AddTablet() there will be the first notification.
	want := &TabletHealth{
		Tablet:              tablet,
		Target:              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:             false,
		Stats:               nil,
		MasterTermStartTime: 0,
	}
	result := <-resultChan
	mustMatch(t, want, result, "Wrong TabletHealth data")

	shr := &querypb.StreamHealthResponse{
		TabletAlias: tablet.Alias,
		Target:      &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:     true,

		TabletExternallyReparentedTimestamp: 0,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.5},
	}
	input <- shr
	result = <-resultChan
	want = &TabletHealth{
		Tablet:              tablet,
		Target:              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:             true,
		Stats:               &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.5},
		MasterTermStartTime: 0,
	}
	// create a context with timeout and select on it and channel
	mustMatch(t, want, result, "Wrong TabletHealth data")

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
	// we can't use assert.Equal here because of the special way we want to compare equality
	assert.True(t, tcslWant.deepEqual(tcsl), "Incorrect cache status:\n Expected: %+v\n Actual:   %+v", tcslWant[0], tcsl[0])
	testChecksum(t, 3487343103, hc.stateChecksum())

	// TabletType changed, should get both old and new event
	shr = &querypb.StreamHealthResponse{
		TabletAlias:                         tablet.Alias,
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
	result = <-resultChan

	mustMatch(t, want, result, "Wrong TabletHealth data")
	testChecksum(t, 1780128002, hc.stateChecksum())

	err := checkErrorCounter("k", "s", topodatapb.TabletType_MASTER, 0)
	require.NoError(t, err, "error checking error counter")

	// Serving & RealtimeStats changed
	shr = &querypb.StreamHealthResponse{
		TabletAlias:                         tablet.Alias,
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
	result = <-resultChan
	mustMatch(t, want, result, "Wrong TabletHealth data")
	testChecksum(t, 1027934207, hc.stateChecksum())

	// HealthError
	shr = &querypb.StreamHealthResponse{
		TabletAlias:                         tablet.Alias,
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
	result = <-resultChan
	//TODO: figure out how to compare objects that contain errors using utils.MustMatch
	assert.True(t, want.DeepEqual(result), "Wrong TabletHealth data\n Expected: %v\n Actual:   %v", want, result)
	testChecksum(t, 1027934207, hc.stateChecksum()) // unchanged

	// remove tablet
	hc.deleteTablet(tablet)
	testChecksum(t, 0, hc.stateChecksum())
}

func TestHealthCheckStreamError(t *testing.T) {
	ts := memorytopo.NewServer("cell")
	hc := createTestHc(ts)
	defer hc.Close()

	tablet := createTestTablet(0, "cell", "a")
	input := make(chan *querypb.StreamHealthResponse)
	resultChan := hc.Subscribe()
	fc := createFakeConn(tablet, input)
	fc.errCh = make(chan error)
	hc.AddTablet(tablet)

	// Immediately after AddTablet() there will be the first notification.
	want := &TabletHealth{
		Tablet:              tablet,
		Target:              &querypb.Target{Keyspace: "k", Shard: "s"},
		Serving:             false,
		MasterTermStartTime: 0,
	}
	result := <-resultChan
	mustMatch(t, want, result, "Wrong TabletHealth data")

	// one tablet after receiving a StreamHealthResponse
	shr := &querypb.StreamHealthResponse{
		TabletAlias:                         tablet.Alias,
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
	result = <-resultChan
	mustMatch(t, want, result, "Wrong TabletHealth data")

	// Stream error
	fc.errCh <- fmt.Errorf("some stream error")
	want = &TabletHealth{
		Tablet:              tablet,
		Target:              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:             false,
		Stats:               &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
		MasterTermStartTime: 0,
		LastError:           fmt.Errorf("some stream error"),
	}
	result = <-resultChan
	//TODO: figure out how to compare objects that contain errors using utils.MustMatch
	assert.True(t, want.DeepEqual(result), "Wrong TabletHealth data\n Expected: %v\n Actual:   %v", want, result)
	// tablet should be removed from healthy list
	a := hc.GetHealthyTabletStats(&querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA})
	assert.Empty(t, a, "wrong result, expected empty list")
}

// TestHealthCheckErrorOnPrimary is the same as TestHealthCheckStreamError except for tablet type
func TestHealthCheckErrorOnPrimary(t *testing.T) {
	ts := memorytopo.NewServer("cell")
	hc := createTestHc(ts)
	defer hc.Close()

	tablet := createTestTablet(0, "cell", "a")
	input := make(chan *querypb.StreamHealthResponse)
	resultChan := hc.Subscribe()
	fc := createFakeConn(tablet, input)
	fc.errCh = make(chan error)
	hc.AddTablet(tablet)

	// Immediately after AddTablet() there will be the first notification.
	want := &TabletHealth{
		Tablet:              tablet,
		Target:              &querypb.Target{Keyspace: "k", Shard: "s"},
		Serving:             false,
		MasterTermStartTime: 0,
	}
	result := <-resultChan
	mustMatch(t, want, result, "Wrong TabletHealth data")

	// one tablet after receiving a StreamHealthResponse
	shr := &querypb.StreamHealthResponse{
		TabletAlias:                         tablet.Alias,
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 10,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}
	want = &TabletHealth{
		Tablet:              tablet,
		Target:              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		Serving:             true,
		Stats:               &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
		MasterTermStartTime: 10,
	}
	input <- shr
	result = <-resultChan
	mustMatch(t, want, result, "Wrong TabletHealth data")

	// Stream error
	fc.errCh <- fmt.Errorf("some stream error")
	want = &TabletHealth{
		Tablet:              tablet,
		Target:              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		Serving:             false,
		Stats:               &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
		MasterTermStartTime: 10,
		LastError:           fmt.Errorf("some stream error"),
	}
	result = <-resultChan
	//TODO: figure out how to compare objects that contain errors using utils.MustMatch
	assert.True(t, want.DeepEqual(result), "Wrong TabletHealth data\n Expected: %v\n Actual:   %v", want, result)
	// tablet should be removed from healthy list
	a := hc.GetHealthyTabletStats(&querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER})
	assert.Empty(t, a, "wrong result, expected empty list")
}

func TestHealthCheckVerifiesTabletAlias(t *testing.T) {
	ts := memorytopo.NewServer("cell")
	hc := createTestHc(ts)
	defer hc.Close()

	tablet := createTestTablet(0, "cell", "a")
	input := make(chan *querypb.StreamHealthResponse, 1)
	fc := createFakeConn(tablet, input)
	resultChan := hc.Subscribe()

	hc.AddTablet(tablet)

	// Immediately after AddTablet() there will be the first notification.
	want := &TabletHealth{
		Tablet:              tablet,
		Target:              &querypb.Target{Keyspace: "k", Shard: "s"},
		Serving:             false,
		MasterTermStartTime: 0,
	}
	result := <-resultChan
	mustMatch(t, want, result, "Wrong TabletHealth data")

	input <- &querypb.StreamHealthResponse{
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		TabletAlias:                         &topodatapb.TabletAlias{Uid: 20, Cell: "cellb"},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 10,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}

	ticker := time.NewTicker(1 * time.Second)
	select {
	case err := <-fc.cbErrCh:
		assert.Contains(t, err.Error(), "health stats mismatch", "wrong error")
	case <-resultChan:
		require.Fail(t, "StreamHealth should have returned a health stats mismatch error")
	case <-ticker.C:
		require.Fail(t, "Timed out waiting for StreamHealth to return a health stats mismatch error")
	}
}

// TestHealthCheckCloseWaitsForGoRoutines tests that Close() waits for all Go
// routines to finish and the listener won't be called anymore.
func TestHealthCheckCloseWaitsForGoRoutines(t *testing.T) {
	ts := memorytopo.NewServer("cell")
	hc := createTestHc(ts)
	tablet := createTestTablet(0, "cell", "a")
	input := make(chan *querypb.StreamHealthResponse, 1)
	createFakeConn(tablet, input)
	resultChan := hc.Subscribe()

	hc.AddTablet(tablet)

	// Immediately after AddTablet() there will be the first notification.
	want := &TabletHealth{
		Tablet:              tablet,
		Target:              &querypb.Target{Keyspace: "k", Shard: "s"},
		Serving:             false,
		MasterTermStartTime: 0,
	}
	result := <-resultChan
	mustMatch(t, want, result, "Wrong TabletHealth data")

	// one tablet after receiving a StreamHealthResponse
	shr := &querypb.StreamHealthResponse{
		TabletAlias:                         tablet.Alias,
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 0,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}
	want = &TabletHealth{
		Tablet:  tablet,
		Target:  &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving: true,
		Stats:   &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},

		MasterTermStartTime: 0,
	}
	input <- shr
	result = <-resultChan
	mustMatch(t, want, result, "Wrong TabletHealth data")

	// Change input to distinguish between stats sent before and after Close().
	shr.TabletExternallyReparentedTimestamp = 11
	// Close the healthcheck. Tablet connections are closed asynchronously and
	// Close() will block until all Go routines (one per connection) are done.
	assert.Nil(t, hc.Close(), "Close returned error")
	// Try to send more updates. They should be ignored and nothing should change
	input <- shr

	select {
	case result = <-resultChan:
		assert.Nil(t, result, "healthCheck still running after Close(): received result: %v", result)
	case <-time.After(1 * time.Millisecond):
		// No response after timeout. Success.
	}

	hc.mu.Lock()
	defer hc.mu.Unlock()
	assert.Nil(t, hc.healthByAlias, "health data should be nil")
}

func TestHealthCheckTimeout(t *testing.T) {
	// reset counters
	hcErrorCounters.ResetAll()
	ts := memorytopo.NewServer("cell")
	hc := createTestHc(ts)
	hc.healthCheckTimeout = 500 * time.Millisecond
	defer hc.Close()
	tablet := createTestTablet(0, "cell", "a")
	input := make(chan *querypb.StreamHealthResponse)
	fc := createFakeConn(tablet, input)
	resultChan := hc.Subscribe()
	hc.AddTablet(tablet)
	// Immediately after AddTablet() there will be the first notification.
	want := &TabletHealth{
		Tablet:              tablet,
		Target:              &querypb.Target{Keyspace: "k", Shard: "s"},
		Serving:             false,
		MasterTermStartTime: 0,
	}
	result := <-resultChan
	mustMatch(t, want, result, "Wrong TabletHealth data")

	// one tablet after receiving a StreamHealthResponse
	shr := &querypb.StreamHealthResponse{
		TabletAlias:                         tablet.Alias,
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
	result = <-resultChan
	mustMatch(t, want, result, "Wrong TabletHealth data")
	assert.Nil(t, checkErrorCounter("k", "s", topodatapb.TabletType_REPLICA, 0))

	// wait for timeout period
	time.Sleep(hc.healthCheckTimeout + 100*time.Millisecond)
	t.Logf(`Sleep(1.1 * timeout)`)
	result = <-resultChan
	assert.False(t, result.Serving, "tabletHealthCheck: %+v; want not serving", result)
	assert.Nil(t, checkErrorCounter("k", "s", topodatapb.TabletType_REPLICA, 1))
	assert.True(t, fc.isCanceled(), "StreamHealth should be canceled after timeout, but is not")

	// tablet should be removed from healthy list
	a := hc.GetHealthyTabletStats(&querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA})
	assert.Empty(t, a, "wrong result, expected empty list")

	// repeat the wait. It will timeout one more time trying to get the connection.
	fc.resetCanceledFlag()
	time.Sleep(hc.healthCheckTimeout)

	result = <-resultChan
	assert.False(t, result.Serving, "tabletHealthCheck: %+v; want not serving", result)
	assert.Nil(t, checkErrorCounter("k", "s", topodatapb.TabletType_REPLICA, 2))
	assert.True(t, fc.isCanceled(), "StreamHealth should be canceled again after timeout, but is not")

	// send a healthcheck response, it should be serving again
	fc.resetCanceledFlag()
	input <- shr

	// wait for the exponential backoff to wear off and health monitoring to resume.
	result = <-resultChan
	mustMatch(t, want, result, "Wrong TabletHealth data")
}

func TestWaitForAllServingTablets(t *testing.T) {
	ts := memorytopo.NewServer("cell")
	hc := createTestHc(ts)
	defer hc.Close()
	tablet := createTestTablet(0, "cell", "a")
	tablet.Type = topodatapb.TabletType_REPLICA
	targets := []*query.Target{
		{
			Keyspace:   tablet.Keyspace,
			Shard:      tablet.Shard,
			TabletType: tablet.Type,
		},
	}
	input := make(chan *querypb.StreamHealthResponse)
	createFakeConn(tablet, input)

	// create a channel and subscribe to healthcheck
	resultChan := hc.Subscribe()
	hc.AddTablet(tablet)
	// there will be a first result, get and discard it
	<-resultChan
	// empty
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := hc.WaitForAllServingTablets(ctx, targets)
	assert.NotNil(t, err, "error should not be nil")

	shr := &querypb.StreamHealthResponse{
		TabletAlias:                         tablet.Alias,
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 0,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}

	input <- shr
	<-resultChan
	// // check it's there

	targets = []*query.Target{
		{
			Keyspace:   tablet.Keyspace,
			Shard:      tablet.Shard,
			TabletType: tablet.Type,
		},
	}

	err = hc.WaitForAllServingTablets(ctx, targets)
	assert.Nil(t, err, "error should be nil. Targets are found")

	targets = []*query.Target{
		{
			Keyspace:   tablet.Keyspace,
			Shard:      tablet.Shard,
			TabletType: tablet.Type,
		},
		{
			Keyspace:   "newkeyspace",
			Shard:      tablet.Shard,
			TabletType: tablet.Type,
		},
	}

	err = hc.WaitForAllServingTablets(ctx, targets)
	assert.NotNil(t, err, "error should not be nil (there are no tablets on this keyspace")

	targets = []*query.Target{
		{
			Keyspace:   tablet.Keyspace,
			Shard:      tablet.Shard,
			TabletType: tablet.Type,
		},
		{
			Keyspace:   "newkeyspace",
			Shard:      tablet.Shard,
			TabletType: tablet.Type,
		},
	}

	KeyspacesToWatch = []string{tablet.Keyspace}

	err = hc.WaitForAllServingTablets(ctx, targets)
	assert.Nil(t, err, "error should be nil. Keyspace with no tablets is filtered")

	KeyspacesToWatch = []string{}
}

// TestRemoveTablet tests the behavior when a tablet goes away.
func TestRemoveTablet(t *testing.T) {
	ts := memorytopo.NewServer("cell")
	hc := createTestHc(ts)
	defer hc.Close()
	tablet := createTestTablet(0, "cell", "a")
	tablet.Type = topodatapb.TabletType_REPLICA
	input := make(chan *querypb.StreamHealthResponse)
	createFakeConn(tablet, input)

	// create a channel and subscribe to healthcheck
	resultChan := hc.Subscribe()
	hc.AddTablet(tablet)
	// there will be a first result, get and discard it
	<-resultChan

	shr := &querypb.StreamHealthResponse{
		TabletAlias:                         tablet.Alias,
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 0,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}
	want := []*TabletHealth{{
		Tablet:              tablet,
		Target:              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:             true,
		Stats:               &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
		MasterTermStartTime: 0,
	}}
	input <- shr
	<-resultChan
	// check it's there
	a := hc.GetHealthyTabletStats(&querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA})
	mustMatch(t, want, a, "unexpected result")

	// delete the tablet
	hc.RemoveTablet(tablet)
	a = hc.GetHealthyTabletStats(&querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA})
	assert.Empty(t, a, "wrong result, expected empty list")
}

// TestGetHealthyTablets tests the functionality of GetHealthyTabletStats.
func TestGetHealthyTablets(t *testing.T) {
	ts := memorytopo.NewServer("cell")
	hc := createTestHc(ts)
	defer hc.Close()
	tablet := createTestTablet(0, "cell", "a")
	tablet.Type = topodatapb.TabletType_REPLICA
	input := make(chan *querypb.StreamHealthResponse)
	createFakeConn(tablet, input)

	// create a channel and subscribe to healthcheck
	resultChan := hc.Subscribe()
	hc.AddTablet(tablet)
	// there will be a first result, get and discard it
	<-resultChan
	// empty
	a := hc.GetHealthyTabletStats(&querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER})
	assert.Empty(t, a, "wrong result, expected empty list")

	shr := &querypb.StreamHealthResponse{
		TabletAlias:                         tablet.Alias,
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 0,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}
	want := []*TabletHealth{{
		Tablet:              tablet,
		Target:              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:             true,
		Stats:               &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
		MasterTermStartTime: 0,
	}}
	input <- shr
	<-resultChan
	// check it's there
	a = hc.GetHealthyTabletStats(&querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA})
	mustMatch(t, want, a, "unexpected result")

	// update health with a change that won't change health array
	shr = &querypb.StreamHealthResponse{
		TabletAlias:                         tablet.Alias,
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 0,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 2, CpuUsage: 0.2},
	}
	input <- shr
	// wait for result before checking
	<-resultChan
	// check it's there
	a = hc.GetHealthyTabletStats(&querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA})
	mustMatch(t, want, a, "unexpected result")

	// update stats with a change that will change health array
	shr = &querypb.StreamHealthResponse{
		TabletAlias:                         tablet.Alias,
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 0,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 35, CpuUsage: 0.2},
	}
	want = []*TabletHealth{{
		Tablet:              tablet,
		Target:              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:             true,
		Stats:               &querypb.RealtimeStats{SecondsBehindMaster: 35, CpuUsage: 0.2},
		MasterTermStartTime: 0,
	}}
	input <- shr
	// wait for result before checking
	<-resultChan
	// check it's there
	a = hc.GetHealthyTabletStats(&querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA})
	mustMatch(t, want, a, "unexpected result")

	// add a second tablet
	tablet2 := createTestTablet(11, "cell", "host2")
	tablet2.Type = topodatapb.TabletType_REPLICA
	input2 := make(chan *querypb.StreamHealthResponse)
	createFakeConn(tablet2, input2)
	hc.AddTablet(tablet2)
	// there will be a first result, get and discard it
	<-resultChan

	shr2 := &querypb.StreamHealthResponse{
		TabletAlias:                         tablet2.Alias,
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 0,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 10, CpuUsage: 0.2},
	}
	want2 := []*TabletHealth{{
		Tablet:              tablet,
		Target:              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:             true,
		Stats:               &querypb.RealtimeStats{SecondsBehindMaster: 35, CpuUsage: 0.2},
		MasterTermStartTime: 0,
	}, {
		Tablet:              tablet2,
		Target:              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:             true,
		Stats:               &querypb.RealtimeStats{SecondsBehindMaster: 10, CpuUsage: 0.2},
		MasterTermStartTime: 0,
	}}
	input2 <- shr2
	// wait for result
	<-resultChan
	a = hc.GetHealthyTabletStats(&querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA})
	assert.Equal(t, 2, len(a), "Wrong number of results")
	if a[0].Tablet.Alias.Uid == 11 {
		a[0], a[1] = a[1], a[0]
	}
	mustMatch(t, want2, a, "unexpected result")

	shr2 = &querypb.StreamHealthResponse{
		TabletAlias:                         tablet2.Alias,
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:                             false,
		TabletExternallyReparentedTimestamp: 0,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 10, CpuUsage: 0.2},
	}
	input2 <- shr2
	// wait for result
	<-resultChan
	a = hc.GetHealthyTabletStats(&querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA})
	assert.Equal(t, 1, len(a), "Wrong number of results")

	// second tablet turns into a master
	shr2 = &querypb.StreamHealthResponse{
		TabletAlias: tablet2.Alias,
		Target:      &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		Serving:     true,

		TabletExternallyReparentedTimestamp: 10,

		RealtimeStats: &querypb.RealtimeStats{SecondsBehindMaster: 0, CpuUsage: 0.2},
	}
	input2 <- shr2
	// wait for result
	<-resultChan
	// check we only have 1 healthy replica left
	a = hc.GetHealthyTabletStats(&querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA})
	mustMatch(t, want, a, "unexpected result")

	want2 = []*TabletHealth{{
		Tablet:              tablet2,
		Target:              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		Serving:             true,
		Stats:               &querypb.RealtimeStats{SecondsBehindMaster: 0, CpuUsage: 0.2},
		MasterTermStartTime: 10,
	}}
	// check we have a master now
	a = hc.GetHealthyTabletStats(&querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER})
	mustMatch(t, want2, a, "unexpected result")

	// reparent: old replica goes into master
	shr = &querypb.StreamHealthResponse{
		TabletAlias:                         tablet.Alias,
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 20,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 0, CpuUsage: 0.2},
	}
	input <- shr
	<-resultChan
	want = []*TabletHealth{{
		Tablet:              tablet,
		Target:              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		Serving:             true,
		Stats:               &querypb.RealtimeStats{SecondsBehindMaster: 0, CpuUsage: 0.2},
		MasterTermStartTime: 20,
	}}

	// check we lost all replicas, and master is new one
	a = hc.GetHealthyTabletStats(&querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA})
	assert.Empty(t, a, "Wrong number of results")
	a = hc.GetHealthyTabletStats(&querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER})
	mustMatch(t, want, a, "unexpected result")

	// old master sending an old ping should be ignored
	input2 <- shr2
	<-resultChan
	a = hc.GetHealthyTabletStats(&querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER})
	mustMatch(t, want, a, "unexpected result")
}

func TestMasterInOtherCell(t *testing.T) {
	ts := memorytopo.NewServer("cell1", "cell2")
	hc := NewHealthCheck(context.Background(), 1*time.Millisecond, time.Hour, ts, "cell1", "cell1, cell2")
	defer hc.Close()

	// add a tablet as master in different cell
	tablet := createTestTablet(1, "cell2", "host1")
	tablet.Type = topodatapb.TabletType_MASTER
	input := make(chan *querypb.StreamHealthResponse)
	fc := createFakeConn(tablet, input)
	// create a channel and subscribe to healthcheck
	resultChan := hc.Subscribe()
	hc.AddTablet(tablet)
	// should get a result, but this will hang if multi-cell logic is broken
	// so wait and timeout
	ticker := time.NewTicker(1 * time.Second)
	select {
	case err := <-fc.cbErrCh:
		require.Fail(t, "Unexpected error: %v", err)
	case <-resultChan:
	case <-ticker.C:
		require.Fail(t, "Timed out waiting for HealthCheck update")
	}

	shr := &querypb.StreamHealthResponse{
		TabletAlias:                         tablet.Alias,
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 20,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 0, CpuUsage: 0.2},
	}
	want := &TabletHealth{
		Tablet:              tablet,
		Target:              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		Serving:             true,
		Stats:               &querypb.RealtimeStats{SecondsBehindMaster: 0, CpuUsage: 0.2},
		MasterTermStartTime: 20,
	}

	input <- shr
	ticker = time.NewTicker(1 * time.Second)
	select {
	case err := <-fc.cbErrCh:
		require.Fail(t, "Unexpected error: %v", err)
	case got := <-resultChan:
		// check that we DO receive health check update for MASTER in other cell
		mustMatch(t, want, got, "Wrong TabletHealth data")
	case <-ticker.C:
		require.Fail(t, "Timed out waiting for HealthCheck update")
	}

	// check that MASTER tablet from other cell IS in healthy tablet list
	a := hc.GetHealthyTabletStats(&querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER})
	require.Len(t, a, 1, "")
	mustMatch(t, want, a[0], "Expecting healthy master")
}

func TestReplicaInOtherCell(t *testing.T) {
	ts := memorytopo.NewServer("cell1", "cell2")
	hc := NewHealthCheck(context.Background(), 1*time.Millisecond, time.Hour, ts, "cell1", "cell1, cell2")
	defer hc.Close()

	// add a tablet as replica
	local := createTestTablet(1, "cell1", "host1")
	local.Type = topodatapb.TabletType_REPLICA
	input := make(chan *querypb.StreamHealthResponse)
	fc := createFakeConn(local, input)
	// create a channel and subscribe to healthcheck
	resultChan := hc.Subscribe()
	hc.AddTablet(local)

	ticker := time.NewTicker(1 * time.Second)
	select {
	case err := <-fc.cbErrCh:
		require.Fail(t, "Unexpected error: %v", err)
	case <-resultChan:
	case <-ticker.C:
		require.Fail(t, "Timed out waiting for HealthCheck update")
	}

	shr := &querypb.StreamHealthResponse{
		TabletAlias:                         local.Alias,
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 0,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 10, CpuUsage: 0.2},
	}
	want := &TabletHealth{
		Tablet:              local,
		Target:              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:             true,
		Stats:               &querypb.RealtimeStats{SecondsBehindMaster: 10, CpuUsage: 0.2},
		MasterTermStartTime: 0,
	}

	input <- shr
	ticker = time.NewTicker(1 * time.Second)
	select {
	case err := <-fc.cbErrCh:
		require.Fail(t, "Unexpected error: %v", err)
	case got := <-resultChan:
		// check that we DO receive health check update for REPLICA in other cell
		mustMatch(t, want, got, "Wrong TabletHealth data")
	case <-ticker.C:
		require.Fail(t, "Timed out waiting for HealthCheck update")
	}

	// add a tablet as replica in different cell
	remote := createTestTablet(2, "cell2", "host2")
	remote.Type = topodatapb.TabletType_REPLICA
	input2 := make(chan *querypb.StreamHealthResponse)
	fc2 := createFakeConn(remote, input2)
	// create a channel and subscribe to healthcheck
	resultChan2 := hc.Subscribe()
	hc.AddTablet(remote)
	// should get a result, but this will hang if multi-cell logic is broken
	// so wait and timeout
	ticker = time.NewTicker(1 * time.Second)
	select {
	case err := <-fc2.cbErrCh:
		require.Fail(t, "Unexpected error: %v", err)
	case <-resultChan2:
	case <-ticker.C:
		require.Fail(t, "Timed out waiting for HealthCheck update")
	}

	shr2 := &querypb.StreamHealthResponse{
		TabletAlias:                         remote.Alias,
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 0,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 10, CpuUsage: 0.2},
	}
	want2 := &TabletHealth{
		Tablet:              remote,
		Target:              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:             true,
		Stats:               &querypb.RealtimeStats{SecondsBehindMaster: 10, CpuUsage: 0.2},
		MasterTermStartTime: 0,
	}

	input2 <- shr2
	ticker = time.NewTicker(1 * time.Second)
	select {
	case err := <-fc.cbErrCh:
		require.Fail(t, "Unexpected error: %v", err)
	case got := <-resultChan2:
		// check that we DO receive health check update for REPLICA in other cell
		mustMatch(t, want2, got, "Wrong TabletHealth data")
	case <-ticker.C:
		require.Fail(t, "Timed out waiting for HealthCheck update")
	}

	// check that only REPLICA tablet from cell1 is in healthy tablet list
	a := hc.GetHealthyTabletStats(&querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA})
	require.Len(t, a, 1, "")
	mustMatch(t, want, a[0], "Expecting healthy local replica")
}

func TestCellAliases(t *testing.T) {
	ts := memorytopo.NewServer("cell1", "cell2")
	hc := NewHealthCheck(context.Background(), 1*time.Millisecond, time.Hour, ts, "cell1", "cell1, cell2")
	defer hc.Close()

	cellsAlias := &topodatapb.CellsAlias{
		Cells: []string{"cell1", "cell2"},
	}
	assert.Nil(t, ts.CreateCellsAlias(context.Background(), "region1", cellsAlias), "failed to create cell alias")
	defer ts.DeleteCellsAlias(context.Background(), "region1")

	// add a tablet as replica in diff cell, same region
	tablet := createTestTablet(1, "cell2", "host2")
	tablet.Type = topodatapb.TabletType_REPLICA
	input := make(chan *querypb.StreamHealthResponse)
	fc := createFakeConn(tablet, input)
	// create a channel and subscribe to healthcheck
	resultChan := hc.Subscribe()
	hc.AddTablet(tablet)
	// should get a result, but this will hang if cell alias logic is broken
	// so wait and timeout
	ticker := time.NewTicker(1 * time.Second)
	select {
	case err := <-fc.cbErrCh:
		require.Fail(t, "Unexpected error: %v", err)
	case <-resultChan:
	case <-ticker.C:
		require.Fail(t, "Timed out waiting for HealthCheck update")
	}

	shr := &querypb.StreamHealthResponse{
		TabletAlias:                         tablet.Alias,
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 0,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 10, CpuUsage: 0.2},
	}
	want := []*TabletHealth{{
		Tablet:              tablet,
		Target:              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:             true,
		Stats:               &querypb.RealtimeStats{SecondsBehindMaster: 10, CpuUsage: 0.2},
		MasterTermStartTime: 0,
	}}

	input <- shr
	ticker = time.NewTicker(1 * time.Second)
	select {
	case err := <-fc.cbErrCh:
		require.Fail(t, "Unexpected error: %v", err)
	case <-resultChan:
	case <-ticker.C:
		require.Fail(t, "Timed out waiting for HealthCheck update")
	}

	// check it's there
	a := hc.GetHealthyTabletStats(&querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA})
	mustMatch(t, want, a, "Wrong TabletHealth data")
}

func TestHealthCheckChecksGrpcPort(t *testing.T) {
	ts := memorytopo.NewServer("cell")
	hc := createTestHc(ts)
	defer hc.Close()

	tablet := createTestTablet(0, "cell", "a")
	tablet.PortMap["grpc"] = 0
	resultChan := hc.Subscribe()

	// AddTablet should not add the tablet because port is 0
	hc.AddTablet(tablet)

	select {
	case result := <-resultChan:
		assert.Nil(t, result, "healthCheck received result: %v", result)
	case <-time.After(2 * time.Millisecond):
		// No response after timeout. Success.
	}
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
	//log error
	if err2 := flag.Set("tablet_url_template", "https://{{.GetHostNameLevel 0}}.bastion.{{.Tablet.Alias.Cell}}.corp"); err2 != nil {
		log.Errorf("flag.Set(\"tablet_url_template\", \"https://{{.GetHostNameLevel 0}}.bastion.{{.Tablet.Alias.Cell}}.corp\") failed : %v", err2)
	}
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

func tabletDialer(tablet *topodatapb.Tablet, _ grpcclient.FailFast) (queryservice.QueryService, error) {
	connMapMu.Lock()
	defer connMapMu.Unlock()

	key := TabletToMapKey(tablet)
	if qs, ok := connMap[key]; ok {
		return qs, nil
	}
	return nil, fmt.Errorf("tablet %v not found", key)
}

func createTestHc(ts *topo.Server) *HealthCheckImpl {
	return NewHealthCheck(context.Background(), 1*time.Millisecond, time.Hour, ts, "cell", "")
}

type fakeConn struct {
	queryservice.QueryService
	tablet *topodatapb.Tablet
	// If fixedResult is set, the channels are not used.
	fixedResult *querypb.StreamHealthResponse
	// hcChan should be an unbuffered channel which holds the tablet's next health response.
	hcChan chan *querypb.StreamHealthResponse
	// errCh is either an unbuffered channel which holds the stream error to return, or nil.
	errCh chan error
	// cbErrCh is a channel which receives errors returned from the supplied callback.
	cbErrCh chan error

	mu       sync.Mutex
	canceled bool
}

func createFakeConn(tablet *topodatapb.Tablet, c chan *querypb.StreamHealthResponse) *fakeConn {
	connMapMu.Lock()
	defer connMapMu.Unlock()
	key := TabletToMapKey(tablet)
	conn := &fakeConn{
		QueryService: fakes.ErrorQueryService,
		tablet:       tablet,
		hcChan:       c,
		cbErrCh:      make(chan error, 1),
	}
	connMap[key] = conn
	return conn
}

// StreamHealth implements queryservice.QueryService.
func (fc *fakeConn) StreamHealth(ctx context.Context, callback func(shr *querypb.StreamHealthResponse) error) error {
	if fc.fixedResult != nil {
		return callback(fc.fixedResult)
	}
	for {
		select {
		case shr := <-fc.hcChan:
			if err := callback(shr); err != nil {
				if err == io.EOF {
					return nil
				}
				select {
				case fc.cbErrCh <- err:
				case <-ctx.Done():
				}
				return err
			}
		case err := <-fc.errCh:
			return err
		case <-ctx.Done():
			fc.mu.Lock()
			fc.canceled = true
			fc.mu.Unlock()
			return nil
		}
	}
}

func (fc *fakeConn) isCanceled() bool {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	return fc.canceled
}

func (fc *fakeConn) resetCanceledFlag() {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.canceled = false
}

func checkErrorCounter(keyspace, shard string, tabletType topodatapb.TabletType, want int64) error {
	statsKey := []string{keyspace, shard, topoproto.TabletTypeLString(tabletType)}
	name := strings.Join(statsKey, ".")
	got, ok := hcErrorCounters.Counts()[name]
	if !ok {
		return fmt.Errorf("hcErrorCounters not correctly initialized")
	}
	if got != want {
		return fmt.Errorf("wrong value for hcErrorCounters got = %v, want = %v", got, want)
	}
	return nil
}

func createFixedHealthConn(tablet *topodatapb.Tablet, fixedResult *querypb.StreamHealthResponse) *fakeConn {
	key := TabletToMapKey(tablet)
	conn := &fakeConn{
		QueryService: fakes.ErrorQueryService,
		tablet:       tablet,
		fixedResult:  fixedResult,
	}
	connMapMu.Lock()
	defer connMapMu.Unlock()
	connMap[key] = conn
	return conn
}

func createTestTablet(uid uint32, cell, host string) *topodatapb.Tablet {
	tablet := topo.NewTablet(uid, cell, host)
	tablet.PortMap["vt"] = 1
	tablet.PortMap["grpc"] = 2
	tablet.Keyspace = "k"
	tablet.Shard = "s"
	return tablet
}

var mustMatch = utils.MustMatchFn(
	[]interface{}{ // types with unexported fields
		TabletHealth{},
	},
	[]string{".Conn"}, // ignored fields
)
