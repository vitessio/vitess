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
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/status"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/queryservice/fakes"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var connMap map[string]*fakeConn

func init() {
	tabletconn.RegisterDialer("fake_discovery", discoveryDialer)
	flag.Set("tablet_protocol", "fake_discovery")
	connMap = make(map[string]*fakeConn)
}

func testChecksum(t *testing.T, want, got int64) {
	t.Helper()
	if want != got {
		t.Errorf("want checksum %v, got %v", want, got)
	}
}

func TestHealthCheck(t *testing.T) {
	tablet := topo.NewTablet(0, "cell", "a")
	tablet.PortMap["vt"] = 1
	input := make(chan *querypb.StreamHealthResponse)
	createFakeConn(tablet, input)
	t.Logf(`createFakeConn({Host: "a", PortMap: {"vt": 1}}, c)`)
	l := newListener()
	hc := NewHealthCheck(1*time.Millisecond, time.Hour).(*HealthCheckImpl)
	hc.SetListener(l, true)
	testChecksum(t, 0, hc.stateChecksum())
	hc.AddTablet(tablet, "")
	t.Logf(`hc = HealthCheck(); hc.AddTablet({Host: "a", PortMap: {"vt": 1}}, "")`)

	// Immediately after AddTablet() there will be the first notification.
	want := &TabletStats{
		Key:     "a,vt:1",
		Tablet:  tablet,
		Target:  &querypb.Target{},
		Up:      true,
		Serving: false,
	}
	res := <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}
	testChecksum(t, 401258919, hc.stateChecksum())

	// one tablet after receiving a StreamHealthResponse
	shr := &querypb.StreamHealthResponse{
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 10,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}
	want = &TabletStats{
		Key:                                 "a,vt:1",
		Tablet:                              tablet,
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		Up:                                  true,
		Serving:                             true,
		Stats:                               &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
		TabletExternallyReparentedTimestamp: 10,
	}
	input <- shr
	t.Logf(`input <- {{Keyspace: "k", Shard: "s", TabletType: MASTER}, Serving: true, TabletExternallyReparentedTimestamp: 10, {SecondsBehindMaster: 1, CpuUsage: 0.2}}`)
	res = <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}

	// Verify that the error count is initialized to 0 after the first tablet response.
	if err := checkErrorCounter("k", "s", topodatapb.TabletType_MASTER, 0); err != nil {
		t.Errorf("%v", err)
	}

	tcsl := hc.CacheStatus()
	tcslWant := TabletsCacheStatusList{{
		Cell:   "cell",
		Target: &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		TabletsStats: TabletStatsList{{
			Key:                                 "a,vt:1",
			Tablet:                              tablet,
			Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
			Up:                                  true,
			Serving:                             true,
			Stats:                               &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
			TabletExternallyReparentedTimestamp: 10,
		}},
	}}
	if !reflect.DeepEqual(tcsl, tcslWant) {
		t.Errorf("hc.CacheStatus() =\n%+v; want\n%+v", tcsl[0], tcslWant[0])
	}
	testChecksum(t, 1562785705, hc.stateChecksum())

	// TabletType changed, should get both old and new event
	shr = &querypb.StreamHealthResponse{
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 0,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.5},
	}
	input <- shr
	t.Logf(`input <- {{Keyspace: "k", Shard: "s", TabletType: REPLICA}, Serving: true, TabletExternallyReparentedTimestamp: 0, {SecondsBehindMaster: 1, CpuUsage: 0.5}}`)
	want = &TabletStats{
		Key:                                 "a,vt:1",
		Tablet:                              tablet,
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		Up:                                  false,
		Serving:                             true,
		Stats:                               &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
		TabletExternallyReparentedTimestamp: 10,
	}
	res = <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}
	want = &TabletStats{
		Key:                                 "a,vt:1",
		Tablet:                              tablet,
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Up:                                  true,
		Serving:                             true,
		Stats:                               &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.5},
		TabletExternallyReparentedTimestamp: 0,
	}
	res = <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}

	if err := checkErrorCounter("k", "s", topodatapb.TabletType_REPLICA, 0); err != nil {
		t.Errorf("%v", err)
	}
	testChecksum(t, 1906892404, hc.stateChecksum())

	// Serving & RealtimeStats changed
	shr = &querypb.StreamHealthResponse{
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:                             false,
		TabletExternallyReparentedTimestamp: 0,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.3},
	}
	want = &TabletStats{
		Key:                                 "a,vt:1",
		Tablet:                              tablet,
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Up:                                  true,
		Serving:                             false,
		Stats:                               &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.3},
		TabletExternallyReparentedTimestamp: 0,
	}
	input <- shr
	t.Logf(`input <- {{Keyspace: "k", Shard: "s", TabletType: REPLICA}, TabletExternallyReparentedTimestamp: 0, {SecondsBehindMaster: 1, CpuUsage: 0.3}}`)
	res = <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}
	testChecksum(t, 1200695592, hc.stateChecksum())

	// HealthError
	shr = &querypb.StreamHealthResponse{
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 0,
		RealtimeStats:                       &querypb.RealtimeStats{HealthError: "some error", SecondsBehindMaster: 1, CpuUsage: 0.3},
	}
	want = &TabletStats{
		Key:                                 "a,vt:1",
		Tablet:                              tablet,
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Up:                                  true,
		Serving:                             false,
		Stats:                               &querypb.RealtimeStats{HealthError: "some error", SecondsBehindMaster: 1, CpuUsage: 0.3},
		TabletExternallyReparentedTimestamp: 0,
		LastError:                           fmt.Errorf("vttablet error: some error"),
	}
	input <- shr
	t.Logf(`input <- {{Keyspace: "k", Shard: "s", TabletType: REPLICA}, Serving: true, TabletExternallyReparentedTimestamp: 0, {HealthError: "some error", SecondsBehindMaster: 1, CpuUsage: 0.3}}`)
	res = <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}
	testChecksum(t, 1200695592, hc.stateChecksum()) // unchanged

	// remove tablet
	hc.deleteConn(tablet)
	t.Logf(`hc.RemoveTablet({Host: "a", PortMap: {"vt": 1}})`)
	want = &TabletStats{
		Key:                                 "a,vt:1",
		Tablet:                              tablet,
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Up:                                  false,
		Serving:                             false,
		Stats:                               &querypb.RealtimeStats{HealthError: "some error", SecondsBehindMaster: 1, CpuUsage: 0.3},
		TabletExternallyReparentedTimestamp: 0,
		LastError:                           context.Canceled,
	}
	res = <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf("<-l.output:\n%+v; want\n%+v", res, want)
	}
	testChecksum(t, 0, hc.stateChecksum())

	// close healthcheck
	hc.Close()
}

func TestHealthCheckStreamError(t *testing.T) {
	tablet := topo.NewTablet(0, "cell", "a")
	tablet.PortMap["vt"] = 1
	input := make(chan *querypb.StreamHealthResponse)
	fc := createFakeConn(tablet, input)
	fc.errCh = make(chan error)
	t.Logf(`createFakeConn({Host: "a", PortMap: {"vt": 1}}, c)`)
	l := newListener()
	hc := NewHealthCheck(1*time.Millisecond, time.Hour).(*HealthCheckImpl)
	hc.SetListener(l, true)
	hc.AddTablet(tablet, "")
	t.Logf(`hc = HealthCheck(); hc.AddTablet({Host: "a", PortMap: {"vt": 1}}, "")`)

	// Immediately after AddTablet() there will be the first notification.
	want := &TabletStats{
		Key:     "a,vt:1",
		Tablet:  tablet,
		Target:  &querypb.Target{},
		Up:      true,
		Serving: false,
	}
	res := <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}

	// one tablet after receiving a StreamHealthResponse
	shr := &querypb.StreamHealthResponse{
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 0,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}
	want = &TabletStats{
		Key:                                 "a,vt:1",
		Tablet:                              tablet,
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Up:                                  true,
		Serving:                             true,
		Stats:                               &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
		TabletExternallyReparentedTimestamp: 0,
	}
	input <- shr
	t.Logf(`input <- {{Keyspace: "k", Shard: "s", TabletType: MASTER}, Serving: true, TabletExternallyReparentedTimestamp: 10, {SecondsBehindMaster: 1, CpuUsage: 0.2}}`)
	res = <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}

	// Stream error
	fc.errCh <- fmt.Errorf("some stream error")
	want = &TabletStats{
		Key:                                 "a,vt:1",
		Tablet:                              tablet,
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		Up:                                  true,
		Serving:                             false,
		Stats:                               &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
		TabletExternallyReparentedTimestamp: 0,
		LastError:                           fmt.Errorf("some stream error"),
	}
	res = <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf("<-l.output:\n%+v; want\n%+v", res, want)
	}

	// close healthcheck
	hc.Close()
}

func TestHealthCheckVerifiesTabletAlias(t *testing.T) {
	t.Logf("starting")
	tablet := topo.NewTablet(1, "cell", "a")
	tablet.PortMap["vt"] = 1
	input := make(chan *querypb.StreamHealthResponse, 1)
	fc := createFakeConn(tablet, input)

	t.Logf(`createFakeConn({Host: "a", PortMap: {"vt": 1}}, c)`)

	l := newListener()
	hc := NewHealthCheck(1*time.Millisecond, time.Hour).(*HealthCheckImpl)
	hc.SetListener(l, false)
	hc.AddTablet(tablet, "")
	t.Logf(`hc = HealthCheck(); hc.AddTablet({Host: "a", PortMap: {"vt": 1}}, "")`)

	// Immediately after AddTablet() there will be the first notification.
	want := &TabletStats{
		Key:     "a,vt:1",
		Tablet:  tablet,
		Target:  &querypb.Target{},
		Up:      true,
		Serving: false,
	}
	res := <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}

	input <- &querypb.StreamHealthResponse{
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		TabletAlias:                         &topodatapb.TabletAlias{Uid: 20, Cell: "cellb"},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 10,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}

	select {
	case err := <-fc.cbErrCh:
		t.Logf("<-fc.cbErrCh: %v", err)
		if prefix := "health stats mismatch"; !strings.HasPrefix(err.Error(), prefix) {
			t.Fatalf("wrong error, got %v; want prefix %v", err, prefix)
		}
	case <-l.output:
		t.Fatalf("StreamHealth should have returned a health stats mismatch error")
	}

	input <- &querypb.StreamHealthResponse{
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		TabletAlias:                         &topodatapb.TabletAlias{Uid: 1, Cell: "cell"},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 10,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}

	select {
	case err := <-fc.cbErrCh:
		t.Fatalf("wanted listener output, got error: %v", err)
	case res := <-l.output:
		t.Logf("<-l.output: %+v", res)
	}

	// close healthcheck
	hc.Close()
}

// TestHealthCheckCloseWaitsForGoRoutines tests that Close() waits for all Go
// routines to finish and the listener won't be called anymore.
func TestHealthCheckCloseWaitsForGoRoutines(t *testing.T) {
	tablet := topo.NewTablet(0, "cell", "a")
	tablet.PortMap["vt"] = 1
	input := make(chan *querypb.StreamHealthResponse, 1)
	createFakeConn(tablet, input)

	t.Logf(`createFakeConn({Host: "a", PortMap: {"vt": 1}}, c)`)

	l := newListener()
	hc := NewHealthCheck(1*time.Millisecond, time.Hour).(*HealthCheckImpl)
	hc.SetListener(l, false)
	hc.AddTablet(tablet, "")
	t.Logf(`hc = HealthCheck(); hc.AddTablet({Host: "a", PortMap: {"vt": 1}}, "")`)

	// Immediately after AddTablet() there will be the first notification.
	want := &TabletStats{
		Key:     "a,vt:1",
		Tablet:  tablet,
		Target:  &querypb.Target{},
		Up:      true,
		Serving: false,
	}
	res := <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}

	// Verify that the listener works in general.
	shr := &querypb.StreamHealthResponse{
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 10,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}
	want = &TabletStats{
		Key:                                 "a,vt:1",
		Tablet:                              tablet,
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		Up:                                  true,
		Serving:                             true,
		Stats:                               &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
		TabletExternallyReparentedTimestamp: 10,
	}
	input <- shr
	t.Logf(`input <- %v`, shr)
	res = <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}

	// Change input to distinguish between stats sent before and after Close().
	shr.TabletExternallyReparentedTimestamp = 11
	// Close the healthcheck. Tablet connections are closed asynchronously and
	// Close() will block until all Go routines (one per connection) are done.
	hc.Close()

	// Try to send more updates. They should be ignored and the listener should
	// not be called from any Go routine anymore.
	// Note that this code is racy by nature. If there is a regression, it should
	// fail in some cases.
	input <- shr
	t.Logf(`input <- %v`, shr)

	// After Close() we'll receive one or two notifications with Serving == false.
	res = <-l.output
	if res.Serving {
		t.Errorf(`Received one more notification with Serving == true: %+v`, res)
	}

	select {
	case res = <-l.output:
		if res.TabletExternallyReparentedTimestamp == 10 && res.LastError == context.Canceled {
			// HealthCheck repeats the previous stats if there is an error.
			// This is expected.
			break
		}
		t.Fatalf("healthCheck still running after Close(): listener received: %v but should not have been called", res)
	case <-time.After(1 * time.Millisecond):
		// No response after timeout. Close probably closed all Go routines
		// properly and won't use the listener anymore.
	}

	// The last notification should have Up = false.
	if res.Up || res.Serving {
		t.Errorf(`Last notification doesn't have Up == false and Serving == false: %+v`, res)
	}

	// Check if there are more updates than the one emitted during Close().
	select {
	case res := <-l.output:
		t.Fatalf("healthCheck still running after Close(): listener received: %v but should not have been called", res)
	case <-time.After(1 * time.Millisecond):
		// No response after timeout. Listener probably not called again. Success.
	}
}

func TestHealthCheckTimeout(t *testing.T) {
	timeout := 500 * time.Millisecond
	tablet := topo.NewTablet(0, "cell", "a")
	tablet.PortMap["vt"] = 1
	input := make(chan *querypb.StreamHealthResponse)
	fc := createFakeConn(tablet, input)
	t.Logf(`createFakeConn({Host: "a", PortMap: {"vt": 1}}, c)`)
	l := newListener()
	hc := NewHealthCheck(1*time.Millisecond, timeout).(*HealthCheckImpl)
	hc.SetListener(l, false)
	hc.AddTablet(tablet, "")
	t.Logf(`hc = HealthCheck(); hc.AddTablet({Host: "a", PortMap: {"vt": 1}}, "")`)

	// Immediately after AddTablet() there will be the first notification.
	want := &TabletStats{
		Key:     "a,vt:1",
		Tablet:  tablet,
		Target:  &querypb.Target{},
		Up:      true,
		Serving: false,
	}
	res := <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}

	// one tablet after receiving a StreamHealthResponse
	shr := &querypb.StreamHealthResponse{
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		Serving:                             true,
		TabletExternallyReparentedTimestamp: 10,
		RealtimeStats:                       &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
	}
	want = &TabletStats{
		Key:                                 "a,vt:1",
		Tablet:                              tablet,
		Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_MASTER},
		Up:                                  true,
		Serving:                             true,
		Stats:                               &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.2},
		TabletExternallyReparentedTimestamp: 10,
	}
	input <- shr
	t.Logf(`input <- {{Keyspace: "k", Shard: "s", TabletType: MASTER}, Serving: true, TabletExternallyReparentedTimestamp: 10, {SecondsBehindMaster: 1, CpuUsage: 0.2}}`)
	res = <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}

	if err := checkErrorCounter("k", "s", topodatapb.TabletType_MASTER, 0); err != nil {
		t.Errorf("%v", err)
	}

	// wait for timeout period
	time.Sleep(2 * timeout)
	t.Logf(`Sleep(2 * timeout)`)
	res = <-l.output
	if res.Serving {
		t.Errorf(`<-l.output: %+v; want not serving`, res)
	}

	if err := checkErrorCounter("k", "s", topodatapb.TabletType_MASTER, 1); err != nil {
		t.Errorf("%v", err)
	}

	if !fc.isCanceled() {
		t.Errorf("StreamHealth should be canceled after timeout, but is not")
	}

	// repeat the wait. It will timeout one more time trying to get the connection.
	fc.resetCanceledFlag()
	time.Sleep(timeout)
	t.Logf(`Sleep(2 * timeout)`)

	res = <-l.output
	if res.Serving {
		t.Errorf(`<-l.output: %+v; want not serving`, res)
	}

	if err := checkErrorCounter("k", "s", topodatapb.TabletType_MASTER, 2); err != nil {
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
	time.Sleep(timeout)
	res = <-l.output
	if !reflect.DeepEqual(res, want) {
		t.Errorf(`<-l.output: %+v; want %+v`, res, want)
	}

	// close healthcheck
	hc.Close()
}

func TestTemplate(t *testing.T) {
	tablet := topo.NewTablet(0, "cell", "a")
	ts := []*TabletStats{
		{
			Key:                                 "a",
			Tablet:                              tablet,
			Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
			Up:                                  true,
			Serving:                             false,
			Stats:                               &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.3},
			TabletExternallyReparentedTimestamp: 0,
		},
	}
	tcs := &TabletsCacheStatus{
		Cell:         "cell",
		Target:       &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		TabletsStats: ts,
	}
	templ := template.New("").Funcs(status.StatusFuncs)
	templ, err := templ.Parse(HealthCheckTemplate)
	if err != nil {
		t.Fatalf("error parsing template: %v", err)
	}
	wr := &bytes.Buffer{}
	if err := templ.Execute(wr, []*TabletsCacheStatus{tcs}); err != nil {
		t.Fatalf("error executing template: %v", err)
	}
}

func TestDebugURLFormatting(t *testing.T) {
	flag.Set("tablet_url_template", "https://{{.GetHostNameLevel 0}}.bastion.{{.Tablet.Alias.Cell}}.corp")
	ParseTabletURLTemplateFromFlag()

	tablet := topo.NewTablet(0, "cell", "host.dc.domain")
	ts := []*TabletStats{
		{
			Key:                                 "a",
			Tablet:                              tablet,
			Target:                              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
			Up:                                  true,
			Serving:                             false,
			Stats:                               &querypb.RealtimeStats{SecondsBehindMaster: 1, CpuUsage: 0.3},
			TabletExternallyReparentedTimestamp: 0,
		},
	}
	tcs := &TabletsCacheStatus{
		Cell:         "cell",
		Target:       &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
		TabletsStats: ts,
	}
	templ := template.New("").Funcs(status.StatusFuncs)
	templ, err := templ.Parse(HealthCheckTemplate)
	if err != nil {
		t.Fatalf("error parsing template: %v", err)
	}
	wr := &bytes.Buffer{}
	if err := templ.Execute(wr, []*TabletsCacheStatus{tcs}); err != nil {
		t.Fatalf("error executing template: %v", err)
	}
	expectedURL := `"https://host.bastion.cell.corp"`
	if !strings.Contains(wr.String(), expectedURL) {
		t.Fatalf("output missing formatted URL, expectedURL: %s , output: %s", expectedURL, wr.String())
	}
}

type listener struct {
	output chan *TabletStats
}

func newListener() *listener {
	return &listener{output: make(chan *TabletStats, 2)}
}

func (l *listener) StatsUpdate(ts *TabletStats) {
	l.output <- ts
}

func createFakeConn(tablet *topodatapb.Tablet, c chan *querypb.StreamHealthResponse) *fakeConn {
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

func discoveryDialer(tablet *topodatapb.Tablet, failFast grpcclient.FailFast) (queryservice.QueryService, error) {
	key := TabletToMapKey(tablet)
	return connMap[key], nil
}

type fakeConn struct {
	queryservice.QueryService
	tablet *topodatapb.Tablet
	// hcChan should be an unbuffered channel which holds the tablet's next health response.
	hcChan chan *querypb.StreamHealthResponse
	// errCh is either an unbuffered channel which holds the stream error to return, or nil.
	errCh chan error
	// cbErrCh is a channel which receives errors returned from the supplied callback.
	cbErrCh chan error

	mu       sync.Mutex
	canceled bool
}

// StreamHealth implements queryservice.QueryService.
func (fc *fakeConn) StreamHealth(ctx context.Context, callback func(shr *querypb.StreamHealthResponse) error) error {
	for {
		select {
		case shr := <-fc.hcChan:
			if err := callback(shr); err != nil {
				if err == io.EOF {
					return nil
				}
				fc.cbErrCh <- err
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
