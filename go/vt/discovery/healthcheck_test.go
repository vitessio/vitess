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

	"vitess.io/vitess/go/vt/topo/memorytopo"

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

func TestHealthCheck(t *testing.T) {
	ts := memorytopo.NewServer("cell")
	tablet := topo.NewTablet(0, "cell", "a")
	tablet.PortMap["vt"] = 1
	input := make(chan *querypb.StreamHealthResponse)
	createFakeConn(tablet, input)
	t.Logf(`createFakeConn({Host: "a", PortMap: {"vt": 1}}, c)`)
	hc := createTestHc(ts)
	testChecksum(t, 0, hc.stateChecksum())
	hc.AddTablet(tablet)
	t.Logf(`hc = HealthCheck(); hc.AddTablet({Host: "a", PortMap: {"vt": 1}}, "")`)

	// close healthcheck
	hc.Close()
}

func TestHealthCheckStreamError(t *testing.T) {
	ts := memorytopo.NewServer("cell")
	tablet := topo.NewTablet(0, "cell", "a")
	tablet.PortMap["vt"] = 1
	input := make(chan *querypb.StreamHealthResponse)
	fc := createFakeConn(tablet, input)
	fc.errCh = make(chan error)
	t.Logf(`createFakeConn({Host: "a", PortMap: {"vt": 1}}, c)`)
	hc := createTestHc(ts)
	hc.AddTablet(tablet)
	t.Logf(`hc = HealthCheck(); hc.AddTablet({Host: "a", PortMap: {"vt": 1}}, "")`)

	// close healthcheck
	hc.Close()
}

func TestHealthCheckVerifiesTabletAlias(t *testing.T) {
	ts := memorytopo.NewServer("cell")
	t.Logf("starting")
	tablet := topo.NewTablet(1, "cell", "a")
	tablet.PortMap["vt"] = 1
	input := make(chan *querypb.StreamHealthResponse, 1)
	createFakeConn(tablet, input)

	t.Logf(`createFakeConn({Host: "a", PortMap: {"vt": 1}}, c)`)

	hc := createTestHc(ts)
	hc.AddTablet(tablet)
	t.Logf(`hc = HealthCheck(); hc.AddTablet({Host: "a", PortMap: {"vt": 1}}, "")`)

	// close healthcheck
	hc.Close()
}

// TestHealthCheckCloseWaitsForGoRoutines tests that Close() waits for all Go
// routines to finish and the listener won't be called anymore.
func TestHealthCheckCloseWaitsForGoRoutines(t *testing.T) {
	ts := memorytopo.NewServer("cell")
	tablet := topo.NewTablet(0, "cell", "a")
	tablet.PortMap["vt"] = 1
	input := make(chan *querypb.StreamHealthResponse, 1)
	createFakeConn(tablet, input)

	t.Logf(`createFakeConn({Host: "a", PortMap: {"vt": 1}}, c)`)

	hc := createTestHc(ts)
	hc.AddTablet(tablet)
	t.Logf(`hc = HealthCheck(); hc.AddTablet({Host: "a", PortMap: {"vt": 1}}, "")`)

	hc.Close()
}

func TestHealthCheckTimeout(t *testing.T) {
	ts := memorytopo.NewServer("cell")
	timeout := 500 * time.Millisecond
	tablet := topo.NewTablet(0, "cell", "a")
	tablet.PortMap["vt"] = 1
	input := make(chan *querypb.StreamHealthResponse)
	createFakeConn(tablet, input)
	t.Logf(`createFakeConn({Host: "a", PortMap: {"vt": 1}}, c)`)
	hc := createTestHc(ts)
	hc.retryDelay = timeout
	hc.AddTablet(tablet)
	t.Logf(`hc = HealthCheck(); hc.AddTablet({Host: "a", PortMap: {"vt": 1}}, "")`)

	// close healthcheck
	hc.Close()
}

func TestTemplate(t *testing.T) {
	tablet := topo.NewTablet(0, "cell", "a")
	ts := []*tabletHealth{
		{
			Tablet:              tablet,
			Target:              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
			Up:                  true,
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
	ts := []*tabletHealth{
		{
			Tablet:              tablet,
			Target:              &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
			Up:                  true,
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

func tabletDialer(tablet *topodatapb.Tablet, failFast grpcclient.FailFast) (queryservice.QueryService, error) {
	key := TabletToMapKey(tablet)
	if qs, ok := connMap[key]; ok {
		return qs, nil
	}
	return nil, fmt.Errorf("tablet %v not found", key)
}

func createTestHc(ts *topo.Server) *HealthCheckImpl {
	return NewHealthCheck(context.Background(), 1*time.Millisecond, time.Hour, ts, "cell").(*HealthCheckImpl)
}
