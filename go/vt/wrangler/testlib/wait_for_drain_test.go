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

package testlib

import (
	"flag"
	"io"
	"strings"
	"testing"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vttablet/grpcqueryservice"
	"vitess.io/vitess/go/vt/vttablet/queryservice/fakes"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

type drainDirective int

const (
	DrainNoCells drainDirective = 1 << iota
	DrainCell1
	DrainCell2
)

func TestWaitForDrain(t *testing.T) {
	testWaitForDrain(t, "both cells selected and drained", "" /* cells */, DrainCell1|DrainCell2, nil /* expectedErrors */)
}

func TestWaitForDrain_SelectCell1(t *testing.T) {
	testWaitForDrain(t, "cell1 selected and drained", "cell1", DrainCell1, nil /* expectedErrors */)
}

func TestWaitForDrain_NoCellDrained(t *testing.T) {
	testWaitForDrain(t, "both cells selected and none drained", "" /* cells */, DrainNoCells, []string{"cell1-0000000000", "cell2-0000000001"})
}

func TestWaitForDrain_SelectCell1ButCell2Drained(t *testing.T) {
	testWaitForDrain(t, "cell1 selected and cell2 drained", "cell1", DrainCell2, []string{"cell1-0000000000"})
}

func testWaitForDrain(t *testing.T, desc, cells string, drain drainDirective, expectedErrors []string) {
	const keyspace = "ks"
	const shard = "-80"

	// This value needs to be bigger than the -initial_wait value below.
	// Otherwise in this test, we close the StreamHealth RPC because of
	// tablet inactivity at the same time as the end of the initial wait,
	// and the test fails.
	flag.Set("vtctl_healthcheck_timeout", "1s")

	ts := memorytopo.NewServer("cell1", "cell2")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	vp := NewVtctlPipe(t, ts)
	defer vp.Close()

	// Create keyspace.
	if err := ts.CreateKeyspace(context.Background(), keyspace, &topodatapb.Keyspace{
		ShardingColumnName: "keyspace_id",
		ShardingColumnType: topodatapb.KeyspaceIdType_UINT64,
	}); err != nil {
		t.Fatalf("CreateKeyspace failed: %v", err)
	}

	t1 := NewFakeTablet(t, wr, "cell1", 0, topodatapb.TabletType_REPLICA, nil,
		TabletKeyspaceShard(t, keyspace, shard))
	t2 := NewFakeTablet(t, wr, "cell2", 1, topodatapb.TabletType_REPLICA, nil,
		TabletKeyspaceShard(t, keyspace, shard))

	target := querypb.Target{
		Keyspace:   keyspace,
		Shard:      shard,
		TabletType: topodatapb.TabletType_REPLICA,
	}
	fqs1 := fakes.NewStreamHealthQueryService(target)
	fqs2 := fakes.NewStreamHealthQueryService(target)
	grpcqueryservice.Register(t1.RPCServer, fqs1)
	grpcqueryservice.Register(t2.RPCServer, fqs2)

	// Start the action loop after having registered the extra services.
	for _, ft := range []*FakeTablet{t1, t2} {
		ft.StartActionLoop(t, wr)
		defer ft.StopActionLoop(t)
	}

	// Run vtctl WaitForDrain and react depending on its output.
	timeout := "0.5s"
	if len(expectedErrors) == 0 {
		// Tests with a positive outcome should have a more generous timeout to
		// avoid flakyness.
		timeout = "30s"
	}
	stream, err := vp.RunAndStreamOutput(
		[]string{"WaitForDrain",
			"-cells", cells,
			"-retry_delay", "100ms",
			"-timeout", timeout,
			"-initial_wait", "100ms",
			keyspace + "/" + shard,
			topodatapb.TabletType_REPLICA.String()})
	if err != nil {
		t.Fatalf("VtctlPipe.RunAndStreamOutput() failed: %v", err)
	}

	// QPS = 1.0. Tablets are not drained yet.
	fqs1.AddHealthResponseWithQPS(1.0)
	fqs2.AddHealthResponseWithQPS(1.0)

	var le *logutilpb.Event
	for {
		le, err = stream.Recv()
		if err != nil {
			break
		}
		line := logutil.EventString(le)
		t.Logf(line)
		if strings.Contains(line, "for all healthy tablets to be drained") {
			t.Log("Successfully waited for WaitForDrain to be blocked because tablets have a QPS rate > 0.0")
			break
		} else {
			t.Log("waiting for WaitForDrain to see a QPS rate > 0.0")
		}
	}

	if drain&DrainCell1 != 0 {
		fqs1.AddHealthResponseWithQPS(0.0)
	} else {
		fqs1.AddHealthResponseWithQPS(2.0)
	}
	if drain&DrainCell2 != 0 {
		fqs2.AddHealthResponseWithQPS(0.0)
	} else {
		fqs2.AddHealthResponseWithQPS(2.0)
	}

	// If a cell was drained, rate should go below <0.0 now.
	// If not all selected cells were drained, this will end after "-timeout".
	for {
		le, err = stream.Recv()
		if err == nil {
			vp.t.Logf(logutil.EventString(le))
		} else {
			break
		}
	}

	if len(expectedErrors) == 0 {
		if err != io.EOF {
			t.Fatalf("TestWaitForDrain: %v: no error expected but got: %v", desc, err)
		}
		// else: Success.
	} else {
		if err == nil || err == io.EOF {
			t.Fatalf("TestWaitForDrain: %v: error expected but got none", desc)
		}
		for _, errString := range expectedErrors {
			if !strings.Contains(err.Error(), errString) {
				t.Fatalf("TestWaitForDrain: %v: error does not include expected string. got: %v want: %v", desc, err, errString)
			}
		}
		// Success.
	}
}
