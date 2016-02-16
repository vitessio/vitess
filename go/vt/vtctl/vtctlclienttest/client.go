// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vtctlclienttest contains the testsuite against which each
// RPC implementation of the vtctlclient interface must be tested.
package vtctlclienttest

// NOTE: This file is not test-only code because it is referenced by tests in
//			 other packages and therefore it has to be regularly visible.

// NOTE: This code is in its own package such that its dependencies (e.g.
//       zookeeper) won't be drawn into production binaries as well.

import (
	"strings"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/tabletmanager/tmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtctl/vtctlclient"
	"github.com/youtube/vitess/go/vt/zktopo/zktestserver"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"

	// import the gRPC client implementation for tablet manager
	_ "github.com/youtube/vitess/go/vt/tabletmanager/grpctmclient"
)

func init() {
	// enforce we will use the right protocol (gRPC) (note the
	// client is unused, but it is initialized, so it needs to exist)
	*tmclient.TabletManagerProtocol = "grpc"
}

// CreateTopoServer returns the test topo server properly configured
func CreateTopoServer(t *testing.T) topo.Server {
	return zktestserver.New(t, []string{"cell1", "cell2"})
}

// TestSuite runs the test suite on the given topo server and client
func TestSuite(t *testing.T, ts topo.Server, client vtctlclient.VtctlClient) {
	ctx := context.Background()

	// Create a fake tablet
	tablet := &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "cell1", Uid: 1},
		Hostname: "localhost",
		Ip:       "10.11.12.13",
		PortMap: map[string]int32{
			"vt":    3333,
			"mysql": 3334,
		},

		Tags:     map[string]string{"tag": "value"},
		Keyspace: "test_keyspace",
		Type:     topodatapb.TabletType_MASTER,
	}
	if err := ts.CreateTablet(ctx, tablet); err != nil {
		t.Errorf("CreateTablet: %v", err)
	}

	// run a command that's gonna return something on the log channel
	logs, errFunc, err := client.ExecuteVtctlCommand(ctx, []string{"ListAllTablets", "cell1"}, 30*time.Second)
	if err != nil {
		t.Fatalf("Remote error: %v", err)
	}
	count := 0
	for e := range logs {
		expected := "cell1-0000000001 test_keyspace <null> master localhost:3333 localhost:3334 [tag: \"value\"]\n"
		if logutil.EventString(e) != expected {
			t.Errorf("Got unexpected log line '%v' expected '%v'", e.String(), expected)
		}
		count++
	}
	if count != 1 {
		t.Errorf("Didn't get expected log line only, got %v lines", count)
	}

	if err := errFunc(); err != nil {
		t.Fatalf("Remote error: %v", err)
	}

	// run a command that's gonna fail
	logs, errFunc, err = client.ExecuteVtctlCommand(ctx, []string{"ListAllTablets", "cell2"}, 30*time.Second)
	if err != nil {
		t.Fatalf("Remote error: %v", err)
	}
	if e, ok := <-logs; ok {
		t.Errorf("Got unexpected line for logs: %v", e.String())
	}

	expected := "node doesn't exist"
	if err := errFunc(); err == nil || !strings.Contains(err.Error(), expected) {
		t.Fatalf("Unexpected remote error, got: '%v' was expecting to find '%v'", err, expected)
	}

	// run a command that's gonna panic
	logs, errFunc, err = client.ExecuteVtctlCommand(ctx, []string{"Panic"}, 30*time.Second)
	if err != nil {
		t.Fatalf("Remote error: %v", err)
	}
	if e, ok := <-logs; ok {
		t.Errorf("Got unexpected line for logs: %v", e.String())
	}
	expected1 := "this command panics on purpose"
	expected2 := "uncaught vtctl panic"
	if err := errFunc(); err == nil || !strings.Contains(err.Error(), expected1) || !strings.Contains(err.Error(), expected2) {
		t.Fatalf("Unexpected remote error, got: '%v' was expecting to find '%v' and '%v'", err, expected1, expected2)
	}

	// and clean up the tablet
	if err := ts.DeleteTablet(ctx, tablet.Alias); err != nil {
		t.Errorf("DeleteTablet: %v", err)
	}
}
