// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vtctlclienttest provides testing library for vtctl
// implementations to use in their tests.
package vtctlclienttest

import (
	"strings"
	"testing"
	"time"

	// register the go rpc tablet manager client
	_ "github.com/youtube/vitess/go/vt/tabletmanager/gorpctmclient"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtctl/vtctlclient"
	"github.com/youtube/vitess/go/vt/zktopo"
)

// CreateTopoServer returns the test topo server properly configured
func CreateTopoServer(t *testing.T) topo.Server {
	return zktopo.NewTestServer(t, []string{"cell1", "cell2"})
}

// VtctlClientTestSuite runs the test suite on the given topo server and client
func VtctlClientTestSuite(t *testing.T, ts topo.Server, client vtctlclient.VtctlClient) {
	// Create a fake tablet
	tablet := &topo.Tablet{
		Alias:    topo.TabletAlias{Cell: "cell1", Uid: 1},
		Hostname: "localhost",
		IPAddr:   "10.11.12.13",
		Portmap: map[string]int{
			"vt":    3333,
			"mysql": 3334,
		},

		Tags:     map[string]string{"tag": "value"},
		Keyspace: "test_keyspace",
		Type:     topo.TYPE_MASTER,
	}
	if err := ts.CreateTablet(tablet); err != nil {
		t.Errorf("CreateTablet: %v", err)
	}

	// run a command that's gonna return something on the log channel
	logs, errFunc := client.ExecuteVtctlCommand([]string{"ListAllTablets", "cell1"}, 30*time.Second, 10*time.Second)
	if err := errFunc(); err != nil {
		t.Fatalf("Cannot execute remote command: %v", err)
	}

	count := 0
	for e := range logs {
		expected := "cell1-0000000001 test_keyspace <null> master localhost:3333 localhost:3334 [tag: \"value\"]\n"
		if e.String() != expected {
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
	logs, errFunc = client.ExecuteVtctlCommand([]string{"ListAllTablets", "cell2"}, 30*time.Second, 10*time.Second)
	if err := errFunc(); err != nil {
		t.Fatalf("Cannot execute remote command: %v", err)
	}

	if e, ok := <-logs; ok {
		t.Errorf("Got unexpected line for logs: %v", e.String())
	}

	expected := "node doesn't exist"
	if err := errFunc(); err == nil || !strings.Contains(err.Error(), expected) {
		t.Fatalf("Unexpected remote error, got: '%v' was expecting to find '%v'", err, expected)
	}
}
