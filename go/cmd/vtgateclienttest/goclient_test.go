// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
	"golang.org/x/net/context"
)

// This file contains the reference test for clients. It tests
// all the corner cases of the API, and makes sure the go client
// is full featured.
//
// It can be used as a template by other languages for their test suites.
//
// TODO(team) add more unit test cases.

// testCallerID adds a caller ID to a context, and makes sure the server
// gets it.
func testCallerID(t *testing.T, conn *vtgateconn.VTGateConn) {
	t.Log("testCallerID")
	ctx := context.Background()
	callerID := callerid.NewEffectiveCallerID("test_principal", "test_component", "test_subcomponent")
	ctx = callerid.NewContext(ctx, callerID, nil)

	data, err := json.Marshal(callerID)
	if err != nil {
		t.Errorf("failed to marshal callerid: %v", err)
		return
	}
	query := callerIDPrefix + string(data)

	// test Execute forwards the callerID
	if _, err := conn.Execute(ctx, query, nil, topo.TYPE_MASTER); err != nil {
		t.Errorf("failed to pass callerid: %v", err)
	}

	// FIXME(alainjobart) add all function calls
}

func testGoClient(t *testing.T, protocol, addr string) {
	// Create a client connecting to the server
	ctx := context.Background()
	conn, err := vtgateconn.DialProtocol(ctx, protocol, addr, 30*time.Second)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}

	testCallerID(t, conn)

	// and clean up
	conn.Close()
}
