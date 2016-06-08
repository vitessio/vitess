// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package goclienttest

import (
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
	"golang.org/x/net/context"
)

const connectionKeyspace = "conn_ks"

// This file contains the reference test for clients. It tests
// all the corner cases of the API, and makes sure the go client
// is full featured.
//
// It can be used as a template by other languages for their test suites.
//
// TODO(team) add more unit test cases.

// TestGoClient runs the test suite for the provided client
func TestGoClient(t *testing.T, protocol, addr string) {
	// Create a client connecting to the server
	ctx := context.Background()
	conn, err := vtgateconn.DialProtocol(ctx, protocol, addr, 30*time.Second, connectionKeyspace)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}

	testCallerID(t, conn)
	testEcho(t, conn)
	testErrors(t, conn)
	testSuccess(t, conn)

	// and clean up
	conn.Close()
}
