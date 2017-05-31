/*
Copyright 2017 Google Inc.

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
	conn, err := vtgateconn.DialProtocol(ctx, protocol, addr, 30*time.Second)
	session := conn.Session(connectionKeyspace, nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}

	testCallerID(t, conn, session)
	testEcho(t, conn, session)
	testErrors(t, conn, session)
	testSuccess(t, conn)

	// and clean up
	conn.Close()
}
