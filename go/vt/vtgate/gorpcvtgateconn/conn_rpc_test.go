// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpcvtgateconn

import (
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/vtgate/vtgateconntest"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconntestutils"
	"golang.org/x/net/context"
)

// This test makes sure the go rpc service works
func TestGoRPCVTGateConn(t *testing.T) {
	service, param := vtgateconntestutils.StartFakeVtGateServer(t)
	defer vtgateconntestutils.StopFakeVtGateServer(service, param)
	// Create a Go RPC client connecting to the server
	ctx := context.Background()
	client, err := dial(ctx, param.Addr(), 30*time.Second)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	// run the test suite
	vtgateconntest.TestSuite(t, client)
}
