// Copyright 2015 Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package goclienttest

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/cmd/vtgateclienttest/services"
	"github.com/youtube/vitess/go/vt/callerid"
	"github.com/youtube/vitess/go/vt/vtgate/vtgateconn"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

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
	query := services.CallerIDPrefix + string(data)

	// test Execute forwards the callerID
	if _, err := conn.Execute(ctx, query, nil, topodatapb.TabletType_MASTER); err != nil {
		if !strings.Contains(err.Error(), "SUCCESS: ") {
			t.Errorf("failed to pass callerid: %v", err)
		}
	}

	// FIXME(alainjobart) add all function calls
}
