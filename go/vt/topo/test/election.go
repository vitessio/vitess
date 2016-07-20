// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package test

import (
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/topo"
	"golang.org/x/net/context"
)

// checkElection runs the tests on the MasterParticipation part of the API
func checkElection(t *testing.T, ts topo.Impl) {
	name := "testmp"

	// create a new MasterParticipation
	id1 := "id1"
	mp1, err := ts.NewMasterParticipation(name, id1)
	if err != nil {
		t.Fatalf("cannot create mp1: %v", err)
	}

	// no master yet, check name
	master, err := mp1.GetCurrentMasterID()
	if err != nil || master != "" {
		t.Fatalf("GetCurrentMasterID returned: %v %v", master, err)
	}

	// wait for it to be the master
	ctx1, err := mp1.WaitForMaster()
	if err != nil {
		t.Fatalf("mp1 cannot become master: %v", err)
	}

	// get the current master name, better be id1
	master, err = mp1.GetCurrentMasterID()
	if err != nil || master != id1 {
		t.Fatalf("GetCurrentMasterID returned: %v %v", master, err)
	}

	// create a second MasterParticipation on same name
	id2 := "id2"
	mp2, err := ts.NewMasterParticipation(name, id2)
	if err != nil {
		t.Fatalf("cannot create mp2: %v", err)
	}

	// wait until mp2 gets to be the master in the background
	mp2IsMaster := make(chan error)
	var ctx2 context.Context
	go func() {
		var err error
		ctx2, err = mp2.WaitForMaster()
		mp2IsMaster <- err
	}()

	// shutdown mp1
	mp1.Shutdown()

	// this should have closed ctx1
	timer := time.NewTimer(5 * time.Second)
	select {
	case <-ctx1.Done():
	case <-timer.C:
		t.Fatalf("shutting down mp1 didn't close ctx1 in time")
	}

	// now mp2 should be master
	err = <-mp2IsMaster
	if err != nil {
		t.Fatalf("mp2 awoke with error: %v", err)
	}

	// shut down mp2, we're done
	mp2.Shutdown()
}
