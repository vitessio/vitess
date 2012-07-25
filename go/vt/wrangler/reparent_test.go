// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zkwrangler

import (
	"log"
	"testing"

	"launchpad.net/gozk/zookeeper"
)

var zconn *zookeeper.Conn

func init() {
	// This test requires considerable setup so short circuit during sanity checks.
	if testing.Short() {
		return
	}
	_zconn, session, err := zookeeper.Dial("localhost:2181", 1e9)
	if err != nil {
		log.Fatal("zk connect failed: %v", err.Error())
	}

	event := <-session
	if event.State != zookeeper.STATE_CONNECTED {
		log.Fatal("zk connect failed: %v", event.State)
	}
	zconn = _zconn
}

func TestGetMasterPosition(t *testing.T) {
	if testing.Short() {
		t.Logf("skipping")
		return
	}
	wr := NewWrangler(zconn, nil)
	tablet, err := wr.readTablet("/zk/test/vt/tablets/0000062344")
	if err != nil {
		t.Error(err)
	}
	replicationPosition, err := getMasterPosition(tablet)
	if err != nil {
		t.Error(err)
	}
	if replicationPosition.MapKey() == ":0" {
		t.Errorf("empty replicationPosition")
	}
	t.Logf("replicationPosition: %#v %v", replicationPosition, replicationPosition.MapKey())
}
