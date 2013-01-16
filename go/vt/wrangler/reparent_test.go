// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"log"
	"testing"
	"time"

	"code.google.com/p/vitess/go/zk"
)

var zconn zk.Conn

func init() {
	// This test requires considerable setup so short circuit during sanity checks.
	if testing.Short() {
		return
	}
	_zconn, _, err := zk.DialZk("localhost:2181", time.Second)
	if err != nil {
		log.Fatal("zk connect failed: %v", err.Error())
	}
	zconn = _zconn
}

func TestGetMasterPosition(t *testing.T) {
	if testing.Short() {
		t.Logf("skipping")
		return
	}
	wr := NewWrangler(zconn, 30*time.Second, 30*time.Second)
	tablet, err := wr.readTablet("/zk/test/vt/tablets/0000062344")
	if err != nil {
		t.Error(err)
	}
	replicationPosition, err := wr.getMasterPosition(tablet)
	if err != nil {
		t.Error(err)
	}
	if replicationPosition.MapKey() == ":0" {
		t.Errorf("empty replicationPosition")
	}
	t.Logf("replicationPosition: %#v %v", replicationPosition, replicationPosition.MapKey())
}
