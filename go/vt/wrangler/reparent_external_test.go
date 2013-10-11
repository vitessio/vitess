// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package wrangler

import (
	"strings"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/zktopo"
)

func TestShardExternallyReparented(t *testing.T) {
	ts := zktopo.NewTestServer(t, []string{"cell1", "cell2"})
	wr := New(ts, time.Minute, time.Second)

	// Creating the tablets with createShardAndKeyspace=true
	// so we don't need to create the keyspace and shard
	if err := wr.InitTablet(&topo.Tablet{
		Cell:           "cell1",
		Uid:            123,
		Parent:         topo.TabletAlias{},
		Addr:           "masterhost:8101",
		SecureAddr:     "masterhost:8102",
		MysqlAddr:      "masterhost:3306",
		MysqlIpAddr:    "1.2.3.4:3306",
		Keyspace:       "test_keyspace",
		Shard:          "0",
		Type:           topo.TYPE_MASTER,
		State:          topo.STATE_READ_WRITE,
		DbNameOverride: "",
		KeyRange:       key.KeyRange{},
	}, false, true, false); err != nil {
		t.Fatalf("cannot create master tablet: %v", err)
	}

	if err := wr.InitTablet(&topo.Tablet{
		Cell: "cell1",
		Uid:  234,
		Parent: topo.TabletAlias{
			Cell: "cell1",
			Uid:  123,
		},
		Addr:           "slavehost:8101",
		SecureAddr:     "slavehost:8102",
		MysqlAddr:      "slavehost:3306",
		MysqlIpAddr:    "2.3.4.5:3306",
		Keyspace:       "test_keyspace",
		Shard:          "0",
		Type:           topo.TYPE_REPLICA,
		State:          topo.STATE_READ_ONLY,
		DbNameOverride: "",
		KeyRange:       key.KeyRange{},
	}, false, false, false); err != nil {
		t.Fatalf("cannot create slave tablet: %v", err)
	}

	if err := wr.ShardExternallyReparented("test_keyspace", "0", topo.TabletAlias{Cell: "cell1", Uid: 123}, false, 80); err == nil {
		t.Fatalf("ShardExternallyReparented(same master) should have failed")
	} else {
		if !strings.Contains(err.Error(), "already master") {
			t.Fatalf("ShardExternallyReparented(same master) should have failed with an error that contains 'already master' but got: %v", err)
		}
	}
}
