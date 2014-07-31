// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlogplayer

import (
	"testing"

	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

func TestPopulateBlpCheckpoint(t *testing.T) {
	gtid := myproto.MustParseGTID("GoogleMysql", "19283")
	want := "INSERT INTO _vt.blp_checkpoint " +
		"(source_shard_uid, gtid, time_updated, transaction_timestamp, flags) " +
		"VALUES (18372, 'GoogleMysql/19283', 481823, 0, 'myflags')"

	got := PopulateBlpCheckpoint(18372, gtid, 481823, "myflags")
	if got != want {
		t.Errorf("PopulateBlpCheckpoint() = %#v, want %#v", got, want)
	}
}

func TestUpdateBlpCheckpoint(t *testing.T) {
	gtid := myproto.MustParseGTID("GoogleMysql", "58283")
	want := "UPDATE _vt.blp_checkpoint " +
		"SET gtid='GoogleMysql/58283', time_updated=88822 " +
		"WHERE source_shard_uid=78522"

	got := UpdateBlpCheckpoint(78522, gtid, 88822, 0)
	if got != want {
		t.Errorf("UpdateBlpCheckpoint() = %#v, want %#v", got, want)
	}
}

func TestUpdateBlpCheckpointTimestamp(t *testing.T) {
	gtid := myproto.MustParseGTID("GoogleMysql", "58283")
	want := "UPDATE _vt.blp_checkpoint " +
		"SET gtid='GoogleMysql/58283', time_updated=88822, transaction_timestamp=481828 " +
		"WHERE source_shard_uid=78522"

	got := UpdateBlpCheckpoint(78522, gtid, 88822, 481828)
	if got != want {
		t.Errorf("UpdateBlpCheckpoint() = %#v, want %#v", got, want)
	}
}

func TestQueryBlpCheckpoint(t *testing.T) {
	want := "SELECT gtid, flags FROM _vt.blp_checkpoint WHERE source_shard_uid=482821"
	got := QueryBlpCheckpoint(482821)
	if got != want {
		t.Errorf("QueryBlpCheckpoint(482821) = %#v, want %#v", got, want)
	}
}
