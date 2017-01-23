// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package binlogplayer

import (
	"testing"

	"github.com/youtube/vitess/go/mysqlconn/replication"
	"github.com/youtube/vitess/go/vt/throttler"
)

func TestPopulateBlpCheckpoint(t *testing.T) {
	want := "INSERT INTO _vt.blp_checkpoint " +
		"(source_shard_uid, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, flags) " +
		"VALUES (18372, 'MariaDB/0-1-1083', 9223372036854775807, 9223372036854775807, 481823, 0, 'myflags')"

	got := PopulateBlpCheckpoint(18372, "MariaDB/0-1-1083", throttler.MaxRateModuleDisabled, throttler.ReplicationLagModuleDisabled, 481823, "myflags")
	if got != want {
		t.Errorf("PopulateBlpCheckpoint() = %#v, want %#v", got, want)
	}
}

func TestUpdateBlpCheckpoint(t *testing.T) {
	gtid := replication.MustParseGTID("MariaDB", "0-1-8283")
	want := "UPDATE _vt.blp_checkpoint " +
		"SET pos='MariaDB/0-1-8283', time_updated=88822 " +
		"WHERE source_shard_uid=78522"

	got := updateBlpCheckpoint(78522, replication.Position{GTIDSet: gtid.GTIDSet()}, 88822, 0)
	if got != want {
		t.Errorf("updateBlpCheckpoint() = %#v, want %#v", got, want)
	}
}

func TestUpdateBlpCheckpointTimestamp(t *testing.T) {
	gtid := replication.MustParseGTID("MariaDB", "0-2-582")
	want := "UPDATE _vt.blp_checkpoint " +
		"SET pos='MariaDB/0-2-582', time_updated=88822, transaction_timestamp=481828 " +
		"WHERE source_shard_uid=78522"

	got := updateBlpCheckpoint(78522, replication.Position{GTIDSet: gtid.GTIDSet()}, 88822, 481828)
	if got != want {
		t.Errorf("updateBlpCheckpoint() = %#v, want %#v", got, want)
	}
}

func TestQueryBlpCheckpoint(t *testing.T) {
	want := "SELECT pos, flags FROM _vt.blp_checkpoint WHERE source_shard_uid=482821"
	got := QueryBlpCheckpoint(482821)
	if got != want {
		t.Errorf("QueryBlpCheckpoint(482821) = %#v, want %#v", got, want)
	}
}

func TestQueryBlpThrottlerSettings(t *testing.T) {
	want := "SELECT max_tps, max_replication_lag FROM _vt.blp_checkpoint WHERE source_shard_uid=482821"
	if got := QueryBlpThrottlerSettings(482821); got != want {
		t.Errorf("QueryBlpCheckpoint(482821) = %#v, want %#v", got, want)
	}
}
