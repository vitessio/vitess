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

package binlogplayer

import (
	"testing"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/throttler"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestCreateVReplicationKeyRange(t *testing.T) {
	want := "INSERT INTO _vt.vreplication " +
		"(workflow, source, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, state) " +
		`VALUES ('Resharding', 'keyspace:\"ks\" shard:\"0\" key_range:<end:\"\\200\" > ', 'MariaDB/0-1-1083', 9223372036854775807, 9223372036854775807, 481823, 0, 'Running')`

	bls := binlogdatapb.BinlogSource{
		Keyspace: "ks",
		Shard:    "0",
		KeyRange: &topodatapb.KeyRange{
			End: []byte{0x80},
		},
	}

	got := CreateVReplication("Resharding", &bls, "MariaDB/0-1-1083", throttler.MaxRateModuleDisabled, throttler.ReplicationLagModuleDisabled, 481823)
	if got != want {
		t.Errorf("CreateVReplication() =\n%v, want\n%v", got, want)
	}
}

func TestCreateVReplicationTables(t *testing.T) {
	want := "INSERT INTO _vt.vreplication " +
		"(workflow, source, pos, max_tps, max_replication_lag, time_updated, transaction_timestamp, state) " +
		`VALUES ('Resharding', 'keyspace:\"ks\" shard:\"0\" tables:\"a\" tables:\"b\" ', 'MariaDB/0-1-1083', 9223372036854775807, 9223372036854775807, 481823, 0, 'Running')`

	bls := binlogdatapb.BinlogSource{
		Keyspace: "ks",
		Shard:    "0",
		Tables:   []string{"a", "b"},
	}

	got := CreateVReplication("Resharding", &bls, "MariaDB/0-1-1083", throttler.MaxRateModuleDisabled, throttler.ReplicationLagModuleDisabled, 481823)
	if got != want {
		t.Errorf("CreateVReplication() =\n%v, want\n%v", got, want)
	}
}

func TestUpdateVReplicationPos(t *testing.T) {
	gtid := mysql.MustParseGTID("MariaDB", "0-1-8283")
	want := "UPDATE _vt.vreplication " +
		"SET pos='MariaDB/0-1-8283', time_updated=88822 " +
		"WHERE id=78522"

	got := updateVReplicationPos(78522, mysql.Position{GTIDSet: gtid.GTIDSet()}, 88822, 0)
	if got != want {
		t.Errorf("updateVReplicationPos() = %#v, want %#v", got, want)
	}
}

func TestUpdateVReplicationTimestamp(t *testing.T) {
	gtid := mysql.MustParseGTID("MariaDB", "0-2-582")
	want := "UPDATE _vt.vreplication " +
		"SET pos='MariaDB/0-2-582', time_updated=88822, transaction_timestamp=481828 " +
		"WHERE id=78522"

	got := updateVReplicationPos(78522, mysql.Position{GTIDSet: gtid.GTIDSet()}, 88822, 481828)
	if got != want {
		t.Errorf("updateVReplicationPos() = %#v, want %#v", got, want)
	}
}

func TestReadVReplicationPos(t *testing.T) {
	want := "SELECT pos FROM _vt.vreplication WHERE id=482821"
	got := ReadVReplicationPos(482821)
	if got != want {
		t.Errorf("ReadVReplicationThrottlerSettings(482821) = %#v, want %#v", got, want)
	}
}

func TestReadVReplicationThrottlerSettings(t *testing.T) {
	want := "SELECT max_tps, max_replication_lag FROM _vt.vreplication WHERE id=482821"
	if got := ReadVReplicationThrottlerSettings(482821); got != want {
		t.Errorf("ReadVReplicationThrottlerSettings(482821) = %#v, want %#v", got, want)
	}
}
