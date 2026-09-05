/*
Copyright 2019 The Vitess Authors.

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

package vreplication

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	qh "vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication/queryhistory"
)

func TestJournalOneToOne(t *testing.T) {
	defer deleteTablet(addTablet(100))
	defer deleteTablet(addOtherTablet(101, "other_keyspace", "0"))

	execStatements(t, []string{
		"create table t(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.t(id int, val varbinary(128), primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t",
		fmt.Sprintf("drop table %s.t", vrepldb),
	})

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "t",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}

	_, firstID := startVReplication(t, bls, "")
	// The stream carries configuration overrides, which the replacement
	// stream must inherit: it is the same workflow.
	execStatements(t, []string{fmt.Sprintf(`update _vt.vreplication set options='{"config": {"vreplication-retry-delay": "5s"}}' where id=%d`, firstID)})

	journal := &binlogdatapb.Journal{
		Id:            1,
		MigrationType: binlogdatapb.MigrationType_SHARDS,
		Participants: []*binlogdatapb.KeyspaceShard{{
			Keyspace: "vttest",
			Shard:    "0",
		}},
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: "other_keyspace",
			Shard:    "0",
			Gtid:     "MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-10",
		}},
	}
	query := fmt.Sprintf("insert into _vt.resharding_journal(id, db_name, val) values (1, 'vttest', %v)", encodeString(journal.String()))
	execStatements(t, []string{query})
	defer execStatements(t, []string{"delete from _vt.resharding_journal"})

	expectDBClientQueries(t, qh.Expect(
		"begin",
		`/insert into _vt.vreplication.*workflow, source, pos.*values.*'test', 'keyspace:"other_keyspace" shard:"0.*'MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-10'.*'\{"config": \{"vreplication-retry-delay": "5s"\}\}'`,
		fmt.Sprintf("delete from _vt.vreplication where id=%d", firstID),
		"commit",
		"/update _vt.vreplication set message='Picked source tablet.*",
		"/update _vt.vreplication set state='Running', message=left\\('', 1000\\) where id.*",
	))

	// Delete all vreplication streams. There should be only one, but we don't know its id.
	deleteAllVReplicationStreams(t)
	expectDeleteQueries(t)
}

func TestJournalOneToMany(t *testing.T) {
	defer deleteTablet(addTablet(100))
	defer deleteTablet(addOtherTablet(101, "other_keyspace", "-80"))
	defer deleteTablet(addOtherTablet(102, "other_keyspace", "80-"))

	execStatements(t, []string{
		"create table t(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.t(id int, val varbinary(128), primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t",
		fmt.Sprintf("drop table %s.t", vrepldb),
	})

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "t",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}

	_, firstID := startVReplication(t, bls, "")

	journal := &binlogdatapb.Journal{
		Id:            1,
		MigrationType: binlogdatapb.MigrationType_SHARDS,
		Participants: []*binlogdatapb.KeyspaceShard{{
			Keyspace: "vttest",
			Shard:    "0",
		}},
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: "other_keyspace",
			Shard:    "-80",
			Gtid:     "MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-5",
		}, {
			Keyspace: "other_keyspace",
			Shard:    "80-",
			Gtid:     "MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:5-10",
		}},
	}
	query := fmt.Sprintf("insert into _vt.resharding_journal(id, db_name, val) values (1, 'vttest', %v)", encodeString(journal.String()))
	execStatements(t, []string{query})
	defer execStatements(t, []string{"delete from _vt.resharding_journal"})

	expectDBClientQueries(t, qh.Expect(
		"begin",
		`/insert into _vt.vreplication.*workflow, source, pos.*values.*'test', 'keyspace:"other_keyspace" shard:"-80.*'MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-5'`,
		`/insert into _vt.vreplication.*workflow, source, pos.*values.*'test', 'keyspace:"other_keyspace" shard:"80-.*'MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:5-10'`,
		fmt.Sprintf("delete from _vt.vreplication where id=%d", firstID),
		"commit",
		"/update _vt.vreplication set message='Picked source tablet.*",
		"/update _vt.vreplication set message='Picked source tablet.*",
		"/update _vt.vreplication set state='Running', message=left\\('', 1000\\) where id.*",
		"/update _vt.vreplication set state='Running', message=left\\('', 1000\\) where id.*",
	))

	// Delete all vreplication streams. There should be only one, but we don't know its id.
	deleteAllVReplicationStreams(t)
	expectDeleteQueries(t)
}

func TestJournalTablePresent(t *testing.T) {
	defer deleteTablet(addTablet(100))
	defer deleteTablet(addOtherTablet(101, "other_keyspace", "0"))

	execStatements(t, []string{
		"create table t(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.t(id int, val varbinary(128), primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t",
		fmt.Sprintf("drop table %s.t", vrepldb),
	})

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "t",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	_, firstID := startVReplication(t, bls, "")

	journal := &binlogdatapb.Journal{
		Id:            1,
		MigrationType: binlogdatapb.MigrationType_TABLES,
		Participants: []*binlogdatapb.KeyspaceShard{{
			Keyspace: "vttest",
			Shard:    "0",
		}},
		Tables: []string{"t"},
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: "other_keyspace",
			Shard:    "0",
			Gtid:     "MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-10",
		}},
	}
	query := fmt.Sprintf("insert into _vt.resharding_journal(id, db_name, val) values (1, 'vttest', %v)", encodeString(journal.String()))
	execStatements(t, []string{query})
	defer execStatements(t, []string{"delete from _vt.resharding_journal"})

	expectDBClientQueries(t, qh.Expect(
		"begin",
		`/insert into _vt.vreplication.*workflow, source, pos.*values.*'test', 'keyspace:"other_keyspace" shard:"0.*'MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-10'`,
		fmt.Sprintf("delete from _vt.vreplication where id=%d", firstID),
		"commit",
		"/update _vt.vreplication set message='Picked source tablet.*",
		"/update _vt.vreplication set state='Running', message=left\\('', 1000\\) where id.*",
	))

	// Delete all vreplication streams. There should be only one, but we don't know its id.
	deleteAllVReplicationStreams(t)
	expectDeleteQueries(t)
}

func TestJournalTableNotPresent(t *testing.T) {
	defer deleteTablet(addTablet(100))
	defer deleteTablet(addOtherTablet(101, "other_keyspace", "0"))

	execStatements(t, []string{
		"create table t(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.t(id int, val varbinary(128), primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t",
		fmt.Sprintf("drop table %s.t", vrepldb),
	})

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "t",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}

	_, _ = startVReplication(t, bls, "")

	journal := &binlogdatapb.Journal{
		Id:            1,
		MigrationType: binlogdatapb.MigrationType_TABLES,
		Participants: []*binlogdatapb.KeyspaceShard{{
			Keyspace: "vttest",
			Shard:    "0",
		}},
		Tables: []string{"t1"},
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: "other_keyspace",
			Shard:    "0",
			Gtid:     "MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-10",
		}},
	}
	query := fmt.Sprintf("insert into _vt.resharding_journal(id, db_name, val) values (1, 'vttest', %v)", encodeString(journal.String()))
	execStatements(t, []string{query})
	defer execStatements(t, []string{"delete from _vt.resharding_journal"})

	// Delete all vreplication streams. There should be only one, but we don't know its id.
	deleteAllVReplicationStreams(t)
	expectDeleteQueries(t)
}

func TestJournalTableMixed(t *testing.T) {
	defer deleteTablet(addTablet(100))
	defer deleteTablet(addOtherTablet(101, "other_keyspace", "0"))

	execStatements(t, []string{
		"create table t(id int, val varbinary(128), primary key(id))",
		"create table t1(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.t(id int, val varbinary(128), primary key(id))", vrepldb),
		fmt.Sprintf("create table %s.t1(id int, val varbinary(128), primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t",
		"drop table t1",
		fmt.Sprintf("drop table %s.t", vrepldb),
		fmt.Sprintf("drop table %s.t1", vrepldb),
	})

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "t",
		}, {
			Match: "t1",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	_, _ = startVReplication(t, bls, "")

	journal := &binlogdatapb.Journal{
		Id:            1,
		MigrationType: binlogdatapb.MigrationType_TABLES,
		Participants: []*binlogdatapb.KeyspaceShard{{
			Keyspace: "vttest",
			Shard:    "0",
		}},
		Tables: []string{"t"},
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: "other_keyspace",
			Shard:    "0",
			Gtid:     "MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-10",
		}},
	}
	query := fmt.Sprintf("insert into _vt.resharding_journal(id, db_name, val) values (1, 'vttest', %v)", encodeString(journal.String()))
	execStatements(t, []string{query})
	defer execStatements(t, []string{"delete from _vt.resharding_journal"})

	expectDBClientQueries(t, qh.Expect(
		"/update _vt.vreplication set state='Stopped', message=left\\('unable to handle journal event: tables were partially matched', 1000\\) where id",
	))

	// Delete all vreplication streams. There should be only one, but we don't know its id.
	deleteAllVReplicationStreams(t)
	expectDeleteQueries(t)
}

// TestJournalTablesLookupVindexIgnored guards against resharding journals
// written by MoveTables SwitchTraffic/ReverseTraffic destroying lookup
// vindex backfill streams (https://github.com/vitessio/vitess/issues/20915).
// A CreateLookupIndex stream must ignore a TABLES journal -- its
// keyspace_id() filter cannot be planned in another keyspace -- and keep
// replicating from its current source, which the paired workflow keeps
// feeding after the switch. SHARDS (Reshard) journals are still followed;
// that behavior is pinned by TestJournalOneToOne/TestJournalOneToMany.
func TestJournalTablesLookupVindexIgnored(t *testing.T) {
	if runNoBlobTest {
		t.Skip("CreateLookupIndex workflows do not support binlog_row_image=noblob")
	}
	// This test's teardown uses defer, not t.Cleanup: execStatements and
	// the other framework helpers run on t.Context(), which is already
	// canceled by the time t.Cleanup callbacks execute.
	defer deleteTablet(addTablet(100))
	defer deleteTablet(addOtherTablet(101, "other_keyspace", "0"))

	execStatements(t, []string{
		"create table t(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.t(id int, val varbinary(128), primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t",
		fmt.Sprintf("drop table %s.t", vrepldb),
	})

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "t",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	cancel, _ := startVReplicationWithWorkflowType(t, bls, "", binlogdatapb.VReplicationWorkflowType_CreateLookupIndex)
	defer cancel()

	journal := &binlogdatapb.Journal{
		Id:            1,
		MigrationType: binlogdatapb.MigrationType_TABLES,
		Participants: []*binlogdatapb.KeyspaceShard{{
			Keyspace: "vttest",
			Shard:    "0",
		}},
		Tables: []string{"t"},
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: "other_keyspace",
			Shard:    "0",
			Gtid:     "MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-10",
		}},
	}
	query := fmt.Sprintf("insert into _vt.resharding_journal(id, db_name, val) values (1, 'vttest', %v)", encodeString(journal.String()))
	execStatements(t, []string{query})
	defer execStatements(t, []string{"delete from _vt.resharding_journal"})

	// The journal must be ignored: no stream deletion/recreation. The
	// stream must still be replicating from the original source, which
	// this insert proves end to end.
	execStatements(t, []string{"insert into t values(1, 'aaa')"})
	expectDBClientQueries(t, qh.Expect(
		"begin",
		"insert into t(id,val) values (1,_binary'aaa')",
		"/update _vt.vreplication set pos=",
		"commit",
	))
}

// TestJournalRegisterLookupVindexRefused guards the engine-side insurance
// for https://github.com/vitessio/vitess/issues/20915: even if a future
// caller bypasses the vplayer's TABLES-journal gate, registerJournal must
// refuse to transition a lookup vindex workflow rather than let
// transitionJournal destroy its streams.
func TestJournalRegisterLookupVindexRefused(t *testing.T) {
	if runNoBlobTest {
		t.Skip("CreateLookupIndex workflows do not support binlog_row_image=noblob")
	}
	// This test's teardown uses defer, not t.Cleanup: execStatements and
	// the other framework helpers run on t.Context(), which is already
	// canceled by the time t.Cleanup callbacks execute.
	defer deleteTablet(addTablet(100))
	defer deleteTablet(addOtherTablet(101, "other_keyspace", "0"))

	execStatements(t, []string{
		"create table t(id int, val varbinary(128), primary key(id))",
		fmt.Sprintf("create table %s.t(id int, val varbinary(128), primary key(id))", vrepldb),
	})
	defer execStatements(t, []string{
		"drop table t",
		fmt.Sprintf("drop table %s.t", vrepldb),
	})

	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "t",
		}},
	}
	bls := &binlogdatapb.BinlogSource{
		Keyspace: env.KeyspaceName,
		Shard:    env.ShardName,
		Filter:   filter,
		OnDdl:    binlogdatapb.OnDDLAction_IGNORE,
	}
	cancel, id := startVReplicationWithWorkflowType(t, bls, "", binlogdatapb.VReplicationWorkflowType_CreateLookupIndex)
	defer cancel()

	journal := &binlogdatapb.Journal{
		Id:            2,
		MigrationType: binlogdatapb.MigrationType_TABLES,
		Participants: []*binlogdatapb.KeyspaceShard{{
			Keyspace: "vttest",
			Shard:    "0",
		}},
		Tables: []string{"t"},
		ShardGtids: []*binlogdatapb.ShardGtid{{
			Keyspace: "other_keyspace",
			Shard:    "0",
			Gtid:     "MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-10",
		}},
	}

	err := playerEngine.registerJournal(journal, int32(id))
	require.ErrorContains(t, err, "lookup vindex")

	// The stream must be untouched: no journaler entry queued, no
	// transition, the controller still registered.
	playerEngine.mu.Lock()
	_, ok := playerEngine.controllers[int32(id)]
	journalerEmpty := len(playerEngine.journaler) == 0
	playerEngine.mu.Unlock()
	assert.True(t, ok, "controller was removed")
	assert.True(t, journalerEmpty, "journaler entry was created")
}
