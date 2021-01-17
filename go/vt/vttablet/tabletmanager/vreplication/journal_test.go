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

	"context"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
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
	env.SchemaEngine.Reload(context.Background())

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
			Shard:    "0",
			Gtid:     "MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-10",
		}},
	}
	query := fmt.Sprintf("insert into _vt.resharding_journal(id, db_name, val) values (1, 'vttest', %v)", encodeString(journal.String()))
	execStatements(t, []string{createReshardingJournalTable, query})
	defer execStatements(t, []string{"delete from _vt.resharding_journal"})

	expectDBClientQueries(t, []string{
		"/update _vt.vreplication set pos=",
		"begin",
		`/insert into _vt.vreplication.*workflow, source, pos.*values.*'test', 'keyspace:\\"other_keyspace\\" shard:\\"0\\.*'MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-10'`,
		fmt.Sprintf("delete from _vt.vreplication where id=%d", firstID),
		"commit",
		"/update _vt.vreplication set message='Picked source tablet.*",
		"/update _vt.vreplication set state='Running', message='' where id.*",
	})

	// Delete all vreplication streams. There should be only one, but we don't know its id.
	if _, err := playerEngine.Exec("delete from _vt.vreplication"); err != nil {
		t.Fatal(err)
	}
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
	env.SchemaEngine.Reload(context.Background())

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
	execStatements(t, []string{createReshardingJournalTable, query})
	defer execStatements(t, []string{"delete from _vt.resharding_journal"})

	expectDBClientQueries(t, []string{
		"/update _vt.vreplication set pos=",
		"begin",
		`/insert into _vt.vreplication.*workflow, source, pos.*values.*'test', 'keyspace:\\"other_keyspace\\" shard:\\"-80\\.*'MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-5'`,
		`/insert into _vt.vreplication.*workflow, source, pos.*values.*'test', 'keyspace:\\"other_keyspace\\" shard:\\"80-\\.*'MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:5-10'`,
		fmt.Sprintf("delete from _vt.vreplication where id=%d", firstID),
		"commit",
		"/update _vt.vreplication set message='Picked source tablet.*",
		"/update _vt.vreplication set message='Picked source tablet.*",
		"/update _vt.vreplication set state='Running', message='' where id.*",
		"/update _vt.vreplication set state='Running', message='' where id.*",
	})

	// Delete all vreplication streams. There should be only one, but we don't know its id.
	if _, err := playerEngine.Exec("delete from _vt.vreplication"); err != nil {
		t.Fatal(err)
	}
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
	env.SchemaEngine.Reload(context.Background())

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
	execStatements(t, []string{createReshardingJournalTable, query})
	defer execStatements(t, []string{"delete from _vt.resharding_journal"})

	expectDBClientQueries(t, []string{
		"/update _vt.vreplication set pos=",
		"begin",
		`/insert into _vt.vreplication.*workflow, source, pos.*values.*'test', 'keyspace:\\"other_keyspace\\" shard:\\"0\\.*'MySQL56/7b04699f-f5e9-11e9-bf88-9cb6d089e1c3:1-10'`,
		fmt.Sprintf("delete from _vt.vreplication where id=%d", firstID),
		"commit",
		"/update _vt.vreplication set message='Picked source tablet.*",
		"/update _vt.vreplication set state='Running', message='' where id.*",
	})

	// Delete all vreplication streams. There should be only one, but we don't know its id.
	if _, err := playerEngine.Exec("delete from _vt.vreplication"); err != nil {
		t.Fatal(err)
	}
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
	env.SchemaEngine.Reload(context.Background())

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
	execStatements(t, []string{createReshardingJournalTable, query})
	defer execStatements(t, []string{"delete from _vt.resharding_journal"})

	// Wait for a heartbeat based update to confirm that the existing vreplication was not transitioned.
	expectDBClientQueries(t, []string{
		"/update _vt.vreplication set pos=",
	})

	// Delete all vreplication streams. There should be only one, but we don't know its id.
	if _, err := playerEngine.Exec("delete from _vt.vreplication"); err != nil {
		t.Fatal(err)
	}
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
	env.SchemaEngine.Reload(context.Background())

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
	execStatements(t, []string{createReshardingJournalTable, query})
	defer execStatements(t, []string{"delete from _vt.resharding_journal"})

	expectDBClientQueries(t, []string{
		"/update _vt.vreplication set pos=",
		"/update _vt.vreplication set state='Stopped', message='unable to handle journal event: tables were partially matched' where id",
	})

	// Delete all vreplication streams. There should be only one, but we don't know its id.
	if _, err := playerEngine.Exec("delete from _vt.vreplication"); err != nil {
		t.Fatal(err)
	}
	expectDeleteQueries(t)
}
