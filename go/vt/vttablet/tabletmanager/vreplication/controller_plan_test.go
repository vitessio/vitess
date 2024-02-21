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
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

type testControllerPlan struct {
	query             string
	opcode            int
	numInserts        int
	selector          string
	applier           string
	delCopyState      string
	delPostCopyAction string
}

func TestControllerPlan(t *testing.T) {
	tcases := []struct {
		in   string
		plan *testControllerPlan
		err  string
	}{{
		// Insert
		in: "insert into _vt.vreplication values(null)",
		plan: &testControllerPlan{
			query:      "insert into _vt.vreplication values(null)",
			opcode:     insertQuery,
			numInserts: 1,
		},
	}, {
		in: "insert into _vt.vreplication(id) values(null)",
		plan: &testControllerPlan{
			query:      "insert into _vt.vreplication(id) values(null)",
			opcode:     insertQuery,
			numInserts: 1,
		},
	}, {
		in: "insert into _vt.vreplication(workflow, id) values('', null)",
		plan: &testControllerPlan{
			query:      "insert into _vt.vreplication(workflow, id) values('', null)",
			opcode:     insertQuery,
			numInserts: 1,
		},
	}, {
		in: "insert into _vt.vreplication values(null), (null)",
		plan: &testControllerPlan{
			query:      "insert into _vt.vreplication values(null), (null)",
			opcode:     insertQuery,
			numInserts: 2,
		},
	}, {
		in: "insert into _vt.resharding_journal values (1)",
		plan: &testControllerPlan{
			query:  "insert into _vt.resharding_journal values (1)",
			opcode: reshardingJournalQuery,
		},
	}, {
		in:  "replace into _vt.vreplication values(null)",
		err: "unsupported construct: replace into _vt.vreplication values (null)",
	}, {
		in:  "insert ignore into _vt.vreplication values(null)",
		err: "unsupported construct: insert ignore into _vt.vreplication values (null)",
	}, {
		in:  "insert into other values(null)",
		err: "invalid database name: ",
	}, {
		in:  "insert into _vt.vreplication partition(a) values(null)",
		err: "unsupported construct: insert into _vt.vreplication partition (a) values (null)",
	}, {
		in:  "insert into _vt.vreplication values(null) on duplicate key update id=3",
		err: "unsupported construct: insert into _vt.vreplication values (null) on duplicate key update id = 3",
	}, {
		in:  "insert into _vt.vreplication select * from a",
		err: "unsupported construct: insert into _vt.vreplication select * from a",
	}, {
		in:  "insert into _vt.vreplication(a, b, id) values(null)",
		err: "malformed statement: insert into _vt.vreplication(a, b, id) values (null)",
	}, {
		in:  "insert into _vt.vreplication(workflow, id) values('aa', 1)",
		err: "id should not have a value: insert into _vt.vreplication(workflow, id) values ('aa', 1)",
	}, {
		in:  "insert into _vt.vreplication values(1)",
		err: "id should not have a value: insert into _vt.vreplication values (1)",

		// Update
	}, {
		in: "update _vt.vreplication set state='Running' where id = 1",
		plan: &testControllerPlan{
			query:    "update _vt.vreplication set state='Running' where id = 1",
			opcode:   updateQuery,
			selector: "select id from _vt.vreplication where id = 1",
			applier:  "update _vt.vreplication set state = 'Running' where id in ::ids",
		},
	}, {
		in:  "update _vt.vreplication set state='Running'",
		err: "unsafe WHERE clause in update without the /*vt+ ALLOW_UNSAFE_VREPLICATION_WRITE */ comment directive: ; should be using = or in with at least one of the following columns: id, workflow",
	}, {
		in: "update /*vt+ ALLOW_UNSAFE_VREPLICATION_WRITE */ _vt.vreplication set state='Running'",
		plan: &testControllerPlan{
			query:    "update /*vt+ ALLOW_UNSAFE_VREPLICATION_WRITE */ _vt.vreplication set state='Running'",
			opcode:   updateQuery,
			selector: "select id from _vt.vreplication",
			applier:  "update /*vt+ ALLOW_UNSAFE_VREPLICATION_WRITE */ _vt.vreplication set state = 'Running' where id in ::ids",
		},
	}, {
		in:  "update _vt.vreplication set state='Running', message='' where id >= 1",
		err: "unsafe WHERE clause in update without the /*vt+ ALLOW_UNSAFE_VREPLICATION_WRITE */ comment directive:  where id >= 1; should be using = or in with at least one of the following columns: id, workflow",
	}, {
		in:  "update _vt.vreplication set state = 'Running' where state in ('Stopped', 'Error')",
		err: "unsafe WHERE clause in update without the /*vt+ ALLOW_UNSAFE_VREPLICATION_WRITE */ comment directive:  where state in ('Stopped', 'Error'); should be using = or in with at least one of the following columns: id, workflow",
	}, {
		in:  "update _vt.vreplication set state='Running', message='' where state='Stopped'",
		err: "unsafe WHERE clause in update without the /*vt+ ALLOW_UNSAFE_VREPLICATION_WRITE */ comment directive:  where state = 'Stopped'; should be using = or in with at least one of the following columns: id, workflow",
	}, {
		in: "update _vt.vreplication set state='Running' where a = 1",
		plan: &testControllerPlan{
			query:    "update _vt.vreplication set state='Running' where a = 1",
			opcode:   updateQuery,
			selector: "select id from _vt.vreplication where a = 1",
			applier:  "update _vt.vreplication set state = 'Running' where id in ::ids",
		},
		err: "unsafe WHERE clause in update without the /*vt+ ALLOW_UNSAFE_VREPLICATION_WRITE */ comment directive:  where a = 1; should be using = or in with at least one of the following columns: id, workflow",
	}, {
		in: "update _vt.resharding_journal set col = 1",
		plan: &testControllerPlan{
			query:  "update _vt.resharding_journal set col = 1",
			opcode: reshardingJournalQuery,
		},
	}, {
		in:  "update _vt.a set state='Running' where id = 1",
		err: "invalid table name: a",
	}, {
		in:  "update _vt.vreplication set state='Running' where id = 1 order by id",
		err: "unsupported construct: update _vt.vreplication set state = 'Running' where id = 1 order by id asc",
	}, {
		in:  "update _vt.vreplication set state='Running' where id = 1 limit 1",
		err: "unsupported construct: update _vt.vreplication set state = 'Running' where id = 1 limit 1",
	}, {
		in:  "update _vt.vreplication set state='Running', id = 2 where id = 1",
		err: "id cannot be changed: id = 2",

		// Delete
	}, {
		in: "delete from _vt.vreplication where id = 1",
		plan: &testControllerPlan{
			query:             "delete from _vt.vreplication where id = 1",
			opcode:            deleteQuery,
			selector:          "select id from _vt.vreplication where id = 1",
			applier:           "delete from _vt.vreplication where id in ::ids",
			delCopyState:      "delete from _vt.copy_state where vrepl_id in ::ids",
			delPostCopyAction: "delete from _vt.post_copy_action where vrepl_id in ::ids",
		},
	}, {
		in:  "delete from _vt.vreplication",
		err: "unsafe WHERE clause in delete without the /*vt+ ALLOW_UNSAFE_VREPLICATION_WRITE */ comment directive: ; should be using = or in with at least one of the following columns: id, workflow",
	}, {
		in: "delete /*vt+ ALLOW_UNSAFE_VREPLICATION_WRITE */ from _vt.vreplication",
		plan: &testControllerPlan{
			query:             "delete /*vt+ ALLOW_UNSAFE_VREPLICATION_WRITE */ from _vt.vreplication",
			opcode:            deleteQuery,
			selector:          "select id from _vt.vreplication",
			applier:           "delete /*vt+ ALLOW_UNSAFE_VREPLICATION_WRITE */ from _vt.vreplication where id in ::ids",
			delCopyState:      "delete from _vt.copy_state where vrepl_id in ::ids",
			delPostCopyAction: "delete from _vt.post_copy_action where vrepl_id in ::ids",
		},
	}, {
		in:  "delete from _vt.vreplication where state='Stopped'",
		err: "unsafe WHERE clause in delete without the /*vt+ ALLOW_UNSAFE_VREPLICATION_WRITE */ comment directive:  where state = 'Stopped'; should be using = or in with at least one of the following columns: id, workflow",
	}, {
		in: "delete from _vt.vreplication where a = 1",
		plan: &testControllerPlan{
			query:             "delete from _vt.vreplication where a = 1",
			opcode:            deleteQuery,
			selector:          "select id from _vt.vreplication where a = 1",
			applier:           "delete from _vt.vreplication where id in ::ids",
			delCopyState:      "delete from _vt.copy_state where vrepl_id in ::ids",
			delPostCopyAction: "delete from _vt.post_copy_action where vrepl_id in ::ids",
		},
		err: "unsafe WHERE clause in delete without the /*vt+ ALLOW_UNSAFE_VREPLICATION_WRITE */ comment directive:  where a = 1; should be using = or in with at least one of the following columns: id, workflow",
	}, {
		in: "delete from _vt.resharding_journal where id = 1",
		plan: &testControllerPlan{
			query:  "delete from _vt.resharding_journal where id = 1",
			opcode: reshardingJournalQuery,
		},
	}, {
		in:  "delete from _vt.a where id = 1",
		err: "invalid table name: a",
	}, {
		in:  "delete a, b from _vt.vreplication where id = 1",
		err: "unsupported construct: delete a, b from _vt.vreplication where id = 1",
	}, {
		in:  "delete from _vt.vreplication where id = 1 order by id",
		err: "unsupported construct: delete from _vt.vreplication where id = 1 order by id asc",
	}, {
		in:  "delete from _vt.vreplication where id = 1 limit 1",
		err: "unsupported construct: delete from _vt.vreplication where id = 1 limit 1",
	}, {
		in:  "delete from _vt.vreplication partition (a) where id = 1 limit 1",
		err: "unsupported construct: delete from _vt.vreplication partition (a) where id = 1 limit 1",

		// Select
	}, {
		in: "select * from _vt.vreplication",
		plan: &testControllerPlan{
			opcode: selectQuery,
			query:  "select * from _vt.vreplication",
		},
	}, {
		in: "select * from _vt.resharding_journal",
		plan: &testControllerPlan{
			opcode: selectQuery,
			query:  "select * from _vt.resharding_journal",
		},
	}, {
		in: "select * from _vt.copy_state",
		plan: &testControllerPlan{
			opcode: selectQuery,
			query:  "select * from _vt.copy_state",
		},
	}, {
		in: "select * from _vt.vreplication_log",
		plan: &testControllerPlan{
			opcode: selectQuery,
			query:  "select * from _vt.vreplication_log",
		},
	}, {
		in:  "select * from _vt.a",
		err: "invalid table name: a",
	}, {
		in:  "select * from nope.a",
		err: "invalid database name: nope",

		// Parser
	}, {
		in:  "bad query",
		err: "syntax error at position 4 near 'bad'",
	}, {
		in:  "set a = 1",
		err: "unsupported construct: set @@a = 1",
	}}
	for _, tcase := range tcases {
		t.Run(tcase.in, func(t *testing.T) {
			pl, err := buildControllerPlan(tcase.in, sqlparser.NewTestParser())
			if tcase.err != "" {
				require.EqualError(t, err, tcase.err)
				return
			}
			require.NoError(t, err)

			gotPlan := &testControllerPlan{
				query:      pl.query,
				opcode:     pl.opcode,
				numInserts: pl.numInserts,
				selector:   pl.selector,
			}
			if pl.applier != nil {
				gotPlan.applier = pl.applier.Query
			}
			if pl.delCopyState != nil {
				gotPlan.delCopyState = pl.delCopyState.Query
			}
			if pl.delPostCopyAction != nil {
				gotPlan.delPostCopyAction = pl.delPostCopyAction.Query
			}
			if !reflect.DeepEqual(gotPlan, tcase.plan) {
				t.Errorf("getPlan(%v):\n%+v, want\n%+v", tcase.in, gotPlan, tcase.plan)
			}
		})
	}
}
