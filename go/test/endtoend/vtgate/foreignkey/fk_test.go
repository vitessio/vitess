/*
Copyright 2023 The Vitess Authors.

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

package foreignkey

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/utils"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

// TestInsertWithFK tests that insertions work as expected when foreign key management is enabled in Vitess.
func TestInsertWithFK(t *testing.T) {
	mcmp, closer := start(t)
	conn := mcmp.VtConn
	defer closer()

	// insert some data.
	utils.Exec(t, conn, `insert into t1(id, col) values (100, 123),(10, 12),(1, 13),(1000, 1234)`)

	// Verify that inserting data into a table that has shard scoped foreign keys works.
	utils.Exec(t, conn, `insert into t2(id, col) values (100, 125), (1, 132)`)

	// Verify that insertion fails if the data doesn't follow the fk constraint.
	_, err := utils.ExecAllowError(t, conn, `insert into t2(id, col) values (1310, 125)`)
	assert.ErrorContains(t, err, "Cannot add or update a child row: a foreign key constraint fails")

	// Verify that insertion fails if the table has cross-shard foreign keys (even if the data follows the constraints).
	_, err = utils.ExecAllowError(t, conn, `insert into t3(id, col) values (100, 100)`)
	assert.ErrorContains(t, err, "VT12002: unsupported: cross-shard foreign keys")

	// insert some data in a table with multicol vindex.
	utils.Exec(t, conn, `insert into multicol_tbl1(cola, colb, colc, msg) values (100, 'a', 'b', 'msg'), (101, 'c', 'd', 'msg2')`)

	// Verify that inserting data into a table that has shard scoped multi-column foreign keys works.
	utils.Exec(t, conn, `insert into multicol_tbl2(cola, colb, colc, msg) values (100, 'a', 'b', 'msg3')`)

	// Verify that insertion fails if the data doesn't follow the fk constraint.
	_, err = utils.ExecAllowError(t, conn, `insert into multicol_tbl2(cola, colb, colc, msg) values (103, 'c', 'd', 'msg2')`)
	assert.ErrorContains(t, err, "Cannot add or update a child row: a foreign key constraint fails")
}

// TestDeleteWithFK tests that deletions work as expected when foreign key management is enabled in Vitess.
func TestDeleteWithFK(t *testing.T) {
	mcmp, closer := start(t)
	conn := mcmp.VtConn
	defer closer()

	// insert some data.
	utils.Exec(t, conn, `insert into t1(id, col) values (100, 123),(10, 12),(1, 13),(1000, 1234)`)
	utils.Exec(t, conn, `insert into t2(id, col) values (100, 125), (1, 132)`)
	utils.Exec(t, conn, `insert into t4(id, col) values (1, 321)`)
	utils.Exec(t, conn, `insert into multicol_tbl1(cola, colb, colc, msg) values (100, 'a', 'b', 'msg'), (101, 'c', 'd', 'msg2')`)
	utils.Exec(t, conn, `insert into multicol_tbl2(cola, colb, colc, msg) values (100, 'a', 'b', 'msg3')`)

	// child foreign key is shard scoped. Query will fail at mysql due to On Delete Restrict.
	_, err := utils.ExecAllowError(t, conn, `delete from t2 where col = 132`)
	assert.ErrorContains(t, err, "Cannot delete or update a parent row: a foreign key constraint fails")

	// child row does not exist so query will succeed.
	qr := utils.Exec(t, conn, `delete from t2 where col = 125`)
	assert.EqualValues(t, 1, qr.RowsAffected)

	// table's child foreign key has cross shard fk, so query will fail at vtgate.
	_, err = utils.ExecAllowError(t, conn, `delete from t1 where id = 42`)
	assert.ErrorContains(t, err, "VT12002: unsupported: cross-shard foreign keys (errno 1235) (sqlstate 42000)")

	// child foreign key is cascade, so this should work as expected.
	qr = utils.Exec(t, conn, `delete from multicol_tbl1 where cola = 100`)
	assert.EqualValues(t, 1, qr.RowsAffected)
	// we also verify that the rows in the child table were deleted.
	qr = utils.Exec(t, conn, `select * from multicol_tbl2 where cola = 100`)
	assert.Zero(t, qr.Rows)

	// Unsharded keyspace tests
	utils.Exec(t, conn, `use uks`)
	// insert some data.
	utils.Exec(t, conn, `insert into u_t1(id, col1) values (100, 123), (10, 12), (1, 13), (1000, 1234)`)
	utils.Exec(t, conn, `insert into u_t2(id, col2) values (342, 123), (19, 1234)`)

	// Delete from u_t1 which has a foreign key constraint to t2 with SET NULL type.
	qr = utils.Exec(t, conn, `delete from u_t1 where id = 100`)
	assert.EqualValues(t, 1, qr.RowsAffected)
	// Verify the result in u_t2 as well
	utils.AssertMatches(t, conn, `select * from u_t2`, `[[INT64(342) NULL] [INT64(19) INT64(1234)]]`)
}

// TestUpdateWithFK tests that update work as expected when foreign key management is enabled in Vitess.
func TestUpdateWithFK(t *testing.T) {
	mcmp, closer := start(t)
	conn := mcmp.VtConn
	defer closer()

	// insert some data.
	utils.Exec(t, conn, `insert into t1(id, col) values (100, 123),(10, 12),(1, 13),(1000, 1234)`)
	utils.Exec(t, conn, `insert into t2(id, col, mycol) values (100, 125, 'foo'), (1, 132, 'bar')`)
	utils.Exec(t, conn, `insert into t4(id, col, t2_col, t2_mycol) values (1, 321, 132, 'bar')`)
	utils.Exec(t, conn, `insert into t5(pk, sk, col1) values (1, 1, 1),(2, 1, 1),(3, 1, 10),(4, 1, 20),(5, 1, 30),(6, 1, 40)`)
	utils.Exec(t, conn, `insert into t6(pk, sk, col1) values (10, 1, 1), (20, 1, 20)`)

	// parent foreign key is shard scoped and value is not updated. Query will succeed.
	_ = utils.Exec(t, conn, `update t4 set t2_mycol = 'bar' where id = 1`)

	// parent foreign key is shard scoped and value does not exists in parent table. Query will fail at mysql due to On Update Restrict.
	_, err := utils.ExecAllowError(t, conn, `update t4 set t2_mycol = 'foo' where id = 1`)
	assert.ErrorContains(t, err, "Cannot add or update a child row: a foreign key constraint fails")

	// updating column which does not have foreign key constraint, so query will succeed.
	qr := utils.Exec(t, conn, `update t4 set col = 20 where id = 1`)
	assert.EqualValues(t, 1, qr.RowsAffected)

	// updating column which does not have foreign key constraint, so query will succeed.
	_ = utils.Exec(t, conn, `update t2 set mycol = 'baz' where id = 100`)
	assert.EqualValues(t, 1, qr.RowsAffected)

	// child table have restrict in shard scoped and value exists in parent table.
	_ = utils.Exec(t, conn, `update t6 set col1 = 40 where pk = 20`)
	assert.EqualValues(t, 1, qr.RowsAffected)

	// Unsharded keyspace tests
	utils.Exec(t, conn, `use uks`)
	// insert some data.
	utils.Exec(t, conn, `insert into u_t1(id, col1) values (100, 123), (10, 12), (1, 13), (1000, 1234)`)
	utils.Exec(t, conn, `insert into u_t2(id, col2) values (342, 123), (19, 1234)`)
	utils.Exec(t, conn, `insert into u_t3(id, col3) values (32, 123), (1, 12)`)

	// Cascade update with a new value
	_ = utils.Exec(t, conn, `update u_t1 set col1 = 2 where id = 100`)
	// Verify the result in u_t2 and u_t3 as well.
	utils.AssertMatches(t, conn, `select * from u_t2 order by id`, `[[INT64(19) INT64(1234)] [INT64(342) NULL]]`)
	utils.AssertMatches(t, conn, `select * from u_t3 order by id`, `[[INT64(1) INT64(12)] [INT64(32) INT64(2)]]`)

	// Update u_t1 which has a foreign key constraint to u_t2 with SET NULL type, and to u_t3 with CASCADE type.
	qr = utils.Exec(t, conn, `update u_t1 set col1 = 13 where id = 100`)
	assert.EqualValues(t, 1, qr.RowsAffected)
	// Verify the result in u_t2 and u_t3 as well.
	utils.AssertMatches(t, conn, `select * from u_t2 order by id`, `[[INT64(19) INT64(1234)] [INT64(342) NULL]]`)
	utils.AssertMatches(t, conn, `select * from u_t3 order by id`, `[[INT64(1) INT64(12)] [INT64(32) INT64(13)]]`)

	// Update u_t1 which has a foreign key constraint to u_t2 with SET NULL type, and to u_t3 with CASCADE type.
	// This update however doesn't change the table.
	qr = utils.Exec(t, conn, `update u_t1 set col1 = 1234 where id = 1000`)
	assert.EqualValues(t, 0, qr.RowsAffected)
	// Verify the result in u_t2 and u_t3 as well.
	utils.AssertMatches(t, conn, `select * from u_t2 order by id`, `[[INT64(19) INT64(1234)] [INT64(342) NULL]]`)
	utils.AssertMatches(t, conn, `select * from u_t3 order by id`, `[[INT64(1) INT64(12)] [INT64(32) INT64(13)]]`)

	// Update with a subquery inside, such that the update is on a foreign key related column.
	qr = utils.Exec(t, conn, `update u_t2 set col2 = (select col1 from u_t1 where id = 100) where id = 342`)
	assert.EqualValues(t, 1, qr.RowsAffected)
	// Verify the result in u_t1, u_t2 and u_t3.
	utils.AssertMatches(t, conn, `select * from u_t1 order by id`, `[[INT64(1) INT64(13)] [INT64(10) INT64(12)] [INT64(100) INT64(13)] [INT64(1000) INT64(1234)]]`)
	utils.AssertMatches(t, conn, `select * from u_t2 order by id`, `[[INT64(19) INT64(1234)] [INT64(342) INT64(13)]]`)
	utils.AssertMatches(t, conn, `select * from u_t3 order by id`, `[[INT64(1) INT64(12)] [INT64(32) INT64(13)]]`)
}

// TestVstreamForFKBinLog tests that dml queries with fks are written with child row first approach in the binary logs.
func TestVstreamForFKBinLog(t *testing.T) {
	vtgateConn, err := cluster.DialVTGate(context.Background(), t.Name(), vtgateGrpcAddress, "fk_user", "")
	require.NoError(t, err)
	defer vtgateConn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan *binlogdatapb.VEvent)
	runVStream(t, ctx, ch, vtgateConn)

	mcmp, closer := start(t)
	conn := mcmp.VtConn
	defer closer()
	defer cancel()

	utils.Exec(t, conn, `use uks`)

	// insert some data.
	utils.Exec(t, conn, `insert into u_t1(id, col1) values (1,2), (11,4), (111,6)`)
	utils.Exec(t, conn, `insert into u_t2(id, col2) values (2,2), (22,4)`)
	utils.Exec(t, conn, `insert into u_t3(id, col3) values (33,4), (333,6)`)
	// drain 3 row events.
	_ = drainEvents(t, ch, 3)

	tcases := []struct {
		query     string
		events    int
		rowEvents []string
	}{{
		query:  `update u_t1 set col1 = 3 where id = 11`,
		events: 3,
		rowEvents: []string{
			`table_name:"uks.u_t3" row_changes:{before:{lengths:2 lengths:1 values:"334"} after:{lengths:2 lengths:1 values:"333"}} keyspace:"uks" shard:"0" flags:3`,
			`table_name:"uks.u_t2" row_changes:{before:{lengths:2 lengths:1 values:"224"} after:{lengths:2 lengths:-1 values:"22"}} keyspace:"uks" shard:"0" flags:1`,
			`table_name:"uks.u_t1" row_changes:{before:{lengths:2 lengths:1 values:"114"} after:{lengths:2 lengths:1 values:"113"}} keyspace:"uks" shard:"0" flags:1`,
		},
	}, {
		query:  `update u_t1 set col1 = 5 where id = 11`,
		events: 2,
		rowEvents: []string{
			`table_name:"uks.u_t3" row_changes:{before:{lengths:2 lengths:1 values:"333"} after:{lengths:2 lengths:1 values:"335"}} keyspace:"uks" shard:"0" flags:3`,
			`table_name:"uks.u_t1" row_changes:{before:{lengths:2 lengths:1 values:"113"} after:{lengths:2 lengths:1 values:"115"}} keyspace:"uks" shard:"0" flags:1`,
		},
	}, {
		query:  `delete from u_t1 where col1 = 6`,
		events: 2,
		rowEvents: []string{
			`table_name:"uks.u_t3" row_changes:{before:{lengths:3 lengths:1 values:"3336"}} keyspace:"uks" shard:"0" flags:1`,
			`table_name:"uks.u_t1" row_changes:{before:{lengths:3 lengths:1 values:"1116"}} keyspace:"uks" shard:"0" flags:1`,
		},
	}, {
		query:  `update u_t1 set col1 = null where id = 11`,
		events: 2,
		rowEvents: []string{
			`table_name:"uks.u_t3" row_changes:{before:{lengths:2 lengths:1 values:"335"} after:{lengths:2 lengths:-1 values:"33"}} keyspace:"uks" shard:"0" flags:3`,
			`table_name:"uks.u_t1" row_changes:{before:{lengths:2 lengths:1 values:"115"} after:{lengths:2 lengths:-1 values:"11"}} keyspace:"uks" shard:"0" flags:1`,
		},
	}, {
		query:  `delete from u_t1 where id = 11`,
		events: 1,
		rowEvents: []string{
			`table_name:"uks.u_t1" row_changes:{before:{lengths:2 lengths:-1 values:"11"}} keyspace:"uks" shard:"0" flags:1`,
		},
	}}
	for _, tcase := range tcases {
		t.Run(tcase.query, func(t *testing.T) {
			utils.Exec(t, conn, tcase.query)
			// drain row events.
			rowEvents := drainEvents(t, ch, tcase.events)
			assert.ElementsMatch(t, tcase.rowEvents, rowEvents)
		})
	}
}

func runVStream(t *testing.T, ctx context.Context, ch chan *binlogdatapb.VEvent, vtgateConn *vtgateconn.VTGateConn) {
	vgtid := &binlogdatapb.VGtid{
		ShardGtids: []*binlogdatapb.ShardGtid{
			{Keyspace: unshardedKs, Shard: "0", Gtid: "current"},
		}}
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match: "/u.*",
		}},
	}
	vReader, err := vtgateConn.VStream(ctx, topodatapb.TabletType_REPLICA, vgtid, filter, nil)
	require.NoError(t, err)

	go func() {
		for {
			evs, err := vReader.Recv()
			if err == io.EOF || ctx.Err() != nil {
				return
			}
			require.NoError(t, err)

			for _, ev := range evs {
				if ev.Type == binlogdatapb.VEventType_ROW {
					ch <- ev
				}
			}
		}
	}()
}

func drainEvents(t *testing.T, ch chan *binlogdatapb.VEvent, count int) []string {
	var rowEvents []string
	for i := 0; i < count; i++ {
		select {
		case re := <-ch:
			rowEvents = append(rowEvents, re.RowEvent.String())
		case <-time.After(10 * time.Second):
			t.Fatalf("timeout waiting for event number: %d", i+1)
		}
	}
	return rowEvents
}

// TestFkScenarios tests the various foreign key scenarios with different constraints
// and makes sure that Vitess works with them as expected. All the tables are present in both sharded and unsharded keyspace
// and all the foreign key constraints are cross-shard ones for the sharded keyspace.
// The test has 4 independent Schemas that are used for testing -
/*
 *                    fk_t1
 *                        │
 *                        │ On Delete Restrict
 *                        │ On Update Restrict
 *                        ▼
 *   ┌────────────────fk_t2────────────────┐
 *   │                                     │
 *   │On Delete Set Null                   │ On Delete Set Null
 *   │On Update Set Null                   │ On Update Set Null
 *   ▼                                     ▼
 * fk_t7                                fk_t3───────────────────┐
 *                                         │                    │
 *                                         │                    │ On Delete Set Null
 *                      On Delete Set Null │                    │ On Update Set Null
 *                      On Update Set Null │                    │
 *                                         ▼                    ▼
 *                                      fk_t4                fk_t6
 *                                         │
 *                                         │
 *                      On Delete Restrict │
 *                      On Update Restrict │
 *                                         │
 *                                         ▼
 *                                      fk_t5
 */
/*
 *                fk_t10
 *                   │
 * On Delete Cascade │
 * On Update Cascade │
 *                   │
 *                   ▼
 *                fk_t11──────────────────┐
 *                   │                    │
 *                   │                    │ On Delete Restrict
 * On Delete Cascade │                    │ On Update Restrict
 * On Update Cascade │                    │
 *                   │                    │
 *                   ▼                    ▼
 *                fk_t12               fk_t13
 */
/*
 *                 fk_t15
 *                    │
 *                    │
 *  On Delete Cascade │
 *  On Update Cascade │
 *                    │
 *                    ▼
 *                 fk_t16
 *                    │
 * On Delete Set Null │
 * On Update Set Null │
 *                    │
 *                    ▼
 *                 fk_t17──────────────────┐
 *                    │                    │
 *                    │                    │ On Delete Set Null
 *  On Delete Cascade │                    │ On Update Set Null
 *  On Update Cascade │                    │
 *                    │                    │
 *                    ▼                    ▼
 *                 fk_t18               fk_t19
 */
/*
	Self referenced foreign key from col2 to col in fk_t20
*/
func TestFkScenarios(t *testing.T) {
	// Wait for schema-tracking to be complete.
	waitForSchemaTrackingForFkTables(t)

	testcases := []struct {
		name             string
		dataQueries      []string
		dmlQuery         string
		dmlShouldErr     bool
		skipShardScoped  bool
		assertionQueries []string
	}{
		{
			name: "Insert failure due to parent key not existing",
			dataQueries: []string{
				"insert into fk_t1(id, col) values (1, 5)",
			},
			dmlQuery:     "insert into t2(id, col) values (1, 7)",
			dmlShouldErr: true,
			assertionQueries: []string{
				"select * from fk_t1 order by id",
				"select * from fk_t2 order by id",
			},
		}, {
			name: "Insert success",
			dataQueries: []string{
				"insert into fk_t1(id, col) values (1, 7)",
			},
			dmlQuery: "insert into fk_t2(id, col) values (1, 7)",
			assertionQueries: []string{
				"select * from fk_t1 order by id",
				"select * from fk_t2 order by id",
			},
		}, {
			name: "Update failure with restrict foreign keys",
			dataQueries: []string{
				"insert into fk_t1(id, col) values (1, 7)",
				"insert into fk_t2(id, col) values (1, 7)",
			},
			dmlShouldErr: true,
			dmlQuery:     "update fk_t1 set col = 5 where id = 1",
			assertionQueries: []string{
				"select * from fk_t1 order by id",
				"select * from fk_t2 order by id",
			},
		}, {
			name: "Update success with restrict foreign keys",
			dataQueries: []string{
				"insert into fk_t1(id, col) values (1, 7), (2, 9)",
				"insert into fk_t2(id, col) values (1, 7)",
			},
			dmlQuery:        "update fk_t1 set col = 5 where id = 2",
			skipShardScoped: true,
			assertionQueries: []string{
				"select * from fk_t1 order by id",
				"select * from fk_t2 order by id",
			},
		}, {
			name: "Delete failure with restrict foreign keys",
			dataQueries: []string{
				"insert into fk_t1(id, col) values (1, 7)",
				"insert into fk_t2(id, col) values (1, 7)",
			},
			dmlQuery:     "delete from fk_t1 where id = 1",
			dmlShouldErr: true,
			assertionQueries: []string{
				"select * from fk_t1 order by id",
				"select * from fk_t2 order by id",
			},
		}, {
			name: "Delete success with restrict foreign keys",
			dataQueries: []string{
				"insert into fk_t1(id, col) values (1, 7), (2, 9)",
				"insert into fk_t2(id, col) values (1, 7)",
			},
			dmlQuery: "delete from fk_t1 where id = 2",
			assertionQueries: []string{
				"select * from fk_t1 order by id",
				"select * from fk_t2 order by id",
			},
		}, {
			name: "Update success with set null foreign key",
			dataQueries: []string{
				"insert into fk_t1(id, col) values (1, 7), (2, 9)",
				"insert into fk_t2(id, col) values (1, 7), (2, 9)",
				"insert into fk_t3(id, col) values (1, 7)",
				"insert into fk_t6(id, col) values (1, 7)",
			},
			dmlQuery:        "update fk_t3 set col = 9 where id = 1",
			skipShardScoped: true,
			assertionQueries: []string{
				"select * from fk_t1 order by id",
				"select * from fk_t2 order by id",
				"select * from fk_t3 order by id",
				"select * from fk_t6 order by id",
			},
		}, {
			name: "Update failure with set null foreign key with child having a restrict foreign key",
			dataQueries: []string{
				"insert into fk_t1(id, col) values (1, 7), (2, 9)",
				"insert into fk_t2(id, col) values (1, 7), (2, 9)",
				"insert into fk_t3(id, col) values (1, 7), (2, 9)",
				"insert into fk_t4(id, col) values (1, 7)",
				"insert into fk_t5(id, col) values (1, 7)",
			},
			dmlQuery:     "update fk_t3 set col = 9 where id = 1",
			dmlShouldErr: true,
			assertionQueries: []string{
				"select * from fk_t1 order by id",
				"select * from fk_t2 order by id",
				"select * from fk_t3 order by id",
				"select * from fk_t4 order by id",
				"select * from fk_t5 order by id",
			},
		}, {
			name: "Update success with cascaded set nulls",
			dataQueries: []string{
				"insert into fk_t1(id, col) values (1, 7), (2, 9)",
				"insert into fk_t2(id, col) values (1, 7), (2, 9)",
				"insert into fk_t3(id, col) values (1, 7), (2, 9)",
				"insert into fk_t4(id, col) values (1, 7), (2, 9)",
				"insert into fk_t6(id, col) values (1, 7), (2, 9)",
			},
			dmlQuery:        "update fk_t2 set col = 9 where id = 1",
			skipShardScoped: true,
			assertionQueries: []string{
				"select * from fk_t1 order by id",
				"select * from fk_t2 order by id",
				"select * from fk_t3 order by id",
				"select * from fk_t4 order by id",
				"select * from fk_t6 order by id",
			},
		}, {
			name: "Delete success with set null foreign key",
			dataQueries: []string{
				"insert into fk_t1(id, col) values (1, 7), (2, 9)",
				"insert into fk_t2(id, col) values (1, 7), (2, 9)",
				"insert into fk_t3(id, col) values (1, 7), (2, 9)",
				"insert into fk_t6(id, col) values (1, 7)",
			},
			dmlQuery:        "delete from fk_t3 where id = 1",
			skipShardScoped: true,
			assertionQueries: []string{
				"select * from fk_t1 order by id",
				"select * from fk_t2 order by id",
				"select * from fk_t3 order by id",
				"select * from fk_t6 order by id",
			},
		}, {
			name: "Delete failure with set null foreign key with child having a restrict foreign key",
			dataQueries: []string{
				"insert into fk_t1(id, col) values (1, 7), (2, 9)",
				"insert into fk_t2(id, col) values (1, 7), (2, 9)",
				"insert into fk_t3(id, col) values (1, 7), (2, 9)",
				"insert into fk_t4(id, col) values (1, 7)",
				"insert into fk_t5(id, col) values (1, 7)",
			},
			dmlQuery:     "delete from fk_t3 where id = 1",
			dmlShouldErr: true,
			assertionQueries: []string{
				"select * from fk_t1 order by id",
				"select * from fk_t2 order by id",
				"select * from fk_t3 order by id",
				"select * from fk_t4 order by id",
				"select * from fk_t5 order by id",
			},
		}, {
			name: "Delete success with cascaded set nulls",
			dataQueries: []string{
				"insert into fk_t1(id, col) values (1, 7), (2, 9)",
				"insert into fk_t2(id, col) values (1, 7), (2, 9)",
				"insert into fk_t3(id, col) values (1, 7), (2, 9)",
				"insert into fk_t4(id, col) values (1, 7), (2, 9)",
				"insert into fk_t6(id, col) values (1, 7), (2, 9)",
			},
			dmlQuery:        "delete from fk_t2 where id = 1",
			skipShardScoped: true,
			assertionQueries: []string{
				"select * from fk_t1 order by id",
				"select * from fk_t2 order by id",
				"select * from fk_t3 order by id",
				"select * from fk_t4 order by id",
				"select * from fk_t6 order by id",
			},
		}, {
			name: "Update success with cascade foreign key",
			dataQueries: []string{
				"insert into fk_t10(id, col) values (1, 7), (2, 9)",
				"insert into fk_t11(id, col) values (1, 7)",
			},
			dmlQuery:        "update fk_t10 set col = 5 where id = 1",
			skipShardScoped: true,
			assertionQueries: []string{
				"select * from fk_t10 order by id",
				"select * from fk_t11 order by id",
			},
		}, {
			name: "Update failure with cascade foreign key with child having a restrict foreign key",
			dataQueries: []string{
				"insert into fk_t10(id, col) values (1, 7), (2, 9)",
				"insert into fk_t11(id, col) values (1, 7)",
				"insert into fk_t13(id, col) values (1, 7)",
			},
			dmlQuery:     "update fk_t10 set col = 5 where id = 1",
			dmlShouldErr: true,
			assertionQueries: []string{
				"select * from fk_t10 order by id",
				"select * from fk_t11 order by id",
				"select * from fk_t13 order by id",
			},
		}, {
			name: "Update success with cascaded cascade foreign keys",
			dataQueries: []string{
				"insert into fk_t10(id, col) values (1, 7), (2, 9)",
				"insert into fk_t11(id, col) values (1, 7)",
				"insert into fk_t12(id, col) values (1, 7)",
			},
			dmlQuery:        "update fk_t10 set col = 5 where id = 1",
			skipShardScoped: true,
			assertionQueries: []string{
				"select * from fk_t10 order by id",
				"select * from fk_t11 order by id",
				"select * from fk_t12 order by id",
			},
		}, {
			name: "Delete success with cascade foreign key",
			dataQueries: []string{
				"insert into fk_t10(id, col) values (1, 7), (2, 9)",
				"insert into fk_t11(id, col) values (1, 7)",
			},
			dmlQuery: "delete from fk_t10 where id = 1",
			assertionQueries: []string{
				"select * from fk_t10 order by id",
				"select * from fk_t11 order by id",
			},
		}, {
			name: "Delete failure with cascade foreign key with child having a restrict foreign key",
			dataQueries: []string{
				"insert into fk_t10(id, col) values (1, 7), (2, 9)",
				"insert into fk_t11(id, col) values (1, 7)",
				"insert into fk_t13(id, col) values (1, 7)",
			},
			dmlQuery:     "delete from fk_t10 where id = 1",
			dmlShouldErr: true,
			assertionQueries: []string{
				"select * from fk_t10 order by id",
				"select * from fk_t11 order by id",
				"select * from fk_t13 order by id",
			},
		}, {
			name: "Delete success with cascaded cascade foreign keys",
			dataQueries: []string{
				"insert into fk_t10(id, col) values (1, 7), (2, 9)",
				"insert into fk_t11(id, col) values (1, 7)",
				"insert into fk_t12(id, col) values (1, 7)",
			},
			dmlQuery: "delete from fk_t10 where id = 1",
			assertionQueries: []string{
				"select * from fk_t10 order by id",
				"select * from fk_t11 order by id",
				"select * from fk_t12 order by id",
			},
		}, {
			name: "Delete success with set null to an update cascade foreign key",
			dataQueries: []string{
				"insert into fk_multicol_t15(id, cola, colb) values (1, 7, 1), (2, 9, 1)",
				"insert into fk_multicol_t16(id, cola, colb) values (1, 7, 1), (2, 9, 1)",
				"insert into fk_multicol_t17(id, cola, colb) values (1, 7, 1)",
				"insert into fk_multicol_t18(id, cola, colb) values (1, 7, 1)",
			},
			dmlQuery:        "delete from fk_multicol_t16 where id = 1",
			skipShardScoped: true,
			assertionQueries: []string{
				"select * from fk_multicol_t15 order by id",
				"select * from fk_multicol_t16 order by id",
				"select * from fk_multicol_t17 order by id",
				"select * from fk_multicol_t18 order by id",
			},
		}, {
			name: "Delete success with cascade to delete with set null to an update set null foreign key",
			dataQueries: []string{
				"insert into fk_multicol_t15(id, cola, colb) values (1, 7, 1), (2, 9, 1)",
				"insert into fk_multicol_t16(id, cola, colb) values (1, 7, 1), (2, 9, 1)",
				"insert into fk_multicol_t17(id, cola, colb) values (1, 7, 1)",
				"insert into fk_multicol_t18(id, cola, colb) values (1, 7, 1)",
			},
			dmlQuery:        "delete from fk_multicol_t15 where id = 1",
			skipShardScoped: true,
			assertionQueries: []string{
				"select * from fk_multicol_t15 order by id",
				"select * from fk_multicol_t16 order by id",
				"select * from fk_multicol_t17 order by id",
				"select * from fk_multicol_t18 order by id",
			},
		}, {
			name: "Update success with cascade to an update set null to an update cascade foreign key",
			dataQueries: []string{
				"insert into fk_multicol_t15(id, cola, colb) values (1, 7, 1), (2, 9, 1)",
				"insert into fk_multicol_t16(id, cola, colb) values (1, 7, 1), (2, 9, 1)",
				"insert into fk_multicol_t17(id, cola, colb) values (1, 7, 1)",
				"insert into fk_multicol_t18(id, cola, colb) values (1, 7, 1)",
			},
			dmlQuery:        "update fk_multicol_t15 set cola = 3 where id = 1",
			skipShardScoped: true,
			assertionQueries: []string{
				"select * from fk_multicol_t15 order by id",
				"select * from fk_multicol_t16 order by id",
				"select * from fk_multicol_t17 order by id",
				"select * from fk_multicol_t18 order by id",
			},
		}, {
			name: "Insert success for self-referenced foreign key",
			dataQueries: []string{
				"insert into fk_t20(id, col, col2) values (1, 7, NULL)",
			},
			skipShardScoped: true,
			dmlQuery:        "insert into fk_t20(id, col, col2) values (2, 9, 7), (3, 10, 9)",
			assertionQueries: []string{
				"select * from fk_t20 order by id",
			},
		}, {
			name: "Insert failure for self-referenced foreign key",
			dataQueries: []string{
				"insert into fk_t20(id, col, col2) values (5, 7, NULL)",
			},
			skipShardScoped: true,
			dmlQuery:        "insert into fk_t20(id, col, col2) values (6, 9, 6)",
			dmlShouldErr:    true,
			assertionQueries: []string{
				"select * from fk_t20 order by id",
			},
		}, {
			name: "Multi Table Delete success",
			dataQueries: []string{
				"insert into fk_multicol_t15(id, cola, colb) values (1, 7, 1), (2, 9, 1)",
				"insert into fk_multicol_t16(id, cola, colb) values (1, 7, 1), (2, 9, 1)",
				"insert into fk_multicol_t17(id, cola, colb) values (1, 7, 1)",
				"insert into fk_multicol_t19(id, cola, colb) values (1, 7, 1)",
			},
			skipShardScoped: true,
			dmlQuery:        "delete fk_multicol_t15 from fk_multicol_t15 join fk_multicol_t17 where fk_multicol_t15.id = fk_multicol_t17.id",
			assertionQueries: []string{
				"select * from fk_multicol_t15 order by id",
				"select * from fk_multicol_t16 order by id",
				"select * from fk_multicol_t17 order by id",
				"select * from fk_multicol_t19 order by id",
			},
		}, {
			name: "Multi Target Delete success",
			dataQueries: []string{
				"insert into fk_multicol_t15(id, cola, colb) values (1, 7, 1), (2, 9, 1), (3, 11, 1), (4, 13, 1)",
				"insert into fk_multicol_t16(id, cola, colb) values (1, 7, 1), (2, 9, 1), (3, 11, 1), (4, 13, 1)",
				"insert into fk_multicol_t17(id, cola, colb) values (1, 7, 1), (2, 9, 1), (3, 11, 1)",
				"insert into fk_multicol_t18(id, cola, colb) values (1, 7, 1), (3, 11, 1)",
				"insert into fk_multicol_t19(id, cola, colb) values (1, 7, 1), (2, 9, 1)",
			},
			skipShardScoped: true,
			dmlQuery:        "delete fk_multicol_t15, fk_multicol_t17 from fk_multicol_t15 join fk_multicol_t17 where fk_multicol_t15.id = fk_multicol_t17.id",
			assertionQueries: []string{
				"select * from fk_multicol_t15 order by id",
				"select * from fk_multicol_t16 order by id",
				"select * from fk_multicol_t17 order by id",
				"select * from fk_multicol_t19 order by id",
			},
		}, {
			name: "Delete with limit success",
			dataQueries: []string{
				"insert into fk_multicol_t15(id, cola, colb) values (1, 7, 1), (2, 9, 1)",
				"insert into fk_multicol_t16(id, cola, colb) values (1, 7, 1), (2, 9, 1)",
				"insert into fk_multicol_t17(id, cola, colb) values (1, 7, 1)",
				"insert into fk_multicol_t19(id, cola, colb) values (1, 7, 1)",
			},
			skipShardScoped: true,
			dmlQuery:        "delete from fk_multicol_t15 order by id limit 1",
			assertionQueries: []string{
				"select * from fk_multicol_t15 order by id",
				"select * from fk_multicol_t16 order by id",
				"select * from fk_multicol_t17 order by id",
				"select * from fk_multicol_t19 order by id",
			},
		}, {
			name:            "Delete with limit 0 success",
			skipShardScoped: true,
			dataQueries: []string{
				"insert into fk_multicol_t15(id, cola, colb) values (1, 7, 1), (2, 9, 1)",
				"insert into fk_multicol_t16(id, cola, colb) values (1, 7, 1), (2, 9, 1)",
				"insert into fk_multicol_t17(id, cola, colb) values (1, 7, 1)",
				"insert into fk_multicol_t19(id, cola, colb) values (1, 7, 1)",
			},
			dmlQuery: "delete from fk_multicol_t15 order by id limit 0",
			assertionQueries: []string{
				"select * from fk_multicol_t15 order by id",
				"select * from fk_multicol_t16 order by id",
				"select * from fk_multicol_t17 order by id",
				"select * from fk_multicol_t19 order by id",
			},
		}, {
			name:            "Update with limit success",
			skipShardScoped: true,
			dataQueries: []string{
				"insert into fk_multicol_t15(id, cola, colb) values (1, 7, 1), (2, 9, 1)",
				"insert into fk_multicol_t16(id, cola, colb) values (1, 7, 1), (2, 9, 1)",
				"insert into fk_multicol_t17(id, cola, colb) values (1, 7, 1)",
				"insert into fk_multicol_t19(id, cola, colb) values (1, 7, 1)",
			},
			dmlQuery: "update fk_multicol_t15 set cola = '2' order by id limit 1",
			assertionQueries: []string{
				"select * from fk_multicol_t15 order by id",
				"select * from fk_multicol_t16 order by id",
				"select * from fk_multicol_t17 order by id",
				"select * from fk_multicol_t19 order by id",
			},
		}, {
			name:            "Update with limit 0 success",
			skipShardScoped: true,
			dataQueries: []string{
				"insert into fk_multicol_t15(id, cola, colb) values (1, 7, 1), (2, 9, 1)",
				"insert into fk_multicol_t16(id, cola, colb) values (1, 7, 1), (2, 9, 1)",
				"insert into fk_multicol_t17(id, cola, colb) values (1, 7, 1)",
				"insert into fk_multicol_t19(id, cola, colb) values (1, 7, 1)",
			},
			dmlQuery: "update fk_multicol_t15 set cola = '8' order by id limit 0",
			assertionQueries: []string{
				"select * from fk_multicol_t15 order by id",
				"select * from fk_multicol_t16 order by id",
				"select * from fk_multicol_t17 order by id",
				"select * from fk_multicol_t19 order by id",
			},
		}, {
			name:            "Update with non-literal update and limit success",
			skipShardScoped: true,
			dataQueries: []string{
				"insert into fk_multicol_t15(id, cola, colb) values (1, 7, 1), (2, 9, 1)",
				"insert into fk_multicol_t16(id, cola, colb) values (1, 7, 1), (2, 9, 1)",
				"insert into fk_multicol_t17(id, cola, colb) values (1, 7, 1)",
				"insert into fk_multicol_t19(id, cola, colb) values (1, 7, 1)",
			},
			dmlQuery: "update fk_multicol_t15 set cola = id + 3 order by id limit 1",
			assertionQueries: []string{
				"select * from fk_multicol_t15 order by id",
				"select * from fk_multicol_t16 order by id",
				"select * from fk_multicol_t17 order by id",
				"select * from fk_multicol_t19 order by id",
			},
		}, {
			name:            "Update with non-literal update order by and limit - multiple update",
			skipShardScoped: true,
			dataQueries: []string{
				"insert into fk_multicol_t15(id, cola, colb) values (1, 7, 1), (2, 9, 1), (3, 12, 1)",
				"insert into fk_multicol_t16(id, cola, colb) values (1, 7, 1), (2, 9, 1), (3, 12, 1)",
				"insert into fk_multicol_t17(id, cola, colb) values (1, 7, 1)",
				"insert into fk_multicol_t19(id, cola, colb) values (1, 7, 1)",
			},
			dmlQuery: "update fk_multicol_t15 set cola = id + 8 order by id asc limit 2",
			assertionQueries: []string{
				"select * from fk_multicol_t15 order by id",
				"select * from fk_multicol_t16 order by id",
				"select * from fk_multicol_t17 order by id",
				"select * from fk_multicol_t19 order by id",
			},
		}, {
			name:            "Update with non-literal update order by and limit - single update",
			skipShardScoped: true,
			dataQueries: []string{
				"insert into fk_multicol_t15(id, cola, colb) values (1, 7, 1), (2, 9, 1), (3, 12, 1)",
				"insert into fk_multicol_t16(id, cola, colb) values (1, 7, 1), (2, 9, 1), (3, 12, 1)",
				"insert into fk_multicol_t17(id, cola, colb) values (1, 7, 1)",
				"insert into fk_multicol_t19(id, cola, colb) values (1, 7, 1)",
			},
			dmlQuery: "update fk_multicol_t15 set cola = id + 8 where id < 3 order by id desc limit 2",
			assertionQueries: []string{
				"select * from fk_multicol_t15 order by id",
				"select * from fk_multicol_t16 order by id",
				"select * from fk_multicol_t17 order by id",
				"select * from fk_multicol_t19 order by id",
			},
		}, {
			name:            "Multi Table Update with non-literal update",
			skipShardScoped: true,
			dataQueries: []string{
				"insert into fk_multicol_t15(id, cola, colb) values (1, 7, 1), (2, 9, 1), (3, 12, 1)",
				"insert into fk_multicol_t16(id, cola, colb) values (1, 7, 1), (2, 9, 1), (3, 12, 1)",
				"insert into fk_multicol_t17(id, cola, colb) values (1, 7, 1)",
				"insert into fk_multicol_t19(id, cola, colb) values (1, 7, 1)",
			},
			dmlQuery: "update fk_multicol_t15 m1 join fk_multicol_t17 on m1.id = fk_multicol_t17.id set m1.cola = m1.id + 8 where m1.id < 3",
			assertionQueries: []string{
				"select * from fk_multicol_t15 order by id",
				"select * from fk_multicol_t16 order by id",
				"select * from fk_multicol_t17 order by id",
				"select * from fk_multicol_t19 order by id",
			},
		}, {
			name:            "Multi Target Update with non-literal update",
			skipShardScoped: true,
			dataQueries: []string{
				"insert into fk_multicol_t15(id, cola, colb) values (1, 7, 1), (2, 9, 1), (3, 12, 1)",
				"insert into fk_multicol_t16(id, cola, colb) values (1, 7, 1), (2, 9, 1), (3, 12, 1)",
				"insert into fk_multicol_t17(id, cola, colb) values (1, 7, 1), (2, 9, 1)",
				"insert into fk_multicol_t19(id, cola, colb) values (1, 7, 1)",
			},
			dmlQuery: "update fk_multicol_t15 m1 join fk_multicol_t17 on m1.id = fk_multicol_t17.id set m1.cola = m1.id + 8, fk_multicol_t17.colb = 32 where m1.id < 3",
			assertionQueries: []string{
				"select * from fk_multicol_t15 order by id",
				"select * from fk_multicol_t16 order by id",
				"select * from fk_multicol_t17 order by id",
				"select * from fk_multicol_t19 order by id",
			},
		},
	}

	for _, tt := range testcases {
		for _, keyspace := range []string{unshardedKs, shardedKs, shardScopedKs} {
			t.Run(getTestName(tt.name, keyspace), func(t *testing.T) {
				mcmp, closer := start(t)
				defer closer()
				if keyspace == shardedKs {
					t.Skip("Skip test since we don't have sharded foreign key support yet")
				}
				if keyspace == shardScopedKs && tt.skipShardScoped {
					t.Skip("Skip test since we don't support updates in primary vindex columns")
				}
				// Set the correct keyspace to use from VtGates.
				_ = utils.Exec(t, mcmp.VtConn, fmt.Sprintf("use `%v`", keyspace))

				// Insert all the data required for running the test.
				for _, query := range tt.dataQueries {
					mcmp.Exec(query)
				}

				// Run the DML query that needs to be tested and verify output with MySQL.
				_, err := mcmp.ExecAllowAndCompareError(tt.dmlQuery)
				if tt.dmlShouldErr {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}

				// Run the assertion queries and verify we get the expected outputs.
				for _, query := range tt.assertionQueries {
					mcmp.Exec(query)
				}
			})
		}
	}

	for _, keyspace := range []string{unshardedKs, shardedKs} {
		t.Run(getTestName("Transactions with intermediate failure", keyspace), func(t *testing.T) {
			mcmp, closer := start(t)
			defer closer()
			if keyspace == shardedKs {
				t.Skip("Skip test since we don't have sharded foreign key support yet")
			}
			// Set the correct keyspace to use from VtGates.
			_ = utils.Exec(t, mcmp.VtConn, fmt.Sprintf("use `%v`", keyspace))

			// Insert some rows
			mcmp.Exec("INSERT INTO fk_t10(id, col) VALUES (1, 7), (2, 9), (3, 5)")
			mcmp.Exec("INSERT INTO fk_t11(id, col) VALUES (1, 7), (2, 9), (3, 5)")
			mcmp.Exec("INSERT INTO fk_t12(id, col) VALUES (1, 7), (2, 9), (3, 5)")

			// Start a transaction
			mcmp.Exec("BEGIN")

			// Insert another row.
			mcmp.Exec("INSERT INTO fk_t13(id, col) VALUES (1, 7)")

			// Delete success for cascaded (2, 9)
			mcmp.Exec("DELETE FROM fk_t10 WHERE id = 2")

			// Verify the results
			mcmp.Exec("SELECT * FROM fk_t10 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t11 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t12 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t13 ORDER BY id")

			// Update that fails
			_, err := mcmp.ExecAllowAndCompareError("UPDATE fk_t10 SET col = 15 WHERE id = 1")
			require.Error(t, err)

			// Verify the results
			// Since we are in a transaction, we still expect the transaction to be ongoing, with no change to the tables
			// since the update should fail.
			mcmp.Exec("SELECT * FROM fk_t10 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t11 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t12 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t13 ORDER BY id")

			// Update that is a success
			mcmp.Exec("UPDATE fk_t10 SET col = 15 where id = 3")

			// Verify the results
			mcmp.Exec("SELECT * FROM fk_t10 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t11 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t12 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t13 ORDER BY id")

			// Insert a new row
			mcmp.Exec("INSERT INTO fk_t13(id, col) VALUES (3, 15)")

			// Verify the results
			mcmp.Exec("SELECT * FROM fk_t10 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t11 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t12 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t13 ORDER BY id")

			// Rollback the transaction.
			mcmp.Exec("ROLLBACK")

			// Verify the results
			mcmp.Exec("SELECT * FROM fk_t10 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t11 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t12 ORDER BY id")
			mcmp.Exec("SELECT * FROM fk_t13 ORDER BY id")
		})
	}
}

// TestFkQueries is for testing a specific set of queries one after the other.
func TestFkQueries(t *testing.T) {
	// Wait for schema-tracking to be complete.
	waitForSchemaTrackingForFkTables(t)
	// Remove all the foreign key constraints for all the replicas.
	// We can then verify that the replica, and the primary have the same data, to ensure
	// that none of the queries ever lead to cascades/updates on MySQL level.
	for _, ks := range []string{shardedKs, unshardedKs} {
		replicas := getReplicaTablets(ks)
		for _, replica := range replicas {
			removeAllForeignKeyConstraints(t, replica, ks)
		}
	}

	testcases := []struct {
		name    string
		queries []string
	}{
		{
			name: "Non-literal update",
			queries: []string{
				"insert into fk_t10 (id, col) values (1,1),(2,2),(3,3),(4,4),(5,5)",
				"insert into fk_t11 (id, col) values (1,1),(2,2),(3,3),(4,4),(5,5)",
				"update fk_t10 set col = id + 3",
			},
		}, {
			name: "Non-literal update with order by",
			queries: []string{
				"insert into fk_t10 (id, col) values (1,1),(2,2),(3,3),(4,4),(5,5)",
				"insert into fk_t11 (id, col) values (1,1),(2,2),(3,3),(4,4),(5,5)",
				"update fk_t10 set col = id + 3 order by id desc",
			},
		}, {
			name: "Non-literal update with order by that require parent and child foreign keys verification - success",
			queries: []string{
				"insert into fk_t10 (id, col) values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8)",
				"insert into fk_t11 (id, col) values (1,1),(2,2),(3,3),(4,4),(5,5)",
				"insert into fk_t12 (id, col) values (1,1),(2,2),(3,3),(4,4),(5,5)",
				"insert into fk_t13 (id, col) values (1,1),(2,2)",
				"update fk_t11 set col = id + 3 where id >= 3",
			},
		}, {
			name: "Non-literal update with order by that require parent and child foreign keys verification - parent fails",
			queries: []string{
				"insert into fk_t10 (id, col) values (1,1),(2,2),(3,3),(4,4),(5,5)",
				"insert into fk_t11 (id, col) values (1,1),(2,2),(3,3),(4,4),(5,5)",
				"insert into fk_t12 (id, col) values (1,1),(2,2),(3,3),(4,4),(5,5)",
				"update fk_t11 set col = id + 3",
			},
		}, {
			name: "Non-literal update with order by that require parent and child foreign keys verification - child fails",
			queries: []string{
				"insert into fk_t10 (id, col) values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8)",
				"insert into fk_t11 (id, col) values (1,1),(2,2),(3,3),(4,4),(5,5)",
				"insert into fk_t12 (id, col) values (1,1),(2,2),(3,3),(4,4),(5,5)",
				"insert into fk_t13 (id, col) values (1,1),(2,2)",
				"update fk_t11 set col = id + 3",
			},
		}, {
			name: "Single column update in a multi-col table - success",
			queries: []string{
				"insert into fk_multicol_t1 (id, cola, colb) values (1, 1, 1), (2, 2, 2)",
				"insert into fk_multicol_t2 (id, cola, colb) values (1, 1, 1)",
				"update fk_multicol_t1 set colb = 4 + (colb) where id = 2",
			},
		}, {
			name: "Single column update in a multi-col table - restrict failure",
			queries: []string{
				"insert into fk_multicol_t1 (id, cola, colb) values (1, 1, 1), (2, 2, 2)",
				"insert into fk_multicol_t2 (id, cola, colb) values (1, 1, 1)",
				"update fk_multicol_t1 set colb = 4 + (colb) where id = 1",
			},
		}, {
			name: "Single column update in multi-col table - cascade and set null",
			queries: []string{
				"insert into fk_multicol_t15 (id, cola, colb) values (1, 1, 1), (2, 2, 2)",
				"insert into fk_multicol_t16 (id, cola, colb) values (1, 1, 1), (2, 2, 2)",
				"insert into fk_multicol_t17 (id, cola, colb) values (1, 1, 1), (2, 2, 2)",
				"update fk_multicol_t15 set colb = 4 + (colb) where id = 1",
			},
		}, {
			name: "Non literal update that evaluates to NULL - restricted",
			queries: []string{
				"insert into fk_t10 (id, col) values (1,1),(2,2),(3,3),(4,4),(5,5)",
				"insert into fk_t11 (id, col) values (1,1),(2,2),(3,3),(4,4),(5,5)",
				"insert into fk_t13 (id, col) values (1,1),(2,2),(3,3),(4,4),(5,5)",
				"update fk_t10 set col = id + null where id = 1",
			},
		}, {
			name: "Non literal update that evaluates to NULL - success",
			queries: []string{
				"insert into fk_t10 (id, col) values (1,1),(2,2),(3,3),(4,4),(5,5)",
				"insert into fk_t11 (id, col) values (1,1),(2,2),(3,3),(4,4),(5,5)",
				"insert into fk_t12 (id, col) values (1,1),(2,2),(3,3),(4,4),(5,5)",
				"update fk_t10 set col = id + null where id = 1",
			},
		}, {
			name: "Multi column foreign key update with one literal and one non-literal update",
			queries: []string{
				"insert into fk_multicol_t15 (id, cola, colb) values (1,1,1),(2,2,2)",
				"insert into fk_multicol_t16 (id, cola, colb) values (1,1,1),(2,2,2)",
				"update fk_multicol_t15 set cola = 3, colb = (id * 2) - 2",
			},
		}, {
			name: "Update that sets to 0 and -0 values",
			queries: []string{
				"insert into fk_t15 (id, col) values (1,'-0'), (2, '0'), (3, '5'), (4, '-5')",
				"insert into fk_t16 (id, col) values (1,'-0'), (2, '0'), (3, '5'), (4, '-5')",
				"update fk_t15 set col = col * (col - (col))",
			},
		},
		{
			name: "Update a child table which doesn't cause an update, but parent doesn't have that value",
			queries: []string{
				"insert into fk_t10 (id, col) values (1,1),(2,2)",
				"insert /*+ SET_VAR(foreign_key_checks=0) */ into fk_t11 (id, col) values (1,1),(2,2),(5,5)",
				"update fk_t11 set col = id where id in (1, 5)",
			},
		},
		{
			name: "Update a child table from a null to a value that parent doesn't have",
			queries: []string{
				"insert into fk_t10 (id, col) values (1,1),(2,2)",
				"insert into fk_t11 (id, col) values (1,1),(2,2),(5,NULL)",
				"update fk_t11 set col = id where id in (1, 5)",
			},
		},
		{
			name: "Update on child to 0 when parent has -0",
			queries: []string{
				"insert into fk_t15 (id, col) values (2, '-0')",
				"insert /*+ SET_VAR(foreign_key_checks=0) */ into fk_t16 (id, col) values (3, '5'), (4, '-5')",
				"update fk_t16 set col = col * (col - (col)) where id = 3",
				"update fk_t16 set col = col * (col - (col)) where id = 4",
			},
		},
		{
			name: "Multi table delete that uses two tables related by foreign keys",
			queries: []string{
				"insert /*+ SET_VAR(foreign_key_checks=0) */ into fk_t10 (id, col) values (1, '5'), (2, NULL), (3, NULL), (4, '4'), (6, '1'), (7, '2')",
				"insert /*+ SET_VAR(foreign_key_checks=0) */ into fk_t11 (id, col) values (4, '1'), (5, '3'), (7, '22'), (8, '5'), (9, NULL), (10, '3')",
				"insert /*+ SET_VAR(foreign_key_checks=0) */ into fk_t12 (id, col) values (2, NULL), (3, NULL), (4, '1'), (6, '6'), (8, NULL), (10, '1')",
				"insert /*+ SET_VAR(foreign_key_checks=0) */ into fk_t13 (id, col) values (2, '1'), (5, '5'), (7, '5')",
				"delete fk_t11 from fk_t11 join fk_t12 using (id) where fk_t11.id = 4",
			},
		},
	}

	for _, tt := range testcases {
		for _, keyspace := range []string{unshardedKs, shardedKs} {
			t.Run(getTestName(tt.name, keyspace), func(t *testing.T) {
				mcmp, closer := start(t)
				defer closer()
				if keyspace == shardedKs {
					t.Skip("Skip test since we don't have sharded foreign key support yet")
				}
				// Set the correct keyspace to use from VtGates.
				_ = utils.Exec(t, mcmp.VtConn, fmt.Sprintf("use `%v`", keyspace))

				// Ensure that the Vitess database is originally empty
				ensureDatabaseState(t, mcmp.VtConn, true)
				ensureDatabaseState(t, mcmp.MySQLConn, true)

				for _, query := range tt.queries {
					_, _ = mcmp.ExecAllowAndCompareError(query)
					if t.Failed() {
						break
					}
				}

				// ensure Vitess database has some data. This ensures not all the commands failed.
				ensureDatabaseState(t, mcmp.VtConn, false)
				// Verify the consistency of the data.
				verifyDataIsCorrect(t, mcmp, 1)
			})
		}
	}
}

// TestShowVschemaKeyspaces verifies the show vschema keyspaces query output for the keyspaces where the foreign keys are
func TestShowVschemaKeyspaces(t *testing.T) {
	mcmp, closer := start(t)
	conn := mcmp.VtConn
	defer closer()

	res := utils.Exec(t, conn, "SHOW VSCHEMA KEYSPACES")
	resStr := fmt.Sprintf("%v", res.Rows)
	require.Contains(t, resStr, `[VARCHAR("uks") VARCHAR("false") VARCHAR("managed") VARCHAR("")]`)
	require.Contains(t, resStr, `[VARCHAR("ks") VARCHAR("true") VARCHAR("managed") VARCHAR("")]`)
}

// TestFkOneCase is for testing a specific set of queries. On the CI this test won't run since we'll keep the queries empty.
func TestFkOneCase(t *testing.T) {
	queries := []string{}
	if len(queries) == 0 {
		t.Skip("No queries to test")
	}
	// Wait for schema-tracking to be complete.
	waitForSchemaTrackingForFkTables(t)
	// Remove all the foreign key constraints for all the replicas.
	// We can then verify that the replica, and the primary have the same data, to ensure
	// that none of the queries ever lead to cascades/updates on MySQL level.
	for _, ks := range []string{shardedKs, unshardedKs} {
		replicas := getReplicaTablets(ks)
		for _, replica := range replicas {
			removeAllForeignKeyConstraints(t, replica, ks)
		}
	}

	mcmp, closer := start(t)
	defer closer()
	_ = utils.Exec(t, mcmp.VtConn, "use `uks`")

	// Ensure that the Vitess database is originally empty
	ensureDatabaseState(t, mcmp.VtConn, true)
	ensureDatabaseState(t, mcmp.MySQLConn, true)

	for _, query := range queries {
		if strings.HasPrefix(query, "vexplain") {
			res := utils.Exec(t, mcmp.VtConn, query)
			log.Errorf("Query %v, Result - %v", query, res.Rows)
			continue
		}
		_, _ = mcmp.ExecAllowAndCompareError(query)
		if t.Failed() {
			log.Errorf("Query failed - %v", query)
			break
		}
	}
	vitessData := collectFkTablesState(mcmp.VtConn)
	for idx, table := range fkTables {
		log.Errorf("Vitess data for %v -\n%v", table, vitessData[idx].Rows)
	}

	// ensure Vitess database has some data. This ensures not all the commands failed.
	ensureDatabaseState(t, mcmp.VtConn, false)
	// Verify the consistency of the data.
	verifyDataIsCorrect(t, mcmp, 1)
}

func TestCyclicFks(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	_ = utils.Exec(t, mcmp.VtConn, "use `uks`")

	// Create a cyclic foreign key constraint.
	utils.Exec(t, mcmp.VtConn, "alter table fk_t10 add constraint test_cyclic_fks foreign key (col) references fk_t12 (col) on delete cascade on update cascade")

	// Wait for schema-tracking to be complete.
	errString := utils.WaitForKsError(t, clusterInstance.VtgateProcess, unshardedKs)
	assert.Contains(t, errString, "VT09019: keyspace 'uks' has cyclic foreign keys")

	// Ensure that the Vitess database is originally empty
	ensureDatabaseState(t, mcmp.VtConn, true)

	_, err := utils.ExecAllowError(t, mcmp.VtConn, "insert into fk_t10(id, col) values (1, 1)")
	require.ErrorContains(t, err, "VT09019: keyspace 'uks' has cyclic foreign keys")

	// Drop the cyclic foreign key constraint.
	utils.Exec(t, mcmp.VtConn, "alter table fk_t10 drop foreign key test_cyclic_fks")

	// Let's clean out the cycle so that the other tests don't fail
	utils.WaitForVschemaCondition(t, clusterInstance.VtgateProcess, unshardedKs, func(t *testing.T, keyspace map[string]any) bool {
		_, fieldPresent := keyspace["error"]
		return !fieldPresent
	}, "wait for error to disappear")
}

func TestReplace(t *testing.T) {
	t.Skip("replace engine marked for failure, hence skipping this.")
	// Wait for schema-tracking to be complete.
	waitForSchemaTrackingForFkTables(t)
	// Remove all the foreign key constraints for all the replicas.
	// We can then verify that the replica, and the primary have the same data, to ensure
	// that none of the queries ever lead to cascades/updates on MySQL level.
	for _, ks := range []string{shardedKs, unshardedKs} {
		replicas := getReplicaTablets(ks)
		for _, replica := range replicas {
			removeAllForeignKeyConstraints(t, replica, ks)
		}
	}

	mcmp1, _ := start(t)
	//	defer closer1()
	_ = utils.Exec(t, mcmp1.VtConn, "use `uks`")

	mcmp2, _ := start(t)
	//	defer closer2()
	_ = utils.Exec(t, mcmp2.VtConn, "use `uks`")

	_ = utils.Exec(t, mcmp1.VtConn, "insert into fk_t2 values(1,5), (2,5)")

	done := false
	go func() {
		number := 1
		for !done {
			query := fmt.Sprintf("replace /* g1q1 - %d */ into fk_t2 values(5,5)", number)
			_, _ = utils.ExecAllowError(t, mcmp1.VtConn, query)
			number++
		}
	}()

	go func() {
		number := 1
		for !done {
			query := fmt.Sprintf("replace /* q1 - %d */ into fk_t3 values(3,5)", number)
			_, _ = utils.ExecAllowError(t, mcmp2.VtConn, query)

			query = fmt.Sprintf("replace /* q2 - %d */ into fk_t3 values(4,5)", number)
			_, _ = utils.ExecAllowError(t, mcmp2.VtConn, query)
			number++
		}
	}()

	totalTime := time.After(1 * time.Minute)
	for !done {
		select {
		case <-totalTime:
			done = true
		case <-time.After(10 * time.Millisecond):
			validateReplication(t)
		}
	}
}

func TestReplaceExplicit(t *testing.T) {
	t.Skip("explicit delete-insert in transaction fails, hence skipping")
	// Wait for schema-tracking to be complete.
	waitForSchemaTrackingForFkTables(t)
	// Remove all the foreign key constraints for all the replicas.
	// We can then verify that the replica, and the primary have the same data, to ensure
	// that none of the queries ever lead to cascades/updates on MySQL level.
	for _, ks := range []string{shardedKs, unshardedKs} {
		replicas := getReplicaTablets(ks)
		for _, replica := range replicas {
			removeAllForeignKeyConstraints(t, replica, ks)
		}
	}

	mcmp1, _ := start(t)
	//	defer closer1()
	_ = utils.Exec(t, mcmp1.VtConn, "use `uks`")

	mcmp2, _ := start(t)
	//	defer closer2()
	_ = utils.Exec(t, mcmp2.VtConn, "use `uks`")

	_ = utils.Exec(t, mcmp1.VtConn, "insert into fk_t2 values(1,5), (2,5)")

	done := false
	go func() {
		number := 0
		for !done {
			number++
			_, _ = utils.ExecAllowError(t, mcmp1.VtConn, "begin")
			query := fmt.Sprintf("delete /* g1q1 - %d */ from fk_t2 where id = 5", number)
			_, err := utils.ExecAllowError(t, mcmp1.VtConn, query)
			if err != nil {
				_, _ = utils.ExecAllowError(t, mcmp1.VtConn, "rollback")
				continue
			}
			query = fmt.Sprintf("insert /* g1q1 - %d */ into fk_t2 values(5,5)", number)
			_, err = utils.ExecAllowError(t, mcmp1.VtConn, query)
			if err != nil {
				_, _ = utils.ExecAllowError(t, mcmp1.VtConn, "rollback")
				continue
			}
			_, _ = utils.ExecAllowError(t, mcmp1.VtConn, "commit")
		}
	}()

	go func() {
		number := 0
		for !done {
			number++
			_, _ = utils.ExecAllowError(t, mcmp2.VtConn, "begin")
			query := fmt.Sprintf("delete /* g1q1 - %d */ from fk_t3 where id = 3 or col = 5", number)
			_, err := utils.ExecAllowError(t, mcmp2.VtConn, query)
			if err != nil {
				_, _ = utils.ExecAllowError(t, mcmp2.VtConn, "rollback")
			} else {
				query = fmt.Sprintf("insert /* g1q1 - %d */ into fk_t3 values(3,5)", number)
				_, err = utils.ExecAllowError(t, mcmp2.VtConn, query)
				if err != nil {
					_, _ = utils.ExecAllowError(t, mcmp2.VtConn, "rollback")
				} else {
					_, _ = utils.ExecAllowError(t, mcmp2.VtConn, "commit")
				}
			}

			_, _ = utils.ExecAllowError(t, mcmp2.VtConn, "begin")
			query = fmt.Sprintf("delete /* g1q1 - %d */ from fk_t3 where id = 4 or col = 5", number)
			_, err = utils.ExecAllowError(t, mcmp2.VtConn, query)
			if err != nil {
				_, _ = utils.ExecAllowError(t, mcmp2.VtConn, "rollback")
				continue
			}
			query = fmt.Sprintf("insert /* g1q1 - %d */ into fk_t3 values(4,5)", number)
			_, err = utils.ExecAllowError(t, mcmp2.VtConn, query)
			if err != nil {
				_, _ = utils.ExecAllowError(t, mcmp2.VtConn, "rollback")
				continue
			}
			_, _ = utils.ExecAllowError(t, mcmp2.VtConn, "commit")
		}
	}()

	totalTime := time.After(1 * time.Minute)
	for !done {
		select {
		case <-totalTime:
			done = true
		case <-time.After(10 * time.Millisecond):
			validateReplication(t)
		}
	}
}

// TestReplaceWithFK tests that replace into work as expected when foreign key management is enabled in Vitess.
func TestReplaceWithFK(t *testing.T) {
	mcmp, closer := start(t)
	conn := mcmp.VtConn
	defer closer()

	// replace some data.
	_, err := utils.ExecAllowError(t, conn, `replace into t1(id, col) values (1, 1)`)
	require.ErrorContains(t, err, "VT12001: unsupported: REPLACE INTO with sharded keyspace (errno 1235) (sqlstate 42000)")

	_ = utils.Exec(t, conn, `use uks`)

	_ = utils.Exec(t, conn, `replace into u_t1(id, col1) values (1, 1), (2, 1)`)
	// u_t1: (1,1) (2,1)

	_ = utils.Exec(t, conn, `replace into u_t2(id, col2) values (1, 1), (2, 1)`)
	// u_t1: (1,1) (2,1)
	// u_t2: (1,1) (2,1)

	_ = utils.Exec(t, conn, `replace into u_t1(id, col1) values (2, 2)`)
	// u_t1: (1,1) (2,2)
	// u_t2: (1,null) (2,null)

	utils.AssertMatches(t, conn, `select * from u_t1`, `[[INT64(1) INT64(1)] [INT64(2) INT64(2)]]`)
	utils.AssertMatches(t, conn, `select * from u_t2`, `[[INT64(1) NULL] [INT64(2) NULL]]`)
}

// TestInsertWithFKOnDup tests that insertion with on duplicate key update works as expected.
func TestInsertWithFKOnDup(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	utils.Exec(t, mcmp.VtConn, "use `uks`")

	// insert some data.
	mcmp.Exec(`insert into u_t1(id, col1) values (100, 1), (200, 2), (300, 3), (400, 4)`)
	mcmp.Exec(`insert into u_t2(id, col2) values (1000, 1), (2000, 2), (3000, 3), (4000, 4)`)

	// updating child to an existing value in parent.
	mcmp.Exec(`insert into u_t2(id, col2) values (4000, 50) on duplicate key update col2 = 1`)
	mcmp.AssertMatches(`select * from u_t2 order by id`, `[[INT64(1000) INT64(1)] [INT64(2000) INT64(2)] [INT64(3000) INT64(3)] [INT64(4000) INT64(1)]]`)

	// updating parent, value not referred in child.
	mcmp.Exec(`insert into u_t1(id, col1) values (400, 50) on duplicate key update col1 = values(col1)`)
	mcmp.AssertMatches(`select * from u_t1 order by id`, `[[INT64(100) INT64(1)] [INT64(200) INT64(2)] [INT64(300) INT64(3)] [INT64(400) INT64(50)]]`)
	mcmp.AssertMatches(`select * from u_t2 order by id`, `[[INT64(1000) INT64(1)] [INT64(2000) INT64(2)] [INT64(3000) INT64(3)] [INT64(4000) INT64(1)]]`)

	// updating parent, child updated to null.
	mcmp.Exec(`insert into u_t1(id, col1) values (100, 75) on duplicate key update col1 = values(col1)`)
	mcmp.AssertMatches(`select * from u_t1 order by id`, `[[INT64(100) INT64(75)] [INT64(200) INT64(2)] [INT64(300) INT64(3)] [INT64(400) INT64(50)]]`)
	mcmp.AssertMatches(`select * from u_t2 order by id`, `[[INT64(1000) NULL] [INT64(2000) INT64(2)] [INT64(3000) INT64(3)] [INT64(4000) NULL]]`)

	// inserting multiple rows in parent, some child rows updated to null.
	mcmp.Exec(`insert into u_t1(id, col1) values (100, 42),(600, 2),(300, 24),(200, 2) on duplicate key update col1 = values(col1)`)
	mcmp.AssertMatches(`select * from u_t1 order by id`, `[[INT64(100) INT64(42)] [INT64(200) INT64(2)] [INT64(300) INT64(24)] [INT64(400) INT64(50)] [INT64(600) INT64(2)]]`)
	mcmp.AssertMatches(`select * from u_t2 order by id`, `[[INT64(1000) NULL] [INT64(2000) INT64(2)] [INT64(3000) NULL] [INT64(4000) NULL]]`)
}

// TestDDLFk tests that table is created with fk constraint when foreign_key_checks is off.
func TestDDLFk(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	utils.Exec(t, mcmp.VtConn, `use uks`)

	createTableDDLTemp1 := `
create table temp1(id bigint auto_increment primary key, col varchar(20) not null,
foreign key (col) references temp2(col))
`
	mcmp.Exec(`set foreign_key_checks = off`)
	// should be able to create `temp1` table without a `temp2`
	mcmp.Exec(createTableDDLTemp1)

	createTableDDLTemp2 := `
create table temp2(id bigint auto_increment primary key, col varchar(20) not null, key (col))
`
	// now create `temp2`
	mcmp.Exec(createTableDDLTemp2)

	// inserting some data with fk constraints on.
	mcmp.Exec(`set foreign_key_checks = on`)
	mcmp.Exec(`insert into temp2(col) values('a'), ('b'), ('c') `)
	mcmp.Exec(`insert into temp1(col) values('a') `)
	mcmp.ExecAllowAndCompareError(`insert into temp1(col) values('d') `)
}
