/*
Copyright 2026 The Vitess Authors.

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

package dml

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/utils"
)

// TestLastInsertIDOnDupKey tests that the wire protocol last_insert_id value
// matches MySQL behavior for INSERT ... ON DUPLICATE KEY UPDATE statements.
//
// See https://github.com/vitessio/vitess/issues/15696
func TestLastInsertIDOnDupKey(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	// On MySQL side, lid_tbl doesn't have AUTO_INCREMENT in the shared DDL,
	// so we add it here. The Vitess side uses a VSchema sequence instead.
	utils.Exec(t, mcmp.MySQLConn, "alter table lid_tbl modify id bigint not null auto_increment")

	resetTable := func(t *testing.T) {
		t.Helper()
		utils.Exec(t, mcmp.VtConn, "delete from lid_tbl")
		utils.Exec(t, mcmp.MySQLConn, "delete from lid_tbl")
		utils.Exec(t, mcmp.MySQLConn, "alter table lid_tbl auto_increment = 1")
	}

	t.Run("insert new row", func(t *testing.T) {
		resetTable(t)

		myQr := utils.Exec(t, mcmp.MySQLConn, "insert into lid_tbl(email, name) values ('new@test.com', 'Alice')")
		vtQr := utils.Exec(t, mcmp.VtConn, "insert into lid_tbl(email, name) values ('new@test.com', 'Alice')")

		// MySQL: InsertID > 0, RowsAffected = 1
		assert.EqualValues(t, 1, myQr.RowsAffected)
		assert.Greater(t, myQr.InsertID, uint64(0), "MySQL: InsertID should be > 0 for new row")

		// Vitess should match behavior
		assert.EqualValues(t, 1, vtQr.RowsAffected)
		assert.Greater(t, vtQr.InsertID, uint64(0), "Vitess: InsertID should be > 0 for new row")
	})

	t.Run("duplicate row changes", func(t *testing.T) {
		resetTable(t)

		// Seed a row on each connection
		myIns := utils.Exec(t, mcmp.MySQLConn, "insert into lid_tbl(email, name) values ('dup@test.com', 'Bob')")
		vtIns := utils.Exec(t, mcmp.VtConn, "insert into lid_tbl(email, name) values ('dup@test.com', 'Bob')")

		// ON DUP KEY UPDATE that changes the name
		myQr := utils.Exec(t, mcmp.MySQLConn, "insert into lid_tbl(email, name) values ('dup@test.com', 'Bobby') on duplicate key update name = values(name)")
		vtQr := utils.Exec(t, mcmp.VtConn, "insert into lid_tbl(email, name) values ('dup@test.com', 'Bobby') on duplicate key update name = values(name)")

		// MySQL: InsertID = seeded row's id, RowsAffected = 2
		assert.EqualValues(t, 2, myQr.RowsAffected)
		assert.EqualValues(t, myIns.InsertID, myQr.InsertID, "MySQL: should return existing row's id")

		// Vitess should match (but currently doesn't — returns new sequence value)
		assert.EqualValues(t, 2, vtQr.RowsAffected)
		assert.EqualValues(t, vtIns.InsertID, vtQr.InsertID, "Vitess: should return existing row's id")
	})

	t.Run("duplicate no change", func(t *testing.T) {
		resetTable(t)

		// Seed a row on each connection
		utils.Exec(t, mcmp.MySQLConn, "insert into lid_tbl(email, name) values ('dup@test.com', 'Bob')")
		utils.Exec(t, mcmp.VtConn, "insert into lid_tbl(email, name) values ('dup@test.com', 'Bob')")

		// ON DUP KEY UPDATE with same value (no actual change)
		myQr := utils.Exec(t, mcmp.MySQLConn, "insert into lid_tbl(email, name) values ('dup@test.com', 'Bob') on duplicate key update name = values(name)")
		vtQr := utils.Exec(t, mcmp.VtConn, "insert into lid_tbl(email, name) values ('dup@test.com', 'Bob') on duplicate key update name = values(name)")

		// MySQL: InsertID = 0, RowsAffected = 0
		assert.EqualValues(t, 0, myQr.RowsAffected)
		assert.EqualValues(t, 0, myQr.InsertID, "MySQL: should return 0 for no-change duplicate")

		// Vitess should match (but currently doesn't — returns new sequence value)
		assert.EqualValues(t, 0, vtQr.RowsAffected)
		assert.EqualValues(t, 0, vtQr.InsertID, "Vitess: should return 0 for no-change duplicate")
	})

	t.Run("multi-row all inserts", func(t *testing.T) {
		resetTable(t)

		myQr := utils.Exec(t, mcmp.MySQLConn, "insert into lid_tbl(email, name) values ('a@test.com', 'A'), ('b@test.com', 'B')")
		vtQr := utils.Exec(t, mcmp.VtConn, "insert into lid_tbl(email, name) values ('a@test.com', 'A'), ('b@test.com', 'B')")

		// MySQL: InsertID = first generated id, RowsAffected = 2
		assert.EqualValues(t, 2, myQr.RowsAffected)
		assert.Greater(t, myQr.InsertID, uint64(0), "MySQL: InsertID should be > 0 for new rows")

		// Vitess should match behavior
		assert.EqualValues(t, 2, vtQr.RowsAffected)
		assert.Greater(t, vtQr.InsertID, uint64(0), "Vitess: InsertID should be > 0 for new rows")
	})

	t.Run("multi-row all updates", func(t *testing.T) {
		resetTable(t)

		// Seed two rows on each connection
		utils.Exec(t, mcmp.MySQLConn, "insert into lid_tbl(email, name) values ('a@test.com', 'A'), ('b@test.com', 'B')")
		utils.Exec(t, mcmp.VtConn, "insert into lid_tbl(email, name) values ('a@test.com', 'A'), ('b@test.com', 'B')")

		// Find the existing ids on each side
		myRows := utils.Exec(t, mcmp.MySQLConn, "select id from lid_tbl order by email")
		vtRows := utils.Exec(t, mcmp.VtConn, "select id from lid_tbl order by email")
		require.Len(t, myRows.Rows, 2)
		require.Len(t, vtRows.Rows, 2)
		myLastID, err := myRows.Rows[1][0].ToUint64()
		require.NoError(t, err)
		vtLastID, err := vtRows.Rows[1][0].ToUint64()
		require.NoError(t, err)

		// ON DUP KEY UPDATE both rows
		myQr := utils.Exec(t, mcmp.MySQLConn, "insert into lid_tbl(email, name) values ('a@test.com', 'A2'), ('b@test.com', 'B2') on duplicate key update name = values(name)")
		vtQr := utils.Exec(t, mcmp.VtConn, "insert into lid_tbl(email, name) values ('a@test.com', 'A2'), ('b@test.com', 'B2') on duplicate key update name = values(name)")

		// MySQL: InsertID = last updated row's id, RowsAffected = 4 (2 rows * 2)
		assert.EqualValues(t, 4, myQr.RowsAffected)
		assert.EqualValues(t, myLastID, myQr.InsertID, "MySQL: should return last updated row's id")

		// Vitess should match (but currently doesn't — returns new sequence value)
		assert.EqualValues(t, 4, vtQr.RowsAffected)
		assert.EqualValues(t, vtLastID, vtQr.InsertID, "Vitess: should return last updated row's id")
	})

	t.Run("multi-row all dups no change", func(t *testing.T) {
		resetTable(t)

		// Seed two rows on each connection
		utils.Exec(t, mcmp.MySQLConn, "insert into lid_tbl(email, name) values ('a@test.com', 'A'), ('b@test.com', 'B')")
		utils.Exec(t, mcmp.VtConn, "insert into lid_tbl(email, name) values ('a@test.com', 'A'), ('b@test.com', 'B')")

		// ON DUP KEY UPDATE with same values (no change)
		myQr := utils.Exec(t, mcmp.MySQLConn, "insert into lid_tbl(email, name) values ('a@test.com', 'A'), ('b@test.com', 'B') on duplicate key update name = values(name)")
		vtQr := utils.Exec(t, mcmp.VtConn, "insert into lid_tbl(email, name) values ('a@test.com', 'A'), ('b@test.com', 'B') on duplicate key update name = values(name)")

		// MySQL: InsertID = 0, RowsAffected = 0
		assert.EqualValues(t, 0, myQr.RowsAffected)
		assert.EqualValues(t, 0, myQr.InsertID, "MySQL: should return 0 for no-change duplicates")

		// Vitess should match (but currently doesn't — returns new sequence value)
		assert.EqualValues(t, 0, vtQr.RowsAffected)
		assert.EqualValues(t, 0, vtQr.InsertID, "Vitess: should return 0 for no-change duplicates")
	})

	t.Run("mix insert and update", func(t *testing.T) {
		resetTable(t)

		// Seed one row on each connection
		utils.Exec(t, mcmp.MySQLConn, "insert into lid_tbl(email, name) values ('a@test.com', 'A')")
		utils.Exec(t, mcmp.VtConn, "insert into lid_tbl(email, name) values ('a@test.com', 'A')")

		// One duplicate (update), one new (insert)
		myQr := utils.Exec(t, mcmp.MySQLConn, "insert into lid_tbl(email, name) values ('a@test.com', 'A2'), ('c@test.com', 'C') on duplicate key update name = values(name)")
		vtQr := utils.Exec(t, mcmp.VtConn, "insert into lid_tbl(email, name) values ('a@test.com', 'A2'), ('c@test.com', 'C') on duplicate key update name = values(name)")

		// MySQL: RowsAffected = 3 (2 for update + 1 for insert), InsertID = newly inserted row's id
		assert.EqualValues(t, 3, myQr.RowsAffected)
		assert.Greater(t, myQr.InsertID, uint64(0), "MySQL: InsertID should be > 0 for mix with insert")

		// Vitess should match behavior
		assert.EqualValues(t, 3, vtQr.RowsAffected)
		assert.Greater(t, vtQr.InsertID, uint64(0), "Vitess: InsertID should be > 0 for mix with insert")
	})

	t.Run("mix update and no change", func(t *testing.T) {
		resetTable(t)

		// Seed two rows on each connection
		utils.Exec(t, mcmp.MySQLConn, "insert into lid_tbl(email, name) values ('a@test.com', 'A'), ('b@test.com', 'B')")
		utils.Exec(t, mcmp.VtConn, "insert into lid_tbl(email, name) values ('a@test.com', 'A'), ('b@test.com', 'B')")

		// Find existing ids — MySQL returns the last row's id that hit a duplicate
		// key, regardless of whether it actually changed. That's b@test.com.
		myRows := utils.Exec(t, mcmp.MySQLConn, "select id from lid_tbl where email = 'b@test.com'")
		vtRows := utils.Exec(t, mcmp.VtConn, "select id from lid_tbl where email = 'b@test.com'")
		require.Len(t, myRows.Rows, 1)
		require.Len(t, vtRows.Rows, 1)
		myLastDupID, err := myRows.Rows[0][0].ToUint64()
		require.NoError(t, err)
		vtLastDupID, err := vtRows.Rows[0][0].ToUint64()
		require.NoError(t, err)

		// a@test.com: changes name (update), b@test.com: same name (no change)
		myQr := utils.Exec(t, mcmp.MySQLConn, "insert into lid_tbl(email, name) values ('a@test.com', 'A2'), ('b@test.com', 'B') on duplicate key update name = values(name)")
		vtQr := utils.Exec(t, mcmp.VtConn, "insert into lid_tbl(email, name) values ('a@test.com', 'A2'), ('b@test.com', 'B') on duplicate key update name = values(name)")

		// MySQL: RowsAffected = 2, InsertID = last dup row's id (b@test.com)
		assert.EqualValues(t, 2, myQr.RowsAffected)
		assert.EqualValues(t, myLastDupID, myQr.InsertID, "MySQL: should return last dup row's id")

		// Vitess should match (but currently doesn't — returns new sequence value)
		assert.EqualValues(t, 2, vtQr.RowsAffected)
		assert.EqualValues(t, vtLastDupID, vtQr.InsertID, "Vitess: should return last dup row's id")
	})

	t.Run("SQL function stickiness", func(t *testing.T) {
		resetTable(t)

		// Seed an existing row FIRST on each connection, so we have a row
		// with a DIFFERENT id than the one we'll insert next.
		utils.Exec(t, mcmp.MySQLConn, "insert into lid_tbl(email, name) values ('existing@test.com', 'Existing')")
		utils.Exec(t, mcmp.VtConn, "insert into lid_tbl(email, name) values ('existing@test.com', 'Existing')")

		// Step 1: Insert a NEW row — both wire and SQL function show the new ID.
		// This is the value that should remain "sticky" throughout the test.
		myIns := utils.Exec(t, mcmp.MySQLConn, "insert into lid_tbl(email, name) values ('sticky@test.com', 'Sticky')")
		vtIns := utils.Exec(t, mcmp.VtConn, "insert into lid_tbl(email, name) values ('sticky@test.com', 'Sticky')")

		myFn := utils.Exec(t, mcmp.MySQLConn, "select last_insert_id()")
		vtFn := utils.Exec(t, mcmp.VtConn, "select last_insert_id()")

		myFnVal, err := myFn.Rows[0][0].ToUint64()
		require.NoError(t, err)
		vtFnVal, err := vtFn.Rows[0][0].ToUint64()
		require.NoError(t, err)

		// After insert, SQL function should match wire protocol InsertID
		assert.EqualValues(t, myIns.InsertID, myFnVal, "MySQL: SQL function should match wire InsertID after insert")
		assert.EqualValues(t, vtIns.InsertID, vtFnVal, "Vitess: SQL function should match wire InsertID after insert")

		// Step 2: ON DUP KEY UPDATE on the EXISTING row (different id than sticky@).
		// Wire protocol should change to existing row's id, but SQL function must stay sticky.
		myDup := utils.Exec(t, mcmp.MySQLConn, "insert into lid_tbl(email, name) values ('existing@test.com', 'Updated') on duplicate key update name = values(name)")
		vtDup := utils.Exec(t, mcmp.VtConn, "insert into lid_tbl(email, name) values ('existing@test.com', 'Updated') on duplicate key update name = values(name)")

		// Wire protocol should show the existing row's id (different from sticky@'s id)
		assert.EqualValues(t, 2, myDup.RowsAffected)
		assert.EqualValues(t, 2, vtDup.RowsAffected)
		assert.NotEqual(t, myIns.InsertID, myDup.InsertID, "MySQL: wire InsertID should be existing row's id, not sticky")
		assert.NotEqual(t, vtIns.InsertID, vtDup.InsertID, "Vitess: wire InsertID should be existing row's id, not sticky")

		myFn2 := utils.Exec(t, mcmp.MySQLConn, "select last_insert_id()")
		vtFn2 := utils.Exec(t, mcmp.VtConn, "select last_insert_id()")

		myFnVal2, err := myFn2.Rows[0][0].ToUint64()
		require.NoError(t, err)
		vtFnVal2, err := vtFn2.Rows[0][0].ToUint64()
		require.NoError(t, err)

		// MySQL: SQL function stays sticky at the insert value from step 1
		assert.EqualValues(t, myIns.InsertID, myFnVal2, "MySQL: SQL function should stay sticky after ON DUP KEY UPDATE")

		// Vitess should match
		assert.EqualValues(t, vtIns.InsertID, vtFnVal2, "Vitess: SQL function should stay sticky after ON DUP KEY UPDATE")

		// Step 3: ON DUP KEY no-change — wire = 0, SQL function should still stay sticky
		utils.Exec(t, mcmp.MySQLConn, "insert into lid_tbl(email, name) values ('existing@test.com', 'Updated') on duplicate key update name = values(name)")
		utils.Exec(t, mcmp.VtConn, "insert into lid_tbl(email, name) values ('existing@test.com', 'Updated') on duplicate key update name = values(name)")

		myFn3 := utils.Exec(t, mcmp.MySQLConn, "select last_insert_id()")
		vtFn3 := utils.Exec(t, mcmp.VtConn, "select last_insert_id()")

		myFnVal3, err := myFn3.Rows[0][0].ToUint64()
		require.NoError(t, err)
		vtFnVal3, err := vtFn3.Rows[0][0].ToUint64()
		require.NoError(t, err)

		// MySQL: SQL function still sticky
		assert.EqualValues(t, myIns.InsertID, myFnVal3, "MySQL: SQL function should stay sticky after no-change ON DUP KEY")

		// Vitess should match
		assert.EqualValues(t, vtIns.InsertID, vtFnVal3, "Vitess: SQL function should stay sticky after no-change ON DUP KEY")
	})

	// Print a summary of the Vitess vs MySQL comparison for debugging visibility
	t.Run("summary", func(t *testing.T) {
		resetTable(t)

		// Seed and run a representative case to show the divergence
		myIns := utils.Exec(t, mcmp.MySQLConn, "insert into lid_tbl(email, name) values ('sum@test.com', 'Sum')")
		vtIns := utils.Exec(t, mcmp.VtConn, "insert into lid_tbl(email, name) values ('sum@test.com', 'Sum')")

		myQr := utils.Exec(t, mcmp.MySQLConn, "insert into lid_tbl(email, name) values ('sum@test.com', 'Sum2') on duplicate key update name = values(name)")
		vtQr := utils.Exec(t, mcmp.VtConn, "insert into lid_tbl(email, name) values ('sum@test.com', 'Sum2') on duplicate key update name = values(name)")

		t.Logf("MySQL: seed InsertID=%d, ON DUP InsertID=%d RowsAffected=%d", myIns.InsertID, myQr.InsertID, myQr.RowsAffected)
		t.Logf("Vitess: seed InsertID=%d, ON DUP InsertID=%d RowsAffected=%d", vtIns.InsertID, vtQr.InsertID, vtQr.RowsAffected)

		if vtQr.InsertID != vtIns.InsertID {
			t.Logf("MISMATCH: Vitess ON DUP InsertID (%d) != seeded row ID (%d) — see issue #15696", vtQr.InsertID, vtIns.InsertID)
		}
	})
}
