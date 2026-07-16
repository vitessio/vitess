/*
Copyright 2022 The Vitess Authors.

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

package single

import (
	"context"
	_ "embed"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/vitesst"
)

var (
	KeyspaceName = "ks"

	//go:embed schema.sql
	SchemaSQL string

	//go:embed vschema.json
	VSchema string
)

func startCluster(t *testing.T) mysql.ConnParams {
	t.Helper()

	cluster, err := vitesst.NewCluster(
		vitesst.WithKeyspace(KeyspaceName).
			WithShardNames("-80", "80-").
			WithSchema(SchemaSQL).
			WithVSchema(VSchema),
		vitesst.WithVTGateArgs("--transaction-mode", "SINGLE"),
	)
	require.NoError(t, err)

	cleanup, err := cluster.Start(t.Context())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Minute)
		defer cancel()
		if t.Failed() {
			cluster.DumpDiagnostics(ctx, t.Logf)
		}
		if err := cleanup(ctx); err != nil {
			t.Logf("cluster teardown: %v", err)
		}
	})
	require.NoError(t, err)

	return cluster.VTParams(t.Context(), "")
}

func TestSingleOneWay(t *testing.T) {
	vtParams := startCluster(t)
	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	defer func() {
		vitesst.Exec(t, conn, `delete from txn_unique_constraints where txn_id = 'txn1'`)
		vitesst.Exec(t, conn, `delete from txn_unique_constraints where txn_id = 'txn3'`)
	}()

	vitesst.Exec(t, conn, `begin`)
	vitesst.Exec(t, conn, `INSERT INTO txn_unique_constraints(id, txn_id, unique_constraint) VALUES (UUID(), 'txn1', 'txn_info_txn_id_txn1')`)
	vitesst.Exec(t, conn, `INSERT INTO txn_unique_constraints(id, txn_id, unique_constraint) VALUES (UUID(), 'txn1', 'txn_info_mti_mc___mti1_mc1')`)
	vitesst.Exec(t, conn, `commit`)

	vitesst.Exec(t, conn, `begin`)
	vitesst.Exec(t, conn, `INSERT INTO txn_unique_constraints(id, txn_id, unique_constraint) VALUES (UUID(), 'txn3', 'txn_info_txn_id_txn3')`)
	// should fail with duplicate key error and not with multi-db transaction
	vitesst.AssertContainsError(t, conn, `INSERT INTO txn_unique_constraints(id, txn_id, unique_constraint) VALUES (UUID(), 'txn3', 'txn_info_mti_mc___mti1_mc1')`, `Duplicate entry 'txn_info_mti_mc___mti1_mc1'`)
	vitesst.Exec(t, conn, `rollback`)
}

func TestSingleReverseWay(t *testing.T) {
	vtParams := startCluster(t)
	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	defer func() {
		vitesst.Exec(t, conn, `delete from txn_unique_constraints where txn_id = 'txn1'`)
		vitesst.Exec(t, conn, `delete from txn_unique_constraints where txn_id = 'txn3'`)
	}()

	vitesst.Exec(t, conn, `begin`)
	vitesst.Exec(t, conn, `INSERT INTO txn_unique_constraints(id, txn_id, unique_constraint) VALUES (UUID(), 'txn3', 'txn_info_txn_id_txn3')`)
	vitesst.Exec(t, conn, `INSERT INTO txn_unique_constraints(id, txn_id, unique_constraint) VALUES (UUID(), 'txn3', 'txn_info_mti_mc___mti1_mc1')`)
	vitesst.Exec(t, conn, `commit`)

	vitesst.Exec(t, conn, `begin`)
	vitesst.Exec(t, conn, `INSERT INTO txn_unique_constraints(id, txn_id, unique_constraint) VALUES (UUID(), 'txn1', 'txn_info_txn_id_txn1')`)
	// should fail with duplicate key error and not with multi-db transaction
	vitesst.AssertContainsError(t, conn, `INSERT INTO txn_unique_constraints(id, txn_id, unique_constraint) VALUES (UUID(), 'txn1', 'txn_info_mti_mc___mti1_mc1')`, `Duplicate entry 'txn_info_mti_mc___mti1_mc1'`)
	vitesst.Exec(t, conn, `rollback`)
}

func TestSingleLookupDangleRow(t *testing.T) {
	vtParams := startCluster(t)
	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	defer func() {
		vitesst.Exec(t, conn, `delete from txn_unique_constraints where txn_id = 'txn3'`)
	}()

	// insert a dangling row in lookup table
	vitesst.Exec(t, conn, `INSERT INTO uniqueConstraint_vdx(unique_constraint, keyspace_id) VALUES ('txn_info_mti_mc___mti1_mc1', 'J\xda\xf0p\x0e\xcc(\x8fਁ\xa7P\x86\xa5=')`)

	vitesst.Exec(t, conn, `begin`)
	// should succeed by validating that the original row does not exist for the unique_constraint, so this should succeed.
	vitesst.Exec(t, conn, `INSERT INTO txn_unique_constraints(id, txn_id, unique_constraint) VALUES (UUID(), 'txn3', 'txn_info_mti_mc___mti1_mc1')`)
	vitesst.Exec(t, conn, `commit`)

	vitesst.AssertMatches(t, conn, `select txn_id, unique_constraint from txn_unique_constraints where txn_id = 'txn3'`, `[[VARCHAR("txn3") VARCHAR("txn_info_mti_mc___mti1_mc1")]]`)
}

func TestLookupDangleRowLaterMultiDB(t *testing.T) {
	vtParams := startCluster(t)
	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	defer func() {
		vitesst.Exec(t, conn, `delete from uniqueConstraint_vdx where unique_constraint = 'foo'`)
		vitesst.Exec(t, conn, `delete from uniqueConstraint_vdx where unique_constraint = 'bar'`)
	}()

	// insert a dangling row in lookup table
	vitesst.Exec(t, conn, `INSERT INTO uniqueConstraint_vdx(unique_constraint, keyspace_id) VALUES ('foo', 'J\xda\xf0p\x0e\xcc(\x8fਁ\xa7P\x86\xa5=')`)
	vitesst.Exec(t, conn, `INSERT INTO uniqueConstraint_vdx(unique_constraint, keyspace_id) VALUES ('bar', '\x86\xc8\xc5\x1ac\xfb\x8c+6\xe4\x1f\x03\xd8ϝB')`)
	//
	vitesst.Exec(t, conn, `begin`)
	// should succeed by validating that the original row does not exist for the unique_constraint, so this should succeed.
	vitesst.Exec(t, conn, `INSERT INTO txn_unique_constraints(id, txn_id, unique_constraint) VALUES (UUID(), 'txn1', 'foo')`)
	// this fails as it starts a transaction on another shard. so complete transaction is aborted.
	vitesst.AssertContainsError(t, conn, `INSERT INTO txn_unique_constraints(id, txn_id, unique_constraint) VALUES (UUID(), 'txn3', 'bar')`, `multi-db transaction attempted`)
	vitesst.Exec(t, conn, `commit`)

	// this row should not exist.
	vitesst.AssertMatches(t, conn, `select txn_id from txn_unique_constraints where txn_id = 'txn1' and unique_constraint = 'foo'`, `[]`)
}

func TestLookupDangleRowRecordInSameShard(t *testing.T) {
	vtParams := startCluster(t)
	conn, cleanup := setup(t, vtParams)
	defer cleanup()

	// insert a dangling row in lookup table
	vitesst.Exec(t, conn, `INSERT INTO uniqueConstraint_vdx(unique_constraint, keyspace_id) VALUES ('foo', 'J\xda\xf0p\x0e\xcc(\x8fਁ\xa7P\x86\xa5=')`)
	vitesst.Exec(t, conn, `INSERT INTO uniqueConstraint_vdx(unique_constraint, keyspace_id) VALUES ('bar', '\x86\xc8\xc5\x1ac\xfb\x8c+6\xe4\x1f\x03\xd8ϝB')`)
	//
	vitesst.Exec(t, conn, `begin`)
	// should succeed by validating that the original row does not exist for the unique_constraint, so this should succeed.
	vitesst.Exec(t, conn, `INSERT INTO txn_unique_constraints(id, txn_id, unique_constraint) VALUES (UUID(), 'txn1', 'foo')`)
	// this also passes as it goes to same shard (no multi-shard transaction).
	vitesst.Exec(t, conn, `INSERT INTO txn_unique_constraints(id, txn_id, unique_constraint) VALUES (UUID(), 'txn1', 'bar')`)
	vitesst.Exec(t, conn, `commit`)

	vitesst.AssertMatches(t, conn, `select txn_id, unique_constraint from txn_unique_constraints where txn_id = 'txn1' order by unique_constraint`, `[[VARCHAR("txn1") VARCHAR("bar")] [VARCHAR("txn1") VARCHAR("foo")]]`)
}

func TestMultiDbSecondRecordLookupDangle(t *testing.T) {
	vtParams := startCluster(t)
	conn, cleanup := setup(t, vtParams)
	defer cleanup()

	// insert a dangling row in lookup table
	vitesst.Exec(t, conn, `INSERT INTO uniqueConstraint_vdx(unique_constraint, keyspace_id) VALUES ('bar', '\x86\xc8\xc5\x1ac\xfb\x8c+6\xe4\x1f\x03\xd8ϝB')`)

	vitesst.Exec(t, conn, `begin`)
	// normal query goes to -80.
	vitesst.Exec(t, conn, `INSERT INTO txn_unique_constraints(id, txn_id, unique_constraint) VALUES (UUID(), 'txn1', 'foo')`)
	// dangling row query goes to -80 (where tx already exists). actual query goes to 80- so multi-shard transaction error.
	vitesst.AssertContainsError(t, conn, `INSERT INTO txn_unique_constraints(id, txn_id, unique_constraint) VALUES (UUID(), 'txn3', 'txn_info_txn_id_txn1')`, `multi-db transaction attempted`)
	vitesst.Exec(t, conn, `commit`)

	// no row should exist.
	vitesst.AssertMatches(t, conn, `select txn_id from txn_unique_constraints`, `[]`)

	vitesst.Exec(t, conn, `begin`)
	// normal query goes to -80
	vitesst.Exec(t, conn, `INSERT INTO txn_unique_constraints(id, txn_id, unique_constraint) VALUES (UUID(), 'txn1', 'foo')`)
	// dangling row query goes to 80- (no issue there). actual query goes to 80- so multi-shard transaction error.
	vitesst.AssertContainsError(t, conn, `INSERT INTO txn_unique_constraints(id, txn_id, unique_constraint) VALUES (UUID(), 'txn3', 'bar')`, `multi-db transaction attempted`)
	vitesst.Exec(t, conn, `commit`)

	// no row should exist.
	vitesst.AssertMatches(t, conn, `select txn_id from txn_unique_constraints`, `[]`)
}

// TestNoRecordInTableNotFail test that vindex lookup query creates a transaction on one shard say x.
// To fetch the fields for the actual table, the Select Impossible query should also be reouted to x.
// If it routes to other shard then the test will fail with multi-shard transaction attempted error.
// The fix ensures it does not happen.
func TestNoRecordInTableNotFail(t *testing.T) {
	vtParams := startCluster(t)
	conn, cleanup := setup(t, vtParams)
	defer cleanup()

	vitesst.AssertMatches(t, conn, `select @@transaction_mode`, `[[VARCHAR("SINGLE")]]`)
	// Need to run this test multiple times as shards are picked randomly for Impossible query.
	// After the fix it is not random if a shard session already exists then it reuses that same shard session.
	for range 100 {
		vitesst.Exec(t, conn, `begin`)
		vitesst.Exec(t, conn, `INSERT INTO t1(id, txn_id) VALUES (1, "t1")`)
		vitesst.Exec(t, conn, `SELECT * FROM t2 WHERE id = 1`)
		vitesst.Exec(t, conn, `rollback`)
	}
}

func TestOnlyMultiShardWriteFail(t *testing.T) {
	vtParams := startCluster(t)
	conn, cleanup := setup(t, vtParams)
	defer func() {
		cleanup()
		conn.Close()
	}()

	// basic test to check that multi-shard transaction is not allowed.
	t.Run("insert-select-insert fail", func(t *testing.T) {
		vitesst.Exec(t, conn, `begin`)
		vitesst.Exec(t, conn, `INSERT INTO t1(id, txn_id) VALUES (1, "a")`)
		// read is ok on another shard.
		vitesst.Exec(t, conn, `select * from t1 where txn_id = "b"`)
		// write on it will fail.
		_, err := vitesst.ExecAllowError(t, conn, `INSERT INTO t1(id, txn_id) VALUES (2, "b")`)
		require.ErrorContains(t, err, "multi-db transaction attempted")
		vitesst.Exec(t, conn, `rollback`)
	})

	// test with select query on different shards and one write query.
	t.Run("select-select-insert pass", func(t *testing.T) {
		vitesst.Exec(t, conn, `begin`)
		vitesst.Exec(t, conn, `select * from t1 where txn_id = "a"`)
		vitesst.Exec(t, conn, `select * from t1 where txn_id = "b"`)
		vitesst.Exec(t, conn, `INSERT INTO t1(id, txn_id) VALUES (1, "a")`)
		vitesst.Exec(t, conn, `commit`)
	})

	// test with one write query and multiple select.
	t.Run("insert-select-select pass", func(t *testing.T) {
		vitesst.Exec(t, conn, `begin`)
		vitesst.Exec(t, conn, `INSERT INTO t1(id, txn_id) VALUES (2, "b")`)
		vitesst.Exec(t, conn, `select * from t1 where txn_id in ("a", "b", "c")`)
		vitesst.Exec(t, conn, `select * from t1 where txn_id in ("d", "e", "f")`)
		vitesst.Exec(t, conn, `commit`)
	})
}

func setup(t *testing.T, vtParams mysql.ConnParams) (*mysql.Conn, func()) {
	t.Helper()
	conn, err := mysql.Connect(t.Context(), &vtParams)
	require.NoError(t, err)

	tables := []string{
		"txn_unique_constraints", "uniqueConstraint_vdx",
		"t1", "t1_id_vdx", "t2", "t2_id_vdx",
	}
	cleanup := func() {
		vitesst.Exec(t, conn, "set transaction_mode=multi")
		for _, table := range tables {
			vitesst.Exec(t, conn, fmt.Sprintf("delete from %s /* cleanup */", table))
		}
		vitesst.Exec(t, conn, "set transaction_mode=single")
	}
	cleanup()
	return conn, cleanup
}
