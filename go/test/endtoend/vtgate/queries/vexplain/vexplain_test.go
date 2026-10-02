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

package vexplain

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/capabilities"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/onlineddl"
	"vitess.io/vitess/go/test/endtoend/utils"
)

// keyspaceByName returns the started keyspace with the given name. Tests must
// look keyspaces up by name rather than by position in clusterInstance.Keyspaces,
// whose order follows keyspace startup order in TestMain and is not stable (the
// unsharded keyspace starts first, so Keyspaces[0] is not the sharded one).
func keyspaceByName(t *testing.T, name string) *cluster.Keyspace {
	t.Helper()
	for i := range clusterInstance.Keyspaces {
		if clusterInstance.Keyspaces[i].Name == name {
			return &clusterInstance.Keyspaces[i]
		}
	}
	require.Failf(t, "keyspace not found", "keyspace %q not found in cluster", name)
	return nil
}

// optimizerHintRE matches optimizer hint comments (e.g. /*+ SET_VAR(...) */) that
// newer vtgate versions may inject into the queries sent to the shards.
var optimizerHintRE = regexp.MustCompile(`\s*/\*\+.*?\*/`)

func stripOptimizerHints(rows [][]sqltypes.Value) {
	for _, row := range rows {
		for i, val := range row {
			s := val.ToString()
			if stripped := optimizerHintRE.ReplaceAllString(s, ""); stripped != s {
				row[i] = sqltypes.MakeTrusted(val.Type(), []byte(stripped))
			}
		}
	}
}

func start(t *testing.T) (*mysql.Conn, func()) {
	ctx := t.Context()
	vtConn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)

	deleteAll := func() {
		_, _ = utils.ExecAllowError(t, vtConn, "set workload = oltp")

		tables := []string{"user", "lookup", "lookup_unique"}
		for _, table := range tables {
			_, _ = utils.ExecAllowError(t, vtConn, "delete from "+table)
		}
	}

	deleteAll()

	return vtConn, func() {
		deleteAll()
		vtConn.Close()
	}
}

func TestVtGateVExplain(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	assertVExplainEquals := func(t *testing.T, conn *mysql.Conn, query, expected string) {
		t.Helper()

		qr := utils.Exec(t, conn, query)

		// strip the first column from each row as it is not deterministic in a VExplain query
		for i := range qr.Rows {
			qr.Rows[i] = qr.Rows[i][1:]
		}
		stripOptimizerHints(qr.Rows)

		assert.NoError(t, sqltypes.RowsEqualsStr(expected, qr.Rows))
	}

	utils.AssertContainsError(t, conn,
		`vexplain queries insert into user (id,lookup,lookup_unique) values (4,'apa','foo'),(5,'apa','bar'),(6,'monkey','nobar')`,
		"vexplain queries/all will actually run queries")

	binaryPrefix := "_binary"

	expected := fmt.Sprintf(`[
		[VARCHAR("ks") VARCHAR("-40") VARCHAR("begin")]
		[VARCHAR("ks") VARCHAR("-40") VARCHAR("insert into lookup(lookup, id, keyspace_id) values ('apa', 1, %s'\x16k@\xb4J\xbaK\xd6') on duplicate key update lookup = values(lookup), id = values(id), keyspace_id = values(keyspace_id)")]
		[VARCHAR("ks") VARCHAR("40-80") VARCHAR("begin")]
		[VARCHAR("ks") VARCHAR("40-80") VARCHAR("insert into lookup(lookup, id, keyspace_id) values ('monkey', 3, %s'N\xb1\x90ɢ\xfa\x16\x9c') on duplicate key update lookup = values(lookup), id = values(id), keyspace_id = values(keyspace_id)")]
		[VARCHAR("ks") VARCHAR("-40") VARCHAR("commit")]
		[VARCHAR("ks") VARCHAR("40-80") VARCHAR("commit")]
		[VARCHAR("ks") VARCHAR("40-80") VARCHAR("begin")]
		[VARCHAR("ks") VARCHAR("40-80") VARCHAR("insert into lookup_unique(lookup_unique, keyspace_id) values ('monkey', %s'N\xb1\x90ɢ\xfa\x16\x9c')")]
		[VARCHAR("ks") VARCHAR("-40") VARCHAR("begin")]
		[VARCHAR("ks") VARCHAR("-40") VARCHAR("insert into lookup_unique(lookup_unique, keyspace_id) values ('apa', %s'\x16k@\xb4J\xbaK\xd6')")]
		[VARCHAR("ks") VARCHAR("40-80") VARCHAR("commit")]
		[VARCHAR("ks") VARCHAR("-40") VARCHAR("commit")]
		[VARCHAR("ks") VARCHAR("40-80") VARCHAR("begin")]
		[VARCHAR("ks") VARCHAR("40-80") VARCHAR("insert into `+"`user`"+`(id, lookup, lookup_unique) values (3, 'monkey', 'monkey')")]
		[VARCHAR("ks") VARCHAR("-40") VARCHAR("begin")]
		[VARCHAR("ks") VARCHAR("-40") VARCHAR("insert into `+"`user`"+`(id, lookup, lookup_unique) values (1, 'apa', 'apa')")]
	]`, binaryPrefix, binaryPrefix, binaryPrefix, binaryPrefix)
	assertVExplainEquals(t, conn, `vexplain /*vt+ EXECUTE_DML_QUERIES */ queries insert into user (id,lookup,lookup_unique) values (1,'apa','apa'),(3,'monkey','monkey')`, expected)

	// Assert that the output of vexplain all doesn't have begin queries because they aren't explainable
	utils.AssertMatchesNotContains(t, conn, `vexplain /*vt+ EXECUTE_DML_QUERIES */ all insert into user (id,lookup,lookup_unique) values (2,'apa','bandar')`, `begin`)

	expected = `[[INT32(0) VARCHAR("ks") VARCHAR("-40") VARCHAR("select lookup, keyspace_id from lookup where lookup in ('apa')")]` +
		` [INT32(1) VARCHAR("ks") VARCHAR("-40") VARCHAR("select id from ` + "`user`" + ` where lookup = 'apa'")]]`
	for _, mode := range []string{"oltp", "olap"} {
		t.Run(mode, func(t *testing.T) {
			utils.Exec(t, conn, "set workload = "+mode)
			qr := utils.Exec(t, conn, `vexplain queries select id from user where lookup = "apa"`)
			stripOptimizerHints(qr.Rows)
			got := fmt.Sprintf("%v", qr.Rows)
			assert.Equal(t, expected, got)
		})
	}

	// transaction explicitly started to no commit in the end.
	utils.Exec(t, conn, "begin")
	expected = fmt.Sprintf(`[
		[VARCHAR("ks") VARCHAR("-40") VARCHAR("begin")]
		[VARCHAR("ks") VARCHAR("-40") VARCHAR("insert into lookup(lookup, id, keyspace_id) values ('apa', 4, %s'\xd2\xfd\x88g\xd5\\r-\xfe'), ('apa', 5, %s'p\xbb\x02<\x81\f\xa8z') on duplicate key update lookup = values(lookup), id = values(id), keyspace_id = values(keyspace_id)")]
		[VARCHAR("ks") VARCHAR("40-80") VARCHAR("begin")]
		[VARCHAR("ks") VARCHAR("40-80") VARCHAR("insert into lookup(lookup, id, keyspace_id) values ('monkey', 6, %s'\xf0\x98H\\n\xc4ľq') on duplicate key update lookup = values(lookup), id = values(id), keyspace_id = values(keyspace_id)")]
		[VARCHAR("ks") VARCHAR("-40") VARCHAR("commit")]
		[VARCHAR("ks") VARCHAR("40-80") VARCHAR("commit")]
		[VARCHAR("ks") VARCHAR("-40") VARCHAR("begin")]
		[VARCHAR("ks") VARCHAR("-40") VARCHAR("insert into lookup_unique(lookup_unique, keyspace_id) values ('foo', %s'\xd2\xfd\x88g\xd5\\r-\xfe')")]
		[VARCHAR("ks") VARCHAR("80-c0") VARCHAR("begin")]
		[VARCHAR("ks") VARCHAR("80-c0") VARCHAR("insert into lookup_unique(lookup_unique, keyspace_id) values ('bar', %s'p\xbb\x02<\x81\f\xa8z')")]
		[VARCHAR("ks") VARCHAR("c0-") VARCHAR("begin")]
		[VARCHAR("ks") VARCHAR("c0-") VARCHAR("insert into lookup_unique(lookup_unique, keyspace_id) values ('nobar', %s'\xf0\x98H\\n\xc4ľq')")]
		[VARCHAR("ks") VARCHAR("-40") VARCHAR("commit")]
		[VARCHAR("ks") VARCHAR("80-c0") VARCHAR("commit")]
		[VARCHAR("ks") VARCHAR("c0-") VARCHAR("commit")]
		[VARCHAR("ks") VARCHAR("40-80") VARCHAR("begin")]
		[VARCHAR("ks") VARCHAR("40-80") VARCHAR("insert into `+"`user`"+`(id, lookup, lookup_unique) values (5, 'apa', 'bar')")]
		[VARCHAR("ks") VARCHAR("c0-") VARCHAR("begin")]
		[VARCHAR("ks") VARCHAR("c0-") VARCHAR("insert into `+"`user`"+`(id, lookup, lookup_unique) values (4, 'apa', 'foo'), (6, 'monkey', 'nobar')")]
	]`, binaryPrefix, binaryPrefix, binaryPrefix, binaryPrefix, binaryPrefix, binaryPrefix)
	assertVExplainEquals(t, conn, `vexplain /*vt+ EXECUTE_DML_QUERIES */ queries insert into user (id,lookup,lookup_unique) values (4,'apa','foo'),(5,'apa','bar'),(6,'monkey','nobar')`, expected)

	utils.Exec(t, conn, "rollback")
}

func TestVExplainPlan(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	// the test infra is adding \ to the test output
	utils.AssertMatchesContains(t, conn, `vexplain plan select id from user where lookup = "apa"`, `\"OperatorType\": \"VindexLookup\"`)
	utils.AssertMatchesContains(t, conn, `vexplain plan insert into user (id,lookup,lookup_unique) values (4,'apa','foo'),(5,'apa','bar'),(6,'monkey','nobar')`, "Insert")
}

func TestVExplainAll(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	utils.AssertMatchesContains(t, conn, `vexplain /*vt+ EXECUTE_DML_QUERIES */ all insert into user (id,lookup,lookup_unique) values (4,'apa','foo'),(5,'apa','bar'),(6,'monkey','nobar')`, "Insert", "mysql_explain_json")
	utils.AssertMatchesContains(t, conn, `vexplain all select id from user where lookup = "apa"`, "mysql_explain_json", "ByDestination")
}

func TestVExplainMySQLPlan(t *testing.T) {
	// VEXPLAIN MYSQLPLAN is a v25 syntax; an older vtgate cannot parse it. Under the
	// upgrade/downgrade CI this suite can run current test code against an N-1 vtgate.
	utils.SkipIfBinaryIsBelowVersion(t, 25, "vtgate")

	conn, closer := start(t)
	defer closer()

	// The result must carry the VTGate plan tree (Route) with real per-shard MySQL
	// EXPLAIN output attached, without executing the query.
	utils.AssertMatchesContains(t, conn,
		`vexplain mysqlplan select id from user where id = 1`,
		"mysql_explain_json_by_shard", "Route")

	// query_block is a key emitted only by a genuine MySQL EXPLAIN FORMAT=JSON, so it
	// proves we actually reached MySQL. MariaDB's EXPLAIN JSON does not use it, so gate
	// this assertion to MySQL/Percona 8.0+ (which covers the 8.0 and 8.4 CI flavors).
	shardedKeyspace := keyspaceByName(t, shardedKs)
	mysqlVersion := onlineddl.GetMySQLVersion(t, shardedKeyspace.Shards[0].PrimaryTablet())
	require.NotEmpty(t, mysqlVersion)
	atLeast80, err := capabilities.ServerVersionAtLeast(mysqlVersion, 8, 0, 0)
	require.NoError(t, err)
	if atLeast80 && !strings.Contains(mysqlVersion, "MariaDB") {
		utils.AssertMatchesContains(t, conn,
			`vexplain mysqlplan select id from user where id = 1`,
			"query_block")
	}

	// A scatter SELECT (no WHERE) fans out to every shard, so MYSQLPLAN must run
	// EXPLAIN against each and attach per-shard output keyed by shard name. Assert
	// the output carries at least two distinct shard names to prove the fan-out,
	// not just a single-shard EXPLAIN. Read the raw cell (real quotes) rather than
	// the %v-formatted rows (whose quotes the infra escapes) so the shard-key match
	// is unambiguous.
	scatter := utils.Exec(t, conn, `vexplain mysqlplan select id from user`)
	require.Len(t, scatter.Rows, 1)
	scatterOut := scatter.Rows[0][0].ToString()
	assert.Contains(t, scatterOut, "mysql_explain_json_by_shard")
	distinctShards := 0
	for _, shard := range shardedKsShards {
		if strings.Contains(scatterOut, fmt.Sprintf("%q", shard)) {
			distinctShards++
		}
	}
	assert.GreaterOrEqualf(t, distinctShards, 2,
		"expected per-shard EXPLAIN for at least 2 of shards %v, got output:\n%s", shardedKsShards, scatterOut)
	if atLeast80 && !strings.Contains(mysqlVersion, "MariaDB") {
		assert.Contains(t, scatterOut, "query_block")
	}

	// DML is not supported (its plans are not Route primitives): it must fail closed
	// and point the user to VEXPLAIN ALL, not silently produce a plan with no EXPLAIN.
	utils.AssertContainsError(t, conn,
		`vexplain mysqlplan insert into user (id,lookup,lookup_unique) values (99,'apa','apa')`,
		"use VEXPLAIN ALL instead")

	// A lookup vindex cannot resolve shards without executing, so it must fail closed
	// and point the user to VEXPLAIN ALL.
	utils.AssertContainsError(t, conn,
		`vexplain mysqlplan select id from user where lookup_unique = "apa"`,
		"use VEXPLAIN ALL instead")
}

// TestVExplainMySQLPlanReservedConn verifies that once a session holds a reserved
// connection (here, by creating a temporary table), VEXPLAIN MYSQLPLAN fails
// closed rather than reporting a plan from a separate connection that cannot see
// the session's temporary tables. The plain SELECT still succeeds on the reserved
// connection, which is exactly the asymmetry the rejection guards against.
func TestVExplainMySQLPlanReservedConn(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 25, "vtgate")

	// A dedicated connection, since creating a temp table pins it to a reserved
	// connection for the rest of its life.
	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	// Temporary tables are only allowed on an unsharded keyspace.
	utils.Exec(t, conn, "use "+keyspaceByName(t, unshardedKs).Name)
	utils.Exec(t, conn, `create temporary table temp_user(id bigint primary key)`)
	utils.Exec(t, conn, `insert into temp_user(id) values (1)`)

	// The real SELECT works on the reserved connection that can see the temp table.
	utils.AssertMatches(t, conn, `select id from temp_user`, `[[INT64(1)]]`)

	// But MYSQLPLAN must refuse rather than EXPLAIN on a connection that cannot see
	// the temp table. It must not point the user at VEXPLAIN ALL, which shares the
	// same standalone-EXPLAIN blind spot.
	_, err = utils.ExecAllowError(t, conn, `vexplain mysqlplan select id from temp_user`)
	require.ErrorContains(t, err, "reserved connection")
	require.NotContains(t, err.Error(), "VEXPLAIN ALL")
}

// TestVExplainMySQLPlanIntoOutfileNoSideEffect verifies that VEXPLAIN MYSQLPLAN of a
// SELECT ... INTO OUTFILE / INTO DUMPFILE returns a plan without error and, crucially,
// writes no file - proving that wrapping the query in EXPLAIN FORMAT=JSON does not
// execute its INTO clause. This is the end-to-end (vtgate -> vttablet -> mysqld)
// counterpart of the standalone MySQL check that motivated leaving INTO OUTFILE
// unguarded: it never runs the wrapped query, so there is no side effect to guard.
func TestVExplainMySQLPlanIntoOutfileNoSideEffect(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 25, "vtgate")

	// The unsharded keyspace has a single tablet, so its mysqld exposes one
	// unambiguous secure_file_priv directory that this test - running on the same
	// host - can inspect directly.
	tablet := keyspaceByName(t, unshardedKs).Shards[0].PrimaryTablet()
	res, err := tablet.VttabletProcess.QueryTablet(`select @@secure_file_priv`, unshardedKs, false)
	require.NoError(t, err)
	secureFilePriv := res.Named().Row().AsString("@@secure_file_priv", "")
	if secureFilePriv == "" {
		t.Skip("secure_file_priv is empty; INTO OUTFILE is disabled on this mysqld")
	}

	ctx := t.Context()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	utils.Exec(t, conn, "use "+unshardedKs)

	// A control real SELECT ... INTO OUTFILE must write a file, proving the write
	// path works on this mysqld - so a missing file after EXPLAIN below genuinely
	// means "not executed", not "OUTFILE is broken here".
	controlPath := filepath.Join(secureFilePriv, "vexplain_control.txt")
	utils.Exec(t, conn, `select id from u_user into outfile `+sqltypes.EncodeStringSQL(controlPath))
	t.Cleanup(func() { _ = os.Remove(controlPath) })
	require.FileExists(t, controlPath, "control real SELECT ... INTO OUTFILE did not write a file; the rest of this test is meaningless")

	// VEXPLAIN MYSQLPLAN of INTO OUTFILE / INTO DUMPFILE must return a plan and
	// write nothing: EXPLAIN FORMAT=JSON never executes the wrapped query.
	for _, tc := range []struct {
		name string
		into string
	}{
		{"outfile", "into outfile"},
		{"dumpfile", "into dumpfile"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(secureFilePriv, "vexplain_explain_"+tc.name+".txt")
			t.Cleanup(func() { _ = os.Remove(path) })

			utils.AssertMatchesContains(t, conn,
				fmt.Sprintf(`vexplain mysqlplan select id from u_user %s %s`, tc.into, sqltypes.EncodeStringSQL(path)),
				"mysql_explain_json_by_shard")
			require.NoFileExists(t, path, "VEXPLAIN MYSQLPLAN executed the wrapped query: %s was written", path)
		})
	}
}
