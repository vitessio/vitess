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

/**
This set of tests verifies that queries can be routed to global tables.

Global tables are tables that are either
 - Uniquely named across all keyspaces.
 - Reference tables that explicitly specify a source with the same name.

Global routing is applied when:
 - A connection does not specify a keyspace, and a query for a table is not
   keyspace-qualified.
 - A query is qualified with a user-defined --global-keyspace.

Additional behaviors tested here:
 - If a keyspace is defined in a VSchema, it takes precedence over any
   user-defined global keyspace.
*/

package global

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/utils"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

func start(t *testing.T, params *mysql.ConnParams) (*mysql.Conn, func()) {
	ctx := context.Background()
	vtConn, err := mysql.Connect(ctx, params)
	require.NoError(t, err)

	return vtConn, func() {
		vtConn.Close()
		cluster.PanicHandler(t)
	}
}

func TestGlobalRoutingWithoutKeyspace(t *testing.T) {
	conn, closer := start(t, &mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	})
	defer closer()

	// Unqualified queries for globally unique tables are routed
	// to their respective keyspaces.
	utils.Exec(t, conn, "INSERT INTO t1(id) VALUES(1), (2), (3), (4)")
	utils.AssertMatches(t, conn, "SELECT id FROM t1 ORDER BY id ASC", `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)]]`)
	utils.Exec(t, conn, "DELETE FROM t1")

	utils.Exec(t, conn, "INSERT INTO t2(id) VALUES(5), (6), (7), (8)")
	utils.AssertMatches(t, conn, "SELECT id FROM t2 ORDER BY id ASC", `[[INT64(5)] [INT64(6)] [INT64(7)] [INT64(8)]]`)
	utils.Exec(t, conn, "DELETE FROM t2")
}

func TestGlobalRoutingWithGlobalKeyspace(t *testing.T) {
	conn, closer := start(t, &mysql.ConnParams{
		Host:   "localhost",
		Port:   clusterInstance.VtgateMySQLPort,
		DbName: "vt_global",
	})
	defer closer()

	// Unqualified queries for globally unique tables are routed
	// to their respective keyspaces.
	utils.Exec(t, conn, "INSERT INTO t1(id) VALUES(9), (10), (11), (12)")
	utils.AssertMatches(t, conn, "SELECT id FROM t1 ORDER BY id ASC", `[[INT64(9)] [INT64(10)] [INT64(11)] [INT64(12)]]`)
	utils.Exec(t, conn, "DELETE FROM t1")

	utils.Exec(t, conn, "INSERT INTO t2(id) VALUES(13), (14), (15), (16)")
	utils.AssertMatches(t, conn, "SELECT id FROM t2 ORDER BY id ASC", `[[INT64(13)] [INT64(14)] [INT64(15)] [INT64(16)]]`)
	utils.Exec(t, conn, "DELETE FROM t2")
}

func TestUseUnshardedKeyspaceQualifyQueriesWithGlobalKeyspace(t *testing.T) {
	conn, closer := start(t, &mysql.ConnParams{
		Host:   "localhost",
		Port:   clusterInstance.VtgateMySQLPort,
		DbName: "uks",
	})
	defer closer()

	utils.Exec(t, conn, "INSERT INTO vt_global.t1(id) VALUES(17), (18), (19), (20)")
	utils.AssertMatches(t, conn, "SELECT id FROM vt_global.t1 ORDER BY id ASC", `[[INT64(17)] [INT64(18)] [INT64(19)] [INT64(20)]]`)
	utils.Exec(t, conn, "DELETE FROM vt_global.t1")

	utils.Exec(t, conn, "INSERT INTO vt_global.t2(id) VALUES(21), (22), (23), (24)")
	utils.AssertMatches(t, conn, "SELECT id FROM vt_global.t2 ORDER BY id ASC", `[[INT64(21)] [INT64(22)] [INT64(23)] [INT64(24)]]`)
	utils.Exec(t, conn, "DELETE FROM vt_global.t2")
}

func TestUseShardedKeyspaceQualifyQueriesWithGlobalKeyspace(t *testing.T) {
	// Create a connection using the sharded keyspace.
	conn, closer := start(t, &mysql.ConnParams{
		Host:   "localhost",
		Port:   clusterInstance.VtgateMySQLPort,
		DbName: "sks",
	})
	defer closer()

	// Queries qualified by --global-keyspace are routed to their respective
	// keyspaces.
	utils.Exec(t, conn, "INSERT INTO vt_global.t1(id) VALUES(25), (26), (27), (28)")
	utils.AssertMatches(t, conn, "SELECT id FROM vt_global.t1 ORDER BY id ASC", `[[INT64(25)] [INT64(26)] [INT64(27)] [INT64(28)]]`)
	utils.Exec(t, conn, "DELETE FROM vt_global.t1")

	utils.Exec(t, conn, "INSERT INTO vt_global.t2(id) VALUES(29), (30), (31), (32)")
	utils.AssertMatches(t, conn, "SELECT id FROM vt_global.t2 ORDER BY id ASC", `[[INT64(29)] [INT64(30)] [INT64(31)] [INT64(32)]]`)
	utils.Exec(t, conn, "DELETE FROM vt_global.t1")
}

func TestExistingVSchemaKeyspaceTakesPrecedenceOverGlobalKeyspace(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()

	// Create a connection without a default keyspace.
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	conn1, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn1.Close()

	utils.AssertContainsError(t, conn1, "INSERT INTO uks.t2(id) VALUES(1), (2), (3), (4)", "Table 'vt_uks.t2' doesn't exist")
}
