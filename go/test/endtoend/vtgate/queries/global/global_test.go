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

package global

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/utils"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

// TestGlobalKeyspaceRouting tests that unqualified queries and
// --global-keyspace qualified queries are globally routed.
func TestGlobalKeyspaceRouting(t *testing.T) {
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

	// Unqualified queries for globally unique tables are routed
	// to their respective keyspaces.
	utils.Exec(t, conn1, "INSERT INTO t1(id) VALUES(1), (2), (3), (4)")
	qr := utils.Exec(t, conn1, "SELECT id FROM t1 ORDER BY id ASC")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(4)]]`; got != want {
		t.Errorf("got:\n%v want\n%v", got, want)
	}
	utils.Exec(t, conn1, "DELETE FROM t1")

	utils.Exec(t, conn1, "INSERT INTO t2(id) VALUES(5), (6), (7), (8)")
	qr = utils.Exec(t, conn1, "SELECT id FROM t2 ORDER BY id ASC")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(5)] [INT64(6)] [INT64(7)] [INT64(8)]]`; got != want {
		t.Errorf("got:\n%v want\n%v", got, want)
	}
	utils.Exec(t, conn1, "DELETE FROM t2")

	// Create a connection using a --global-keyspace.
	vtParams = mysql.ConnParams{
		Host:   "localhost",
		Port:   clusterInstance.VtgateMySQLPort,
		DbName: "vt_global",
	}
	conn2, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn2.Close()

	// Unqualified queries for globally unique tables are routed
	// to their respective keyspaces.
	utils.Exec(t, conn2, "INSERT INTO t1(id) VALUES(9), (10), (11), (12)")
	qr = utils.Exec(t, conn2, "SELECT id FROM t1 ORDER BY id ASC")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(9)] [INT64(10)] [INT64(11)] [INT64(12)]]`; got != want {
		t.Errorf("got:\n%v want\n%v", got, want)
	}
	utils.Exec(t, conn2, "DELETE FROM t1")

	utils.Exec(t, conn2, "INSERT INTO t2(id) VALUES(13), (14), (15), (16)")
	qr = utils.Exec(t, conn2, "SELECT id FROM t2 ORDER BY id ASC")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(13)] [INT64(14)] [INT64(15)] [INT64(16)]]`; got != want {
		t.Errorf("got:\n%v want\n%v", got, want)
	}
	utils.Exec(t, conn2, "DELETE FROM t2")

	// Create a connection using the unsharded keyspace.
	vtParams = mysql.ConnParams{
		Host:   "localhost",
		Port:   clusterInstance.VtgateMySQLPort,
		DbName: "uks",
	}
	conn3, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn3.Close()

	// Queries qualified by --global-keyspace are routed to their respective
	// keyspaces.
	utils.Exec(t, conn3, "INSERT INTO vt_global.t1(id) VALUES(17), (18), (19), (20)")
	qr = utils.Exec(t, conn3, "SELECT id FROM vt_global.t1 ORDER BY id ASC")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(17)] [INT64(18)] [INT64(19)] [INT64(20)]]`; got != want {
		t.Errorf("got:\n%v want\n%v", got, want)
	}
	utils.Exec(t, conn3, "DELETE FROM vt_global.t1")

	utils.Exec(t, conn3, "INSERT INTO vt_global.t2(id) VALUES(21), (22), (23), (24)")
	qr = utils.Exec(t, conn3, "SELECT id FROM vt_global.t2 ORDER BY id ASC")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(21)] [INT64(22)] [INT64(23)] [INT64(24)]]`; got != want {
		t.Errorf("got:\n%v want\n%v", got, want)
	}
	utils.Exec(t, conn3, "DELETE FROM vt_global.t2")

	// Create a connection using the sharded keyspace.
	vtParams = mysql.ConnParams{
		Host:   "localhost",
		Port:   clusterInstance.VtgateMySQLPort,
		DbName: "sks",
	}
	conn4, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn4.Close()

	// Queries qualified by --global-keyspace are routed to their respective
	// keyspaces.
	utils.Exec(t, conn4, "INSERT INTO vt_global.t1(id) VALUES(25), (26), (27), (28)")
	qr = utils.Exec(t, conn4, "SELECT id FROM vt_global.t1 ORDER BY id ASC")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(25)] [INT64(26)] [INT64(27)] [INT64(28)]]`; got != want {
		t.Errorf("got:\n%v want\n%v", got, want)
	}
	utils.Exec(t, conn4, "DELETE FROM vt_global.t1")

	utils.Exec(t, conn4, "INSERT INTO vt_global.t2(id) VALUES(29), (30), (31), (32)")
	qr = utils.Exec(t, conn4, "SELECT id FROM vt_global.t2 ORDER BY id ASC")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[INT64(29)] [INT64(30)] [INT64(31)] [INT64(32)]]`; got != want {
		t.Errorf("got:\n%v want\n%v", got, want)
	}
	utils.Exec(t, conn4, "DELETE FROM vt_global.t1")
}

// TestExistingKeyspaceHidesGlobalKeyspace verifies that a pre-existing
// keyspace overrides a --global-keyspace with the same name.
func TestExistingKeyspaceHidesGlobalKeyspace(t *testing.T) {
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
