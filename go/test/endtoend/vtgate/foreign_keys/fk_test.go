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

package foreign_keys

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/utils"
)

// TestInsertions tests that insertions work as expected when foreign key management is enabled in Vitess.
func TestInsertions(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()

	// insert some data.
	utils.Exec(t, mcmp.VtConn, `insert into t1(id, col) values (100, 123),(10, 12),(1, 13),(1000, 1234)`)

	// Verify that inserting data into a table that has shard scoped foreign keys works.
	utils.Exec(t, mcmp.VtConn, `insert into t2(id, col) values (100, 125), (1, 132)`)
	// Verify that insertion fails if the data doesn't follow the fk constraint.
	_, err := utils.ExecAllowError(t, mcmp.VtConn, `insert into t2(id, col) values (1310, 125)`)
	require.ErrorContains(t, err, "Cannot add or update a child row: a foreign key constraint fails")
	// Verify that insertion fails if the table has cross-shard foreign keys (even if the data follows the constraints).
	_, err = utils.ExecAllowError(t, mcmp.VtConn, `insert into t3(id, col) values (100, 100)`)
	require.ErrorContains(t, err, "VT12002: unsupported cross-shard foreign keys")

	// insert some data in a table with multicol vindex.
	utils.Exec(t, mcmp.VtConn, `insert into multicol_tbl1(cola, colb, colc, msg) values (100, 'a', 'b', 'msg'), (101, 'c', 'd', 'msg2')`)
	// Verify that inserting data into a table that has shard scoped multi-column foreign keys works.
	utils.Exec(t, mcmp.VtConn, `insert into multicol_tbl2(cola, colb, colc, msg) values (100, 'a', 'b', 'msg3')`)
	// Verify that insertion fails if the data doesn't follow the fk constraint.
	_, err = utils.ExecAllowError(t, mcmp.VtConn, `insert into multicol_tbl2(cola, colb, colc, msg) values (103, 'c', 'd', 'msg2')`)
	require.ErrorContains(t, err, "Cannot add or update a child row: a foreign key constraint fails")
}
