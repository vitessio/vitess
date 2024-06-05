/*
Copyright 2024 The Vitess Authors.

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

package transaction

import (
	_ "embed"
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"
)

// TestDTCommit tests transaction commit using twopc mode
func TestDTCommit(t *testing.T) {
	conn, closer := start(t)
	defer closer()

	// Insert into multiple shards
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "insert into twopc_user(id, name) values(7,'foo')")
	utils.Exec(t, conn, "insert into twopc_user(id, name) values(8,'bar')")
	utils.Exec(t, conn, "insert into twopc_user(id, name) values(9,'baz')")
	utils.Exec(t, conn, "insert into twopc_user(id, name) values(10,'apa')")
	utils.Exec(t, conn, "commit")

	// Verify the values are present in multiple shards
	utils.Exec(t, conn, "use `ks/-80`")
	utils.AssertMatches(t, conn,
		"select id, name from twopc_user order by id",
		`[[INT64(8) VARCHAR("bar")] [INT64(10) VARCHAR("apa")]]`)
	utils.Exec(t, conn, "use `ks/80-`")
	utils.AssertMatches(t, conn,
		"select id, name from twopc_user order by id",
		`[[INT64(7) VARCHAR("foo")] [INT64(9) VARCHAR("baz")]]`)
	utils.Exec(t, conn, "use `ks`")

	// Update from multiple shard
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "update twopc_user set name='newfoo' where id in (7,8)")
	utils.Exec(t, conn, "commit")

	// VERIFY that values are updated
	utils.AssertMatches(t, conn,
		"select id, name from twopc_user order by id",
		`[[INT64(7) VARCHAR("newfoo")] [INT64(8) VARCHAR("newfoo")] [INT64(9) VARCHAR("baz")] [INT64(10) VARCHAR("apa")]]`)

	// DELETE from multiple shard
	utils.Exec(t, conn, "begin")
	utils.Exec(t, conn, "delete from twopc_user")
	utils.Exec(t, conn, "commit")

	// VERIFY that values are deleted
	utils.AssertMatches(t, conn, "select id, name from twopc_user order by id", `[]`)
}
