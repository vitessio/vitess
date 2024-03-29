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

package misc

import (
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"
)

func TestCTEWithOrderByLimit(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 19, "vtgate")
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("with d as (select id,name from user order by id limit 2) select music.id from music join d on music.user_id = d.id")
}

func TestCTEAggregationOnRHS(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 19, "vtgate")
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("set sql_mode = ''")
	mcmp.Exec("with d as (select id, count(*) as a from user) select d.a from music join d on music.user_id = d.id group by 1")
}

func TestCTERemoveInnerOrderBy(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 19, "vtgate")
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("with toto as (select user.id as oui, music.id as non from user join music on user.id = music.user_id order by user.name) select count(*) from toto")
}

func TestCTEWithHaving(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 19, "vtgate")
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("set sql_mode = ''")
	// For the given query, we can get any id back, because we aren't grouping by it.
	mcmp.AssertMatchesAnyNoCompare("with s as (select id from user having count(*) >= 1) select * from s",
		"[[INT64(1)]]", "[[INT64(2)]]", "[[INT64(3)]]", "[[INT64(4)]]", "[[INT64(5)]]")
}

func TestCTEColumns(t *testing.T) {
	utils.SkipIfBinaryIsBelowVersion(t, 19, "vtgate")
	mcmp, closer := start(t)
	defer closer()

	mcmp.AssertMatches(`with t(id) as (SELECT id FROM user) SELECT t.id FROM t ORDER BY t.id DESC`,
		`[[INT64(5)] [INT64(4)] [INT64(3)] [INT64(2)] [INT64(1)]]`)
}
