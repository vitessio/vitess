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

package aggregation

import (
	"testing"

	"vitess.io/vitess/go/test/endtoend/utils"
)

func TestDistinct(t *testing.T) {
	mcmp, closer := start(t)
	defer closer()
	mcmp.Exec("insert into t3(id5,id6,id7) values(1,3,3), (2,3,4), (3,3,6), (4,5,7), (5,5,6)")
	mcmp.Exec("insert into t7_xxhash(uid,phone) values('1',4), ('2',4), ('3',3), ('4',1), ('5',1)")
	mcmp.Exec("insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'A',1), (3,'b',1), (4,'c',3), (5,'c',4)")
	mcmp.Exec("insert into aggr_test(id, val1, val2) values(6,'d',null), (7,'e',null), (8,'E',1)")
	mcmp.AssertMatches("select distinct val2, count(*) from aggr_test group by val2", `[[NULL INT64(2)] [INT64(1) INT64(4)] [INT64(3) INT64(1)] [INT64(4) INT64(1)]]`)
	mcmp.AssertMatches("select distinct id6 from t3 join t7_xxhash on t3.id5 = t7_xxhash.phone", `[[INT64(3)] [INT64(5)]]`)
}

func TestDistinctIt(t *testing.T) {
	// tests more variations of DISTINCT
	mcmp, closer := start(t)
	defer closer()

	mcmp.Exec("insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'A',1), (3,'b',1), (4,'c',3), (5,'c',4)")
	mcmp.Exec("insert into aggr_test(id, val1, val2) values(6,'d',null), (7,'e',null), (8,'E',1)")

	mcmp.AssertMatchesNoOrder("select distinct val1 from aggr_test", `[[VARCHAR("c")] [VARCHAR("d")] [VARCHAR("e")] [VARCHAR("a")] [VARCHAR("b")]]`)
	mcmp.AssertMatchesNoOrder("select distinct val2 from aggr_test", `[[INT64(1)] [INT64(4)] [INT64(3)] [NULL]]`)
	mcmp.AssertMatchesNoOrder("select distinct id from aggr_test", `[[INT64(1)] [INT64(2)] [INT64(3)] [INT64(5)] [INT64(4)] [INT64(6)] [INT64(7)] [INT64(8)]]`)

	if utils.BinaryIsAtVersion(17, "vtgate") {
		mcmp.AssertMatches("select /*vt+ PLANNER=Gen4 */ distinct val1 from aggr_test order by val1 desc", `[[VARCHAR("e")] [VARCHAR("d")] [VARCHAR("c")] [VARCHAR("b")] [VARCHAR("a")]]`)
		mcmp.AssertMatchesNoOrder("select /*vt+ PLANNER=Gen4 */ distinct val1, count(*) from aggr_test group by val1", `[[VARCHAR("a") INT64(2)] [VARCHAR("b") INT64(1)] [VARCHAR("c") INT64(2)] [VARCHAR("d") INT64(1)] [VARCHAR("e") INT64(2)]]`)
		mcmp.AssertMatchesNoOrder("select /*vt+ PLANNER=Gen4 */ distinct val1+val2 from aggr_test", `[[NULL] [FLOAT64(1)] [FLOAT64(3)] [FLOAT64(4)]]`)
	}
}
