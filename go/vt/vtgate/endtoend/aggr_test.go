/*
Copyright 2019 The Vitess Authors.

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

package endtoend

import (
	"context"
	"fmt"
	"testing"

	"vitess.io/vitess/go/mysql"
)

func TestAggregateTypes(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	exec(t, conn, "insert into aggr_test(id, val1, val2) values(1,'a',1), (2,'a',1), (3,'b',1), (4,'c',3), (5,'c',4)")
	exec(t, conn, "insert into aggr_test(id, val1, val2) values(6,'d',null), (7,'e',null), (8,'e',1)")

	qr := exec(t, conn, "select val1, count(distinct val2), count(*) from aggr_test group by val1")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[VARBINARY("a") INT64(1) INT64(2)] [VARBINARY("b") INT64(1) INT64(1)] [VARBINARY("c") INT64(2) INT64(2)] [VARBINARY("d") INT64(0) INT64(1)] [VARBINARY("e") INT64(1) INT64(2)]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}

	qr = exec(t, conn, "select val1, sum(distinct val2), sum(val2) from aggr_test group by val1")
	if got, want := fmt.Sprintf("%v", qr.Rows), `[[VARBINARY("a") DECIMAL(1) DECIMAL(2)] [VARBINARY("b") DECIMAL(1) DECIMAL(1)] [VARBINARY("c") DECIMAL(7) DECIMAL(7)] [VARBINARY("d") NULL NULL] [VARBINARY("e") DECIMAL(1) DECIMAL(1)]]`; got != want {
		t.Errorf("select:\n%v want\n%v", got, want)
	}
}
