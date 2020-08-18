/*
Copyright 2020 The Vitess Authors.

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

package vtgate

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

func TestFoundRows(t *testing.T) {
	t.Skip("failing at the moment")
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.Nil(t, err)
	defer conn.Close()

	exec(t, conn, "insert into t2(id3,id4) values(1,1), (2,2), (3,3), (4,4), (5,5)")
	defer exec(t, conn, "delete from t2")

	assertFoundRowsValue(t, conn, "select * from t2", 5)
	assertFoundRowsValue(t, conn, "select * from t2 limit 2", 2)
	assertFoundRowsValue(t, conn, "select SQL_CALC_FOUND_ROWS * from t2 limit 2", 5)
}

func assertFoundRowsValue(t *testing.T, conn *mysql.Conn, query string, count int) {
	exec(t, conn, query)
	qr := exec(t, conn, "select found_rows()")
	got := fmt.Sprintf("%v", qr.Rows)
	want := fmt.Sprintf(`[[UINT64(%d)]]`, count)
	require.Equal(t, want, got)
}
