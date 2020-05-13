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
	"testing"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/mysql"
)

func TestRowCount(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	exec(t, conn, "insert into t1_row_count(id, id1) values(1, 1), (2, 1), (3, 3), (4, 3)")
	qr := exec(t, conn, "select row_count()")
	require.Equal(t, "INT64(4)", qr.Rows[0][0].String())

	exec(t, conn, "select * from t1_row_count")
	qr = exec(t, conn, "select row_count()")
	require.Equal(t, "INT64(-1)", qr.Rows[0][0].String())

	exec(t, conn, "update t1_row_count set id1 = 500 where id in (1,3)")
	qr = exec(t, conn, "select row_count()")
	require.Equal(t, "INT64(2)", qr.Rows[0][0].String())

	exec(t, conn, "show tables")
	qr = exec(t, conn, "select row_count()")
	require.Equal(t, "INT64(-1)", qr.Rows[0][0].String())

	exec(t, conn, "set @x = 24")
	qr = exec(t, conn, "select row_count()")
	require.Equal(t, "INT64(0)", qr.Rows[0][0].String())

	exec(t, conn, "delete from t1_row_count")
	qr = exec(t, conn, "select row_count()")
	require.Equal(t, "INT64(4)", qr.Rows[0][0].String())
}
