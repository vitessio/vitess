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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
)

func TestRowCount(t *testing.T) {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()
	type tc struct {
		query    string
		expected int
	}
	tests := []tc{{
		query:    "insert into t1_row_count(id, id1) values(1, 1), (2, 1), (3, 3), (4, 3)",
		expected: 4,
	}, {
		query:    "select * from t1_row_count",
		expected: -1,
	}, {
		query:    "update t1_row_count set id1 = 500 where id in (1,3)",
		expected: 2,
	}, {
		query:    "show tables",
		expected: -1,
	}, {
		query:    "set @x = 24",
		expected: 0,
	}, {
		query:    "delete from t1_row_count",
		expected: 4,
	}}

	for _, test := range tests {
		t.Run(test.query, func(t *testing.T) {
			exec(t, conn, test.query)
			qr := exec(t, conn, "select row_count()")
			require.Equal(t, fmt.Sprintf("INT64(%d)", test.expected), qr.Rows[0][0].String())
		})
	}
}
