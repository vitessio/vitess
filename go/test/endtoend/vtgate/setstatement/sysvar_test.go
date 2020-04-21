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

package setstatement

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

func TestSetSysVar(t *testing.T) {
	defer cluster.PanicHandler(t)
	ctx := context.Background()
	vtParams := mysql.ConnParams{
		Host: "localhost",
		Port: clusterInstance.VtgateMySQLPort,
	}
	type queriesWithExpectations struct {
		query           string
		expectedRows    string
		rowsAffected    int
		errMsg          string
		expectedWarning string
	}

	queries := []queriesWithExpectations{{
		query:        `set @@default_storage_engine = INNODB`,
		expectedRows: ``, rowsAffected: 0,
		expectedWarning: "[[VARCHAR(\"Warning\") UINT16(1235) VARCHAR(\"Ignored inapplicable SET default_storage_engine = INNODB\")]]",
	}, {
		query:        `set @@sql_mode = @@sql_mode`,
		expectedRows: ``, rowsAffected: 0,
	}, {
		query:        `set @@sql_mode = concat(@@sql_mode,"")`,
		expectedRows: ``, rowsAffected: 0,
	}, {
		query:           `set @@sql_mode = concat(@@sql_mode,"ALLOW_INVALID_DATES")`,
		expectedWarning: "[[VARCHAR(\"Warning\") UINT16(1235) VARCHAR(\"Modification not allowed using set construct for: sql_mode\")]]",
	}}

	conn, err := mysql.Connect(ctx, &vtParams)
	require.NoError(t, err)
	defer conn.Close()

	for i, q := range queries {
		t.Run(fmt.Sprintf("%d-%s", i, q.query), func(t *testing.T) {
			qr, err := exec(t, conn, q.query)
			if q.errMsg != "" {
				require.Contains(t, err.Error(), q.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, uint64(q.rowsAffected), qr.RowsAffected, "rows affected wrong for query: %s", q.query)
				if q.expectedRows != "" {
					result := fmt.Sprintf("%v", qr.Rows)
					if diff := cmp.Diff(q.expectedRows, result); diff != "" {
						t.Errorf("%s\nfor query: %s", diff, q.query)
					}
				}
				if q.expectedWarning != "" {
					qr, err := exec(t, conn, "show warnings")
					require.NoError(t, err)
					if got, want := fmt.Sprintf("%v", qr.Rows), q.expectedWarning; got != want {
						t.Errorf("select:\n%v want\n%v", got, want)
					}
				}
			}
		})
	}
}
