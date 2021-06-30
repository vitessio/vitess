/*
Copyright 2021 The Vitess Authors.

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

package stress

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
)

func assertLength(t *testing.T, conn *mysql.Conn, query string, expectedLength int) {
	t.Helper()
	qr := exec(t, conn, query)
	got := len(qr.Rows)
	diff := cmp.Diff(expectedLength, got)
	if diff != "" {
		t.Errorf("Query: %s (-want +got):\n%s", query, diff)
	}
}

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.NoError(t, err, "for query: "+query)
	return qr
}

func newClient(t *testing.T, params mysql.ConnParams) *mysql.Conn {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, &params)
	require.NoError(t, err)
	return conn
}
