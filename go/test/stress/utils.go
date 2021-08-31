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

func (s *Stresser) assertLength(conn *mysql.Conn, query string, expectedLength int) bool {
	s.t.Helper()
	qr := s.exec(conn, query)
	if qr == nil {
		return false
	}
	if diff := cmp.Diff(expectedLength, len(qr.Rows)); diff != "" {
		if s.cfg.PrintErrLogs {
			s.t.Logf("Query: %s (-want +got):\n%s", query, diff)
		}
		return false
	}
	return true
}

func (s *Stresser) exec(conn *mysql.Conn, query string) *sqltypes.Result {
	s.t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	if err != nil {
		if s.cfg.PrintErrLogs {
			s.t.Logf("Err: %s, for query: %s", err.Error(), query)
		}
		return nil
	}
	return qr
}

func newClient(t *testing.T, params *mysql.ConnParams) *mysql.Conn {
	ctx := context.Background()
	conn, err := mysql.Connect(ctx, params)
	require.NoError(t, err)
	return conn
}
