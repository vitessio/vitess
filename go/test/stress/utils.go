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
