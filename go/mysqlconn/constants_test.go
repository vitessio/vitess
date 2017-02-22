package mysqlconn

import (
	"errors"
	"testing"

	"github.com/youtube/vitess/go/sqldb"
)

func TestIsConnErr(t *testing.T) {
	testcases := []struct {
		in   error
		want bool
	}{{
		in: errors.New("t"),
	}, {
		in: sqldb.NewSQLError(5, "", ""),
	}, {
		in:   sqldb.NewSQLError(CRServerGone, "", ""),
		want: true,
	}, {
		in: sqldb.NewSQLError(CRServerLost, "", ""),
	}, {
		in: sqldb.NewSQLError(CRCantReadCharset, "", ""),
	}}
	for _, tcase := range testcases {
		got := IsConnErr(tcase.in)
		if got != tcase.want {
			t.Errorf("IsConnErr(%#v): %v, want %v", tcase.in, got, tcase.want)
		}
	}
}
