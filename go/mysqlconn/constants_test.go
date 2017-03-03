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
		in:   errors.New("t"),
		want: false,
	}, {
		in:   sqldb.NewSQLError(5, "", ""),
		want: false,
	}, {
		in:   sqldb.NewSQLError(CRServerGone, "", ""),
		want: true,
	}, {
		in:   sqldb.NewSQLError(CRServerLost, "", ""),
		want: false,
	}, {
		in:   sqldb.NewSQLError(CRCantReadCharset, "", ""),
		want: false,
	}}
	for _, tcase := range testcases {
		got := IsConnErr(tcase.in)
		if got != tcase.want {
			t.Errorf("IsConnErr(%#v): %v, want %v", tcase.in, got, tcase.want)
		}
	}
}
