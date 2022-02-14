package mysql

import (
	"errors"
	"testing"
)

func TestIsConnErr(t *testing.T) {
	testcases := []struct {
		in   error
		want bool
	}{{
		in:   errors.New("t"),
		want: false,
	}, {
		in:   NewSQLError(5, "", ""),
		want: false,
	}, {
		in:   NewSQLError(CRServerGone, "", ""),
		want: true,
	}, {
		in:   NewSQLError(CRServerLost, "", ""),
		want: true,
	}, {
		in:   NewSQLError(ERQueryInterrupted, "", ""),
		want: true,
	}, {
		in:   NewSQLError(CRCantReadCharset, "", ""),
		want: false,
	}}
	for _, tcase := range testcases {
		got := IsConnErr(tcase.in)
		if got != tcase.want {
			t.Errorf("IsConnErr(%#v): %v, want %v", tcase.in, got, tcase.want)
		}
	}
}

func TestIsConnLostDuringQuery(t *testing.T) {
	testcases := []struct {
		in   error
		want bool
	}{{
		in:   errors.New("t"),
		want: false,
	}, {
		in:   NewSQLError(5, "", ""),
		want: false,
	}, {
		in:   NewSQLError(CRServerGone, "", ""),
		want: false,
	}, {
		in:   NewSQLError(CRServerLost, "", ""),
		want: true,
	}, {
		in:   NewSQLError(ERQueryInterrupted, "", ""),
		want: false,
	}, {
		in:   NewSQLError(CRCantReadCharset, "", ""),
		want: false,
	}}
	for _, tcase := range testcases {
		got := IsConnLostDuringQuery(tcase.in)
		if got != tcase.want {
			t.Errorf("IsConnLostDuringQuery(%#v): %v, want %v", tcase.in, got, tcase.want)
		}
	}
}
