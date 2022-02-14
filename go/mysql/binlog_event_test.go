package mysql

import (
	"strings"
	"testing"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

func TestQueryString(t *testing.T) {
	input := Query{
		Database: "test_database",
		Charset: &binlogdatapb.Charset{
			Client: 12,
			Conn:   34,
			Server: 56,
		},
		SQL: "sql",
	}
	want := `{Database: "test_database", Charset:`
	got := input.String()
	if !strings.HasPrefix(got, want) {
		t.Errorf("%#v.String() = %#v, want %#v", input, got, want)
	}
}

func TestQueryStringNilCharset(t *testing.T) {
	input := Query{
		Database: "test_database",
		Charset:  nil,
		SQL:      "sql",
	}
	want := `{Database: "test_database", Charset: <nil>, SQL: "sql"}`
	if got := input.String(); got != want {
		t.Errorf("%#v.String() = %#v, want %#v", input, got, want)
	}
}

func TestBinlogFormatIsZero(t *testing.T) {
	table := map[*BinlogFormat]bool{
		{}:                 true,
		{FormatVersion: 1}: false,
		{HeaderLength: 1}:  false,
	}
	for input, want := range table {
		if got := input.IsZero(); got != want {
			t.Errorf("%#v.IsZero() = %#v, want %#v", input, got, want)
		}
	}
}
