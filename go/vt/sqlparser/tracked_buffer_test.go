package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildParsedQuery(t *testing.T) {
	testcases := []struct {
		in   string
		args []interface{}
		out  string
	}{{
		in:  "select * from tbl",
		out: "select * from tbl",
	}, {
		in:  "select * from tbl where b=4 or a=3",
		out: "select * from tbl where b=4 or a=3",
	}, {
		in:  "select * from tbl where b = 4 or a = 3",
		out: "select * from tbl where b = 4 or a = 3",
	}, {
		in:   "select * from tbl where name='%s'",
		args: []interface{}{"xyz"},
		out:  "select * from tbl where name='xyz'",
	}}

	for _, tc := range testcases {
		t.Run(tc.in, func(t *testing.T) {
			parsed := BuildParsedQuery(tc.in, tc.args...)
			assert.Equal(t, tc.out, parsed.Query)
		})
	}
}
