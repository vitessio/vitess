package sqlparser

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTruncateQuery(t *testing.T) {
	tests := []struct {
		query string
		max   int
		want  string
	}{
		{
			query: "select * from test where name = 'abc'",
			max:   30,
			want:  "select * from test [TRUNCATED]",
		},
		{
			query: "select * from test where name = 'abc'",
			max:   1005,
			want:  "select * from test where name = 'abc'",
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s-%d", tt.query, tt.max), func(t *testing.T) {
			assert.Equalf(t, tt.want, truncateQuery(tt.query, tt.max), "truncateQuery(%v, %v)", tt.query, tt.max)
		})
	}
}
