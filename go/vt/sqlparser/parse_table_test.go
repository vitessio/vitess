package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseTable(t *testing.T) {
	testcases := []struct {
		input    string
		keyspace string
		table    string
		err      bool
	}{{
		input:    "t",
		keyspace: "",
		table:    "t",
	}, {
		input:    "k.t",
		keyspace: "k",
		table:    "t",
	}, {
		input:    "`k.k`.`t``.t`",
		keyspace: "k.k",
		table:    "t`.t",
	}, {
		input: ".",
		err:   true,
	}, {
		input: "t,",
		err:   true,
	}, {
		input: "t,",
		err:   true,
	}, {
		input: "t..",
		err:   true,
	}, {
		input: "k.t.",
		err:   true,
	}}
	for _, tcase := range testcases {
		keyspace, table, err := ParseTable(tcase.input)
		assert.Equal(t, tcase.keyspace, keyspace)
		assert.Equal(t, tcase.table, table)
		if tcase.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}
}
