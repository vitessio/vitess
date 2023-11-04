package topo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateObjectName(t *testing.T) {
	cases := []struct {
		name string
		err  string
	}{
		{
			name: "valid",
			err:  "",
		},
		{
			name: "validdigits1321",
			err:  "",
		},
		{
			name: "valid-with-dashes",
			err:  "",
		},
		{
			name: "very-long-keyspace-name-that-is-even-too-long-for-mysql-to-handle",
			err:  "name very-long-keyspace-name-that-is-even-too-long-for-mysql-to-handle is too long",
		},
		{
			name: "with<invalid>chars",
			err:  "invalid character < in name with<invalid>chars",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateObjectName(c.name)
			if c.err == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, c.err)
			}
		})
	}
}
