package http

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseQueryParamAsBool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		fragment     string
		param        string
		defaultValue bool
		expected     bool
		shouldErr    bool
	}{
		{
			name:         "successful parse",
			fragment:     "?a=true&b=false",
			param:        "a",
			defaultValue: false,
			expected:     true,
			shouldErr:    false,
		},
		{
			name:         "no query fragment",
			fragment:     "",
			param:        "active_only",
			defaultValue: false,
			expected:     false,
			shouldErr:    false,
		},
		{
			name:         "param not set",
			fragment:     "?foo=bar",
			param:        "baz",
			defaultValue: true,
			expected:     true,
			shouldErr:    false,
		},
		{
			name:         "param not bool-like",
			fragment:     "?foo=bar",
			param:        "foo",
			defaultValue: false,
			shouldErr:    true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rawurl := fmt.Sprintf("http://example.com/%s", tt.fragment)
			u, err := url.Parse(rawurl)
			require.NoError(t, err, "could not parse %s", rawurl)

			r := Request{
				&http.Request{URL: u},
			}

			val, err := r.ParseQueryParamAsBool(tt.param, tt.defaultValue)
			if tt.shouldErr {
				assert.Error(t, err)

				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, val)
		})
	}
}
