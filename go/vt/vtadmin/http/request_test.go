/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
