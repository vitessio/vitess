/*
Copyright 2023 The Vitess Authors.

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
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeprecateQueryParam(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in       map[string][]string
		oldName  string
		newName  string
		expected []string
	}{
		{
			in: map[string][]string{
				"foo":     {"1", "2"},
				"old_bar": {"one", "two"},
			},
			oldName:  "old_bar",
			newName:  "bar",
			expected: []string{"one", "two"},
		},
		{
			in: map[string][]string{
				"foo": {"1", "2"},
				"bar": {"one", "two"},
			},
			oldName:  "old_bar",
			newName:  "bar",
			expected: []string{"one", "two"},
		},
		{
			in: map[string][]string{
				"foo":     {"1", "2"},
				"old_bar": {"one", "three"},
				"bar":     {"one", "two"},
			},
			oldName:  "old_bar",
			newName:  "bar",
			expected: []string{"one", "two", "three"},
		},
		{
			in: map[string][]string{
				"foo": {"1", "2"},
			},
			oldName:  "old_bar",
			newName:  "bar",
			expected: nil,
		},
	}

	for _, tcase := range cases {
		tcase := tcase
		t.Run("", func(t *testing.T) {
			query := url.Values(tcase.in)

			r := http.Request{
				URL: &url.URL{RawQuery: query.Encode()},
			}

			deprecateQueryParam(&r, tcase.newName, tcase.oldName)

			assert.False(t, r.URL.Query().Has(tcase.oldName), "old query param (%s) should not be in transformed query", tcase.oldName)
			assert.Equal(t, tcase.expected, r.URL.Query()[tcase.newName])
		})
	}
}
