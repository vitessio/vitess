/*
Copyright 2026 The Vitess Authors.

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
