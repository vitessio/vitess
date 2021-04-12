/*
Copyright 2020 The Vitess Authors.

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

package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestMergeConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		base     Config
		override Config
		expected Config
	}{
		{
			name: "no flags",
			base: Config{
				ID:   "c1",
				Name: "cluster1",
			},
			override: Config{
				DiscoveryImpl: "consul",
			},
			expected: Config{
				ID:                   "c1",
				Name:                 "cluster1",
				DiscoveryImpl:        "consul",
				DiscoveryFlagsByImpl: FlagsByImpl{},
				VtSQLFlags:           map[string]string{},
				VtctldFlags:          map[string]string{},
			},
		},
		{
			name: "merging discovery flags",
			base: Config{
				ID:   "c1",
				Name: "cluster1",
				DiscoveryFlagsByImpl: map[string]map[string]string{
					"consul": {
						"key1": "val1",
					},
					"zk": {
						"foo": "bar",
					},
				},
				VtSQLFlags: map[string]string{},
			},
			override: Config{
				DiscoveryFlagsByImpl: map[string]map[string]string{
					"zk": {
						"foo": "baz",
					},
				},
			},
			expected: Config{
				ID:   "c1",
				Name: "cluster1",
				DiscoveryFlagsByImpl: map[string]map[string]string{
					"consul": {
						"key1": "val1",
					},
					"zk": {
						"foo": "baz",
					},
				},
				VtSQLFlags:  map[string]string{},
				VtctldFlags: map[string]string{},
			},
		},
		{
			name: "merging vtsql/vtctld flags",
			base: Config{
				ID:   "c1",
				Name: "cluster1",
				VtSQLFlags: map[string]string{
					"one": "one",
					"two": "2",
				},
				VtctldFlags: map[string]string{
					"a": "A",
					"b": "B",
				},
			},
			override: Config{
				ID:   "c1",
				Name: "cluster1",
				VtSQLFlags: map[string]string{
					"two":   "two",
					"three": "three",
				},
				VtctldFlags: map[string]string{
					"a": "alpha",
					"c": "C",
				},
			},
			expected: Config{
				ID:                   "c1",
				Name:                 "cluster1",
				DiscoveryFlagsByImpl: FlagsByImpl{},
				VtSQLFlags: map[string]string{
					"one":   "one",
					"two":   "two",
					"three": "three",
				},
				VtctldFlags: map[string]string{
					"a": "alpha",
					"b": "B",
					"c": "C",
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			actual := tt.base.Merge(tt.override)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestConfigUnmarshalYAML(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		yaml   string
		config Config
		err    error
	}{
		{
			name: "simple",
			yaml: `name: cluster1
id: c1`,
			config: Config{
				ID:                   "c1",
				Name:                 "cluster1",
				DiscoveryFlagsByImpl: map[string]map[string]string{},
			},
			err: nil,
		},
		{
			name: "discovery flags",
			yaml: `name: cluster1
id: c1
discovery: consul
discovery-consul-vtgate-datacenter-tmpl: "dev-{{ .Cluster }}"
discovery-zk-whatever: 5
`,
			config: Config{
				ID:            "c1",
				Name:          "cluster1",
				DiscoveryImpl: "consul",
				DiscoveryFlagsByImpl: map[string]map[string]string{
					"consul": {
						"vtgate-datacenter-tmpl": "dev-{{ .Cluster }}",
					},
					"zk": {
						"whatever": "5",
					},
				},
			},
		},
		{
			name:   "errors",
			yaml:   `name: "cluster1`,
			config: Config{},
			err:    assert.AnError,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := Config{
				DiscoveryFlagsByImpl: map[string]map[string]string{},
			}

			err := yaml.Unmarshal([]byte(tt.yaml), &cfg)
			if tt.err != nil {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.config, cfg)
		})
	}
}
