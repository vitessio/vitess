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

func TestFileConfigUnmarshalYAML(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		yaml   string
		config FileConfig
		err    error
	}{
		{
			name: "simple",
			yaml: `defaults:
    discovery: consul
    discovery-consul-vtctld-datacenter-tmpl: "dev-{{ .Cluster.Name }}"
    discovery-consul-vtctld-service-name: vtctld-svc
    discovery-consul-vtctld-addr-tmpl: "{{ .Hostname }}.example.com:15000"
    discovery-consul-vtgate-datacenter-tmpl: "dev-{{ .Cluster.Name }}"
    discovery-consul-vtgate-service-name: vtgate-svc
    discovery-consul-vtgate-pool-tag: type
    discovery-consul-vtgate-cell-tag: zone
    discovery-consul-vtgate-addr-tmpl: "{{ .Hostname }}.example.com:15999"

clusters:
    c1:
        name: testcluster1
        discovery-consul-vtgate-datacenter-tmpl: "dev-{{ .Cluster.Name }}-test"
    c2:
        name: devcluster`,
			config: FileConfig{
				Defaults: Config{
					DiscoveryImpl: "consul",
					DiscoveryFlagsByImpl: map[string]map[string]string{
						"consul": {
							"vtctld-datacenter-tmpl": "dev-{{ .Cluster.Name }}",
							"vtctld-service-name":    "vtctld-svc",
							"vtctld-addr-tmpl":       "{{ .Hostname }}.example.com:15000",
							"vtgate-datacenter-tmpl": "dev-{{ .Cluster.Name }}",
							"vtgate-service-name":    "vtgate-svc",
							"vtgate-pool-tag":        "type",
							"vtgate-cell-tag":        "zone",
							"vtgate-addr-tmpl":       "{{ .Hostname }}.example.com:15999",
						},
					},
				},
				Clusters: map[string]Config{
					"c1": {
						ID:   "c1",
						Name: "testcluster1",
						DiscoveryFlagsByImpl: map[string]map[string]string{
							"consul": {
								"vtgate-datacenter-tmpl": "dev-{{ .Cluster.Name }}-test",
							},
						},
						VtSQLFlags:  map[string]string{},
						VtctldFlags: map[string]string{},
					},
					"c2": {
						ID:                   "c2",
						Name:                 "devcluster",
						DiscoveryFlagsByImpl: map[string]map[string]string{},
						VtSQLFlags:           map[string]string{},
						VtctldFlags:          map[string]string{},
					},
				},
			},
			err: nil,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := FileConfig{
				Defaults: Config{
					DiscoveryFlagsByImpl: map[string]map[string]string{},
				},
				Clusters: map[string]Config{},
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

func TestCombine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		fc       FileConfig
		defaults Config
		configs  map[string]Config
		expected []Config
	}{
		{
			name: "default overrides file",
			fc: FileConfig{
				Defaults: Config{
					DiscoveryImpl: "consul",
					DiscoveryFlagsByImpl: map[string]map[string]string{
						"consul": {
							"vtgate-datacenter-tmpl": "dev-{{ .Cluster }}",
						},
					},
				},
			},
			defaults: Config{
				DiscoveryImpl:        "zk",
				DiscoveryFlagsByImpl: map[string]map[string]string{},
			},
			configs: map[string]Config{
				"1": {
					ID:   "1",
					Name: "one",
				},
				"2": {
					ID:   "2",
					Name: "two",
					DiscoveryFlagsByImpl: map[string]map[string]string{
						"consul": {
							"vtgate-datacenter-tmpl": "dev-{{ .Cluster }}-test",
						},
					},
				},
			},
			expected: []Config{
				{
					ID:            "1",
					Name:          "one",
					DiscoveryImpl: "zk",
					DiscoveryFlagsByImpl: map[string]map[string]string{
						"consul": {
							"vtgate-datacenter-tmpl": "dev-{{ .Cluster }}",
						},
					},
					VtSQLFlags:  map[string]string{},
					VtctldFlags: map[string]string{},
				},
				{
					ID:            "2",
					Name:          "two",
					DiscoveryImpl: "zk",
					DiscoveryFlagsByImpl: map[string]map[string]string{
						"consul": {
							"vtgate-datacenter-tmpl": "dev-{{ .Cluster }}-test",
						},
					},
					VtSQLFlags:  map[string]string{},
					VtctldFlags: map[string]string{},
				},
			},
		},
		{
			name: "mixed",
			fc: FileConfig{
				Defaults: Config{
					DiscoveryImpl: "consul",
				},
				Clusters: map[string]Config{
					"c1": {
						ID:   "c1",
						Name: "cluster1",
					},
					"c2": {
						ID:   "c2",
						Name: "cluster2",
					},
				},
			},
			defaults: Config{
				DiscoveryFlagsByImpl: map[string]map[string]string{
					"zk": {
						"flag": "val",
					},
				},
			},
			configs: map[string]Config{
				"c1": {
					ID:   "c1",
					Name: "cluster1",
				},
				"c3": {
					ID:   "c3",
					Name: "cluster3",
				},
			},
			expected: []Config{
				{
					ID:            "c1",
					Name:          "cluster1",
					DiscoveryImpl: "consul",
					DiscoveryFlagsByImpl: map[string]map[string]string{
						"zk": {
							"flag": "val",
						},
					},
					VtSQLFlags:  map[string]string{},
					VtctldFlags: map[string]string{},
				},
				{
					ID:            "c2",
					Name:          "cluster2",
					DiscoveryImpl: "consul",
					DiscoveryFlagsByImpl: map[string]map[string]string{
						"zk": {
							"flag": "val",
						},
					},
					VtSQLFlags:  map[string]string{},
					VtctldFlags: map[string]string{},
				},
				{
					ID:            "c3",
					Name:          "cluster3",
					DiscoveryImpl: "consul",
					DiscoveryFlagsByImpl: map[string]map[string]string{
						"zk": {
							"flag": "val",
						},
					},
					VtSQLFlags:  map[string]string{},
					VtctldFlags: map[string]string{},
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			actual := tt.fc.Combine(tt.defaults, tt.configs)
			assert.ElementsMatch(t, tt.expected, actual)
		})
	}
}
