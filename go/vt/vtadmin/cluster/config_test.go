package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
