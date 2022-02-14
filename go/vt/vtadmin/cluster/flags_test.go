package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMergeFlagsByImpl(t *testing.T) {
	t.Parallel()

	var NilMap map[string]map[string]string

	tests := []struct {
		name     string
		base     map[string]map[string]string
		in       map[string]map[string]string
		expected map[string]map[string]string
	}{
		{
			name:     "nil",
			base:     nil,
			in:       nil,
			expected: map[string]map[string]string{},
		},
		{
			name:     "wrapped nil",
			base:     NilMap,
			in:       NilMap,
			expected: map[string]map[string]string{},
		},
		{
			name: "all overrides",
			base: nil,
			in: map[string]map[string]string{
				"consul": {
					"flag1": "value1",
				},
			},
			expected: map[string]map[string]string{
				"consul": {
					"flag1": "value1",
				},
			},
		},
		{
			name: "all defaults",
			base: map[string]map[string]string{
				"consul": {
					"flag1": "value1",
				},
			},
			in: nil,
			expected: map[string]map[string]string{
				"consul": {
					"flag1": "value1",
				},
			},
		},
		{
			name: "mixed",
			base: map[string]map[string]string{
				"consul": {
					"flag1": "value1",
					"flag2": "value2",
				},
				"other": {},
			},
			in: map[string]map[string]string{
				"consul": {
					"flag1": "othervalue",
				},
			},
			expected: map[string]map[string]string{
				"consul": {
					"flag1": "othervalue",
					"flag2": "value2",
				},
				"other": {},
			},
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			flags := FlagsByImpl(tt.base)
			flags.Merge(tt.in)
			assert.Equal(t, FlagsByImpl(tt.expected), flags)
		})
	}
}
