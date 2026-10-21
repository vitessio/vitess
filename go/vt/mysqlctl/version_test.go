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

package mysqlctl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestServerVersion_CompareForReplication(t *testing.T) {
	tests := []struct {
		name string
		a    ServerVersion
		b    ServerVersion
		want int
	}{
		{
			name: "same version",
			a:    ServerVersion{8, 0, 35},
			b:    ServerVersion{8, 0, 35},
			want: 0,
		},
		{
			name: "lower minor",
			a:    ServerVersion{8, 0, 35},
			b:    ServerVersion{8, 4, 0},
			want: -1,
		},
		{
			name: "higher minor",
			a:    ServerVersion{8, 4, 0},
			b:    ServerVersion{8, 0, 35},
			want: 1,
		},
		{
			name: "lower major",
			a:    ServerVersion{5, 7, 40},
			b:    ServerVersion{8, 0, 35},
			want: -1,
		},
		{
			name: "8.0 patch ignored when both at or above 8.0.34",
			a:    ServerVersion{8, 0, 34},
			b:    ServerVersion{8, 0, 40},
			want: 0,
		},
		{
			name: "8.0 patch significant when lower patch is below 8.0.34",
			a:    ServerVersion{8, 0, 20},
			b:    ServerVersion{8, 0, 35},
			want: -1,
		},
		{
			name: "8.0 patch significant, higher wins comparison",
			a:    ServerVersion{8, 0, 35},
			b:    ServerVersion{8, 0, 20},
			want: 1,
		},
		{
			name: "8.0 patch significant between two pre-34 patches",
			a:    ServerVersion{8, 0, 20},
			b:    ServerVersion{8, 0, 30},
			want: -1,
		},
		{
			name: "8.0 equal pre-34 patches",
			a:    ServerVersion{8, 0, 20},
			b:    ServerVersion{8, 0, 20},
			want: 0,
		},
		{
			name: "8.4 patch ignored (not the 8.0 series)",
			a:    ServerVersion{8, 4, 0},
			b:    ServerVersion{8, 4, 5},
			want: 0,
		},
		{
			name: "5.7 patch ignored (not the 8.0 series)",
			a:    ServerVersion{5, 7, 10},
			b:    ServerVersion{5, 7, 40},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.a.CompareForReplication(tt.b)
			assert.Equal(t, tt.want, got)
			// Comparison must be antisymmetric.
			assert.Equal(t, -tt.want, tt.b.CompareForReplication(tt.a))
		})
	}
}
