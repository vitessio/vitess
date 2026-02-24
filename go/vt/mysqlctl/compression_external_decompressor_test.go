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

func TestResolveExternalDecompressor(t *testing.T) {
	tests := []struct {
		name                 string
		cliDecompressorCmd   string
		useManifest          bool
		manifestDecompressor string
		expected             string
	}{
		{
			name:                 "CLI flag takes precedence over manifest",
			cliDecompressorCmd:   "zstd -d",
			useManifest:          true,
			manifestDecompressor: "gzip -d",
			expected:             "zstd -d",
		},
		{
			name:                 "CLI flag takes precedence even when use-manifest is false",
			cliDecompressorCmd:   "zstd -d",
			useManifest:          false,
			manifestDecompressor: "gzip -d",
			expected:             "zstd -d",
		},
		{
			name:                 "manifest used when use-manifest is true and no CLI flag",
			cliDecompressorCmd:   "",
			useManifest:          true,
			manifestDecompressor: "gzip -d",
			expected:             "gzip -d",
		},
		{
			name:                 "manifest ignored when use-manifest is false",
			cliDecompressorCmd:   "",
			useManifest:          false,
			manifestDecompressor: "gzip -d",
			expected:             "",
		},
		{
			name:                 "empty when nothing is set",
			cliDecompressorCmd:   "",
			useManifest:          false,
			manifestDecompressor: "",
			expected:             "",
		},
		{
			name:                 "empty when use-manifest is true but manifest is empty",
			cliDecompressorCmd:   "",
			useManifest:          true,
			manifestDecompressor: "",
			expected:             "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			origCmd := ExternalDecompressorCmd
			origAllow := ExternalDecompressorUseManifest
			t.Cleanup(func() {
				ExternalDecompressorCmd = origCmd
				ExternalDecompressorUseManifest = origAllow
			})

			ExternalDecompressorCmd = tt.cliDecompressorCmd
			ExternalDecompressorUseManifest = tt.useManifest

			result := resolveExternalDecompressor(tt.manifestDecompressor)
			assert.Equal(t, tt.expected, result)
		})
	}
}
