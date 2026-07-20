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

package reshard

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateShardFlags(t *testing.T) {
	tests := []struct {
		name         string
		sourceShards []string
		targetShards []string
		wantErr      string
	}{
		{name: "unsharded to sharded", sourceShards: []string{"0"}, targetShards: []string{"-80", "80-"}},
		{name: "sharded to sharded", sourceShards: []string{"-80", "80-"}, targetShards: []string{"-40", "40-80", "80-"}},
		{name: "missing source shards", targetShards: []string{"-80", "80-"}, wantErr: "--source-shards is required"},
		{name: "missing target shards", sourceShards: []string{"0"}, wantErr: "--target-shards is required"},
		{name: "malformed source shard", sourceShards: []string{"x80-"}, targetShards: []string{"-80", "80-"}, wantErr: "invalid --source-shards value"},
		{name: "malformed target shard", sourceShards: []string{"0"}, targetShards: []string{"80"}, wantErr: "invalid --target-shards value"},
		{name: "empty string shard", sourceShards: []string{""}, targetShards: []string{"-80", "80-"}, wantErr: "invalid --source-shards value"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				reshardCreateOptions.sourceShards = nil
				reshardCreateOptions.targetShards = nil
			}()
			reshardCreateOptions.sourceShards = tt.sourceShards
			reshardCreateOptions.targetShards = tt.targetShards

			err := validateShardFlags()
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}
