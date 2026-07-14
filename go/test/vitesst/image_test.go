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

package vitesst

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestComponentImageResolution(t *testing.T) {
	const (
		clusterImage  = "vitesst:cluster"
		keyspaceImage = "vitesst:keyspace"
	)

	tests := []struct {
		name          string
		vtgateEnv     string
		vtctldEnv     string
		vttabletEnv   string
		keyspaceImage string
		wantVTGate    string
		wantVTCtld    string
		wantVTTablet  string
	}{
		{
			name:         "all unset falls back to cluster image",
			wantVTGate:   clusterImage,
			wantVTCtld:   clusterImage,
			wantVTTablet: clusterImage,
		},
		{
			name:         "per-component overrides",
			vtgateEnv:    "vitesst:gate",
			vtctldEnv:    "vitesst:ctld",
			vttabletEnv:  "vitesst:tablet",
			wantVTGate:   "vitesst:gate",
			wantVTCtld:   "vitesst:ctld",
			wantVTTablet: "vitesst:tablet",
		},
		{
			name:          "keyspace image wins over vttablet env",
			vttabletEnv:   "vitesst:tablet",
			keyspaceImage: keyspaceImage,
			wantVTGate:    clusterImage,
			wantVTCtld:    clusterImage,
			wantVTTablet:  keyspaceImage,
		},
		{
			name:          "keyspace image wins over cluster image without env",
			keyspaceImage: keyspaceImage,
			wantVTGate:    clusterImage,
			wantVTCtld:    clusterImage,
			wantVTTablet:  keyspaceImage,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// An empty value clears the variable for the helpers, whose
			// fallback treats an unset and an empty override alike.
			t.Setenv("VITESST_VTGATE_IMAGE", tt.vtgateEnv)
			t.Setenv("VITESST_VTCTLD_IMAGE", tt.vtctldEnv)
			t.Setenv("VITESST_VTTABLET_IMAGE", tt.vttabletEnv)

			c := &Cluster{
				image: clusterImage,
				opts:  &clusterOptions{keyspaces: []keyspaceConfig{{name: "ks", image: tt.keyspaceImage}}},
			}

			assert.Equal(t, tt.wantVTGate, c.vtgateImage())
			assert.Equal(t, tt.wantVTCtld, c.vtctldImage())
			assert.Equal(t, tt.wantVTTablet, c.vttabletImage("ks"))
		})
	}
}
