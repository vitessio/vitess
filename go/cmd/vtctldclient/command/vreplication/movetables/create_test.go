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

package movetables

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateViewOptions(t *testing.T) {
	tests := []struct {
		name         string
		allViews     bool
		includeViews []string
		excludeViews []string
		wantErr      string
	}{
		{
			name:         "exclude views without selector is invalid",
			excludeViews: []string{"view1"},
			wantErr:      "exclude-views requires --views or --all-views",
		},
		{
			name:         "views and all views are mutually exclusive",
			allViews:     true,
			includeViews: []string{"view1"},
			wantErr:      "cannot specify both --views and --all-views",
		},
		{
			name:         "views is valid",
			includeViews: []string{"view1", "view2"},
		},
		{
			name:         "all views with exclude views is valid",
			allViews:     true,
			excludeViews: []string{"view1"},
		},
		{
			name: "no view flags is valid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateViewOptions(tt.allViews, tt.includeViews, tt.excludeViews)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
		})
	}
}
