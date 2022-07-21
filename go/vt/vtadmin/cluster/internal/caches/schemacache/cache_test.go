/*
Copyright 2022 The Vitess Authors.

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

package schemacache

import (
	"testing"

	"github.com/stretchr/testify/assert"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

func TestLoadOptions(t *testing.T) {
	t.Parallel()

	t.Run("isFullPayload", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name          string
			opts          LoadOptions
			isFullPayload bool
		}{
			{
				name: "full payload",
				opts: LoadOptions{
					BaseRequest: &vtctldatapb.GetSchemaRequest{
						IncludeViews: true,
					},
					AggregateSizes: true,
				},
				isFullPayload: true,
			},
			{
				name: "tables",
				opts: LoadOptions{
					BaseRequest: &vtctldatapb.GetSchemaRequest{
						Tables:       []string{"t1"},
						IncludeViews: true,
					},
					AggregateSizes: true,
				},
				isFullPayload: false,
			},
			{
				name: "exclude tables",
				opts: LoadOptions{
					BaseRequest: &vtctldatapb.GetSchemaRequest{
						ExcludeTables: []string{"t1"},
						IncludeViews:  true,
					},
					AggregateSizes: true,
				},
				isFullPayload: false,
			},
			{
				name: "no views",
				opts: LoadOptions{
					BaseRequest: &vtctldatapb.GetSchemaRequest{
						IncludeViews: false,
					},
					AggregateSizes: true,
				},
				isFullPayload: false,
			},
			{
				name: "names only",
				opts: LoadOptions{
					BaseRequest: &vtctldatapb.GetSchemaRequest{
						IncludeViews:   true,
						TableNamesOnly: true,
					},
					AggregateSizes: true,
				},
				isFullPayload: false,
			},
			{
				name: "sizes only",
				opts: LoadOptions{
					BaseRequest: &vtctldatapb.GetSchemaRequest{
						IncludeViews:   true,
						TableSizesOnly: true,
					},
					AggregateSizes: true,
				},
				isFullPayload: false,
			},
			{
				name: "schema only",
				opts: LoadOptions{
					BaseRequest: &vtctldatapb.GetSchemaRequest{
						IncludeViews:    true,
						TableSchemaOnly: true,
					},
					AggregateSizes: true,
				},
				isFullPayload: false,
			},
			{
				name: "no size aggregation",
				opts: LoadOptions{
					BaseRequest: &vtctldatapb.GetSchemaRequest{
						IncludeViews: true,
					},
					AggregateSizes: false,
				},
				isFullPayload: false,
			},
			{
				name: "missing request",
				opts: LoadOptions{
					BaseRequest:    nil,
					AggregateSizes: true,
				},
				isFullPayload: false,
			},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				assert.Equal(t, tt.isFullPayload, tt.opts.isFullPayload(), "%+v", tt.opts)
			})
		}
	})
}
