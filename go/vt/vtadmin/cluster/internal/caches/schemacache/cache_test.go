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
