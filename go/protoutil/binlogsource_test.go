/*
Copyright 2024 The Vitess Authors.

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

package protoutil

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

func TestSortBinlogSourceTables(t *testing.T) {
	tests := []struct {
		name      string
		inSource  *binlogdatapb.BinlogSource
		outSource *binlogdatapb.BinlogSource
	}{
		{
			name: "Basic",
			inSource: &binlogdatapb.BinlogSource{
				Tables: []string{"wuts1", "atable", "1table", "ztable2", "table3"},
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{
						{
							Match: "ztable2",
						},
						{
							Match: "table3",
						},
						{
							Match: "/wuts",
						},
						{
							Match:  "1table",
							Filter: "a",
						},
						{
							Match: "1table",
						},
						{
							Match: "atable",
						},
					},
				},
			},
			outSource: &binlogdatapb.BinlogSource{
				Tables: []string{"1table", "atable", "table3", "wuts1", "ztable2"},
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{
						{
							Match: "1table",
						},
						{
							Match:  "1table",
							Filter: "a",
						},
						{
							Match: "atable",
						},
						{
							Match: "table3",
						},
						{
							Match: "/wuts",
						},
						{
							Match: "ztable2",
						},
					},
				},
			},
		},
		{
			name: "With excludes",
			inSource: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{
						{
							Match: "./*",
						},
						{
							Match:  "no4",
							Filter: "exclude",
						},
						{
							Match:  "no2",
							Filter: "exclude",
						},
						{
							Match: "ztable2",
						},
						{
							Match: "atable2",
						},
					},
				},
			},
			outSource: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{
						{
							Match:  "no2",
							Filter: "exclude",
						},
						{
							Match:  "no4",
							Filter: "exclude",
						},
						{
							Match: "./*",
						},
						{
							Match: "atable2",
						},
						{
							Match: "ztable2",
						},
					},
				},
			},
		},
		{
			name: "With excludes",
			inSource: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{
						{
							Match:  "no4",
							Filter: "exclude",
						},
						{
							Match:  "no2",
							Filter: "exclude",
						},
						{
							Match: "./*",
						},
					},
				},
			},
			outSource: &binlogdatapb.BinlogSource{
				Filter: &binlogdatapb.Filter{
					Rules: []*binlogdatapb.Rule{
						{
							Match:  "no2",
							Filter: "exclude",
						},
						{
							Match:  "no4",
							Filter: "exclude",
						},
						{
							Match: "./*",
						},
					},
				},
			},
		},
		{
			name:      "Nil",
			inSource:  nil,
			outSource: nil,
		},
		{
			name: "No filter",
			inSource: &binlogdatapb.BinlogSource{
				Tables: []string{"wuts1", "atable", "1table", "ztable2", "table3"},
				Filter: nil,
			},
			outSource: &binlogdatapb.BinlogSource{
				Tables: []string{"1table", "atable", "table3", "wuts1", "ztable2"},
				Filter: nil,
			},
		},
		{
			name: "No filter rules",
			inSource: &binlogdatapb.BinlogSource{
				Tables: []string{"wuts1", "atable", "1table", "ztable2", "table3"},
				Filter: &binlogdatapb.Filter{},
			},
			outSource: &binlogdatapb.BinlogSource{
				Tables: []string{"1table", "atable", "table3", "wuts1", "ztable2"},
				Filter: &binlogdatapb.Filter{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SortBinlogSourceTables(tt.inSource)
			require.True(t, proto.Equal(tt.inSource, tt.outSource), "got: %s, want: %s", tt.inSource.String(), tt.outSource.String())
		})
	}
}
