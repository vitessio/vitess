/*
Copyright 2019 The Vitess Authors.

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

package topotools

import (
	"testing"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestCheckOwnership(t *testing.T) {
	type testCase struct {
		oldTablet, newTablet *topodatapb.Tablet
		wantError            bool
	}
	table := []testCase{
		{
			oldTablet: &topodatapb.Tablet{
				Hostname: "host1",
				PortMap:  map[string]int32{"vt": 123, "grpc": 555},
			},
			newTablet: &topodatapb.Tablet{
				Hostname: "host1",
				PortMap:  map[string]int32{"vt": 123, "grpc": 222},
			},
			wantError: false,
		},
		{
			oldTablet: &topodatapb.Tablet{
				Hostname: "host2",
				PortMap:  map[string]int32{"vt": 123},
			},
			newTablet: &topodatapb.Tablet{
				Hostname: "host1",
				PortMap:  map[string]int32{"vt": 123},
			},
			wantError: true,
		},
		{
			oldTablet: &topodatapb.Tablet{
				Hostname: "host1",
				PortMap:  map[string]int32{"vt": 123},
			},
			newTablet: &topodatapb.Tablet{
				Hostname: "host1",
				PortMap:  map[string]int32{"vt": 321},
			},
			wantError: true,
		},
		{
			newTablet: &topodatapb.Tablet{
				Hostname: "host1",
				PortMap:  map[string]int32{"vt": 123},
			},
			wantError: true,
		},
		{
			oldTablet: &topodatapb.Tablet{
				PortMap: map[string]int32{"vt": 123},
			},
			newTablet: &topodatapb.Tablet{
				Hostname: "host1",
				PortMap:  map[string]int32{"vt": 123},
			},
			wantError: true,
		},
		{
			oldTablet: &topodatapb.Tablet{
				Hostname: "host1",
			},
			newTablet: &topodatapb.Tablet{
				Hostname: "host1",
				PortMap:  map[string]int32{"vt": 123},
			},
			wantError: true,
		},
	}

	for i, tc := range table {
		gotError := CheckOwnership(tc.oldTablet, tc.newTablet) != nil
		if gotError != tc.wantError {
			t.Errorf("[%v]: got error = %v, want error = %v", i, gotError, tc.wantError)
		}
	}
}

type identTestCase struct {
	tablet *topodatapb.Tablet
	ident  string
}

func TestIdent(t *testing.T) {
	tests := []identTestCase{
		{
			tablet: &topodatapb.Tablet{
				Keyspace: "ks1",
				Alias: &topodatapb.TabletAlias{
					Cell: "cell",
					Uid:  1,
				},
				Hostname: "host1",
			},
			ident: "cell-1 (host1)",
		},
		{
			tablet: &topodatapb.Tablet{
				Keyspace: "ks1",
				Alias: &topodatapb.TabletAlias{
					Cell: "cell",
					Uid:  1,
				},
				Hostname: "host1",
				Tags: map[string]string{
					"tag1": "val1",
				},
			},
			ident: "cell-1 (host1 tag1=val1)",
		},
	}

	for _, test := range tests {
		got := TabletIdent(test.tablet)
		if got != test.ident {
			t.Errorf("TableIdent mismatch: got %s want %s", got, test.ident)
		}
	}
}
