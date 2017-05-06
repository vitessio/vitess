/*
Copyright 2017 Google Inc.

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

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

func TestCheckOwnership(t *testing.T) {
	type testCase struct {
		oldTablet, newTablet *topodatapb.Tablet
		wantError            bool
	}
	table := []testCase{
		{
			oldTablet: &topodatapb.Tablet{
				Ip:      "1.2.3.4",
				PortMap: map[string]int32{"vt": 123, "mysql": 555},
			},
			newTablet: &topodatapb.Tablet{
				Ip:      "1.2.3.4",
				PortMap: map[string]int32{"vt": 123, "mysql": 222},
			},
			wantError: false,
		},
		{
			oldTablet: &topodatapb.Tablet{
				Ip:      "4.3.2.1",
				PortMap: map[string]int32{"vt": 123},
			},
			newTablet: &topodatapb.Tablet{
				Ip:      "1.2.3.4",
				PortMap: map[string]int32{"vt": 123},
			},
			wantError: true,
		},
		{
			oldTablet: &topodatapb.Tablet{
				Ip:      "1.2.3.4",
				PortMap: map[string]int32{"vt": 123},
			},
			newTablet: &topodatapb.Tablet{
				Ip:      "1.2.3.4",
				PortMap: map[string]int32{"vt": 321},
			},
			wantError: true,
		},
		{
			newTablet: &topodatapb.Tablet{
				Ip:      "1.2.3.4",
				PortMap: map[string]int32{"vt": 123},
			},
			wantError: true,
		},
		{
			oldTablet: &topodatapb.Tablet{
				PortMap: map[string]int32{"vt": 123},
			},
			newTablet: &topodatapb.Tablet{
				Ip:      "1.2.3.4",
				PortMap: map[string]int32{"vt": 123},
			},
			wantError: true,
		},
		{
			oldTablet: &topodatapb.Tablet{
				Ip: "1.2.3.4",
			},
			newTablet: &topodatapb.Tablet{
				Ip:      "1.2.3.4",
				PortMap: map[string]int32{"vt": 123},
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
