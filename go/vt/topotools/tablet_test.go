// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
