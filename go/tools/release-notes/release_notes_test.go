/*
Copyright 2021 The Vitess Authors.

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

package main

import (
	"reflect"
	"testing"
)

func Test_groupPRs(t *testing.T) {
	tests := []struct {
		name    string
		prInfos []prInfo
		want    map[string]map[string][]prInfo
	}{
		{name: "Single PR info with no labels", prInfos: []prInfo{{Title: "pr 1", Number: 1}}, want: map[string]map[string][]prInfo{"Other": {"Other": []prInfo{{Title: "pr 1", Number: 1}}}}},
		{name: "Single PR info with type label", prInfos: []prInfo{{Title: "pr 1", Number: 1, Labels: []label{{Name: prefixType + "Bug"}}}}, want: map[string]map[string][]prInfo{"Bug": {"Other": []prInfo{{Title: "pr 1", Number: 1, Labels: []label{{Name: prefixType + "Bug"}}}}}}},
		{name: "Single PR info with type and component labels", prInfos: []prInfo{{Title: "pr 1", Number: 1, Labels: []label{{Name: prefixType + "Bug"}, {Name: prefixComponent + "VTGate"}}}}, want: map[string]map[string][]prInfo{"Bug": {"VTGate": []prInfo{{Title: "pr 1", Number: 1, Labels: []label{{Name: prefixType + "Bug"}, {Name: prefixComponent + "VTGate"}}}}}}},
		{name: "Multiple PR infos with type and component labels", prInfos: []prInfo{
			{Title: "pr 1", Number: 1, Labels: []label{{Name: prefixType + "Bug"}, {Name: prefixComponent + "VTGate"}}},
			{Title: "pr 2", Number: 2, Labels: []label{{Name: prefixType + "Feature"}, {Name: prefixComponent + "VTTablet"}}}},
			want: map[string]map[string][]prInfo{"Bug": {"VTGate": []prInfo{{Title: "pr 1", Number: 1, Labels: []label{{Name: prefixType + "Bug"}, {Name: prefixComponent + "VTGate"}}}}}, "Feature": {"VTTablet": []prInfo{{Title: "pr 2", Number: 2, Labels: []label{{Name: prefixType + "Feature"}, {Name: prefixComponent + "VTTablet"}}}}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := groupPRs(tt.prInfos); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("groupPRs() = %v, want %v", got, tt.want)
			}
		})
	}
}
