/*
Copyright 2020 The Vitess Authors.

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

package vtadminproto

import (
	"vitess.io/vitess/go/vt/topo/topoproto"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// Tablets is a list of Tablet protobuf objects.
type Tablets []*vtadminpb.Tablet

// AliasStringList returns a list of TabletAlias strings for each tablet in the
// list.
func (tablets Tablets) AliasStringList() []string {
	aliases := make([]string, len(tablets))

	for i, tablet := range tablets {
		aliases[i] = topoproto.TabletAliasString(tablet.Tablet.Alias)
	}

	return aliases
}

// FilterTablets returns a subset of tablets (not exceeding maxResults) that
// satisfy the given condition.
//
// If maxResults is negative, len(tablets) is used instead.
func FilterTablets(condition func(tablet *vtadminpb.Tablet) bool, tablets []*vtadminpb.Tablet, maxResults int) []*vtadminpb.Tablet {
	if maxResults < 0 {
		maxResults = len(tablets)
	}

	results := make([]*vtadminpb.Tablet, 0, maxResults)

	for _, tablet := range tablets {
		if len(results) >= maxResults {
			break
		}

		if condition(tablet) {
			results = append(results, tablet)
		}
	}

	return results
}

// ParseTabletServingState returns a ServingState value from the given string.
// If the string does not map to a valid value, this function returns UNKNOWN.
func ParseTabletServingState(state string) vtadminpb.Tablet_ServingState {
	if s, ok := vtadminpb.Tablet_ServingState_value[state]; ok {
		return vtadminpb.Tablet_ServingState(s)
	}

	return vtadminpb.Tablet_UNKNOWN
}

// TabletServingStateString returns a ServingState represented as a string. If
// the state does not map to a valid value, this function returns "UNKNOWN".
func TabletServingStateString(state vtadminpb.Tablet_ServingState) string {
	if s, ok := vtadminpb.Tablet_ServingState_name[int32(state)]; ok {
		return s
	}

	return "UNKNOWN"
}
