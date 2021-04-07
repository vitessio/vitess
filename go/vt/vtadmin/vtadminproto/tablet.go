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

import vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"

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
