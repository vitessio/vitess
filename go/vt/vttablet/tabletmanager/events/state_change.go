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

// Package events contains event structs used by the tabletmanager package.
package events

import topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"

// StateChange is an event that describes state changes in the tablet as seen
// by the TabletManager. It is triggered after the tablet has processed a state
// change, which might have been initiated internally, or when the tablet
// noticed that an external process modified its topo record.
type StateChange struct {
	// OldTablet is the topo record of the tablet before the change.
	OldTablet topodatapb.Tablet
	// NewTablet is the topo record representing the current state.
	NewTablet topodatapb.Tablet
	// Reason is an optional string that describes the source of the change.
	Reason string
}
