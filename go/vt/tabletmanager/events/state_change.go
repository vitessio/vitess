// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package events contains event structs used by the tabletmanager package.
package events

import (
	"github.com/youtube/vitess/go/vt/topo"
)

// StateChange is an event that describes state changes in the tablet as seen
// by the TabletManager. It is triggered after the tablet has processed a state
// change, which might have been initiated internally, or when the tablet
// noticed that an external process modified its topo record.
type StateChange struct {
	// OldTablet is the topo record of the tablet before the change.
	OldTablet topo.Tablet
	// NewTablet is the topo record representing the current state.
	NewTablet topo.Tablet
	// Reason is an optional string that describes the source of the change.
	Reason string
}
