// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package events defines common structures used for events dispatched from
// various other package.
package events

import (
	"time"
)

// StatusUpdater is a base struct for multi-part events with a status string
// that gets updated as the process progresses. StatusUpdater implements
// event.Updater, so if you embed a StatusUpdater into an event type, you can
// set a new status and dispatch that event in one call with DispatchUpdate.
//
// For example:
//
//   type MyEvent struct {
//     StatusUpdater
//   }
//   ev := &MyEvent{}
//   event.DispatchUpdate(ev, "new status")
type StatusUpdater struct {
	Status string

	// EventID is used to group the steps of a multi-part event.
	// It is set internally the first time Update() is called.
	EventID int64
}

// Update sets a new status and initializes the EventID if necessary.
// This implements event.Updater.Update().
func (su *StatusUpdater) Update(status interface{}) {
	su.Status = status.(string)

	// initialize event ID
	if su.EventID == 0 {
		su.EventID = time.Now().UnixNano()
	}
}
