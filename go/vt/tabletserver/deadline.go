// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"errors"
	"time"
)

var TimeoutErr = errors.New("timed out")

// Deadline supports deadline oriented convenience functions.
type Deadline time.Time

// NewDeadline creates a deadline using the specified timeout.
// If the timeout is 0, then the deadline is a zero value.
func NewDeadline(timeout time.Duration) Deadline {
	var dl Deadline
	if timeout == 0 {
		return dl
	}
	return Deadline(time.Now().Add(timeout))
}

// Timeout returns the timeout based on the current time.
// If the computed timeout is less than 10ms, it returns
// a TimeoutErr. If deadline is a zero value, it returns
// a timeout of 0.
func (dl Deadline) Timeout() (time.Duration, error) {
	if time.Time(dl).IsZero() {
		return 0, nil
	}
	timeout := time.Time(dl).Sub(time.Now())
	if timeout <= 10*time.Millisecond {
		return 0, TimeoutErr
	}
	return timeout, nil
}
