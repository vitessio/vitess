// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package servenv

import (
	"net"
	"time"
)

// ThrottledListener throttles the number connections
// accepted to the specified rate.
type ThrottledListener struct {
	net.Listener
	minDelay time.Duration
}

// NewThrottledListener creates a ThrottledListener. maxRate
// specifies the maximum rate of accepts per second.
func NewThrottledListener(l net.Listener, maxRate int64) net.Listener {
	return &ThrottledListener{l, time.Duration(1e9 / maxRate)}
}

// Accept accepts a new connection, but ensures that the
// rate does not exceed the specified maxRate.
func (tln *ThrottledListener) Accept() (c net.Conn, err error) {
	// We assume Accept is called in a tight loop.
	// So we can just sleep for minDelay
	time.Sleep(tln.minDelay)
	return tln.Listener.Accept()
}
