// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package servenv

import (
	"net"
	"time"
)

type handshaker interface {
	Handshake() error
}

// ThrottledListener throttles the number connections
// accepted to the specified rate.
type ThrottledListener struct {
	net.Listener
	throttle <-chan time.Time
}

// NewThrottledListener creates a ThrottledListener. maxRate
// specifies the maximum rate of accepts per second.
func NewThrottledListener(l net.Listener, maxRate int64) net.Listener {
	return &ThrottledListener{l, time.Tick(time.Duration(1e9 / maxRate))}
}

// Accept accepts a new connection only if the accept rate
// will not exceed the throttling limit. Otherwise, it waits
// before accepting.
func (tln *ThrottledListener) Accept() (c net.Conn, err error) {
	<-tln.throttle
	return tln.Listener.Accept()
}
