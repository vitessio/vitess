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
	minDelay   time.Duration
	lastAccept time.Time
}

// NewThrottledListener creates a ThrottledListener. maxRate
// specifies the maximum rate of accepts per second.
func NewThrottledListener(l net.Listener, maxRate int64) net.Listener {
	return &ThrottledListener{
		Listener:   l,
		minDelay:   time.Duration(1e9 / maxRate),
		lastAccept: time.Now(),
	}
}

// Accept accepts a new connection only if the accept rate
// will not exceed the throttling limit. Otherwise, it waits
// before accepting.
func (tln *ThrottledListener) Accept() (c net.Conn, err error) {
	curDelay := time.Now().Sub(tln.lastAccept)
	if curDelay < tln.minDelay {
		time.Sleep(tln.minDelay - curDelay)
	}
	c, err = tln.Listener.Accept()
	tln.lastAccept = time.Now()
	return c, err
}
