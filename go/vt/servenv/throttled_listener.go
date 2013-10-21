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
	connBuffer chan net.Conn
	lastError  error
}

// NewThrottledListener creates a ThrottledListener. maxRate
// specifies the maximum rate of accepts per second. If the backlog
// exceeds maxBuffer, the newly accepted connection is immediately closed.
func NewThrottledListener(l net.Listener, maxRate int64, maxBuffer int) net.Listener {
	tln := &ThrottledListener{
		Listener:   l,
		minDelay:   time.Duration(1e9 / maxRate),
		connBuffer: make(chan net.Conn, maxBuffer),
	}
	go tln.acceptLoop()
	return tln
}

func (tln *ThrottledListener) acceptLoop() {
	for {
		c, err := tln.Listener.Accept()
		// Accept will use lastError if channel is closed.
		if err != nil {
			tln.lastError = err
			close(tln.connBuffer)
			return
		}
		select {
		case tln.connBuffer <- c:
			continue
		default:
			// If the channel is full, we reject the connection
			// by closing it immediately.
			c.Close()
		}
	}
}

// Accept accepts a new connection, but ensures that the
// rate does not exceed the specified maxRate.
func (tln *ThrottledListener) Accept() (c net.Conn, err error) {
	// We assume Accept is called in a tight loop.
	// So we can just sleep for minDelay.
	time.Sleep(tln.minDelay)
	c = <-tln.connBuffer
	// If the channel is closed, return lastError.
	if c == nil {
		return nil, tln.lastError
	}
	return c, nil
}
