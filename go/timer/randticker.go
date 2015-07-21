// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package timer

import (
	"math/rand"
	"time"
)

// RandTicker is just like time.Ticker, except that
// it adds randomness to the events.
type RandTicker struct {
	C    <-chan time.Time
	done chan struct{}
}

// NewRandTicker creates a new RandTicker. d is the duration,
// and variance specifies the variance. The ticker will tick
// every d +/- variance.
func NewRandTicker(d, variance time.Duration) *RandTicker {
	c := make(chan time.Time, 1)
	done := make(chan struct{})
	go func() {
		rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
		for {
			vr := time.Duration(rnd.Int63n(int64(2*variance)) - int64(variance))
			tmr := time.NewTimer(d + vr)
			select {
			case <-tmr.C:
				select {
				case c <- time.Now():
				default:
				}
			case <-done:
				tmr.Stop()
				close(c)
				return
			}
		}
	}()
	return &RandTicker{
		C:    c,
		done: done,
	}
}

// Stop stops the ticker and closes the underlying channel.
func (tkr *RandTicker) Stop() {
	close(tkr.done)
}
