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
