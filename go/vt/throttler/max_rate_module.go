// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package throttler

import "github.com/youtube/vitess/go/sync2"

// MaxRateModule allows to set and retrieve a maximum rate limit.
// It implements the Module interface.
type MaxRateModule struct {
	maxRate        sync2.AtomicInt64
	rateUpdateChan chan<- struct{}
}

// NewMaxRateModule will create a new module instance and set the initial
// rate limit to maxRate.
func NewMaxRateModule(maxRate int64) *MaxRateModule {
	return &MaxRateModule{
		maxRate: sync2.NewAtomicInt64(maxRate),
	}
}

// Start currently does nothing. It implements the Module interface.
func (m *MaxRateModule) Start(rateUpdateChan chan<- struct{}) {
	m.rateUpdateChan = rateUpdateChan
}

// Stop currently does nothing. It implements the Module interface.
func (m *MaxRateModule) Stop() {}

// MaxRate returns the current maximum allowed rate.
func (m *MaxRateModule) MaxRate() int64 {
	return m.maxRate.Get()
}

// SetMaxRate sets the current max rate and notifies the throttler about the
// rate update.
func (m *MaxRateModule) SetMaxRate(rate int64) {
	m.maxRate.Set(rate)
	m.rateUpdateChan <- struct{}{}
}
