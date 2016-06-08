// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package throttler

import "github.com/youtube/vitess/go/sync2"

// MaxRateModule allows to set and retrieve a maximum rate limit.
// It implements the Module interface.
type MaxRateModule struct {
	maxRate sync2.AtomicInt64
}

// NewMaxRateModule will create a new module instance and set the initial
// rate limit to maxRate.
func NewMaxRateModule(maxRate int64) Module {
	return &MaxRateModule{sync2.NewAtomicInt64(maxRate)}
}

// Start currently does nothing. It implements the Module interface.
func (m *MaxRateModule) Start(rateUpdateChan chan<- struct{}) {}

// Stop currently does nothing. It implements the Module interface.
func (m *MaxRateModule) Stop() {}

// MaxRate returns the current maximum allowed rate.
func (m *MaxRateModule) MaxRate() int64 {
	return m.maxRate.Get()
}
