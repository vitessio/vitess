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

package throttler

import (
	"vitess.io/vitess/go/sync2"
)

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
