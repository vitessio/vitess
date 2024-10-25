/*
Copyright 2024 The Vitess Authors.

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

package mathstats

import (
	"math"
	"sync/atomic"
)

type SimpleEWMA struct {
	// The current value of the average. After adding with Add(), this is
	// updated to reflect the average of all values seen thus far.
	value atomic.Uint64
}

// Add adds a value to the series and updates the moving average.
func (e *SimpleEWMA) Add(value uint64) {
	const (
		AVG_METRIC_AGE float64 = 30.0
		DECAY          float64 = 2 / (float64(AVG_METRIC_AGE) + 1)
	)

	fvalue := float64(value)

	for {
		v := e.value.Load()

		var nv float64
		if v == 0 {
			nv = fvalue
		} else {
			nv = (fvalue * DECAY) + (math.Float64frombits(v) * (1 - DECAY))
		}

		if e.value.CompareAndSwap(v, math.Float64bits(nv)) {
			return
		}
	}
}

// Value returns the current value of the moving average.
func (e *SimpleEWMA) Value() uint64 {
	return uint64(math.Float64frombits(e.value.Load()))
}
