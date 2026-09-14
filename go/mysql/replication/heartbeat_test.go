/*
Copyright 2026 The Vitess Authors.

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

package replication

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHeartbeatIntervalForNetTimeout(t *testing.T) {
	assert.Equal(t, 30.0, HeartbeatIntervalForNetTimeout(60))
	assert.Equal(t, 0.5, HeartbeatIntervalForNetTimeout(1))
	assert.Equal(t, 0.0, HeartbeatIntervalForNetTimeout(0))
}

func TestHeartbeatIntervalsEqual(t *testing.T) {
	for _, tt := range []struct {
		name  string
		a, b  float64
		equal bool
	}{
		{name: "exact", a: 30, b: 30, equal: true},
		{name: "within half second below", a: 29.75, b: 30, equal: true},
		{name: "within half second above", a: 30.249, b: 30, equal: true},
		{name: "outside half second below", a: 29.749, b: 30},
		{name: "outside half second above", a: 30.25, b: 30},
		{name: "different", a: 15, b: 30},
	} {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.equal, HeartbeatIntervalsEqual(tt.a, tt.b))
			assert.Equal(t, tt.equal, HeartbeatIntervalsEqual(tt.b, tt.a))
		})
	}
}

// TestHeartbeatHelpersAgree pins the contract VTOrc and vttablet share: the
// heartbeat derived from a net timeout must compare equal to itself, and the
// comparison must match the ReplicaMisconfigured rule VTOrc used before the
// helpers existed.
func TestHeartbeatHelpersAgree(t *testing.T) {
	for _, timeout := range []int32{1, 2, 3, 59, 60, 61, 3600} {
		want := HeartbeatIntervalForNetTimeout(timeout)
		assert.True(t, HeartbeatIntervalsEqual(want, want))
		for _, configured := range []float64{want, want - 0.2, want + 0.2, want - 1, want + 1} {
			legacyMisconfigured := math.Round(configured*2) != float64(timeout)
			assert.Equal(t, !legacyMisconfigured, HeartbeatIntervalsEqual(configured, want), "timeout %d configured %v", timeout, configured)
		}
	}
}
