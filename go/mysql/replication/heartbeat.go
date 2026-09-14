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

import "math"

// HeartbeatIntervalForNetTimeout returns the heartbeat interval a replica
// must use for the given replica net timeout: half the timeout, so the
// replica misses two heartbeats before it drops the connection to the
// source. VTOrc requests this value and vttablet compares against it, so
// both must derive it from one place.
func HeartbeatIntervalForNetTimeout(replicaNetTimeout int32) float64 {
	return float64(replicaNetTimeout) / 2
}

// HeartbeatIntervalsEqual reports whether two heartbeat intervals agree once
// rounded to the nearest half second. MySQL stores the interval as a float
// with millisecond precision, so an exact comparison would flag a value it
// rounded on write as misconfigured and make VTOrc repair it forever.
func HeartbeatIntervalsEqual(a, b float64) bool {
	return math.Round(a*2) == math.Round(b*2)
}
