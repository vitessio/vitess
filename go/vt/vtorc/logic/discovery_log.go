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

package logic

import "vitess.io/vitess/go/vt/vtorc/inst"

// discoveryLogCheck holds the inputs needed to decide whether a successful
// instance discovery should log at INFO level rather than DEBUG.
type discoveryLogCheck struct {
	// prev is the previously persisted instance state, if one was found.
	prev *inst.Instance

	// curr is the instance state returned by the current discovery poll.
	curr *inst.Instance

	// found reports whether a previous instance record existed in the backend.
	found bool

	// forceDiscovery reports whether this poll was explicitly forced.
	forceDiscovery bool

	// lagThreshold is the replication lag threshold in seconds used to detect
	// lag threshold crossings.
	lagThreshold int64
}

// shouldLogDiscoveryInfo reports whether a successful instance discovery logs
// at INFO level rather than DEBUG. Routine polls with unchanged operational
// state return false.
func shouldLogDiscoveryInfo(check discoveryLogCheck) bool {
	if check.forceDiscovery || !check.found || check.prev == nil || check.curr == nil {
		return true
	}

	if !check.prev.IsLastCheckValid || !check.curr.IsLastCheckValid {
		return true
	}

	return check.curr.OperationalStateChanged(check.prev, check.lagThreshold)
}
