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

package inst

import (
	"sync"
	"time"

	"vitess.io/vitess/go/vt/vtorc/config"
)

const minPrimaryHealthCheckFailures = 2

type primaryHealthEvent struct {
	at      time.Time
	success bool
}

type primaryHealthWindow struct {
	events    []primaryHealthEvent
	unhealthy bool
}

var (
	primaryHealthMu      sync.Mutex
	primaryHealthByAlias = make(map[string]*primaryHealthWindow)
)

// RecordPrimaryHealthCheck records the outcome of a primary health check.
func RecordPrimaryHealthCheck(tabletAlias string, success bool) {
	recordPrimaryHealthCheckAt(tabletAlias, success, time.Now())
}

// IsPrimaryHealthCheckUnhealthy reports whether the primary health checks are unhealthy.
func IsPrimaryHealthCheckUnhealthy(tabletAlias string) bool {
	window := config.GetPrimaryHealthCheckTimeoutWindow()
	if window <= 0 || tabletAlias == "" {
		return false
	}

	primaryHealthMu.Lock()
	defer primaryHealthMu.Unlock()

	state := primaryHealthByAlias[tabletAlias]
	if state == nil {
		return false
	}
	updatePrimaryHealthWindowLocked(state, time.Now(), window)
	return state.unhealthy
}

func recordPrimaryHealthCheckAt(tabletAlias string, success bool, now time.Time) {
	window := config.GetPrimaryHealthCheckTimeoutWindow()
	if window <= 0 || tabletAlias == "" {
		return
	}

	primaryHealthMu.Lock()
	defer primaryHealthMu.Unlock()

	state := primaryHealthByAlias[tabletAlias]
	if state == nil {
		state = &primaryHealthWindow{}
		primaryHealthByAlias[tabletAlias] = state
	}
	state.events = append(state.events, primaryHealthEvent{at: now, success: success})
	updatePrimaryHealthWindowLocked(state, now, window)
}

func updatePrimaryHealthWindowLocked(state *primaryHealthWindow, now time.Time, window time.Duration) {
	if state == nil {
		return
	}

	successCount := 0
	failureCount := 0
	pruned := state.events[:0]
	for _, event := range state.events {
		if now.Sub(event.at) > window {
			continue
		}
		pruned = append(pruned, event)
		if event.success {
			successCount++
		} else {
			failureCount++
		}
	}
	state.events = pruned

	if state.unhealthy {
		if failureCount == 0 && successCount > 0 {
			state.unhealthy = false
		}
		return
	}

	if failureCount >= minPrimaryHealthCheckFailures && failureCount > successCount {
		state.unhealthy = true
	}
}
