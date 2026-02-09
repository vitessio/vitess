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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func resetPrimaryHealthState() {
	primaryHealthMu.Lock()
	defer primaryHealthMu.Unlock()
	primaryHealthByAlias = make(map[string]*primaryHealthState)
}

func TestPrimaryHealthWindow(t *testing.T) {
	resetPrimaryHealthState()
	alias := "zone1-0000000100"
	start := time.Now()

	recordPrimaryHealthCheckAt(alias, false, start)
	recordPrimaryHealthCheckAt(alias, false, start.Add(100*time.Millisecond))

	window := primaryHealthWindow()
	primaryHealthMu.Lock()
	state := primaryHealthByAlias[alias]
	updatePrimaryHealthWindowLocked(state, start.Add(100*time.Millisecond), window)
	require.True(t, state.unhealthy)
	primaryHealthMu.Unlock()

	recordPrimaryHealthCheckAt(alias, true, start.Add(window+time.Millisecond))
	primaryHealthMu.Lock()
	state = primaryHealthByAlias[alias]
	updatePrimaryHealthWindowLocked(state, start.Add(window+time.Millisecond), window)
	require.True(t, state.unhealthy)
	primaryHealthMu.Unlock()

	recordPrimaryHealthCheckAt(alias, true, start.Add(2*window+10*time.Millisecond))
	primaryHealthMu.Lock()
	state = primaryHealthByAlias[alias]
	updatePrimaryHealthWindowLocked(state, start.Add(2*window+10*time.Millisecond), window)
	require.False(t, state.unhealthy)
	primaryHealthMu.Unlock()

	primaryHealthMu.Lock()
	updatePrimaryHealthWindowLocked(state, start.Add(4*window+10*time.Millisecond), window)
	if shouldEvictPrimaryHealthWindow(state) {
		delete(primaryHealthByAlias, alias)
	}
	_, ok := primaryHealthByAlias[alias]
	primaryHealthMu.Unlock()
	require.False(t, ok)
}
