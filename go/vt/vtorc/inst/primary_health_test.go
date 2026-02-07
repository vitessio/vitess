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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vtorc/config"
)

func resetPrimaryHealthState() {
	primaryHealthMu.Lock()
	defer primaryHealthMu.Unlock()
	primaryHealthByAlias = make(map[string]*primaryHealthWindow)
}

func TestPrimaryHealthWindow(t *testing.T) {
	resetPrimaryHealthState()
	oldWindow := config.GetPrimaryHealthCheckTimeoutWindow()
	config.SetPrimaryHealthCheckTimeoutWindow(50 * time.Millisecond)
	defer config.SetPrimaryHealthCheckTimeoutWindow(oldWindow)

	alias := "zone1-0000000100"
	RecordPrimaryHealthCheck(alias, false)
	require.False(t, IsPrimaryHealthCheckUnhealthy(alias))

	RecordPrimaryHealthCheck(alias, false)
	require.True(t, IsPrimaryHealthCheckUnhealthy(alias))

	time.Sleep(60 * time.Millisecond)
	RecordPrimaryHealthCheck(alias, true)

	assert.Eventually(t, func() bool {
		return !IsPrimaryHealthCheckUnhealthy(alias)
	}, 200*time.Millisecond, 10*time.Millisecond)
}
