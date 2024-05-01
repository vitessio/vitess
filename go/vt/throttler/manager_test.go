/*
Copyright 2019 The Vitess Authors.

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
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	throttlerdatapb "vitess.io/vitess/go/vt/proto/throttlerdata"
)

// We base our test data on these defaults.
var (
	defaultTargetLag              = defaultMaxReplicationLagModuleConfig.TargetReplicationLagSec
	defaultIgnoreNSlowestReplicas = defaultMaxReplicationLagModuleConfig.IgnoreNSlowestReplicas
)

type managerTestFixture struct {
	m      *managerImpl
	t1, t2 *Throttler
}

func (f *managerTestFixture) setUp() error {
	f.m = newManager()
	var err error
	f.t1, err = newThrottler(f.m, "t1", "TPS", 1 /* threadCount */, MaxRateModuleDisabled, ReplicationLagModuleDisabled, time.Now)
	if err != nil {
		return err
	}
	f.t2, err = newThrottler(f.m, "t2", "TPS", 1 /* threadCount */, MaxRateModuleDisabled, ReplicationLagModuleDisabled, time.Now)
	if err != nil {
		return err
	}
	return nil
}

func (f *managerTestFixture) tearDown() {
	f.t1.Close()
	f.t2.Close()
}

func TestManager_Registration(t *testing.T) {
	m := newManager()
	t1, err := newThrottler(m, "t1", "TPS", 1 /* threadCount */, MaxRateModuleDisabled, ReplicationLagModuleDisabled, time.Now)
	require.NoError(t, err)

	err = m.registerThrottler("t1", t1)
	require.Error(t, err, "manager should not accept a duplicate registration of a throttler")

	t1.Close()

	// Unregistering an unregistered throttler should log an error.
	m.unregisterThrottler("t1")
}

func TestManager_SetMaxRate(t *testing.T) {
	f := &managerTestFixture{}
	if err := f.setUp(); err != nil {
		t.Fatal(err)
	}
	defer f.tearDown()

	// Test SetMaxRate().
	want := []string{"t1", "t2"}
	got := f.m.SetMaxRate(23)
	assert.Equal(t, want, got, "manager did not set the rate on all throttlers")

	// Test MaxRates().
	wantRates := map[string]int64{
		"t1": 23,
		"t2": 23,
	}
	gotRates := f.m.MaxRates()
	assert.Equal(t, wantRates, gotRates, "manager did not set the rate on all throttlers")
}

func TestManager_GetConfiguration(t *testing.T) {
	f := &managerTestFixture{}
	if err := f.setUp(); err != nil {
		t.Fatal(err)
	}
	defer f.tearDown()

	// Test GetConfiguration() when all throttlers are returned.
	want := map[string]*throttlerdatapb.Configuration{
		"t1": defaultMaxReplicationLagModuleConfig.Clone().Configuration,
		"t2": defaultMaxReplicationLagModuleConfig.Clone().Configuration,
	}
	got, err := f.m.GetConfiguration("" /* all */)
	require.NoError(t, err)
	assert.Equal(t, want, got, "manager did not return the correct initial config for all throttlers")

	// Test GetConfiguration() when a specific throttler is requested.
	wantT2 := map[string]*throttlerdatapb.Configuration{
		"t2": defaultMaxReplicationLagModuleConfig.Clone().Configuration,
	}
	gotT2, err := f.m.GetConfiguration("t2")
	require.NoError(t, err)
	assert.Equal(t, wantT2, gotT2, "manager did not return the correct initial config for throttler: t2")

	// Now change the config and then reset it back.
	newConfig := &throttlerdatapb.Configuration{
		TargetReplicationLagSec: defaultTargetLag + 1,
		IgnoreNSlowestReplicas:  defaultIgnoreNSlowestReplicas + 1,
	}
	allNames, err := f.m.UpdateConfiguration("", newConfig, false /* copyZeroValues */)
	require.NoError(t, err)

	err = checkConfig(f.m, []string{"t1", "t2"}, allNames, defaultTargetLag+1, defaultIgnoreNSlowestReplicas+1)
	require.NoError(t, err)

	// Reset only "t2".
	names, err := f.m.ResetConfiguration("t2")
	require.NoError(t, err)
	assert.Equal(t, []string{"t2"}, names, "Reset failed or returned wrong throttler names")

	gotT2AfterReset, err := f.m.GetConfiguration("t2")
	require.NoError(t, err)
	assert.Equal(t, wantT2, gotT2AfterReset, "manager did not return the correct initial config for throttler t2 after reset")

	// Reset all throttlers.

	names, err = f.m.ResetConfiguration("")
	require.NoError(t, err)
	assert.Equal(t, []string{"t1", "t2"}, names, "Reset failed or returned wrong throttler names")

	gotAfterReset, err := f.m.GetConfiguration("")
	require.NoError(t, err)
	assert.Equal(t, want, gotAfterReset, "manager did not return the correct initial config for all throttlers after reset")
}

func TestManager_UpdateConfiguration_Error(t *testing.T) {
	f := &managerTestFixture{}
	err := f.setUp()
	require.NoError(t, err)
	defer f.tearDown()

	// Check that errors from Verify() are correctly propagated.
	invalidConfig := &throttlerdatapb.Configuration{
		// max < 2 is not allowed.
		MaxReplicationLagSec: 1,
	}
	_, err = f.m.UpdateConfiguration("t2", invalidConfig, false /* copyZeroValues */)
	wantErr := "max_replication_lag_sec must be >= 2"
	require.ErrorContains(t, err, wantErr)
}

func TestManager_UpdateConfiguration_Partial(t *testing.T) {
	f := &managerTestFixture{}
	err := f.setUp()
	require.NoError(t, err)
	defer f.tearDown()

	// Verify that a partial update only updates that one field.
	wantIgnoreNSlowestReplicas := defaultIgnoreNSlowestReplicas + 1
	partialConfig := &throttlerdatapb.Configuration{
		IgnoreNSlowestReplicas: wantIgnoreNSlowestReplicas,
	}
	names, err := f.m.UpdateConfiguration("t2", partialConfig, false /* copyZeroValues */)
	require.NoError(t, err)

	err = checkConfig(f.m, []string{"t2"}, names, defaultTargetLag, wantIgnoreNSlowestReplicas)
	require.NoError(t, err)

	// Repeat test for all throttlers.
	allNames, err := f.m.UpdateConfiguration("" /* all */, partialConfig, false /* copyZeroValues */)
	require.NoError(t, err)

	err = checkConfig(f.m, []string{"t1", "t2"}, allNames, defaultTargetLag, wantIgnoreNSlowestReplicas)
	require.NoError(t, err)
}

func TestManager_UpdateConfiguration_ZeroValues(t *testing.T) {
	f := &managerTestFixture{}
	err := f.setUp()
	require.NoError(t, err)
	defer f.tearDown()

	// Test the explicit copy of zero values.
	zeroValueConfig := defaultMaxReplicationLagModuleConfig.Configuration.CloneVT()
	zeroValueConfig.IgnoreNSlowestReplicas = 0
	names, err := f.m.UpdateConfiguration("t2", zeroValueConfig, true /* copyZeroValues */)
	require.NoError(t, err)

	err = checkConfig(f.m, []string{"t2"}, names, defaultTargetLag, 0)
	require.NoError(t, err)

	// Repeat test for all throttlers.
	allNames, err := f.m.UpdateConfiguration("" /* all */, zeroValueConfig, true /* copyZeroValues */)
	require.NoError(t, err)

	err = checkConfig(f.m, []string{"t1", "t2"}, allNames, defaultTargetLag, 0)
	require.NoError(t, err)
}

func checkConfig(m *managerImpl, throttlers []string, updatedThrottlers []string, targetLag int64, ignoreNSlowestReplicas int32) error {
	// Sort list of throttler names because they came from a randomized Go map.
	sort.Strings(updatedThrottlers)

	if !reflect.DeepEqual(throttlers, updatedThrottlers) {
		return fmt.Errorf("list of updated throttler names is wrong. got = %v, want = %v", throttlers, updatedThrottlers)
	}

	// Get configuration(s).
	var throttlerName string
	if len(throttlers) == 1 {
		throttlerName = throttlers[0]
	}
	configs, err := m.GetConfiguration(throttlerName)
	if err != nil {
		return err
	}

	if throttlerName != "" {
		if len(configs) != 1 {
			return fmt.Errorf("GetConfiguration() should return only the configuration for throttler: %v but returned: %v", throttlerName, configs)
		}
	}

	// Check each configuration.
	for _, name := range throttlers {
		if got, want := configs[name].IgnoreNSlowestReplicas, ignoreNSlowestReplicas; got != want {
			return fmt.Errorf("%v: wrong throttler config for IgnoreNSlowestReplicas: got = %v, want = %v", name, got, want)
		}
		if got, want := configs[name].TargetReplicationLagSec, targetLag; got != want {
			return fmt.Errorf("%v: wrong throttler config for TargetReplicationLagSec: got = %v, want = %v", name, got, want)
		}
	}

	return nil
}
