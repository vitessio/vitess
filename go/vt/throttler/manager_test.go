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
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/proto/throttlerdata"
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
	if err != nil {
		t.Fatal(err)
	}
	if err := m.registerThrottler("t1", t1); err == nil {
		t.Fatalf("manager should not accept a duplicate registration of a throttler: %v", err)
	}
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
	if got := f.m.SetMaxRate(23); !reflect.DeepEqual(got, want) {
		t.Errorf("manager did not set the rate on all throttlers. got = %v, want = %v", got, want)
	}

	// Test MaxRates().
	wantRates := map[string]int64{
		"t1": 23,
		"t2": 23,
	}
	if gotRates := f.m.MaxRates(); !reflect.DeepEqual(gotRates, wantRates) {
		t.Errorf("manager did not set the rate on all throttlers. got = %v, want = %v", gotRates, wantRates)
	}
}

func TestManager_GetConfiguration(t *testing.T) {
	f := &managerTestFixture{}
	if err := f.setUp(); err != nil {
		t.Fatal(err)
	}
	defer f.tearDown()

	// Test GetConfiguration() when all throttlers are returned.
	want := map[string]*throttlerdata.Configuration{
		"t1": &defaultMaxReplicationLagModuleConfig.Configuration,
		"t2": &defaultMaxReplicationLagModuleConfig.Configuration,
	}
	got, err := f.m.GetConfiguration("" /* all */)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("manager did not return the correct initial config for all throttlers. got = %v, want = %v", got, want)
	}

	// Test GetConfiguration() when a specific throttler is requested.
	wantT2 := map[string]*throttlerdata.Configuration{
		"t2": &defaultMaxReplicationLagModuleConfig.Configuration,
	}
	gotT2, err := f.m.GetConfiguration("t2")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotT2, wantT2) {
		t.Errorf("manager did not return the correct initial config for throttler: %v got = %v, want = %v", "t2", gotT2, wantT2)
	}

	// Now change the config and then reset it back.
	newConfig := &throttlerdata.Configuration{
		TargetReplicationLagSec: defaultTargetLag + 1,
		IgnoreNSlowestReplicas:  defaultIgnoreNSlowestReplicas + 1,
	}
	allNames, err := f.m.UpdateConfiguration("", newConfig, false /* copyZeroValues */)
	if err != nil {
		t.Fatal(err)
	}
	// Verify it was changed.
	if err := checkConfig(f.m, []string{"t1", "t2"}, allNames, defaultTargetLag+1, defaultIgnoreNSlowestReplicas+1); err != nil {
		t.Fatal(err)
	}
	// Reset only "t2".
	if names, err := f.m.ResetConfiguration("t2"); err != nil || !reflect.DeepEqual(names, []string{"t2"}) {
		t.Fatalf("Reset failed or returned wrong throttler names: %v err: %v", names, err)
	}
	gotT2AfterReset, err := f.m.GetConfiguration("t2")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotT2AfterReset, wantT2) {
		t.Errorf("manager did not return the correct initial config for throttler %v after reset: got = %v, want = %v", "t2", gotT2AfterReset, wantT2)
	}
	// Reset all throttlers.
	if names, err := f.m.ResetConfiguration(""); err != nil || !reflect.DeepEqual(names, []string{"t1", "t2"}) {
		t.Fatalf("Reset failed or returned wrong throttler names: %v err: %v", names, err)
	}
	gotAfterReset, err := f.m.GetConfiguration("")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotAfterReset, want) {
		t.Errorf("manager did not return the correct initial config for all throttlers after reset. got = %v, want = %v", got, want)
	}
}

func TestManager_UpdateConfiguration_Error(t *testing.T) {
	f := &managerTestFixture{}
	if err := f.setUp(); err != nil {
		t.Fatal(err)
	}
	defer f.tearDown()

	// Check that errors from Verify() are correctly propagated.
	invalidConfig := &throttlerdata.Configuration{
		// max < 2 is not allowed.
		MaxReplicationLagSec: 1,
	}
	if _, err := f.m.UpdateConfiguration("t2", invalidConfig, false /* copyZeroValues */); err == nil {
		t.Fatal("expected error but got nil")
	} else {
		want := "max_replication_lag_sec must be >= 2"
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("received wrong error. got = %v, want contains = %v", err, want)
		}
	}
}

func TestManager_UpdateConfiguration_Partial(t *testing.T) {
	f := &managerTestFixture{}
	if err := f.setUp(); err != nil {
		t.Fatal(err)
	}
	defer f.tearDown()

	// Verify that a partial update only updates that one field.
	wantIgnoreNSlowestReplicas := defaultIgnoreNSlowestReplicas + 1
	partialConfig := &throttlerdata.Configuration{
		IgnoreNSlowestReplicas: wantIgnoreNSlowestReplicas,
	}
	names, err := f.m.UpdateConfiguration("t2", partialConfig, false /* copyZeroValues */)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkConfig(f.m, []string{"t2"}, names, defaultTargetLag, wantIgnoreNSlowestReplicas); err != nil {
		t.Fatal(err)
	}
	// Repeat test for all throttlers.
	allNames, err := f.m.UpdateConfiguration("" /* all */, partialConfig, false /* copyZeroValues */)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkConfig(f.m, []string{"t1", "t2"}, allNames, defaultTargetLag, wantIgnoreNSlowestReplicas); err != nil {
		t.Fatal(err)
	}
}

func TestManager_UpdateConfiguration_ZeroValues(t *testing.T) {
	f := &managerTestFixture{}
	if err := f.setUp(); err != nil {
		t.Fatal(err)
	}
	defer f.tearDown()

	// Test the explicit copy of zero values.
	zeroValueConfig := defaultMaxReplicationLagModuleConfig.Configuration
	zeroValueConfig.IgnoreNSlowestReplicas = 0
	names, err := f.m.UpdateConfiguration("t2", &zeroValueConfig, true /* copyZeroValues */)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkConfig(f.m, []string{"t2"}, names, defaultTargetLag, 0); err != nil {
		t.Fatal(err)
	}
	// Repeat test for all throttlers.
	allNames, err := f.m.UpdateConfiguration("" /* all */, &zeroValueConfig, true /* copyZeroValues */)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkConfig(f.m, []string{"t1", "t2"}, allNames, defaultTargetLag, 0); err != nil {
		t.Fatal(err)
	}
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
