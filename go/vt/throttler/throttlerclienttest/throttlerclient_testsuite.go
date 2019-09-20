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

// Package throttlerclienttest contains the testsuite against which each
// RPC implementation of the throttlerclient interface must be tested.
package throttlerclienttest

// NOTE: This file is not test-only code because it is referenced by
// tests in other packages and therefore it has to be regularly
// visible.

// NOTE: This code is in its own package such that its dependencies
// (e.g.  zookeeper) won't be drawn into production binaries as well.

import (
	"reflect"
	"strings"
	"testing"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/throttler"
	"vitess.io/vitess/go/vt/throttler/throttlerclient"

	throttlerdatapb "vitess.io/vitess/go/vt/proto/throttlerdata"
)

// TestSuite runs the test suite on the given throttlerclient and throttlerserver.
func TestSuite(t *testing.T, c throttlerclient.Client) {
	tf := &testFixture{}
	if err := tf.setUp(); err != nil {
		t.Fatal(err)
	}
	defer tf.tearDown()

	tf.maxRates(t, c)

	tf.setMaxRate(t, c)

	tf.configuration(t, c)
}

// TestSuitePanics tests the panic handling of each RPC method. Unlike TestSuite
// it does not use the real throttler.managerImpl. Instead, it uses FakeManager
// which allows us to panic on each RPC.
func TestSuitePanics(t *testing.T, c throttlerclient.Client) {
	maxRatesPanics(t, c)

	setMaxRatePanics(t, c)

	getConfigurationPanics(t, c)

	updateConfigurationPanics(t, c)

	resetConfigurationPanics(t, c)
}

var throttlerNames = []string{"t1", "t2"}

type testFixture struct {
	throttlers []*throttler.Throttler
}

func (tf *testFixture) setUp() error {
	for _, name := range throttlerNames {
		t, err := throttler.NewThrottler(name, "TPS", 1 /* threadCount */, 1, throttler.ReplicationLagModuleDisabled)
		if err != nil {
			return err
		}
		tf.throttlers = append(tf.throttlers, t)
	}
	return nil
}

func (tf *testFixture) tearDown() {
	for _, t := range tf.throttlers {
		t.Close()
	}
}

func (tf *testFixture) maxRates(t *testing.T, client throttlerclient.Client) {
	_, err := client.SetMaxRate(context.Background(), 23)
	if err != nil {
		t.Fatalf("Cannot execute remote command: %v", err)
	}

	got, err := client.MaxRates(context.Background())
	if err != nil {
		t.Fatalf("Cannot execute remote command: %v", err)
	}
	want := map[string]int64{
		"t1": 23,
		"t2": 23,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rate was not updated on all registered throttlers. got = %v, want = %v", got, throttlerNames)
	}
}

func (tf *testFixture) setMaxRate(t *testing.T, client throttlerclient.Client) {
	got, err := client.SetMaxRate(context.Background(), 23)
	if err != nil {
		t.Fatalf("Cannot execute remote command: %v", err)
	}

	if !reflect.DeepEqual(got, throttlerNames) {
		t.Fatalf("rate was not updated on all registered throttlers. got = %v, want = %v", got, throttlerNames)
	}
}

func (tf *testFixture) configuration(t *testing.T, client throttlerclient.Client) {
	initialConfigs, err := client.GetConfiguration(context.Background(), "" /* all */)
	if err != nil {
		t.Fatalf("Cannot execute remote command: %v", err)
	}

	// Test UpdateConfiguration.
	config := &throttlerdatapb.Configuration{
		TargetReplicationLagSec:        1,
		MaxReplicationLagSec:           2,
		InitialRate:                    3,
		MaxIncrease:                    0.4,
		EmergencyDecrease:              0.5,
		MinDurationBetweenIncreasesSec: 6,
		MaxDurationBetweenIncreasesSec: 7,
		MinDurationBetweenDecreasesSec: 8,
		SpreadBacklogAcrossSec:         9,
		IgnoreNSlowestReplicas:         10,
		IgnoreNSlowestRdonlys:          11,
		AgeBadRateAfterSec:             12,
		BadRateIncrease:                0.13,
		MaxRateApproachThreshold:       0.9,
	}
	names, err := client.UpdateConfiguration(context.Background(), "t2", config /* false */, true /* copyZeroValues */)
	if err != nil {
		t.Fatalf("Cannot execute remote command: %v", err)
	}
	if got, want := names, []string{"t2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("returned names of updated throttlers is wrong. got = %v, want = %v", got, want)
	}

	// Test GetConfiguration.
	configs, err := client.GetConfiguration(context.Background(), "t2")
	if err != nil {
		t.Fatalf("Cannot execute remote command: %v", err)
	}
	if len(configs) != 1 || configs["t2"] == nil {
		t.Fatalf("wrong named configuration returned. got = %v, want configuration for t2", configs)
	}
	if got, want := configs["t2"], config; !proto.Equal(got, want) {
		t.Fatalf("did not read updated config. got = %v, want = %v", got, want)
	}

	// Reset should return the initial configs.
	namesForReset, err := client.ResetConfiguration(context.Background(), "" /* all */)
	if err != nil {
		t.Fatalf("Cannot execute remote command: %v", err)
	}
	if got, want := namesForReset, throttlerNames; !reflect.DeepEqual(got, want) {
		t.Fatalf("returned names of reset throttlers is wrong. got = %v, want = %v", got, want)
	}

	// Verify that it was correctly set.
	configsAfterReset, err := client.GetConfiguration(context.Background(), "" /* all */)
	if err != nil {
		t.Fatalf("Cannot execute remote command: %v", err)
	}
	if got, want := configsAfterReset, initialConfigs; !reflect.DeepEqual(got, want) {
		t.Fatalf("wrong configurations after reset. got = %v, want = %v", got, want)
	}
}

// FakeManager implements the throttler.Manager interface and panics on all
// methods defined in the interface.
type FakeManager struct {
}

const panicMsg = "RPC server implementation should handle this"

// MaxRates implements the throttler.Manager interface. It always panics.
func (fm *FakeManager) MaxRates() map[string]int64 {
	panic(panicMsg)
}

// SetMaxRate implements the throttler.Manager interface. It always panics.
func (fm *FakeManager) SetMaxRate(int64) []string {
	panic(panicMsg)
}

// GetConfiguration implements the throttler.Manager interface. It always panics.
func (fm *FakeManager) GetConfiguration(throttlerName string) (map[string]*throttlerdatapb.Configuration, error) {
	panic(panicMsg)
}

// UpdateConfiguration implements the throttler.Manager interface. It always panics.
func (fm *FakeManager) UpdateConfiguration(throttlerName string, configuration *throttlerdatapb.Configuration, copyZeroValues bool) ([]string, error) {
	panic(panicMsg)
}

// ResetConfiguration implements the throttler.Manager interface. It always panics.
func (fm *FakeManager) ResetConfiguration(throttlerName string) ([]string, error) {
	panic(panicMsg)
}

// Test methods which test for each RPC that panics are caught.

func maxRatesPanics(t *testing.T, client throttlerclient.Client) {
	_, err := client.MaxRates(context.Background())
	if !errorFromPanicHandler(err) {
		t.Fatalf("MaxRates RPC implementation does not catch panics properly: %v", err)
	}
}

func setMaxRatePanics(t *testing.T, client throttlerclient.Client) {
	_, err := client.SetMaxRate(context.Background(), 23)
	if !errorFromPanicHandler(err) {
		t.Fatalf("SetMaxRate RPC implementation does not catch panics properly: %v", err)
	}
}

func getConfigurationPanics(t *testing.T, client throttlerclient.Client) {
	_, err := client.GetConfiguration(context.Background(), "")
	if !errorFromPanicHandler(err) {
		t.Fatalf("GetConfiguration RPC implementation does not catch panics properly: %v", err)
	}
}

func updateConfigurationPanics(t *testing.T, client throttlerclient.Client) {
	_, err := client.UpdateConfiguration(context.Background(), "", nil, false)
	if !errorFromPanicHandler(err) {
		t.Fatalf("UpdateConfiguration RPC implementation does not catch panics properly: %v", err)
	}
}

func resetConfigurationPanics(t *testing.T, client throttlerclient.Client) {
	_, err := client.ResetConfiguration(context.Background(), "")
	if !errorFromPanicHandler(err) {
		t.Fatalf("ResetConfiguration RPC implementation does not catch panics properly: %v", err)
	}
}

func errorFromPanicHandler(err error) bool {
	if err == nil || !strings.Contains(err.Error(), panicMsg) {
		return false
	}
	return true
}
