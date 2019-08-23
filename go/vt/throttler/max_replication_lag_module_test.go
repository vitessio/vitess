/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package throttler

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/discovery"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// The tests below tests the heuristic of the MaxReplicationLagModule.
// Depending on a replica's replication lag and the historic throttler rate,
// the module does increase or decrease the throttler rate.

// Most of the tests assume that there are two REPLICA tablets r1 and r2 which
// both broadcast their replication lag every 20s.
// r1 starts to broadcast at 0s, r2 at 10s.
// Additionally, there are two RDONLY tablets rdonly1 and rdonly2 which are only
// used by the "IgnoreNSlowestReplicas" tests.
const (
	// Automatic enumeration starts at 0. But there's no r0. Ignore it.
	_ = iota
	r1
	r2
	rdonly1
	rdonly2
)

type testFixture struct {
	m            *MaxReplicationLagModule
	ratesHistory *fakeRatesHistory
}

func newTestFixtureWithMaxReplicationLag(maxReplicationLag int64) (*testFixture, error) {
	config := NewMaxReplicationLagModuleConfig(maxReplicationLag)
	return newTestFixture(config)
}

func newTestFixture(config MaxReplicationLagModuleConfig) (*testFixture, error) {
	ratesHistory := newFakeRatesHistory()
	fc := &fakeClock{}
	// Do not start at 0*time.Second because than the code cannot distinguish
	// between a legimate value and a zero time.Time value.
	fc.setNow(1 * time.Second)
	m, err := NewMaxReplicationLagModule(config, ratesHistory.aggregatedIntervalHistory, fc.now)
	if err != nil {
		return nil, err
	}
	// Updates for the throttler go into a big channel and will be ignored.
	m.rateUpdateChan = make(chan<- struct{}, 1000)

	return &testFixture{
		m:            m,
		ratesHistory: ratesHistory,
	}, nil
}

// process does the same thing as MaxReplicationLagModule.ProcessRecords() does
// for a new "lagRecord".
func (tf *testFixture) process(lagRecord replicationLagRecord) {
	tf.m.processRecord(lagRecord)
}

func (tf *testFixture) checkState(state state, rate int64, lastRateChange time.Time) error {
	if got, want := tf.m.currentState, state; got != want {
		return fmt.Errorf("module in wrong state. got = %v, want = %v", got, want)
	}
	if got, want := tf.m.MaxRate(), rate; got != want {
		return fmt.Errorf("module has wrong MaxRate(). got = %v, want = %v", got, want)
	}
	if got, want := tf.m.lastRateChange, lastRateChange; got != want {
		return fmt.Errorf("module has wrong lastRateChange time. got = %v, want = %v", got, want)
	}
	return nil
}

func TestMaxReplicationLagModule_RateNotZeroWhenDisabled(t *testing.T) {
	tf, err := newTestFixtureWithMaxReplicationLag(ReplicationLagModuleDisabled)
	if err != nil {
		t.Fatal(err)
	}

	// Initial rate must not be zero. It's ReplicationLagModuleDisabled instead.
	if err := tf.checkState(stateIncreaseRate, ReplicationLagModuleDisabled, sinceZero(1*time.Second)); err != nil {
		t.Fatal(err)
	}
}

func TestMaxReplicationLagModule_InitialStateAndWait(t *testing.T) {
	config := NewMaxReplicationLagModuleConfig(5)
	// Overwrite the default config to make sure we test a non-default value.
	config.InitialRate = 123
	config.MaxDurationBetweenIncreasesSec = 23
	tf, err := newTestFixture(config)
	if err != nil {
		t.Fatal(err)
	}

	// Initial rate must be config.InitialRate.
	if err := tf.checkState(stateIncreaseRate, config.InitialRate, sinceZero(1*time.Second)); err != nil {
		t.Fatal(err)
	}
	// After startup, the next increment won't happen until
	// config.MaxDurationBetweenIncreasesSec elapsed.
	if got, want := tf.m.nextAllowedChangeAfterInit, sinceZero(config.MaxDurationBetweenIncreases()+1*time.Second); got != want {
		t.Fatalf("got = %v, want = %v", got, want)
	}
}

// TestMaxReplicationLagModule_Increase tests only the continuous increase of the
// rate and assumes that we are well below the replica capacity.
func TestMaxReplicationLagModule_Increase(t *testing.T) {
	tf, err := newTestFixtureWithMaxReplicationLag(5)
	if err != nil {
		t.Fatal(err)
	}

	// We start at config.InitialRate.
	if err := tf.checkState(stateIncreaseRate, 100, sinceZero(1*time.Second)); err != nil {
		t.Fatal(err)
	}
	// After the initial wait period of 62s
	// (config.MaxDurationBetweenIncreasesSec), regular increments start.

	// r2 @  70s, 0s lag
	// NOTE: We don't add a record for 70s itself because the throttler only
	// publishes it at the "end" of that second i.e. when the time at 71s started.
	tf.ratesHistory.add(sinceZero(69*time.Second), 100)
	tf.process(lagRecord(sinceZero(70*time.Second), r2, 0))
	// Rate was increased to 200 based on actual rate of 100 within [0s, 69s].
	// r2 becomes the "replica under test".
	if err := tf.checkState(stateIncreaseRate, 200, sinceZero(70*time.Second)); err != nil {
		t.Fatal(err)
	}
	// We have to wait at least config.MinDurationBetweenIncreasesSec (40s) before
	// the next increase.
	if got, want := tf.m.replicaUnderTest.nextAllowedChange, sinceZero(70*time.Second).Add(tf.m.config.MinDurationBetweenIncreases()); got != want {
		t.Fatalf("got = %v, want = %v", got, want)
	}
	// r2 @  75s, 0s lag
	tf.ratesHistory.add(sinceZero(70*time.Second), 100)
	tf.ratesHistory.add(sinceZero(74*time.Second), 200)
	tf.process(lagRecord(sinceZero(75*time.Second), r2, 0))
	// Lag record was ignored because it's within the wait period.
	if err := tf.checkState(stateIncreaseRate, 200, sinceZero(70*time.Second)); err != nil {
		t.Fatal(err)
	}

	// r1 @  80s, 0s lag
	tf.ratesHistory.add(sinceZero(79*time.Second), 200)
	tf.process(lagRecord(sinceZero(80*time.Second), r1, 0))
	// The r1 lag update was ignored because an increment "under test" is always
	// locked in with the replica which triggered the increase (r2 this time).
	if err := tf.checkState(stateIncreaseRate, 200, sinceZero(70*time.Second)); err != nil {
		t.Fatal(err)
	}

	// No increase is possible for the next 20 seconds.

	// r2 @  90s, 0s lag
	tf.ratesHistory.add(sinceZero(80*time.Second), 200)
	tf.ratesHistory.add(sinceZero(89*time.Second), 200)
	tf.process(lagRecord(sinceZero(90*time.Second), r2, 0))
	if err := tf.checkState(stateIncreaseRate, 200, sinceZero(70*time.Second)); err != nil {
		t.Fatal(err)
	}

	// r1 @ 100s, 0s lag
	tf.ratesHistory.add(sinceZero(99*time.Second), 200)
	tf.process(lagRecord(sinceZero(100*time.Second), r1, 0))
	if err := tf.checkState(stateIncreaseRate, 200, sinceZero(70*time.Second)); err != nil {
		t.Fatal(err)
	}

	// Next rate increase is possible after testing the rate for 40s.

	// r2 @ 110s, 0s lag
	tf.ratesHistory.add(sinceZero(109*time.Second), 200)
	tf.process(lagRecord(sinceZero(110*time.Second), r2, 0))
	if err := tf.checkState(stateIncreaseRate, 400, sinceZero(110*time.Second)); err != nil {
		t.Fatal(err)
	}
}

// TestMaxReplicationLagModule_ReplicaUnderTest_LastErrorOrNotUp is
// almost identical to TestMaxReplicationLagModule_Increase but this time we
// test that the system makes progress if the currently tracked replica has
// LastError set or is no longer tracked.
func TestMaxReplicationLagModule_ReplicaUnderTest_LastErrorOrNotUp(t *testing.T) {
	tf, err := newTestFixtureWithMaxReplicationLag(5)
	if err != nil {
		t.Fatal(err)
	}

	// r2 @  70s, 0s lag
	tf.ratesHistory.add(sinceZero(69*time.Second), 100)
	tf.process(lagRecord(sinceZero(70*time.Second), r2, 0))
	// Rate was increased to 200 based on actual rate of 100 within [0s, 69s].
	// r2 becomes the "replica under test".
	if err := tf.checkState(stateIncreaseRate, 200, sinceZero(70*time.Second)); err != nil {
		t.Fatal(err)
	}

	// r2 @  75s, 0s lag, LastError set
	rError := lagRecord(sinceZero(75*time.Second), r2, 0)
	rError.LastError = errors.New("HealthCheck reporting broken")
	tf.m.replicaLagCache.add(rError)

	// r1 @ 110s, 0s lag
	tf.ratesHistory.add(sinceZero(70*time.Second), 100)
	tf.ratesHistory.add(sinceZero(109*time.Second), 200)
	tf.process(lagRecord(sinceZero(110*time.Second), r1, 0))
	// We ignore r2 as "replica under test" because it has LastError set.
	// Instead, we act on r1.
	// r1 becomes the "replica under test".
	if err := tf.checkState(stateIncreaseRate, 400, sinceZero(110*time.Second)); err != nil {
		t.Fatal(err)
	}

	// We'll simulate a shutdown of r1 i.e. we're no longer tracking it.
	// r1 @ 115s, 0s lag, !Up
	tf.ratesHistory.add(sinceZero(110*time.Second), 200)
	tf.ratesHistory.add(sinceZero(114*time.Second), 400)
	rNotUp := lagRecord(sinceZero(115*time.Second), r1, 0)
	rNotUp.Up = false
	tf.m.replicaLagCache.add(rNotUp)

	// r2 @ 150s, 0s lag (lastError no longer set)
	tf.ratesHistory.add(sinceZero(149*time.Second), 400)
	tf.process(lagRecord(sinceZero(150*time.Second), r2, 0))
	// We ignore r1 as "replica under test" because it has !Up set.
	// Instead, we act on r2.
	// r2 becomes the "replica under test".
	if err := tf.checkState(stateIncreaseRate, 800, sinceZero(150*time.Second)); err != nil {
		t.Fatal(err)
	}
}

// TestMaxReplicationLagModule_ReplicaUnderTest_Timeout tests the safe guard
// that a "replica under test" which didn't report its lag for a while will be
// cleared such that any other replica can become the new "replica under test".
func TestMaxReplicationLagModule_ReplicaUnderTest_Timeout(t *testing.T) {
	tf, err := newTestFixtureWithMaxReplicationLag(5)
	if err != nil {
		t.Fatal(err)
	}

	// r2 @  70s, 0s lag
	tf.ratesHistory.add(sinceZero(69*time.Second), 100)
	tf.process(lagRecord(sinceZero(70*time.Second), r2, 0))
	// Rate was increased to 200 based on actual rate of 100 within [0s, 69s].
	// r2 becomes the "replica under test".
	if err := tf.checkState(stateIncreaseRate, 200, sinceZero(70*time.Second)); err != nil {
		t.Fatal(err)
	}

	// r1 @  80s, 0s lag (ignored because r2 is the "replica under test")
	tf.ratesHistory.add(sinceZero(70*time.Second), 100)
	tf.ratesHistory.add(sinceZero(79*time.Second), 200)
	tf.process(lagRecord(sinceZero(80*time.Second), r1, 0))
	if err := tf.checkState(stateIncreaseRate, 200, sinceZero(70*time.Second)); err != nil {
		t.Fatal(err)
	}

	// r2 as "replica under test" did not report its lag for too long.
	// We'll ignore it from now and let other replicas trigger rate changes.
	// r1 @  173s, 0s lag
	// time for r1 must be > 172s (70s + 40s + 62s) which is
	// (last rate change + test duration + max duration between increases).
	tf.ratesHistory.add(sinceZero(172*time.Second), 200)
	tf.process(lagRecord(sinceZero(173*time.Second), r1, 0))
	if err := tf.checkState(stateIncreaseRate, 400, sinceZero(173*time.Second)); err != nil {
		t.Fatal(err)
	}
}

// TestMaxReplicationLagModule_ReplicaUnderTest_IncreaseToDecrease verifies that
// the current "replica under test" is ignored when our state changes from
// "stateIncreaseRate" to "stateDecreaseAndGuessRate".
func TestMaxReplicationLagModule_ReplicaUnderTest_IncreaseToDecrease(t *testing.T) {
	tf, err := newTestFixtureWithMaxReplicationLag(5)
	if err != nil {
		t.Fatal(err)
	}

	// r2 @  70s, 0s lag (triggers the increase state)
	tf.ratesHistory.add(sinceZero(69*time.Second), 100)
	tf.process(lagRecord(sinceZero(70*time.Second), r2, 0))
	// Rate was increased to 200 based on actual rate of 100 within [0s, 69s].
	// r2 becomes the "replica under test".
	if err := tf.checkState(stateIncreaseRate, 200, sinceZero(70*time.Second)); err != nil {
		t.Fatal(err)
	}

	// r1 @  80s, 0s lag
	// This lag record is required in the next step to correctly calculate how
	// much r1 lags behind due to the rate increase.
	tf.ratesHistory.add(sinceZero(70*time.Second), 100)
	tf.ratesHistory.add(sinceZero(79*time.Second), 200)
	tf.process(lagRecord(sinceZero(80*time.Second), r1, 0))
	// r1 remains the "replica under test".
	if err := tf.checkState(stateIncreaseRate, 200, sinceZero(70*time.Second)); err != nil {
		t.Fatal(err)
	}

	// r2 @  90s, 0s lag (ignored because the test duration is not up yet)
	tf.ratesHistory.add(sinceZero(89*time.Second), 200)
	tf.process(lagRecord(sinceZero(90*time.Second), r2, 0))
	if err := tf.checkState(stateIncreaseRate, 200, sinceZero(70*time.Second)); err != nil {
		t.Fatal(err)
	}

	// r1 @ 100s, 3s lag (above target, provokes a decrease)
	tf.ratesHistory.add(sinceZero(99*time.Second), 200)
	tf.process(lagRecord(sinceZero(100*time.Second), r1, 3))
	// r1 becomes the "replica under test".
	// r1's high lag triggered the decrease state and therefore we did not wait
	// for the pending increase of "replica under test" r2.
	if err := tf.checkState(stateDecreaseAndGuessRate, 140, sinceZero(100*time.Second)); err != nil {
		t.Fatal(err)
	}

	// r2 lag records are ignored while r1 is the "replica under test".
	// r2 @ 110s, 0s lag
	tf.ratesHistory.add(sinceZero(100*time.Second), 200)
	tf.ratesHistory.add(sinceZero(109*time.Second), 140)
	tf.process(lagRecord(sinceZero(110*time.Second), r2, 0))
	if err := tf.checkState(stateDecreaseAndGuessRate, 140, sinceZero(100*time.Second)); err != nil {
		t.Fatal(err)
	}

	// r1 leaves the "replica under test" as soon as the test duration is up
	// or its lag improved to a better state.
	// Here we simulate that its lag improved and the resulting state goes from
	// "decrease" to "increase".
	// r1 @ 119s, 0s lag (triggers the increase state)
	tf.ratesHistory.add(sinceZero(118*time.Second), 140)
	tf.process(lagRecord(sinceZero(119*time.Second), r1, 0))
	// Rate increases to 170, the middle of: [good, bad] = [140, 200].
	if err := tf.checkState(stateIncreaseRate, 170, sinceZero(119*time.Second)); err != nil {
		t.Fatal(err)
	}
}

// TestMaxReplicationLagModule_ReplicaUnderTest_DecreaseToEmergency verifies
// that the current "replica under test" is ignored when our state changes from
// "stateDecreaseAndGuessRate" to "stateEmergency".
func TestMaxReplicationLagModule_ReplicaUnderTest_DecreaseToEmergency(t *testing.T) {
	tf, err := newTestFixtureWithMaxReplicationLag(5)
	if err != nil {
		t.Fatal(err)
	}

	// INCREASE

	// r1 @  20s, 0s lag
	// This lag record is required in the next step to correctly calculate how
	// much r1 lags behind due to the rate increase.
	tf.ratesHistory.add(sinceZero(19*time.Second), 100)
	tf.process(lagRecord(sinceZero(20*time.Second), r1, 0))
	if err := tf.checkState(stateIncreaseRate, 100, sinceZero(1*time.Second)); err != nil {
		t.Fatal(err)
	}

	// DECREASE

	// r1 @  40s, 3s lag (above target, provokes a decrease)
	tf.ratesHistory.add(sinceZero(39*time.Second), 100)
	tf.process(lagRecord(sinceZero(40*time.Second), r1, 3))
	// r1 becomes the "replica under test".
	if err := tf.checkState(stateDecreaseAndGuessRate, 70, sinceZero(40*time.Second)); err != nil {
		t.Fatal(err)
	}

	// EMERGENCY

	// r2 lag is even worse and triggers the "emergency" state.
	// r2 @  50s, 10s lag
	tf.ratesHistory.add(sinceZero(40*time.Second), 100)
	tf.ratesHistory.add(sinceZero(49*time.Second), 70)
	tf.process(lagRecord(sinceZero(50*time.Second), r2, 10))
	// r1 overrides r2 as new "replica under test".
	if err := tf.checkState(stateEmergency, 35, sinceZero(50*time.Second)); err != nil {
		t.Fatal(err)
	}

	// r1 lag becomes worse than the r1 lag now. We don't care and keep r1 as
	// "replica under test" for now.
	// r1 @  60s, 15s lag
	tf.ratesHistory.add(sinceZero(50*time.Second), 70)
	tf.ratesHistory.add(sinceZero(59*time.Second), 35)
	tf.process(lagRecord(sinceZero(60*time.Second), r1, 15))
	if err := tf.checkState(stateEmergency, 35, sinceZero(50*time.Second)); err != nil {
		t.Fatal(err)
	}

	// INCREASE

	// r2 fully recovers and triggers an increase.
	// r2 @  70s, 0s lag
	tf.ratesHistory.add(sinceZero(69*time.Second), 35)
	tf.process(lagRecord(sinceZero(70*time.Second), r2, 0))
	// r2 becomes the new "replica under test".
	if err := tf.checkState(stateIncreaseRate, 70, sinceZero(70*time.Second)); err != nil {
		t.Fatal(err)
	}

	// EMERGENCY

	// r1 hasn't recovered yet and forces us to go back to the "emergency" state.
	// r1 @  80s, 15s lag
	tf.ratesHistory.add(sinceZero(70*time.Second), 35)
	tf.ratesHistory.add(sinceZero(79*time.Second), 70)
	tf.process(lagRecord(sinceZero(80*time.Second), r1, 15))
	// r1 becomes the new "replica under test".
	if err := tf.checkState(stateEmergency, 35, sinceZero(80*time.Second)); err != nil {
		t.Fatal(err)
	}
}

// TestMaxReplicationLagModule_Increase_BadRateUpperBound verifies that a
// known bad rate is always the upper bound for any rate increase.
func TestMaxReplicationLagModule_Increase_BadRateUpperBound(t *testing.T) {
	tf, err := newTestFixtureWithMaxReplicationLag(5)
	if err != nil {
		t.Fatal(err)
	}

	// We start at config.InitialRate.
	if err := tf.checkState(stateIncreaseRate, 100, sinceZero(1*time.Second)); err != nil {
		t.Fatal(err)
	}

	// Assume that a bad value of 150 was set @ 30s.
	tf.m.memory.markBad(150, sinceZero(30*time.Second))

	// r2 @  70s, 0s lag
	tf.ratesHistory.add(sinceZero(69*time.Second), 100)
	tf.process(lagRecord(sinceZero(70*time.Second), r2, 0))
	// Rate should get increased to 200 based on actual rate of 100 within
	// [0s, 69s].
	// However, this would go over the bad rate. Therefore, the new rate will be
	// the middle of [100, 150] ([actual rate, bad rate]).
	if err := tf.checkState(stateIncreaseRate, 125, sinceZero(70*time.Second)); err != nil {
		t.Fatal(err)
	}
}

// TestMaxReplicationLagModule_Increase_MinimumProgress verifies that the
// calculated new rate is never identical to the current rate and at least by
// "memoryGranularity" higher.
func TestMaxReplicationLagModule_Increase_MinimumProgress(t *testing.T) {
	tf, err := newTestFixtureWithMaxReplicationLag(5)
	if err != nil {
		t.Fatal(err)
	}

	// We start at config.InitialRate.
	if err := tf.checkState(stateIncreaseRate, 100, sinceZero(1*time.Second)); err != nil {
		t.Fatal(err)
	}

	// Assume that a bad value of 105 was set @ 30s.
	tf.m.memory.markBad(105, sinceZero(30*time.Second))

	// r2 @  70s, 0s lag
	// Assume that the actual rate was below the limit (95 instead of 100).
	tf.ratesHistory.add(sinceZero(69*time.Second), 95)
	tf.process(lagRecord(sinceZero(70*time.Second), r2, 0))
	// Rate should get increased to 190 based on an actual rate of 95 within
	// [0s, 69s].
	// However, this would go over the bad rate. Therefore, the new rate should
	// be the middle of [95, 105] ([actual rate, bad rate]).
	// But then the new rate is identical to the old set rate of 100.
	// In such a case, we always advance the new rate by "memoryGranularity"
	// (which is currently 5).
	if err := tf.checkState(stateIncreaseRate, 105, sinceZero(70*time.Second)); err != nil {
		t.Fatal(err)
	}
}

// TestMaxReplicationLagModule_Decrease verifies that we correctly calculate the
// replica (slave) rate in the decreaseAndGuessRate state.
func TestMaxReplicationLagModule_Decrease(t *testing.T) {
	tf, err := newTestFixtureWithMaxReplicationLag(5)
	if err != nil {
		t.Fatal(err)
	}

	// r2 @  70s, 0s lag
	tf.ratesHistory.add(sinceZero(69*time.Second), 100)
	tf.process(lagRecord(sinceZero(70*time.Second), r2, 0))
	// Rate was increased to 200 based on actual rate of 100 within [0s, 69s].
	if err := tf.checkState(stateIncreaseRate, 200, sinceZero(70*time.Second)); err != nil {
		t.Fatal(err)
	}

	// r2 @  90s, 3s lag (above target, provokes a decrease)
	tf.ratesHistory.add(sinceZero(70*time.Second), 100)
	tf.ratesHistory.add(sinceZero(89*time.Second), 200)
	tf.process(lagRecord(sinceZero(90*time.Second), r2, 3))
	// The guessed replica (slave) rate is 140 because of the 3s lag increase
	// within the elapsed 20s.
	// The replica processed only 17s worth of work (20s duration - 3s lag increase)
	// 17s / 20s * 200 QPS actual rate => 170 QPS replica rate
	//
	// This results in a backlog of 3s * 200 QPS = 600 queries.
	// Since this backlog is spread across SpreadBacklogAcrossSec (20s),
	// the guessed rate gets further reduced by 30 QPS (600 queries / 20s).
	// Hence, the rate is set to 140 QPS (170 - 30).
	if err := tf.checkState(stateDecreaseAndGuessRate, 140, sinceZero(90*time.Second)); err != nil {
		t.Fatal(err)
	}
}

// TestMaxReplicationLagModule_Decrease_NoReplicaHistory skips decreasing the
// rate when the lag of r2 goes above the target value because we don't have
// replication lag value since the last rate change for r2. Therefore, we cannot
// reliably guess its rate and wait for the next available record.
func TestMaxReplicationLagModule_Decrease_NoReplicaHistory(t *testing.T) {
	tf, err := newTestFixtureWithMaxReplicationLag(10)
	if err != nil {
		t.Fatal(err)
	}

	// r2 @  70s, 0s lag
	tf.ratesHistory.add(sinceZero(69*time.Second), 100)
	tf.process(lagRecord(sinceZero(70*time.Second), r2, 0))
	// Rate was increased to 200 based on actual rate of 100 within [0s, 69s].
	if err := tf.checkState(stateIncreaseRate, 200, sinceZero(70*time.Second)); err != nil {
		t.Fatal(err)
	}

	// r1 @  80s, 3s lag (above target, but no decrease triggered)
	tf.ratesHistory.add(sinceZero(70*time.Second), 100)
	tf.ratesHistory.add(sinceZero(79*time.Second), 200)
	tf.process(lagRecord(sinceZero(80*time.Second), r1, 3))
	// Rate was decreased by 25% (half the emergency decrease) as safety measure.
	if err := tf.checkState(stateDecreaseAndGuessRate, 150, sinceZero(80*time.Second)); err != nil {
		t.Fatal(err)
	}

	// r2 @  90s, 0s lag
	tf.ratesHistory.add(sinceZero(80*time.Second), 200)
	tf.ratesHistory.add(sinceZero(89*time.Second), 150)
	tf.process(lagRecord(sinceZero(90*time.Second), r2, 0))
	// r2 is ignored because r1 is the "replica under test".
	if err := tf.checkState(stateDecreaseAndGuessRate, 150, sinceZero(80*time.Second)); err != nil {
		t.Fatal(err)
	}

	// r1 recovers after the rate decrease and triggers a new increase.
	// r1 @ 100s, 0s lag
	tf.ratesHistory.add(sinceZero(99*time.Second), 150)
	tf.process(lagRecord(sinceZero(100*time.Second), r1, 0))
	if err := tf.checkState(stateIncreaseRate, 300, sinceZero(100*time.Second)); err != nil {
		t.Fatal(err)
	}
}

func TestMaxReplicationLagModule_IgnoreNSlowestReplicas_REPLICA(t *testing.T) {
	testIgnoreNSlowestReplicas(t, r1, r2)
}

func TestMaxReplicationLagModule_IgnoreNSlowestReplicas_RDONLY(t *testing.T) {
	testIgnoreNSlowestReplicas(t, rdonly1, rdonly2)
}

func testIgnoreNSlowestReplicas(t *testing.T, r1UID, r2UID uint32) {
	config := NewMaxReplicationLagModuleConfig(5)
	typ := "REPLICA"
	if r1UID == r1 {
		config.IgnoreNSlowestReplicas = 1
	} else {
		config.IgnoreNSlowestRdonlys = 1
		typ = "RDONLY"
	}
	tf, err := newTestFixture(config)
	if err != nil {
		t.Fatal(err)
	}

	// r1 @  80s, 0s lag
	tf.ratesHistory.add(sinceZero(79*time.Second), 100)
	tf.process(lagRecord(sinceZero(80*time.Second), r1UID, 0))
	if err := tf.checkState(stateIncreaseRate, 200, sinceZero(80*time.Second)); err != nil {
		t.Fatal(err)
	}

	// r2 @  90s, 10s lag
	tf.ratesHistory.add(sinceZero(80*time.Second), 100)
	tf.ratesHistory.add(sinceZero(90*time.Second), 200)
	tf.process(lagRecord(sinceZero(90*time.Second), r2UID, 10))
	// Although r2's lag is high, it's ignored because it's the 1 slowest replica.
	if err := tf.checkState(stateIncreaseRate, 200, sinceZero(80*time.Second)); err != nil {
		t.Fatal(err)
	}
	results := tf.m.results.latestValues()
	if got, want := len(results), 2; got != want {
		t.Fatalf("skipped replica should have been recorded on the results page. got = %v, want = %v", got, want)
	}
	if got, want := results[0].Reason, fmt.Sprintf("skipping this replica because it's among the 1 slowest %v tablets", typ); got != want {
		t.Fatalf("skipped replica should have been recorded as skipped on the results page. got = %v, want = %v", got, want)
	}

	// r1 @ 100s, 20s lag
	tf.ratesHistory.add(sinceZero(99*time.Second), 200)
	tf.process(lagRecord(sinceZero(100*time.Second), r1UID, 20))
	// r1 would become the new 1 slowest replica. However, we do not ignore it
	// because then we would ignore all known replicas in a row.
	// => react to the high lag and reduce the rate by 50% from 200 to 100.
	if err := tf.checkState(stateEmergency, 100, sinceZero(100*time.Second)); err != nil {
		t.Fatal(err)
	}
}

func TestMaxReplicationLagModule_IgnoreNSlowestReplicas_NotEnoughReplicas(t *testing.T) {
	config := NewMaxReplicationLagModuleConfig(5)
	config.IgnoreNSlowestReplicas = 1
	tf, err := newTestFixture(config)
	if err != nil {
		t.Fatal(err)
	}

	// r2 @  70s, 10s lag
	tf.ratesHistory.add(sinceZero(69*time.Second), 100)
	tf.process(lagRecord(sinceZero(70*time.Second), r2, 10))
	// r2 is the 1 slowest replica. However, it's not ignored because then we
	// would ignore all replicas. Therefore, we react to its lag increase.
	if err := tf.checkState(stateEmergency, 50, sinceZero(70*time.Second)); err != nil {
		t.Fatal(err)
	}
}

// TestMaxReplicationLagModule_IgnoreNSlowestReplicas_IsIgnoredDuringIncrease
// is almost identical to
// TestMaxReplicationLagModule_ReplicaUnderTest_LastErrorOrNotUp.
// r2 triggers an increase and we wait for its next update after the increase
// waiting period. However, it becomes the slowest replica in the meantime and
// will be completely ignored. In consequence, we no longer wait for r2 and r1
// can trigger another increase instead.
func TestMaxReplicationLagModule_IgnoreNSlowestReplicas_IsIgnoredDuringIncrease(t *testing.T) {
	config := NewMaxReplicationLagModuleConfig(5)
	config.IgnoreNSlowestReplicas = 1
	tf, err := newTestFixture(config)
	if err != nil {
		t.Fatal(err)
	}

	// r2 @  70s, 0s lag
	tf.ratesHistory.add(sinceZero(69*time.Second), 100)
	tf.process(lagRecord(sinceZero(70*time.Second), r2, 0))
	// Rate was increased to 200 based on actual rate of 100 within [0s, 69s].
	if err := tf.checkState(stateIncreaseRate, 200, sinceZero(70*time.Second)); err != nil {
		t.Fatal(err)
	}

	// r1 @  80s, 0s lag
	tf.ratesHistory.add(sinceZero(70*time.Second), 100)
	tf.ratesHistory.add(sinceZero(79*time.Second), 200)
	tf.process(lagRecord(sinceZero(80*time.Second), r1, 0))
	// Lag record was ignored because it's within the wait period.
	if err := tf.checkState(stateIncreaseRate, 200, sinceZero(70*time.Second)); err != nil {
		t.Fatal(err)
	}

	// r2 becomes slow and will be ignored now.
	// r2 @  90s, 10s lag
	tf.ratesHistory.add(sinceZero(89*time.Second), 200)
	tf.m.replicaLagCache.add(lagRecord(sinceZero(90*time.Second), r2, 10))
	// We ignore the 1 slowest replica and do not decrease despite r2's high lag.
	if err := tf.checkState(stateIncreaseRate, 200, sinceZero(70*time.Second)); err != nil {
		t.Fatal(err)
	}

	// r1 @ 110s, 0s lag
	tf.ratesHistory.add(sinceZero(109*time.Second), 200)
	tf.process(lagRecord(sinceZero(110*time.Second), r1, 0))
	// Meanwhile, r1 is doing fine and will trigger the next increase because
	// we're no longer waiting for the ignored r2.
	if err := tf.checkState(stateIncreaseRate, 400, sinceZero(110*time.Second)); err != nil {
		t.Fatal(err)
	}
}

// TestMaxReplicationLagModule_IgnoreNSlowestReplicas_IncludeRdonly is the same
// as TestMaxReplicationLagModule_IgnoreNSlowestReplicas but includes
// RDONLY tablets as well.
func TestMaxReplicationLagModule_IgnoreNSlowestReplicas_IncludeRdonly(t *testing.T) {
	config := NewMaxReplicationLagModuleConfig(5)
	// We ignore up to 1 REPLICA and 1 RDONLY tablet.
	config.IgnoreNSlowestReplicas = 1
	config.IgnoreNSlowestRdonlys = 1
	tf, err := newTestFixture(config)
	if err != nil {
		t.Fatal(err)
	}

	// r1     @  80s, 0s lag
	tf.ratesHistory.add(sinceZero(79*time.Second), 100)
	tf.process(lagRecord(sinceZero(80*time.Second), r1, 0))
	if err := tf.checkState(stateIncreaseRate, 200, sinceZero(80*time.Second)); err != nil {
		t.Fatal(err)
	}

	// rdonly1 @  85s, 0s lag
	tf.ratesHistory.add(sinceZero(80*time.Second), 100)
	tf.ratesHistory.add(sinceZero(84*time.Second), 200)
	tf.process(lagRecord(sinceZero(85*time.Second), rdonly1, 0))
	if err := tf.checkState(stateIncreaseRate, 200, sinceZero(80*time.Second)); err != nil {
		t.Fatal(err)
	}

	// r2     @  90s, 10s lag
	tf.ratesHistory.add(sinceZero(89*time.Second), 200)
	tf.process(lagRecord(sinceZero(90*time.Second), r2, 10))
	// Although r2's lag is high, it's ignored because it's the 1 slowest REPLICA tablet.
	if err := tf.checkState(stateIncreaseRate, 200, sinceZero(80*time.Second)); err != nil {
		t.Fatal(err)
	}
	results := tf.m.results.latestValues()
	if got, want := len(results), 3; got != want {
		t.Fatalf("skipped replica should have been recorded on the results page. got = %v, want = %v", got, want)
	}
	if got, want := results[0].Reason, "skipping this replica because it's among the 1 slowest REPLICA tablets"; got != want {
		t.Fatalf("skipped replica should have been recorded as skipped on the results page. got = %v, want = %v", got, want)
	}

	// rdonly2 @  95s, 10s lag
	tf.ratesHistory.add(sinceZero(94*time.Second), 200)
	tf.process(lagRecord(sinceZero(95*time.Second), rdonly2, 10))
	// Although rdonly2's lag is high, it's ignored because it's the 1 slowest RDONLY tablet.
	if err := tf.checkState(stateIncreaseRate, 200, sinceZero(80*time.Second)); err != nil {
		t.Fatal(err)
	}
	results = tf.m.results.latestValues()
	if got, want := len(results), 4; got != want {
		t.Fatalf("skipped replica should have been recorded on the results page. got = %v, want = %v", got, want)
	}
	if got, want := results[0].Reason, "skipping this replica because it's among the 1 slowest RDONLY tablets"; got != want {
		t.Fatalf("skipped replica should have been recorded as skipped on the results page. got = %v, want = %v", got, want)
	}

	// r1     @ 100s, 11s lag
	tf.ratesHistory.add(sinceZero(99*time.Second), 200)
	tf.process(lagRecord(sinceZero(100*time.Second), r1, 10))
	// r1 would become the new 1 slowest REPLICA tablet. However, we do not ignore
	// it because then we would ignore all known replicas in a row.
	// => react to the high lag and reduce the rate by 50% from 200 to 100.
	if err := tf.checkState(stateEmergency, 100, sinceZero(100*time.Second)); err != nil {
		t.Fatal(err)
	}

	// r2 and rdonly are omitted here for brevity.

	// r1 recovers, but then rdonly1 becomes slow as well.

	// r1     @ 120s, 0s lag
	tf.ratesHistory.add(sinceZero(119*time.Second), 100)
	tf.process(lagRecord(sinceZero(120*time.Second), r1, 0))
	if err := tf.checkState(stateIncreaseRate, 200, sinceZero(120*time.Second)); err != nil {
		t.Fatal(err)
	}

	// rdonly1 @ 125s, 11s lag
	tf.ratesHistory.add(sinceZero(120*time.Second), 100)
	tf.ratesHistory.add(sinceZero(124*time.Second), 200)
	tf.process(lagRecord(sinceZero(125*time.Second), rdonly1, 11))
	// rdonly1 would become the new 1 slowest RDONLY tablet. However, we do not
	// ignore it because then we would ignore all known replicas in a row.
	// => react to the high lag and reduce the rate by 50% from 200 to 100.
	if err := tf.checkState(stateEmergency, 100, sinceZero(125*time.Second)); err != nil {
		t.Fatal(err)
	}
}

// TestMaxReplicationLagModule_EmergencyDoesNotChangeBadValues verifies that a
// replica, which triggers the emergency state and drags the rate all the way
// down to the minimum, does not influence the stored bad rate in the "memory".
// This situation occurs when a tablet gets restarted and restores from a
// backup. During that time, the reported replication lag is very high and won't
// go down until the restore finished (which may take hours). Once the restore
// is done, the throttler should immediately continue from the last good rate
// before the emergency and not have to test rates starting from the minimum.
// In particular, the stored bad rate must not be close the minimum rate or it
// may take hours until e.g. a bad rate of 2 ages out to the actual bad rate of
// e.g. 201.
func TestMaxReplicationLagModule_EmergencyDoesNotChangeBadValues(t *testing.T) {
	config := NewMaxReplicationLagModuleConfig(5)
	// Use a very aggressive aging rate to verify that bad rates do not age while
	// we're in the "emergency" state.
	config.AgeBadRateAfterSec = 21
	tf, err := newTestFixture(config)
	if err != nil {
		t.Fatal(err)
	}

	// INCREASE (necessary to set a "good" rate in the memory)

	// r2 @  70s, 0s lag
	tf.ratesHistory.add(sinceZero(69*time.Second), 100)
	tf.process(lagRecord(sinceZero(70*time.Second), r2, 0))
	if err := tf.checkState(stateIncreaseRate, 200, sinceZero(70*time.Second)); err != nil {
		t.Fatal(err)
	}

	// r2 @ 110s, 0s lag
	tf.ratesHistory.add(sinceZero(70*time.Second), 100)
	tf.ratesHistory.add(sinceZero(109*time.Second), 200)
	tf.process(lagRecord(sinceZero(110*time.Second), r2, 0))
	if err := tf.checkState(stateIncreaseRate, 400, sinceZero(110*time.Second)); err != nil {
		t.Fatal(err)
	}
	if got, want := tf.m.memory.highestGood(), int64(200); got != want {
		t.Fatalf("wrong good rate: got = %v, want = %v", got, want)
	}

	// DECREASE (necessary to set a "bad" rate in the memory)

	// r2 @ 130s, 3s lag (above target, provokes a decrease)
	tf.ratesHistory.add(sinceZero(110*time.Second), 200)
	tf.ratesHistory.add(sinceZero(129*time.Second), 400)
	tf.process(lagRecord(sinceZero(130*time.Second), r2, 3))
	if err := tf.checkState(stateDecreaseAndGuessRate, 280, sinceZero(130*time.Second)); err != nil {
		t.Fatal(err)
	}
	if got, want := tf.m.memory.lowestBad(), int64(400); got != want {
		t.Fatalf("wrong bad rate: got = %v, want = %v", got, want)
	}

	// memory: [good, bad] now is [200, 400].

	// EMERGENCY

	// Assume that "r1" was just restored and has a very high replication lag.
	// r1 @ 140s, 3600s lag
	tf.ratesHistory.add(sinceZero(130*time.Second), 400)
	tf.ratesHistory.add(sinceZero(139*time.Second), 280)
	tf.process(lagRecord(sinceZero(140*time.Second), r1, 3600))
	if err := tf.checkState(stateEmergency, 140, sinceZero(140*time.Second)); err != nil {
		t.Fatal(err)
	}
	if got, want := tf.m.memory.lowestBad(), int64(280); got != want {
		t.Fatalf("bad rate should change when we transition to the emergency state: got = %v, want = %v", got, want)
	}

	// memory: [good, bad] now is [200, 280].

	// r2 @ 150s, 0s lag (ignored because r1 is the "replica under test")
	tf.ratesHistory.add(sinceZero(140*time.Second), 280)
	tf.ratesHistory.add(sinceZero(149*time.Second), 140)
	tf.process(lagRecord(sinceZero(150*time.Second), r2, 0))
	if err := tf.checkState(stateEmergency, 140, sinceZero(140*time.Second)); err != nil {
		t.Fatal(err)
	}
	tf.ratesHistory.add(sinceZero(160*time.Second), 140)

	// r1 keeps to drive the throttler rate down, but not the bad rate.

	times := []int{160, 180, 200, 220, 240, 260, 280, 300, 320}
	rates := []int{70, 35, 17, 8, 4, 2, 1, 1, 1}

	for i, tm := range times {
		// r1 @ <tt>s, 3600s lag
		r1Time := sinceZero(time.Duration(tm) * time.Second)
		if i > 0 {
			tf.ratesHistory.add(r1Time, int64(rates[i-1]))
		}
		tf.process(lagRecord(r1Time, r1, 3600))
		if err := tf.checkState(stateEmergency, int64(rates[i]), r1Time); err != nil {
			t.Fatalf("time=%d: %v", tm, err)
		}
		if got, want := tf.m.memory.lowestBad(), int64(280); got != want {
			t.Fatalf("time=%d: bad rate must not change when the old state is the emergency state: got = %v, want = %v", tm, got, want)
		}

		// r2 @ <tt+10>s, 0s lag (ignored because r1 is the "replica under test")
		r2Time := sinceZero(time.Duration(tm+10) * time.Second)
		tf.ratesHistory.add(r2Time, int64(rates[i]))
		tf.process(lagRecord(r2Time, r2, 0))
		if err := tf.checkState(stateEmergency, int64(rates[i]), r1Time); err != nil {
			t.Fatalf("time=%d: %v", tm, err)
		}
	}

	// INCREASE

	// r1 is fully restored now and its lag is zero.
	// We'll leave the emergency state and increase the rate based of the last
	// stored "good" rate.

	// r1 @ 340s, 0s lag
	tf.ratesHistory.add(sinceZero(339*time.Second), 1)
	tf.process(lagRecord(sinceZero(340*time.Second), r1, 0))
	// New rate is 240, the middle of [good, bad] = [200, 240].
	if err := tf.checkState(stateIncreaseRate, 240, sinceZero(340*time.Second)); err != nil {
		t.Fatal(err)
	}
}

func TestMaxReplicationLagModule_NoIncreaseIfMaxRateWasNotApproached(t *testing.T) {
	config := NewMaxReplicationLagModuleConfig(5)
	tf, err := newTestFixture(config)
	if err != nil {
		t.Fatal(err)
	}

	// r1 @  20s, 0s lag
	// This lag record is required in the next step to correctly calculate how
	// much r1 lags behind due to the rate increase.
	tf.process(lagRecord(sinceZero(20*time.Second), r1, 0))
	if err := tf.checkState(stateIncreaseRate, 100, sinceZero(1*time.Second)); err != nil {
		t.Fatal(err)
	}

	// Master gets 10 QPS in second 69.
	// r1 @  70s, 0s lag.
	// Rate does not double to 200 as it does in the other tests because
	// the actual rate is much smaller than the current rate of 100.
	tf.ratesHistory.add(sinceZero(69*time.Second), 10)
	tf.process(lagRecord(sinceZero(70*time.Second), r1, 0))
	// r1 becomes the "replica under test".
	if err := tf.checkState(stateIncreaseRate, 100, sinceZero(1*time.Second)); err != nil {
		t.Fatal(err)
	}
}

// lagRecord creates a fake record using a fake TabletStats object.
func lagRecord(t time.Time, uid, lag uint32) replicationLagRecord {
	return replicationLagRecord{t, tabletStats(uid, lag)}
}

// tabletStats creates fake tablet health data.
func tabletStats(uid, lag uint32) discovery.TabletStats {
	typ := topodatapb.TabletType_REPLICA
	if uid == rdonly1 || uid == rdonly2 {
		typ = topodatapb.TabletType_RDONLY
	}
	tablet := &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "cell1", Uid: uid},
		Keyspace: "ks1",
		Shard:    "-80",
		Type:     typ,
		PortMap:  map[string]int32{"vt": int32(uid)},
	}
	return discovery.TabletStats{
		Tablet: tablet,
		Key:    discovery.TabletToMapKey(tablet),
		Target: &querypb.Target{
			Keyspace:   "ks1",
			Shard:      "-80",
			TabletType: typ,
		},
		Up:      true,
		Serving: true,
		Stats: &querypb.RealtimeStats{
			SecondsBehindMaster: lag,
		},
		TabletExternallyReparentedTimestamp: 22,
		LastError:                           nil,
	}
}

func TestApplyLatestConfig(t *testing.T) {
	config := NewMaxReplicationLagModuleConfig(5)
	tf, err := newTestFixture(config)
	if err != nil {
		t.Fatal(err)
	}

	// We start at config.InitialRate.
	if err := tf.checkState(stateIncreaseRate, 100, sinceZero(1*time.Second)); err != nil {
		t.Fatal(err)
	}
	// Change the default MaxIncrease from 100% to 200% and test that it's
	// correctly propagated.
	config.MaxIncrease = 2
	tf.m.updateConfiguration(&config.Configuration, true /* copyZeroValues */)

	// r2 @  70s, 0s lag
	tf.ratesHistory.add(sinceZero(69*time.Second), 100)
	tf.process(lagRecord(sinceZero(70*time.Second), r2, 0))
	// Rate was increased to 300 based on an actual rate of 100 within [0s, 69s].
	// That's a 200% increase.
	if err := tf.checkState(stateIncreaseRate, 300, sinceZero(70*time.Second)); err != nil {
		t.Fatal(err)
	}

	// Now reset the config to its default values.
	tf.m.resetConfiguration()
	// Let's assume that the current rate of 300 was actually fine.
	// After the reset, we'll increase it only by 100% to 600.

	// r2 @ 110s, 0s lag
	tf.ratesHistory.add(sinceZero(80*time.Second), 300)
	tf.ratesHistory.add(sinceZero(109*time.Second), 300)
	tf.process(lagRecord(sinceZero(110*time.Second), r2, 0))
	if err := tf.checkState(stateIncreaseRate, 600, sinceZero(110*time.Second)); err != nil {
		t.Fatal(err)
	}
}
