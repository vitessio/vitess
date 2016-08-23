package throttler

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/discovery"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// The tests below tests the heuristic of the MaxReplicationLagModule.
// Depending on a replica's replication lag and the historic throttler rate,
// the module does increase or decrease the throttler rate.

// Most of the tests assume that there are two replicas r1 and r2 which both
// broadcast their replication lag every 20s.
// r1 starts to broadcast at 0s, r2 at 10s.
const (
	// Automatic enumeration starts at 0. But there's no r0. Ignore it.
	_ = iota
	r1
	r2
)

type testFixture struct {
	m            *MaxReplicationLagModule
	fc           *fakeClock
	ratesHistory *fakeRatesHistory
}

func newTestFixtureWithMaxReplicationLag(maxReplicationLag int64) (*testFixture, error) {
	config := NewMaxReplicationLagModuleConfig(maxReplicationLag)
	return newTestFixture(config)
}

func newTestFixture(config MaxReplicationLagModuleConfig) (*testFixture, error) {
	ratesHistory := newFakeRatesHistory()
	fc := &fakeClock{}
	m, err := NewMaxReplicationLagModule(config, ratesHistory.aggregatedIntervalHistory, fc.now)
	if err != nil {
		return nil, err
	}
	// Updates for the throttler go into a big channel and will be ignored.
	m.rateUpdateChan = make(chan<- struct{}, 1000)

	return &testFixture{
		m:            m,
		fc:           fc,
		ratesHistory: ratesHistory,
	}, nil
}

// process does the same thing as MaxReplicationLagModule.ProcessRecords() does
// for a new "lagRecord".
func (tf *testFixture) process(lagRecord replicationLagRecord) {
	// Advance the fake clock. This way the test writer does not have to do it.
	tf.fc.setNow(lagRecord.time.Sub(time.Time{}))

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
	if err := tf.checkState(stateIncreaseRate, ReplicationLagModuleDisabled, sinceZero(0*time.Second)); err != nil {
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
	if err := tf.checkState(stateIncreaseRate, config.InitialRate, sinceZero(0*time.Second)); err != nil {
		t.Fatal(err)
	}
	// After startup, the next increment won't happen until
	// config.MaxDurationBetweenIncreasesSec elapsed.
	if got, want := tf.m.nextAllowedIncrease, tf.fc.now().Add(config.MaxDurationBetweenIncreases()); got != want {
		t.Fatalf("got = %v, want = %v", got, want)
	}
}

// TestMaxReplicationLagModule_Increase tests only the continous increase of the
// rate and assumes that we are well below the replica capacity.
func TestMaxReplicationLagModule_Increase(t *testing.T) {
	tf, err := newTestFixtureWithMaxReplicationLag(5)
	if err != nil {
		t.Fatal(err)
	}

	// We start at config.InitialRate.
	if err := tf.checkState(stateIncreaseRate, 100, sinceZero(0*time.Second)); err != nil {
		t.Fatal(err)
	}
	// After the initial wait period of 62s (config.MaxDurationBetweenChangesSec),
	// regular increments start.

	// r2 @  70s, 0s lag
	tf.ratesHistory.add(sinceZero(69*time.Second), 100)
	tf.process(lagRecord(sinceZero(70*time.Second), r2, 0))
	// Rate was increased to 200 based on actual rate of 100 within [0s, 69s].
	if err := tf.checkState(stateIncreaseRate, 200, sinceZero(70*time.Second)); err != nil {
		t.Fatal(err)
	}
	// We have to wait at least config.MinDurationBetweenChangesSec (10s) before
	// the next increase.
	if got, want := tf.m.nextAllowedIncrease, sinceZero(70*time.Second).Add(tf.m.config.MinDurationBetweenChanges()); got != want {
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

	// r2 @  90s, 0s lag
	tf.ratesHistory.add(sinceZero(80*time.Second), 200)
	tf.ratesHistory.add(sinceZero(89*time.Second), 200)
	tf.process(lagRecord(sinceZero(90*time.Second), r2, 0))
	if err := tf.checkState(stateIncreaseRate, 400, sinceZero(90*time.Second)); err != nil {
		t.Fatal(err)
	}
}

// TestMaxReplicationLagModule_Increase_LastErrorOrNotUp is almost identical to
// TestMaxReplicationLagModule_Increase but this time we test that the system
// makes progress if the currently tracked replica has LastError set or is
// no longer tracked.
func TestMaxReplicationLagModule_Increase_LastErrorOrNotUp(t *testing.T) {
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

	// r2 @  75s, 0s lag, LastError set
	rError := lagRecord(sinceZero(75*time.Second), r2, 0)
	rError.LastError = errors.New("HealthCheck reporting broken")
	tf.m.lagCache.add(rError)

	// r1 @  80s, 0s lag
	tf.ratesHistory.add(sinceZero(70*time.Second), 100)
	tf.ratesHistory.add(sinceZero(79*time.Second), 200)
	tf.process(lagRecord(sinceZero(80*time.Second), r1, 0))
	// The r1 lag update triggered an increase and did not wait for r2
	// because r2 has LastError set.
	if err := tf.checkState(stateIncreaseRate, 400, sinceZero(80*time.Second)); err != nil {
		t.Fatal(err)
	}

	// Now the increase triggered by r1 is under test and we have to wait for it.
	// However, we'll simulate a shutdown of r1 i.e. we're no longer tracking it.
	// r1 @  85s, 0s lag, !Up
	tf.ratesHistory.add(sinceZero(80*time.Second), 200)
	tf.ratesHistory.add(sinceZero(84*time.Second), 400)
	rNotUp := lagRecord(sinceZero(85*time.Second), r1, 0)
	rNotUp.Up = false
	tf.m.lagCache.add(rNotUp)

	// r2 @  90s, 0s lag (lastError no longer set)
	tf.ratesHistory.add(sinceZero(89*time.Second), 400)
	tf.process(lagRecord(sinceZero(90*time.Second), r2, 0))
	// The r1 lag update triggered an increase and did not wait for r2
	// because r2 has !Up set.
	if err := tf.checkState(stateIncreaseRate, 800, sinceZero(90*time.Second)); err != nil {
		t.Fatal(err)
	}
}

// TestMaxReplicationLagModule_Reset_ReplicaUnderIncreaseTest verifies that
// the field "replicaUnderIncreaseTest" is correctly reset if we're leaving
// the increase state.
func TestMaxReplicationLagModule_Reset_ReplicaUnderIncreaseTest(t *testing.T) {
	tf, err := newTestFixtureWithMaxReplicationLag(5)
	if err != nil {
		t.Fatal(err)
	}

	// r2 @  70s, 0s lag (triggers the increase state)
	tf.ratesHistory.add(sinceZero(69*time.Second), 100)
	tf.process(lagRecord(sinceZero(70*time.Second), r2, 0))
	// Rate was increased to 200 based on actual rate of 100 within [0s, 69s].
	if err := tf.checkState(stateIncreaseRate, 200, sinceZero(70*time.Second)); err != nil {
		t.Fatal(err)
	}

	// r1 @  75s, 10s lag (triggers the emergency state)
	tf.ratesHistory.add(sinceZero(70*time.Second), 100)
	tf.ratesHistory.add(sinceZero(74*time.Second), 200)
	tf.process(lagRecord(sinceZero(75*time.Second), r1, 10))
	// The r1 lag update triggered the emergency state and did not wait for the
	// pending increase of r2.
	if err := tf.checkState(stateEmergency, 100, sinceZero(75*time.Second)); err != nil {
		t.Fatal(err)
	}

	// Now everything goes back to normal and the minimum time between increases
	// (10s) has passed as well. r2 or r1 can start an increase now.
	// r1 @  80s, 0s lag (triggers the increase state)
	tf.ratesHistory.add(sinceZero(75*time.Second), 200)
	tf.ratesHistory.add(sinceZero(79*time.Second), 100)
	tf.process(lagRecord(sinceZero(79*time.Second), r1, 0))
	if err := tf.checkState(stateIncreaseRate, 200, sinceZero(79*time.Second)); err != nil {
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
	tf.process(lagRecord(sinceZero(90*time.Second), r2, uint32(tf.m.config.TargetReplicationLagSec+1)))
	// The guessed replica (slave) rate is 140 because of the 3s lag increase
	// within the elapsed 20s.
	// The replica processed only 17s worth of work (20s duration - 3s lag increase)
	// 17s / 20s * 200 QPS actual rate => 170 QPS replica rate
	//
	// This results in a backlog of 3s * 200 QPS = 600 queries.
	// Since this backlog is spread across MinDurationBetweenChangesSec (10s),
	// the guessed rate gets further reduced by 60 QPS (600 queries / 10s).
	// Hence, the rate is set to 110 QPS (170 - 60).
	if err := tf.checkState(stateDecreaseAndGuessRate, 110, sinceZero(90*time.Second)); err != nil {
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
	tf.process(lagRecord(sinceZero(80*time.Second), r1, uint32(tf.m.config.TargetReplicationLagSec+1)))
	// Rate was not decreased because r1 has no lag record @ 70s or higher.
	if err := tf.checkState(stateIncreaseRate, 200, sinceZero(70*time.Second)); err != nil {
		t.Fatal(err)
	}

	// r2 @  90s, 3s lag (above target, provokes a decrease)
	tf.ratesHistory.add(sinceZero(89*time.Second), 200)
	tf.process(lagRecord(sinceZero(90*time.Second), r2, uint32(tf.m.config.TargetReplicationLagSec+1)*2))
	// Rate was decreased because r2 has a lag record @ 70s.
	//
	// The guessed replica (slave) rate is 140 because of the 6s lag increase
	// within the elapsed 20s.
	// The replica processed only 14s worth of work (20s elapsed - 6s lag increase)
	// 14s / 20s * 200 QPS actual rate => 140 QPS replica rate
	//
	// This results in a backlog of 6s * 200 QPS = 1200 queries.
	// Since this backlog is spread across MinDurationBetweenChangesSec (10s),
	// the guessed rate gets further reduced by 120 QPS (1200 queries / 10s).
	// Hence, the rate is set to 20 QPS.
	if err := tf.checkState(stateDecreaseAndGuessRate, 20, sinceZero(90*time.Second)); err != nil {
		t.Fatal(err)
	}
}

func TestMaxReplicationLagModule_IgnoreNSlowestReplicas(t *testing.T) {
	config := NewMaxReplicationLagModuleConfig(5)
	config.IgnoreNSlowestReplicas = 1
	tf, err := newTestFixture(config)
	if err != nil {
		t.Fatal(err)
	}

	// r1 @   0s, 0s lag
	tf.process(lagRecord(sinceZero(0*time.Second), r1, 0))
	if err := tf.checkState(stateIncreaseRate, 100, sinceZero(0*time.Second)); err != nil {
		t.Fatal(err)
	}

	// r2 @  10s, 10s lag
	tf.ratesHistory.add(sinceZero(9*time.Second), 100)
	tf.process(lagRecord(sinceZero(10*time.Second), r2, 10))
	// Although r2's lag is high, it's ignored because it's the 1 slowest replica.
	if err := tf.checkState(stateIncreaseRate, 100, sinceZero(0*time.Second)); err != nil {
		t.Fatal(err)
	}

	// r1 @  20s, 20s lag
	tf.ratesHistory.add(sinceZero(19*time.Second), 100)
	tf.process(lagRecord(sinceZero(20*time.Second), r1, 20))
	// r1 would become the new 1 slowest replica. However, we do not ignore it
	// because then we would ignore all known replicas in a row.
	if err := tf.checkState(stateEmergency, 50, sinceZero(20*time.Second)); err != nil {
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

	// r2 @  10s, 10s lag
	tf.ratesHistory.add(sinceZero(9*time.Second), 100)
	tf.process(lagRecord(sinceZero(10*time.Second), r2, 10))
	// r2 is the 1 slowest replica. However, it's not ignored because then we
	// would ignore all replicas. Therefore, we react to its lag increase.
	if err := tf.checkState(stateEmergency, 50, sinceZero(10*time.Second)); err != nil {
		t.Fatal(err)
	}
}

// TestMaxReplicationLagModule_IgnoreNSlowestReplicas_IsIgnoredDuringIncrease
// is almost identical to TestMaxReplicationLagModule_Increase_LastErrorOrNotUp.
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
	tf.m.lagCache.add(lagRecord(sinceZero(90*time.Second), r2, 10))
	// We ignore the 1 slowest replica and do not decrease despite r2's high lag.
	if err := tf.checkState(stateIncreaseRate, 200, sinceZero(70*time.Second)); err != nil {
		t.Fatal(err)
	}

	// r1 @ 100s, 0s lag
	tf.ratesHistory.add(sinceZero(99*time.Second), 200)
	tf.process(lagRecord(sinceZero(100*time.Second), r1, 0))
	// Meanwhile, r1 is doing fine and will trigger the next increase because
	// we're no longer waiting for the ignored r2.
	if err := tf.checkState(stateIncreaseRate, 400, sinceZero(100*time.Second)); err != nil {
		t.Fatal(err)
	}
}

// lagRecord creates a fake record using a fake TabletStats object.
func lagRecord(t time.Time, uid, lag uint32) replicationLagRecord {
	return replicationLagRecord{t, tabletStats(uid, lag)}
}

// tabletStats creates fake tablet health data.
func tabletStats(uid, lag uint32) discovery.TabletStats {
	tablet := &topodatapb.Tablet{
		Alias:    &topodatapb.TabletAlias{Cell: "cell1", Uid: uid},
		Keyspace: "ks1",
		Shard:    "-80",
		Type:     topodatapb.TabletType_REPLICA,
		PortMap:  map[string]int32{"vt": int32(uid)},
	}
	return discovery.TabletStats{
		Tablet: tablet,
		Key:    discovery.TabletToMapKey(tablet),
		Target: &querypb.Target{
			Keyspace:   "ks1",
			Shard:      "-80",
			TabletType: topodatapb.TabletType_REPLICA,
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
	if err := tf.checkState(stateIncreaseRate, 100, sinceZero(0*time.Second)); err != nil {
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

	// r2 @  90s, 0s lag
	tf.ratesHistory.add(sinceZero(80*time.Second), 300)
	tf.ratesHistory.add(sinceZero(89*time.Second), 300)
	tf.process(lagRecord(sinceZero(90*time.Second), r2, 0))
	if err := tf.checkState(stateIncreaseRate, 600, sinceZero(90*time.Second)); err != nil {
		t.Fatal(err)
	}
}
