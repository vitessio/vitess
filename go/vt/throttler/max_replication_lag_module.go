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
	"math"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo/topoproto"

	throttlerdatapb "vitess.io/vitess/go/vt/proto/throttlerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

type state string

const (
	stateIncreaseRate         state = "I"
	stateDecreaseAndGuessRate state = "D"
	stateEmergency            state = "E"
)

type replicationLagChange int

const (
	unknown replicationLagChange = iota
	less
	equal
	greater
)

// replicaUnderTest refers to the replica to which we're currently "locked in"
// i.e. we'll ignore lag records with lower lag from other replicas while we're
// waiting for the next record of this replica under test.
type replicaUnderTest struct {
	// key holds the discovery.TabletStats.Key value for the replica.
	key        string
	alias      string
	tabletType topodatapb.TabletType
	// state is the "state under test" which triggered the rate change.
	state             state
	nextAllowedChange time.Time
}

// memoryGranularity is the granularity at which values are rounded down in the
// memory instance. This is done to bucket similar rates (see also memory.go).
const memoryGranularity = 5

// MaxReplicationLagModule calculates the maximum rate based on observed
// replication lag and throttler rate changes.
// It implements the Module interface.
// Unless specified, the fields below are not guarded by a Mutex because they
// are only accessed within the Go routine running ProcessRecords().
type MaxReplicationLagModule struct {
	// config holds all parameters for this module.
	// It is only accessed by the Go routine which runs ProcessRecords() and does
	// not require locking.
	config MaxReplicationLagModuleConfig

	// initialMaxReplicationLagSec is the initial value of
	// config.MaxReplicationLagSec with which the module was started. We remember
	// it in case we need to reset the configuration.
	initialMaxReplicationLagSec int64

	// mutableConfigMu guards all fields in the group below.
	mutableConfigMu sync.Mutex
	// mutableConfig is the mutable copy of "config" which is currently used by
	// the module. By modifying "mutableConfig" and setting "applyMutableConfig"
	// to true, the next ProcessRecords() execution will copy "mutableConfig"
	// to "config" and become effective in the module.
	mutableConfig MaxReplicationLagModuleConfig
	// applyMutableConfig is set to true when ProcessRecords() should copy over
	// "mutableConfig" to "config".
	applyMutableConfig bool

	// rate is the rate calculated for the throttler.
	rate         sync2.AtomicInt64
	currentState state
	// lastRateChange is the time when rate was adjusted last.
	lastRateChange             time.Time
	replicaUnderTest           *replicaUnderTest
	nextAllowedChangeAfterInit time.Time

	actualRatesHistory *aggregatedIntervalHistory
	// replicaLagCache stores the past lag records for all REPLICA tablets.
	replicaLagCache *replicationLagCache
	// rdonlyLagCache stores the past lag records for all RDONLY tablets.
	rdonlyLagCache *replicationLagCache
	// memory tracks known good and bad throttler rates.
	memory *memory

	// rateUpdateChan is the notification channel to tell the throttler when our
	// max rate calculation has changed. The field is immutable (set in Start().)
	rateUpdateChan chan<- struct{}

	// lagRecords buffers the replication lag records received by the HealthCheck
	// listener. ProcessRecords() will process them.
	lagRecords chan replicationLagRecord
	wg         sync.WaitGroup

	// results caches the results of the latest processed replication lag records.
	results *resultRing
}

// NewMaxReplicationLagModule will create a new module instance and set the
// initial max replication lag limit to maxReplicationLag.
func NewMaxReplicationLagModule(config MaxReplicationLagModuleConfig, actualRatesHistory *aggregatedIntervalHistory, nowFunc func() time.Time) (*MaxReplicationLagModule, error) {
	if err := config.Verify(); err != nil {
		return nil, fmt.Errorf("invalid NewMaxReplicationLagModuleConfig: %v", err)
	}
	rate := int64(ReplicationLagModuleDisabled)
	if config.MaxReplicationLagSec != ReplicationLagModuleDisabled {
		rate = config.InitialRate
	}

	m := &MaxReplicationLagModule{
		initialMaxReplicationLagSec: config.MaxReplicationLagSec,
		// Register "config" for a future config update.
		mutableConfig:      config,
		applyMutableConfig: true,
		// Always start off with a non-zero rate because zero means all requests
		// get throttled.
		rate:           sync2.NewAtomicInt64(rate),
		currentState:   stateIncreaseRate,
		lastRateChange: nowFunc(),
		memory:         newMemory(memoryGranularity, config.AgeBadRateAfter(), config.BadRateIncrease),
		lagRecords:     make(chan replicationLagRecord, 10),
		// Prevent an immediate increase of the initial rate.
		nextAllowedChangeAfterInit: nowFunc().Add(config.MaxDurationBetweenIncreases()),
		actualRatesHistory:         actualRatesHistory,
		replicaLagCache:            newReplicationLagCache(1000),
		rdonlyLagCache:             newReplicationLagCache(1000),
		results:                    newResultRing(1000),
	}

	// Enforce a config update.
	m.applyLatestConfig()

	return m, nil
}

// Start launches a Go routine which reacts on replication lag records.
// It implements the Module interface.
func (m *MaxReplicationLagModule) Start(rateUpdateChan chan<- struct{}) {
	m.rateUpdateChan = rateUpdateChan
	m.wg.Add(1)
	go m.ProcessRecords()
}

// Stop blocks until the module's Go routine is stopped.
// It implements the Module interface.
func (m *MaxReplicationLagModule) Stop() {
	close(m.lagRecords)
	m.wg.Wait()
}

// MaxRate returns the current maximum allowed rate.
// It implements the Module interface.
func (m *MaxReplicationLagModule) MaxRate() int64 {
	return m.rate.Get()
}

// applyLatestConfig checks if "mutableConfig" should be applied as the new
// "config" and does so when necessary.
func (m *MaxReplicationLagModule) applyLatestConfig() {
	var config MaxReplicationLagModuleConfig
	applyConfig := false

	m.mutableConfigMu.Lock()
	if m.applyMutableConfig {
		config = m.mutableConfig
		applyConfig = true
		m.applyMutableConfig = false
	}
	m.mutableConfigMu.Unlock()

	// No locking required here because this method is only called from the same
	// Go routine as ProcessRecords() or the constructor.
	if applyConfig {
		m.config = config
		m.memory.updateAgingConfiguration(config.AgeBadRateAfter(), config.BadRateIncrease)
	}
}

func (m *MaxReplicationLagModule) getConfiguration() *throttlerdatapb.Configuration {
	m.mutableConfigMu.Lock()
	defer m.mutableConfigMu.Unlock()

	configCopy := m.mutableConfig.Configuration
	return &configCopy
}

func (m *MaxReplicationLagModule) updateConfiguration(configuration *throttlerdatapb.Configuration, copyZeroValues bool) error {
	m.mutableConfigMu.Lock()
	defer m.mutableConfigMu.Unlock()

	newConfig := m.mutableConfig

	if copyZeroValues {
		newConfig.Configuration = *proto.Clone(configuration).(*throttlerdatapb.Configuration)
	} else {
		proto.Merge(&newConfig.Configuration, configuration)
	}

	if err := newConfig.Verify(); err != nil {
		return err
	}
	m.mutableConfig = newConfig
	m.applyMutableConfig = true
	return nil
}

func (m *MaxReplicationLagModule) resetConfiguration() {
	m.mutableConfigMu.Lock()
	defer m.mutableConfigMu.Unlock()

	m.mutableConfig = NewMaxReplicationLagModuleConfig(m.initialMaxReplicationLagSec)
	m.applyMutableConfig = true
}

// RecordReplicationLag records the current replication lag for processing.
func (m *MaxReplicationLagModule) RecordReplicationLag(t time.Time, ts *discovery.TabletStats) {
	m.mutableConfigMu.Lock()
	if m.mutableConfig.MaxReplicationLagSec == ReplicationLagModuleDisabled {
		m.mutableConfigMu.Unlock()
		return
	}
	m.mutableConfigMu.Unlock()

	// Buffer data point for now to unblock the HealthCheck listener and process
	// it asynchronously in ProcessRecords().
	m.lagRecords <- replicationLagRecord{t, *ts}
}

// ProcessRecords is the main loop, run in a separate Go routine, which
// reacts to any replication lag updates (recordings).
func (m *MaxReplicationLagModule) ProcessRecords() {
	defer m.wg.Done()

	for lagRecord := range m.lagRecords {
		m.processRecord(lagRecord)
	}
}

func (m *MaxReplicationLagModule) processRecord(lagRecord replicationLagRecord) {
	m.applyLatestConfig()

	m.lagCache(lagRecord).add(lagRecord)

	m.lagCache(lagRecord).sortByLag(m.getNSlowestReplicasConfig(lagRecord), m.config.MaxReplicationLagSec+1)

	m.recalculateRate(lagRecord)
}

func (m *MaxReplicationLagModule) lagCache(lagRecord replicationLagRecord) *replicationLagCache {
	return m.lagCacheByType(lagRecord.Target.TabletType)
}

func (m *MaxReplicationLagModule) lagCacheByType(tabletType topodatapb.TabletType) *replicationLagCache {
	switch tabletType {
	case topodatapb.TabletType_REPLICA:
		return m.replicaLagCache
	case topodatapb.TabletType_RDONLY:
		return m.rdonlyLagCache
	default:
		panic(fmt.Sprintf("BUG: invalid TabletType forwarded: %v", tabletType))
	}
}

func (m *MaxReplicationLagModule) getNSlowestReplicasConfig(lagRecord replicationLagRecord) int {
	switch lagRecord.Target.TabletType {
	case topodatapb.TabletType_REPLICA:
		return int(m.config.IgnoreNSlowestReplicas)
	case topodatapb.TabletType_RDONLY:
		return int(m.config.IgnoreNSlowestRdonlys)
	default:
		panic(fmt.Sprintf("BUG: invalid TabletType forwarded: %v", lagRecord))
	}
}

func (m *MaxReplicationLagModule) recalculateRate(lagRecordNow replicationLagRecord) {
	if lagRecordNow.isZero() {
		panic("rate recalculation was triggered with a zero replication lag record")
	}
	now := lagRecordNow.time
	lagNow := lagRecordNow.lag()

	m.memory.ageBadRate(now)

	r := result{
		Now:            now,
		RateChange:     unchangedRate,
		lastRateChange: m.lastRateChange,
		OldState:       m.currentState,
		NewState:       m.currentState,
		OldRate:        m.rate.Get(),
		NewRate:        m.rate.Get(),
		LagRecordNow:   lagRecordNow,
	}
	if lagNow <= m.config.TargetReplicationLagSec {
		// Lag in range: [0, target]
		r.TestedState = stateIncreaseRate
	} else if lagNow <= m.config.MaxReplicationLagSec {
		// Lag in range: (target, max]
		r.TestedState = stateDecreaseAndGuessRate
	} else {
		// Lag in range: (max, infinite]
		r.TestedState = stateEmergency
	}

	// Declare new variables before we use "goto". Required by the Go compiler.
	var clear bool
	var clearReason string

	if m.lagCache(lagRecordNow).ignoreSlowReplica(lagRecordNow.Key) {
		r.Reason = fmt.Sprintf("skipping this replica because it's among the %d slowest %v tablets", m.getNSlowestReplicasConfig(lagRecordNow), lagRecordNow.Target.TabletType.String())
		goto logResult
	}

	clear, clearReason = m.clearReplicaUnderTest(now, r.TestedState, lagRecordNow)
	if clear {
		clearReason = fmt.Sprintf("; previous replica under test (%v) cleared because %v", m.replicaUnderTest.alias, clearReason)
		m.replicaUnderTest = nil
	}

	if r.TestedState == stateIncreaseRate && now.Before(m.nextAllowedChangeAfterInit) {
		r.Reason = fmt.Sprintf("waiting %.1f more seconds since init until we try the first increase", m.nextAllowedChangeAfterInit.Sub(now).Seconds())
		goto logResult
	}

	if !m.isReplicaUnderTest(&r, now, r.TestedState, lagRecordNow) {
		goto logResult
	}

	// Process the lag record and adjust the rate.
	if m.replicaUnderTest != nil {
		// We're checking the same replica again. The old value is no longer needed.
		m.replicaUnderTest = nil
	}
	switch r.TestedState {
	case stateIncreaseRate:
		m.increaseRate(&r, now, lagRecordNow)
	case stateDecreaseAndGuessRate:
		m.decreaseAndGuessRate(&r, now, lagRecordNow)
	case stateEmergency:
		m.emergency(&r, now, lagRecordNow)
	default:
		panic(fmt.Sprintf("BUG: invalid state: %v", r.TestedState))
	}

logResult:
	r.HighestGood = m.memory.highestGood()
	r.LowestBad = m.memory.lowestBad()

	if clear {
		r.Reason += clearReason
	}

	log.Infof("%v", r)
	m.results.add(r)
}

// clearReplicaUnderTest returns true if the current "replica under test" should
// be cleared e.g. because the new lag record is more severe or we did not hear
// back from the replica under test for a while.
func (m *MaxReplicationLagModule) clearReplicaUnderTest(now time.Time, testedState state, lagRecordNow replicationLagRecord) (bool, string) {
	if m.replicaUnderTest == nil {
		return false, ""
	}

	if stateGreater(testedState, m.replicaUnderTest.state) {
		// Example: "decrease" > "increase" will return true.
		return true, fmt.Sprintf("the new state is more severe (%v -> %v)", m.replicaUnderTest.state, testedState)
	}

	// Verify that the current replica under test is not in an error state.
	lr := lagRecordNow
	if m.replicaUnderTest.key != lr.Key {
		lr = m.lagCacheByType(m.replicaUnderTest.tabletType).latest(m.replicaUnderTest.key)
	}
	if lr.isZero() {
		// Replica is no longer tracked by the cache i.e. may be offline.
		return true, "it is no longer actively tracked"
	}
	if lr.LastError != nil {
		// LastError is set i.e. HealthCheck module cannot connect and the cached
		// data for the replica might be outdated.
		return true, "it has LastError set i.e. is no longer correctly tracked"
	}

	if m.lagCacheByType(m.replicaUnderTest.tabletType).isIgnored(m.replicaUnderTest.key) {
		// "replica under test" has become a slow, ignored replica.
		return true, "it is ignored as a slow replica"
	}

	if now.After(m.replicaUnderTest.nextAllowedChange.Add(m.config.MaxDurationBetweenIncreases())) {
		// We haven't heard from the replica under test for too long. Assume it did
		// timeout.
		return true, fmt.Sprintf("we didn't see a recent record from it within the last %.1f seconds", m.config.MaxDurationBetweenIncreases().Seconds())
	}

	return false, ""
}

// stateGreater returns true if a > b i.e. the state "a" is more severe than
// "b". For example, "decrease" > "increase" returns true.
func stateGreater(a, b state) bool {
	switch a {
	case stateIncreaseRate:
		return false
	case stateDecreaseAndGuessRate:
		return b == stateIncreaseRate
	case stateEmergency:
		return b == stateIncreaseRate || b == stateDecreaseAndGuessRate
	default:
		panic(fmt.Sprintf("BUG: cannot compare states: %v and %v", a, b))
	}
}

// isReplicaUnderTest returns true if a 'replica under test' is currently set
// and we should not skip the current replica ("lagRecordNow").
// Even if it's the same replica we may skip it and return false because
// we want to wait longer for the propagation of the current rate change.
func (m *MaxReplicationLagModule) isReplicaUnderTest(r *result, now time.Time, testedState state, lagRecordNow replicationLagRecord) bool {
	if m.replicaUnderTest == nil {
		return true
	}

	if m.replicaUnderTest.key != lagRecordNow.Key {
		r.Reason = fmt.Sprintf("skipping this replica because we're waiting for the next lag record from the 'replica under test': %v", m.replicaUnderTest.alias)
		return false
	}

	if stateGreater(m.replicaUnderTest.state, testedState) {
		// The state of the "replica under test" improved e.g. it went from
		// "decreased" to "increased".
		// Stop testing (i.e. skipping) it and do not wait until the full test
		// duration is up.
		return true
	}

	if now.Before(m.replicaUnderTest.nextAllowedChange) {
		r.Reason = fmt.Sprintf("waiting %.1f more seconds to see if the lag has changed", m.replicaUnderTest.nextAllowedChange.Sub(now).Seconds())
		return false
	}

	return true
}

func (m *MaxReplicationLagModule) increaseRate(r *result, now time.Time, lagRecordNow replicationLagRecord) {
	m.markCurrentRateAsBadOrGood(r, now, stateIncreaseRate, unknown)

	oldRate := m.rate.Get()
	actualRate := m.actualRatesHistory.average(m.lastRateChange, now)
	// Do not increase the rate if we didn't see an actual rate that approached the current max rate.
	// actualRate will be NaN if there were no observations in the history.
	if math.IsNaN(actualRate) ||
		actualRate < float64(oldRate)*m.config.MaxRateApproachThreshold {
		r.RateChange = unchangedRate
		r.OldRate = oldRate
		r.NewRate = oldRate
		r.Reason = fmt.Sprintf("Skipping periodic increase of the max rate (%v) since the actual: average rate (%v) did not approach it.", oldRate, actualRate)
		r.CurrentRate = int64(actualRate)
		return
	}

	// Calculate new rate based on the previous (preferably highest good) rate.
	highestGood := m.memory.highestGood()
	previousRateSource := "highest known good rate"
	previousRate := float64(highestGood)
	if previousRate == 0.0 {
		// No known high good rate. Use the actual value instead.
		// (It might be lower because the system was slower or the throttler rate was
		// set by a different module and not us.)
		previousRateSource = "previous actual rate"
		previousRate = actualRate
	}

	// a) Increase rate by MaxIncrease.
	increaseReason := fmt.Sprintf("a max increase of %.1f%%", m.config.MaxIncrease*100)
	rate := previousRate * (1 + m.config.MaxIncrease)

	// b) Make the increase less aggressive if it goes above the bad rate.
	lowestBad := float64(m.memory.lowestBad())
	if lowestBad != 0 {
		if rate > lowestBad {
			// New rate will be the middle value of [previous rate, lowest bad rate].
			rate = previousRate + (lowestBad-previousRate)/2
			increaseReason += fmt.Sprintf(" (but limited to the middle value in the range [previous rate, lowest bad rate]: [%.0f, %.0f]", previousRate, lowestBad)
		}
	}
	// c) Always make minimum progress compared to oldRate.
	// Necessary for the following cases:
	// - MaxIncrease is too low and the rate might not increase
	// - after the new rate was limited by the bad rate, we got the old rate
	//   (In this case we might slightly go above the bad rate which we accept.)
	if int64(rate) <= oldRate {
		rate = float64(oldRate) + memoryGranularity
		increaseReason += fmt.Sprintf(" (minimum progress by %v)", memoryGranularity)
		previousRateSource = "previous set rate"
		previousRate = float64(oldRate)
	}

	increase := (rate - previousRate) / previousRate
	minTestDuration := m.minTestDurationUntilNextIncrease(increase)
	reason := fmt.Sprintf("periodic increase of the %v from %d to %d (by %.1f%%) based on %v to find out the maximum - next allowed increase in %.0f seconds",
		previousRateSource, int64(previousRate), int64(rate), increase*100, increaseReason, minTestDuration.Seconds())
	m.updateRate(r, stateIncreaseRate, int64(rate), reason, now, lagRecordNow, minTestDuration)
}

func (m *MaxReplicationLagModule) minTestDurationUntilNextIncrease(increase float64) time.Duration {
	minDuration := m.config.MinDurationBetweenIncreases()
	// We may have to wait longer than the configured minimum duration
	// until we see an effect of the increase.
	// Example: If the increase was fully over the capacity, it will take
	// 1 / increase seconds until the replication lag goes up by 1 second.
	// E.g.
	// (If the system was already at its maximum capacity (e.g. 1k QPS) and we
	// increase the rate by e.g. 5% to 1050 QPS, it will take 20 seconds until
	// 1000 extra queries are buffered and the lag increases by 1 second.)
	// On top of that, add 2 extra seconds to account for a delayed propagation
	// of the data (because the throttler takes over the updated rate only every
	// second and it publishes its rate history only after a second is over).
	// TODO(mberlin): Instead of adding 2 seconds, should we wait for twice the
	// calculated time instead?
	minPropagationTime := time.Duration(1.0/increase+2) * time.Second
	if minPropagationTime > minDuration {
		minDuration = minPropagationTime
	}
	if minDuration > m.config.MaxDurationBetweenIncreases() {
		// Cap the rate to a reasonable amount of time (very small increases may
		// result into a 20 minutes wait otherwise.)
		minDuration = m.config.MaxDurationBetweenIncreases()
	}
	return minDuration
}

func (m *MaxReplicationLagModule) decreaseAndGuessRate(r *result, now time.Time, lagRecordNow replicationLagRecord) {
	// Guess slave rate based on the difference in the replication lag of this
	// particular replica.
	lagRecordBefore := m.lagCache(lagRecordNow).atOrAfter(lagRecordNow.Key, m.lastRateChange)
	if lagRecordBefore.isZero() {
		// We should see at least "lagRecordNow" here because we did just insert it
		// in processRecord().
		panic(fmt.Sprintf("BUG: replicationLagCache did not return the lagRecord for current replica: %v or a previous record of it. lastRateChange: %v replicationLagCache size: %v entries: %v", lagRecordNow, m.lastRateChange, len(m.lagCache(lagRecordNow).entries), m.lagCache(lagRecordNow).entries))
	}
	// Store the record in the result.
	r.LagRecordBefore = lagRecordBefore
	if lagRecordBefore.time == lagRecordNow.time {
		// No lag record for this replica in the time span
		// [last rate change, current lag record).
		// Without it we won't be able to guess the slave rate.
		// We err on the side of caution and reduce the rate by half the emergency
		// decrease percentage.
		decreaseReason := fmt.Sprintf("no previous lag record for this replica available since the last rate change (%.1f seconds ago)", now.Sub(m.lastRateChange).Seconds())
		m.decreaseRateByPercentage(r, now, lagRecordNow, stateDecreaseAndGuessRate, m.config.EmergencyDecrease/2, decreaseReason)
		return
	}

	// Analyze if the past rate was good or bad.
	lagBefore := lagRecordBefore.lag()
	lagNow := lagRecordNow.lag()
	replicationLagChange := less
	// Note that we consider lag changes of 1 second as equal as well because
	// they might be a rounding error in MySQL due to using timestamps at
	// second granularity.
	if lagNow == lagBefore || math.Abs(float64(lagNow-lagBefore)) == 1 {
		replicationLagChange = equal
	} else if lagNow > lagBefore {
		replicationLagChange = greater
	}
	m.markCurrentRateAsBadOrGood(r, now, stateDecreaseAndGuessRate, replicationLagChange)

	if replicationLagChange == equal {
		// The replication lag did not change. Keep going at the current rate.
		r.Reason = fmt.Sprintf("did not decrease the rate because the lag did not change (assuming a 1s error margin)")
		return
	}

	// Find out the average rate (per second) at which we inserted data
	// at the master during the observed timespan.
	from := lagRecordBefore.time
	to := lagRecordNow.time
	avgMasterRate := m.actualRatesHistory.average(from, to)
	if math.IsNaN(avgMasterRate) {
		// NaN (0.0/0.0) occurs when no observations were in the timespan.
		// Wait for more rate observations.
		r.Reason = fmt.Sprintf("did not decrease the rate because the throttler has not recorded its historic rates in the range [%v , %v]", from.Format("15:04:05"), to.Format("15:04:05"))
		return
	}

	// Sanity check and correct the data points if necessary.
	d := lagRecordNow.time.Sub(lagRecordBefore.time)
	lagDifference := time.Duration(lagRecordNow.lag()-lagRecordBefore.lag()) * time.Second
	if lagDifference > d {
		log.Errorf("Replication lag increase is higher than the elapsed time: %v > %v. This should not happen. Replication Lag Data points: Before: %+v Now: %+v", lagDifference, d, lagRecordBefore, lagRecordNow)
		d = lagDifference
	}

	// Guess the slave capacity based on the replication lag change.
	rate, reason := m.guessSlaveRate(r, avgMasterRate, lagBefore, lagNow, lagDifference, d)

	m.updateRate(r, stateDecreaseAndGuessRate, rate, reason, now, lagRecordNow, m.config.MinDurationBetweenDecreases())
}

// guessSlaveRate guesses the actual slave rate based on the new bac
// Note that "lagDifference" can be positive (lag increased) or negative (lag
// decreased).
func (m *MaxReplicationLagModule) guessSlaveRate(r *result, avgMasterRate float64, lagBefore, lagNow int64, lagDifference, d time.Duration) (int64, string) {
	// avgSlaveRate is the average rate (per second) at which the slave
	// applied transactions from the replication stream. We infer the value
	// from the relative change in the replication lag.
	avgSlaveRate := avgMasterRate * (d - lagDifference).Seconds() / d.Seconds()
	if avgSlaveRate <= 0 {
		log.Warningf("guessed slave rate was <= 0 (%v). master rate: %v d: %.1f lag difference: %.1f", avgSlaveRate, avgMasterRate, d.Seconds(), lagDifference.Seconds())
		avgSlaveRate = 1
	}
	r.MasterRate = int64(avgMasterRate)
	r.GuessedSlaveRate = int64(avgSlaveRate)

	oldRequestsBehind := 0.0
	// If the old lag was > 0s, the slave needs to catch up on that as well.
	if lagNow > lagBefore {
		oldRequestsBehind = avgSlaveRate * float64(lagBefore)
	}
	newRequestsBehind := 0.0
	// If the lag increased (i.e. slave rate was slower), the slave must make up
	// for the difference in the future.
	if avgSlaveRate < avgMasterRate {
		newRequestsBehind = (avgMasterRate - avgSlaveRate) * d.Seconds()
	}
	requestsBehind := oldRequestsBehind + newRequestsBehind
	r.GuessedSlaveBacklogOld = int(oldRequestsBehind)
	r.GuessedSlaveBacklogNew = int(newRequestsBehind)

	newRate := avgSlaveRate
	// Reduce the new rate such that it has time to catch up the requests it's
	// behind within the next interval.
	futureRequests := newRate * m.config.SpreadBacklogAcross().Seconds()
	newRate *= (futureRequests - requestsBehind) / futureRequests
	var reason string
	if newRate < 1 {
		// Backlog is too high. Reduce rate to 1 request/second.
		// TODO(mberlin): Make this a constant.
		newRate = 1
		reason = fmt.Sprintf("based on the guessed slave rate of: %v the slave won't be able to process the guessed backlog of %d requests within the next %.f seconds", int64(avgSlaveRate), int64(requestsBehind), m.config.SpreadBacklogAcross().Seconds())
	} else {
		reason = fmt.Sprintf("new rate is %d lower than the guessed slave rate to account for a guessed backlog of %d requests over %.f seconds", int64(avgSlaveRate-newRate), int64(requestsBehind), m.config.SpreadBacklogAcross().Seconds())
	}

	return int64(newRate), reason
}

func (m *MaxReplicationLagModule) emergency(r *result, now time.Time, lagRecordNow replicationLagRecord) {
	m.markCurrentRateAsBadOrGood(r, now, stateEmergency, unknown)

	decreaseReason := fmt.Sprintf("replication lag went beyond max: %d > %d", lagRecordNow.lag(), m.config.MaxReplicationLagSec)
	m.decreaseRateByPercentage(r, now, lagRecordNow, stateEmergency, m.config.EmergencyDecrease, decreaseReason)
}

func (m *MaxReplicationLagModule) decreaseRateByPercentage(r *result, now time.Time, lagRecordNow replicationLagRecord, newState state, decrease float64, decreaseReason string) {
	oldRate := m.rate.Get()
	rate := int64(float64(oldRate) - float64(oldRate)*decrease)
	if rate == 0 {
		// Never fully stop throttling.
		rate = 1
	}

	reason := fmt.Sprintf("%v: reducing previous rate of %d by %.f%% to: %v", decreaseReason, oldRate, decrease*100, rate)
	m.updateRate(r, newState, rate, reason, now, lagRecordNow, m.config.MinDurationBetweenDecreases())
}

func (m *MaxReplicationLagModule) updateRate(r *result, newState state, rate int64, reason string, now time.Time, lagRecordNow replicationLagRecord, testDuration time.Duration) {
	oldRate := m.rate.Get()

	m.currentState = newState

	// Update result with the new state.
	r.NewState = newState
	r.NewRate = rate
	r.Reason = reason
	if rate > oldRate {
		r.RateChange = increasedRate
	} else if rate < oldRate {
		r.RateChange = decreasedRate
	}

	m.lastRateChange = now
	m.replicaUnderTest = &replicaUnderTest{lagRecordNow.Key, topoproto.TabletAliasString(lagRecordNow.Tablet.Alias), lagRecordNow.Target.TabletType, newState, now.Add(testDuration)}

	if rate == oldRate {
		return
	}
	m.rate.Set(int64(rate))
	// Notify the throttler that we updated our max rate.
	m.rateUpdateChan <- struct{}{}
}

// markCurrentRateAsBadOrGood determines the actual rate between the last rate
// change and "now" and determines if that rate was bad or good.
func (m *MaxReplicationLagModule) markCurrentRateAsBadOrGood(r *result, now time.Time, newState state, replicationLagChange replicationLagChange) {
	if m.lastRateChange.IsZero() {
		// Module was just started. We don't have any data points yet.
		r.GoodOrBad = ignoredRate
		r.MemorySkipReason = "rate was never changed before (initial start)"
		return
	}

	// Use the actual rate instead of the set rate.
	// (It might be lower because the system was slower or the throttler rate was
	// set by a different module and not us.)
	rate := m.actualRatesHistory.average(m.lastRateChange, now)
	if math.IsNaN(rate) {
		// NaN (0.0/0.0) occurs when no records were in the timespan.
		// Wait for more records.
		r.GoodOrBad = ignoredRate
		r.MemorySkipReason = "cannot determine actual rate: no records in [lastRateChange, now]"
		return
	}

	rateIsGood := false

	switch m.currentState {
	case stateIncreaseRate:
		switch newState {
		case stateIncreaseRate:
			rateIsGood = true
		case stateDecreaseAndGuessRate:
			rateIsGood = false
		case stateEmergency:
			rateIsGood = false
		}
	case stateDecreaseAndGuessRate:
		switch newState {
		case stateIncreaseRate:
			rateIsGood = true
		case stateDecreaseAndGuessRate:
			switch replicationLagChange {
			case unknown:
				return
			case less:
				rateIsGood = true
			case equal:
				// Replication lag kept constant. Impossible to judge if the rate is good or bad.
				return
			case greater:
				rateIsGood = false
			}
		case stateEmergency:
			rateIsGood = false
		}
	case stateEmergency:
		// Rate changes initiated during an "emergency" phase provide no meaningful data point.
		r.MemorySkipReason = "not marking a rate as good or bad while in the emergency state"
		m.memory.touchBadRateAge(now)
		return
	}

	r.CurrentRate = int64(rate)
	if rateIsGood {
		if err := m.memory.markGood(int64(rate)); err == nil {
			r.GoodOrBad = goodRate
		} else {
			r.MemorySkipReason = err.Error()
		}
	} else {
		if err := m.memory.markBad(int64(rate), now); err == nil {
			r.GoodOrBad = badRate
		} else {
			r.MemorySkipReason = err.Error()
		}
	}
}

func (m *MaxReplicationLagModule) log() []result {
	return m.results.latestValues()
}
