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

package tabletserver

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

type memoryPressureState int32

const (
	memoryPressureStateDisabled memoryPressureState = iota
	memoryPressureStateNormal
	memoryPressureStateSoft
	memoryPressureStateHard

	defaultMemoryPressureRefreshInterval = time.Millisecond
)

type memoryPressureController struct {
	enabled         bool
	softThreshold   float64
	hardThreshold   float64
	resumeThreshold float64
	getMemoryUsage  func() float64
	refreshInterval time.Duration

	mu sync.Mutex

	lastUsageBits      atomic.Uint64
	lastSampleUnixNano atomic.Int64
	lastState          atomic.Int32

	rejectedRequests *stats.CountersWithMultiLabels
}

func newMemoryPressureController(cfg *tabletenv.MemoryPressureConfig, exporter *servenv.Exporter, getMemoryUsage func() float64) *memoryPressureController {
	controller := &memoryPressureController{
		getMemoryUsage:  getMemoryUsage,
		refreshInterval: defaultMemoryPressureRefreshInterval,
	}
	if cfg != nil {
		controller.enabled = cfg.Enable
		controller.softThreshold = cfg.SoftThreshold
		controller.hardThreshold = cfg.HardThreshold
		controller.resumeThreshold = cfg.ResumeThreshold
	}

	initialState := memoryPressureStateDisabled
	if controller.enabled {
		initialState = memoryPressureStateNormal
	}
	controller.lastState.Store(int32(initialState))
	controller.lastUsageBits.Store(math.Float64bits(-1))
	controller.lastSampleUnixNano.Store(0)

	if exporter != nil {
		exporter.NewGaugeFunc("MemoryPressureState", "Current memory-pressure admission state: disabled=0, normal=1, soft=2, hard=3.", func() int64 {
			return int64(controller.state())
		})
		exporter.Publish("MemoryPressureUtilization", stats.FloatFunc(func() float64 {
			return controller.usage()
		}))
		exporter.Publish("MemoryPressureSoftThreshold", stats.FloatFunc(func() float64 {
			return controller.softThreshold
		}))
		exporter.Publish("MemoryPressureHardThreshold", stats.FloatFunc(func() float64 {
			return controller.hardThreshold
		}))
		exporter.Publish("MemoryPressureResumeThreshold", stats.FloatFunc(func() float64 {
			return controller.resumeThreshold
		}))
		controller.rejectedRequests = exporter.NewCountersWithMultiLabels(
			"MemoryPressureRejectedRequests",
			"Requests rejected because memory-pressure backpressure was active.",
			[]string{"Request", "Threshold", "State"},
		)
	}

	return controller
}

func (s memoryPressureState) String() string {
	switch s {
	case memoryPressureStateDisabled:
		return "disabled"
	case memoryPressureStateNormal:
		return "normal"
	case memoryPressureStateSoft:
		return "soft"
	case memoryPressureStateHard:
		return "hard"
	default:
		return "unknown"
	}
}

func (c *memoryPressureController) state() memoryPressureState {
	return memoryPressureState(c.lastState.Load())
}

func (c *memoryPressureController) usage() float64 {
	return math.Float64frombits(c.lastUsageBits.Load())
}

func (c *memoryPressureController) reject(requestName string) error {
	if !c.enabled || requestName == "" {
		return nil
	}

	state, usage := c.observe()
	switch {
	case state < memoryPressureStateSoft:
		return nil
	case state >= memoryPressureStateHard:
		if shouldBypassHardMemoryPressure(requestName) {
			return nil
		}
		return c.newRejectError(requestName, usage, memoryPressureStateHard, state)
	case shouldRejectOnSoftMemoryPressure(requestName):
		return c.newRejectError(requestName, usage, memoryPressureStateSoft, state)
	default:
		return nil
	}
}

func (c *memoryPressureController) rejectIfAtLeast(requestName string, threshold memoryPressureState) error {
	if !c.enabled || requestName == "" {
		return nil
	}

	state, usage := c.observe()
	if state < threshold {
		return nil
	}
	return c.newRejectError(requestName, usage, threshold, state)
}

func (c *memoryPressureController) observe() (memoryPressureState, float64) {
	now := time.Now()
	if state, usage, ok := c.cachedObservation(now); ok {
		return state, usage
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	usage := c.observeUsageLocked(now)
	state := c.nextStateLocked(usage)
	c.lastState.Store(int32(state))

	return state, usage
}

func (c *memoryPressureController) cachedObservation(now time.Time) (memoryPressureState, float64, bool) {
	if c.refreshInterval <= 0 {
		return 0, 0, false
	}

	lastSampleUnixNano := c.lastSampleUnixNano.Load()
	if lastSampleUnixNano == 0 || now.UnixNano()-lastSampleUnixNano >= c.refreshInterval.Nanoseconds() {
		return 0, 0, false
	}

	state := memoryPressureState(c.lastState.Load())
	if state >= memoryPressureStateSoft {
		return 0, 0, false
	}

	usage := c.usage()
	if c.lastSampleUnixNano.Load() != lastSampleUnixNano {
		return 0, 0, false
	}

	return state, usage, true
}

func (c *memoryPressureController) observeUsageLocked(now time.Time) float64 {
	if c.getMemoryUsage == nil {
		c.lastUsageBits.Store(math.Float64bits(-1))
		c.lastSampleUnixNano.Store(0)
		return -1
	}

	if c.refreshInterval > 0 && c.state() < memoryPressureStateSoft {
		lastSampleUnixNano := c.lastSampleUnixNano.Load()
		if lastSampleUnixNano != 0 && now.UnixNano()-lastSampleUnixNano < c.refreshInterval.Nanoseconds() {
			return c.usage()
		}
	}

	usage := c.getMemoryUsage()
	c.lastUsageBits.Store(math.Float64bits(usage))
	c.lastSampleUnixNano.Store(now.UnixNano())
	return usage
}

func (c *memoryPressureController) nextStateLocked(usage float64) memoryPressureState {
	if !c.enabled {
		return memoryPressureStateDisabled
	}
	if usage < 0 {
		return memoryPressureStateNormal
	}

	switch c.state() {
	case memoryPressureStateHard:
		if usage <= c.resumeThreshold {
			return memoryPressureStateNormal
		}
		return memoryPressureStateHard
	case memoryPressureStateSoft:
		if usage >= c.hardThreshold {
			return memoryPressureStateHard
		}
		if usage <= c.resumeThreshold {
			return memoryPressureStateNormal
		}
		return memoryPressureStateSoft
	default:
		if usage >= c.hardThreshold {
			return memoryPressureStateHard
		}
		if usage >= c.softThreshold {
			return memoryPressureStateSoft
		}
		return memoryPressureStateNormal
	}
}

func (c *memoryPressureController) newRejectError(requestName string, usage float64, threshold memoryPressureState, state memoryPressureState) error {
	if c.rejectedRequests != nil {
		c.rejectedRequests.Add([]string{requestName, threshold.String(), state.String()}, 1)
	}
	return vterrors.Errorf(
		vtrpcpb.Code_RESOURCE_EXHAUSTED,
		"memory pressure: request %s rejected at %.4f utilization (threshold %s=%.4f, current state=%s)",
		requestName,
		usage,
		threshold.String(),
		c.thresholdValue(threshold),
		state.String(),
	)
}

func (c *memoryPressureController) thresholdValue(threshold memoryPressureState) float64 {
	switch threshold {
	case memoryPressureStateSoft:
		return c.softThreshold
	case memoryPressureStateHard:
		return c.hardThreshold
	default:
		return 0
	}
}

func (c *memoryPressureController) setForTests(cfg tabletenv.MemoryPressureConfig, getMemoryUsage func() float64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.enabled = cfg.Enable
	c.softThreshold = cfg.SoftThreshold
	c.hardThreshold = cfg.HardThreshold
	c.resumeThreshold = cfg.ResumeThreshold
	c.getMemoryUsage = getMemoryUsage
	c.refreshInterval = 0

	state := memoryPressureStateDisabled
	if c.enabled {
		state = memoryPressureStateNormal
	}
	c.lastState.Store(int32(state))
	c.lastUsageBits.Store(math.Float64bits(-1))
	c.lastSampleUnixNano.Store(0)
}

func shouldRejectOnSoftMemoryPressure(requestName string) bool {
	switch requestName {
	case "Begin", "StreamExecute", "Reserve", "ReserveBegin", "VStream", "VStreamRows", "VStreamTables", "VStreamResults":
		return true
	default:
		return false
	}
}

func shouldBypassHardMemoryPressure(requestName string) bool {
	switch requestName {
	case "Commit", "Rollback", "Release", "CommitPrepared", "RollbackPrepared", "StartCommit", "SetRollback", "ConcludeTransaction":
		return true
	default:
		return false
	}
}
