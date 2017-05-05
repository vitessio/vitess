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
	"sort"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/proto/throttlerdata"
)

// GlobalManager is the per-process manager which manages all active throttlers.
var GlobalManager = newManager()

// Manager defines the public interface of the throttler manager. It is used
// for example by the different RPC implementations.
type Manager interface {
	// MaxRates returns the max rate of all known throttlers.
	MaxRates() map[string]int64

	// SetMaxRate sets the max rate on all known throttlers.
	// It returns the names of the updated throttlers.
	SetMaxRate(rate int64) []string

	// GetConfiguration returns the configuration of the MaxReplicationlag module
	// for the given throttler or all throttlers if "throttlerName" is empty.
	GetConfiguration(throttlerName string) (map[string]*throttlerdata.Configuration, error)

	// UpdateConfiguration (partially) updates the configuration of the
	// MaxReplicationlag module for the given throttler or all throttlers if
	// "throttlerName" is empty.
	// If "copyZeroValues" is true, fields with zero values will be copied
	// as well.
	// The function returns the names of the updated throttlers.
	UpdateConfiguration(throttlerName string, configuration *throttlerdata.Configuration, copyZeroValues bool) ([]string, error)

	// ResetConfiguration resets the configuration of the MaxReplicationlag module
	// to the initial configuration for the given throttler or all throttlers if
	// "throttlerName" is empty.
	// The function returns the names of the updated throttlers.
	ResetConfiguration(throttlerName string) ([]string, error)
}

// managerImpl controls multiple throttlers and also aggregates their
// statistics. It implements the "Manager" interface.
type managerImpl struct {
	// mu guards all fields in this group.
	mu sync.Mutex
	// throttlers tracks all running throttlers (by their name).
	throttlers map[string]*Throttler
}

func newManager() *managerImpl {
	return &managerImpl{
		throttlers: make(map[string]*Throttler),
	}
}

func (m *managerImpl) registerThrottler(name string, throttler *Throttler) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.throttlers[name]; ok {
		return fmt.Errorf("registerThrottler(): throttler with name '%v' is already registered", name)
	}
	m.throttlers[name] = throttler
	return nil
}

func (m *managerImpl) unregisterThrottler(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.throttlers[name]; !ok {
		log.Errorf("unregisterThrottler(): throttler with name '%v' is not registered", name)
		return
	}
	delete(m.throttlers, name)
}

// MaxRates returns the max rate of all known throttlers.
func (m *managerImpl) MaxRates() map[string]int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	rates := make(map[string]int64, len(m.throttlers))
	for name, t := range m.throttlers {
		rates[name] = t.MaxRate()
	}
	return rates
}

// SetMaxRate sets the max rate on all known throttlers.
func (m *managerImpl) SetMaxRate(rate int64) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, t := range m.throttlers {
		t.SetMaxRate(rate)
	}
	return m.throttlerNamesLocked()
}

// GetConfiguration implements the "Manager" interface.
func (m *managerImpl) GetConfiguration(throttlerName string) (map[string]*throttlerdata.Configuration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	configurations := make(map[string]*throttlerdata.Configuration)

	if throttlerName != "" {
		t, ok := m.throttlers[throttlerName]
		if !ok {
			return nil, fmt.Errorf("throttler: %v does not exist", throttlerName)
		}
		configurations[throttlerName] = t.GetConfiguration()
		return configurations, nil
	}

	for name, t := range m.throttlers {
		configurations[name] = t.GetConfiguration()
	}
	return configurations, nil
}

// UpdateConfiguration implements the "Manager" interface.
func (m *managerImpl) UpdateConfiguration(throttlerName string, configuration *throttlerdata.Configuration, copyZeroValues bool) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Note: The calls to t.UpdateConfiguration() below return no error but the
	// called protobuf library functions may panic. This is fine because the
	// throttler RPC service has a panic handler which will catch this.

	if throttlerName != "" {
		t, ok := m.throttlers[throttlerName]
		if !ok {
			return nil, fmt.Errorf("throttler: %v does not exist", throttlerName)
		}
		if err := t.UpdateConfiguration(configuration, copyZeroValues); err != nil {
			return nil, fmt.Errorf("failed to update throttler: %v err: %v", throttlerName, err)
		}
		return []string{throttlerName}, nil
	}

	for name, t := range m.throttlers {
		if err := t.UpdateConfiguration(configuration, copyZeroValues); err != nil {
			return nil, fmt.Errorf("failed to update throttler: %v err: %v", name, err)
		}
	}
	return m.throttlerNamesLocked(), nil
}

// ResetConfiguration implements the "Manager" interface.
func (m *managerImpl) ResetConfiguration(throttlerName string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if throttlerName != "" {
		t, ok := m.throttlers[throttlerName]
		if !ok {
			return nil, fmt.Errorf("throttler: %v does not exist", throttlerName)
		}
		t.ResetConfiguration()
		return []string{throttlerName}, nil
	}

	for _, t := range m.throttlers {
		t.ResetConfiguration()
	}
	return m.throttlerNamesLocked(), nil
}

// Throttlers returns the sorted list of active throttlers.
func (m *managerImpl) Throttlers() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.throttlerNamesLocked()
}

func (m *managerImpl) throttlerNamesLocked() []string {
	var names []string
	for k := range m.throttlers {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

// Log returns the most recent changes of the MaxReplicationLag module.
// There will be one result for each processed replication lag record.
func (m *managerImpl) Log(throttlerName string) ([]result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	t, ok := m.throttlers[throttlerName]
	if !ok {
		return nil, fmt.Errorf("throttler: %v does not exist", throttlerName)
	}

	return t.Log(), nil
}
