// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package throttler

import (
	"fmt"
	"sync"

	log "github.com/golang/glog"
)

// GlobalManager is the per-process manager which manages all active throttlers.
var GlobalManager = newManager()

// Manager controls multiple throttlers and also aggregates their statistics.
type Manager struct {
	// mu guards all fields in this group.
	mu sync.Mutex
	// throttlers tracks all running throttlers (by their name).
	throttlers map[string]*Throttler
}

func newManager() *Manager {
	return &Manager{
		throttlers: make(map[string]*Throttler),
	}
}

func (m *Manager) registerThrottler(name string, throttler *Throttler) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.throttlers[name]; ok {
		return fmt.Errorf("registerThrottler(): throttler with name '%v' is already registered", name)
	}
	m.throttlers[name] = throttler
	return nil
}

func (m *Manager) unregisterThrottler(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.throttlers[name]; !ok {
		log.Errorf("unregisterThrottler(): throttler with name '%v' is not registered", name)
		return
	}
	delete(m.throttlers, name)
}

// MaxRates returns the max rate of all known throttlers.
func (m *Manager) MaxRates() map[string]int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	rates := make(map[string]int64, len(m.throttlers))
	for name, t := range m.throttlers {
		rates[name] = t.MaxRate()
	}
	return rates
}

// SetMaxRate sets the max rate on all known throttlers.
func (m *Manager) SetMaxRate(rate int64) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	var names []string
	for name, t := range m.throttlers {
		t.SetMaxRate(rate)
		names = append(names, name)
	}
	return names
}
