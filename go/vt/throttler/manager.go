// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package throttler

import (
	"fmt"
	"sort"
	"sync"

	log "github.com/golang/glog"
)

// GlobalManager is the per-process manager which manages all active throttlers.
var GlobalManager = newManager()

// Manager defines the public interface of the throttler manager. It is used
// for example by the different RPC implementations.
type Manager interface {
	// MaxRates returns the max rate of all known throttlers.
	MaxRates() map[string]int64

	// SetMaxRate sets the max rate on all known throttlers.
	SetMaxRate(rate int64) []string
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

	var names []string
	for name, t := range m.throttlers {
		t.SetMaxRate(rate)
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
