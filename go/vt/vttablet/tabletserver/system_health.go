/*
Copyright 2023 The Vitess Authors.

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
	"context"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"

	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

const (
	systemHealthMonitorCPUSampleWindow = time.Millisecond * 200
	systemHealthMonitorInterval        = time.Millisecond * 1000
)

// systemHealthCollector is a collector for system health.
type systemHealthCollector struct {
	config          *tabletenv.TabletConfig
	cpuSampleWindow time.Duration
	cpuUsage        sync2.AtomicFloat64
	interval        time.Duration
	mu              sync.Mutex
	started         bool
	stop            chan bool
	wg              sync.WaitGroup
}

// newSystemHealthMonitor initiates a new systemHealthCollector.
func newSystemHealthMonitor(env tabletenv.Env) systemHealthMonitor {
	return &systemHealthCollector{
		config:          env.Config(),
		cpuSampleWindow: systemHealthMonitorCPUSampleWindow,
		interval:        systemHealthMonitorInterval,
	}
}

// Open start the collection of system health metrics.
func (s *systemHealthCollector) Open() error {
	if !s.config.EnableSystemHealthMonitor {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return nil
	}

	s.stop = make(chan bool, 1)
	go s.startCollection()
	s.started = true

	return s.collectCPUUsage()
}

// Close stops collection of system health metrics.
func (s *systemHealthCollector) Close() {
	if !s.config.EnableSystemHealthMonitor {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return
	}

	s.stop <- true
	close(s.stop)

	s.wg.Wait()
	s.started = false
}

// GetCPUUsage returns the average cpu usage for the system
// running vttablet as a percent.
func (s *systemHealthCollector) GetCPUUsage() float64 {
	return s.cpuUsage.Get()
}

// startCollection begins the collection of system health metrics.
func (s *systemHealthCollector) startCollection() {
	s.wg.Add(1)
	defer s.wg.Done()

	ticker := time.NewTicker(s.interval)
	for {
		select {
		case <-s.stop:
			ticker.Stop()
			return
		case <-ticker.C:
			if err := s.collectCPUUsage(); err != nil {
				log.Errorf("Failed to gather system cpu usage: %v", err)
			}
		}
	}
}

// collectCPUUsage collects the CPU usage percent of the system running vttablet.
func (s *systemHealthCollector) collectCPUUsage() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.interval)
	defer cancel()

	// get avg cpu of all cpu cores. passing 'false' to .PercentWithContext
	// causes a single average of all cores to be returned as a percentage.
	cpuPercents, err := cpu.PercentWithContext(ctx, s.cpuSampleWindow, false)
	if err == nil && len(cpuPercents) > 0 {
		s.cpuUsage.Set(cpuPercents[0])
	}
	return err
}
