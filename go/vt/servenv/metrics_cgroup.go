//go:build linux
// +build linux

/*
Copyright 2025 The Vitess Authors.

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

package servenv

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/containerd/cgroups"
	"github.com/containerd/cgroups/v3/cgroup2"
	"github.com/shirou/gopsutil/v4/mem"

	"vitess.io/vitess/go/vt/log"
)

var (
	once                         sync.Once
	cgroupManager                *cgroup2.Manager
	lastCpu                      uint64
	lastTime                     time.Time
	errCgroupMetricsNotAvailable = errors.New("cgroup metrics are not available")
)

func setup() {
	if cgroups.Mode() != cgroups.Unified {
		log.Warning("cgroup metrics are only supported with cgroup v2, will use host metrics")
		return
	}
	manager, err := getCgroupManager()
	if err != nil {
		log.Warningf("Failed to init cgroup manager for metrics, will use host metrics: %v", err)
	}
	cgroupManager = manager
	lastCpu, err = getCurrentCgroupCpuUsage()
	if err != nil {
		log.Warningf("Failed to get initial cgroup CPU usage: %v", err)
	}
	lastTime = time.Now()
}

func getCgroupManager() (*cgroup2.Manager, error) {
	path, err := cgroup2.NestedGroupPath("")
	if err != nil {
		return nil, fmt.Errorf("failed to build nested cgroup paths: %w", err)
	}
	cgroupManager, err := cgroup2.Load(path)
	if err != nil {
		return nil, fmt.Errorf("failed to load cgroup manager: %w", err)
	}
	return cgroupManager, nil
}

func getCgroupCpuUsage() (float64, error) {
	once.Do(setup)
	var (
		currentUsage uint64
		err          error
	)
	currentTime := time.Now()
	currentUsage, err = getCurrentCgroupCpuUsage()
	if err != nil {
		return -1, fmt.Errorf("failed to read current cgroup CPU usage: %w", err)
	}
	duration := currentTime.Sub(lastTime)
	usage, err := getCpuUsageFromSamples(lastCpu, currentUsage, duration)
	if err != nil {
		return -1, err
	}
	lastCpu = currentUsage
	lastTime = currentTime
	return usage, nil
}

func getCurrentCgroupCpuUsage() (uint64, error) {
	if cgroupManager == nil {
		return 0, errCgroupMetricsNotAvailable
	}
	stat1, err := cgroupManager.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to get initial cgroup CPU stats: %w", err)
	}
	currentUsage := stat1.CPU.UsageUsec
	return currentUsage, nil
}

func getCpuUsageFromSamples(usage1 uint64, usage2 uint64, interval time.Duration) (float64, error) {
	if usage1 == 0 && usage2 == 0 {
		return -1, errors.New("CPU usage for both samples is zero")
	}

	deltaUsage := usage2 - usage1
	deltaTime := float64(interval.Microseconds())

	cpuCount := float64(runtime.NumCPU())
	cpuUsage := (float64(deltaUsage) / deltaTime) / cpuCount

	return cpuUsage, nil
}

func getCgroupMemoryUsage() (float64, error) {
	once.Do(setup)
	if cgroupManager == nil {
		return -1, errCgroupMetricsNotAvailable
	}
	stats, err := cgroupManager.Stat()
	if err != nil {
		return -1, fmt.Errorf("failed to get cgroup stats: %w", err)
	}
	usage := stats.Memory.Usage
	limit := stats.Memory.UsageLimit
	return computeMemoryUsage(usage, limit)
}

func computeMemoryUsage(usage uint64, limit uint64) (float64, error) {
	if usage == 0 || usage == math.MaxUint64 {
		return -1, fmt.Errorf("invalid memory usage value: %d", usage)
	}
	if limit == 0 {
		return -1, fmt.Errorf("invalid memory limit: %d", limit)
	}
	if limit == math.MaxUint64 {
		vmem, err := mem.VirtualMemory()
		if err != nil {
			return -1, fmt.Errorf("failed to get virtual memory stats: %w", err)
		}
		limit = vmem.Total
	}
	return float64(usage) / float64(limit), nil
}
