//go:build linux

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
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/containerd/cgroups/v3"
	"github.com/containerd/cgroups/v3/cgroup2"
	"github.com/shirou/gopsutil/v4/mem"

	"vitess.io/vitess/go/vt/log"
)

var (
	once                         sync.Once
	cgroupManager                *cgroup2.Manager
	cgroupMemoryPath             string
	cgroupMemoryLimit            atomic.Uint64
	lastCpu                      uint64
	lastTime                     time.Time
	errCgroupMetricsNotAvailable = errors.New("cgroup metrics are not available")
)

func setup() {
	if cgroups.Mode() != cgroups.Unified {
		log.Warn("cgroup metrics are only supported with cgroup v2, will use host metrics")
		return
	}
	groupPath, err := cgroup2.NestedGroupPath("")
	if err != nil {
		log.Warn(fmt.Sprintf("Failed to resolve cgroup path for metrics, will use host metrics: %v", err))
		return
	}
	cgroupMemoryPath = filepath.Join("/sys/fs/cgroup", groupPath)

	manager, err := cgroup2.Load(groupPath)
	if err != nil {
		log.Warn(fmt.Sprintf("Failed to init cgroup manager for metrics, will use host metrics: %v", err))
	}
	cgroupManager = manager
	lastCpu, err = getCurrentCgroupCpuUsage()
	if err != nil {
		log.Warn(fmt.Sprintf("Failed to get initial cgroup CPU usage: %v", err))
	}
	lastTime = time.Now()
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
	if cgroupMemoryPath == "" {
		return -1, errCgroupMetricsNotAvailable
	}
	return getCgroupMemoryUsageAtPath(cgroupMemoryPath, &cgroupMemoryLimit)
}

func getCgroupMemoryUsageAtPath(path string, cachedLimit *atomic.Uint64) (float64, error) {
	usage, err := readCgroupMemoryValue(filepath.Join(path, "memory.current"))
	if err != nil {
		return -1, fmt.Errorf("failed to read cgroup memory.current: %w", err)
	}
	limit, err := getCgroupMemoryLimitAtPath(path, cachedLimit)
	if err != nil {
		return -1, err
	}
	return computeMemoryUsage(usage, limit)
}

func getCgroupMemoryLimitAtPath(path string, cachedLimit *atomic.Uint64) (uint64, error) {
	if cachedLimit != nil {
		if limit := cachedLimit.Load(); limit != 0 {
			return limit, nil
		}
	}

	limit, err := readCgroupMemoryValue(filepath.Join(path, "memory.max"))
	if err != nil {
		return 0, fmt.Errorf("failed to read cgroup memory.max: %w", err)
	}
	if limit == math.MaxUint64 {
		vmem, err := mem.VirtualMemory()
		if err != nil {
			return 0, fmt.Errorf("failed to get virtual memory stats: %w", err)
		}
		limit = vmem.Total
	}
	if cachedLimit != nil && limit != 0 {
		cachedLimit.Store(limit)
	}
	return limit, nil
}

func readCgroupMemoryValue(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}

	value := strings.TrimSpace(string(data))
	if value == "" {
		return 0, errors.New("empty cgroup value")
	}
	if value == "max" {
		return math.MaxUint64, nil
	}

	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse cgroup value %q: %w", value, err)
	}
	return parsed, nil
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
