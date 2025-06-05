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
	"fmt"
	"math"
	"runtime"
	"time"

	"github.com/containerd/cgroups"
	"github.com/containerd/cgroups/v3/cgroup1"
	"github.com/containerd/cgroups/v3/cgroup2"
	"github.com/shirou/gopsutil/v4/mem"

	"vitess.io/vitess/go/vt/log"
)

var (
	cgroup2Manager *cgroup2.Manager
	cgroup1Manager cgroup1.Cgroup
	lastCpu        uint64
	lastTime       time.Time
)

func init() {
	if cgroups.Mode() == cgroups.Unified {
		manager, err := getCgroup2()
		if err != nil {
			log.Errorf("Failed to init cgroup2 manager: %v", err)
		}
		cgroup2Manager = manager
		lastCpu, err = getCgroup2CpuUsage()
		if err != nil {
			log.Errorf("Failed to init cgroup2 cpu %v", err)
		}
	} else {
		cgroup, err := getCgroup1()
		if err != nil {
			log.Errorf("Failed to init cgroup1 manager: %v", err)
		}
		cgroup1Manager = cgroup
		lastCpu, err = getCgroup1CpuUsage()
		if err != nil {
			log.Errorf("Failed to init cgroup1 cpu %v", err)
		}
	}
	lastTime = time.Now()
}

func isCgroupV2() bool {
	return cgroups.Mode() == cgroups.Unified
}

func getCgroup1() (cgroup1.Cgroup, error) {
	path := cgroup1.NestedPath("")
	cgroup, err := cgroup1.Load(path)
	if err != nil {
		return nil, fmt.Errorf("cgroup1 manager is nil")
	}
	return cgroup, nil
}

func getCgroup2() (*cgroup2.Manager, error) {
	path, err := cgroup2.NestedGroupPath("")
	if err != nil {
		return nil, fmt.Errorf("failed to load cgroup2 manager: %w", err)
	}
	cgroupManager, err := cgroup2.Load(path)
	if err != nil {
		return nil, fmt.Errorf("cgroup2 manager is nil")
	}
	return cgroupManager, nil
}

func getCgroupCpuUsage() (float64, error) {
	var (
		currentUsage uint64
		err          error
	)
	currentTime := time.Now()
	if isCgroupV2() {
		currentUsage, err = getCgroup2CpuUsage()
	} else {
		currentUsage, err = getCgroup1CpuUsage()
	}
	if err != nil {
		return -1, fmt.Errorf("Could not read cpu usage")
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

func getCgroupMemoryUsage() (float64, error) {
	if isCgroupV2() {
		return getCgroup2MemoryUsage()
	} else {
		return getCgroup1MemoryUsage()
	}
}

func getCgroup1CpuUsage() (uint64, error) {
	stat1, err := cgroup1Manager.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to get initial CPU stat: %w", err)
	}
	currentUsage := stat1.CPU.Usage.Total
	return currentUsage, nil
}

func getCgroup2CpuUsage() (uint64, error) {
	stat1, err := cgroup2Manager.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to get initial CPU stat: %w", err)
	}
	currentUsage := stat1.CPU.UsageUsec
	return currentUsage, nil
}

func getCpuUsageFromSamples(usage1 uint64, usage2 uint64, interval time.Duration) (float64, error) {
	if usage1 == 0 && usage2 == 0 {
		return -1, fmt.Errorf("CPU usage for both samples is zero")
	}

	deltaUsage := usage2 - usage1
	deltaTime := float64(interval.Microseconds())

	cpuCount := float64(runtime.NumCPU())
	cpuUsage := (float64(deltaUsage) / deltaTime) / cpuCount

	return cpuUsage, nil
}

func getCgroup1MemoryUsage() (float64, error) {
	stats, err := cgroup1Manager.Stat()
	if err != nil {
		return -1, fmt.Errorf("failed to get cgroup2 stats: %w", err)
	}
	usage := stats.Memory.Usage.Usage
	limit := stats.Memory.Usage.Limit
	return computeMemoryUsage(usage, limit)
}

func getCgroup2MemoryUsage() (float64, error) {
	stats, err := cgroup2Manager.Stat()
	if err != nil {
		return -1, fmt.Errorf("failed to get cgroup2 stats: %w", err)
	}
	usage := stats.Memory.Usage
	limit := stats.Memory.UsageLimit
	return computeMemoryUsage(usage, limit)
}

func computeMemoryUsage(usage uint64, limit uint64) (float64, error) {
	if usage == 0 || usage == math.MaxUint64 {
		return -1, fmt.Errorf("Failed to find memory usage with invalid value: %d", usage)
	}
	if limit == 0 {
		return -1, fmt.Errorf("Failed to compute memory usage with invalid limit: %d", limit)
	}
	if limit == math.MaxUint64 {
		vmem, err := mem.VirtualMemory()
		if err != nil {
			return -1, fmt.Errorf("Failed to fall back to system max memory: %w", err)
		}
		limit = vmem.Total
	}
	return float64(usage) / float64(limit), nil
}
