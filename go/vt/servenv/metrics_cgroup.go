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
	"log"
	"runtime"
	"time"

	"github.com/containerd/cgroups"
	"github.com/containerd/cgroups/v3/cgroup1"
	"github.com/containerd/cgroups/v3/cgroup2"
)

var (
	cgroup2Manager *cgroup2.Manager
	cgroup1Manager cgroup1.Cgroup
)

func init() {
	if cgroups.Mode() == cgroups.Unified {
		manager, err := getCGroup2()
		if err != nil {
			log.Printf("Failed to load cgroup2 manager: %v", err)
			return
		}
		cgroup2Manager = manager
	} else {
		cgroup, err := getCGroup1()
		if err != nil {
			log.Printf("Failed to load cgroup1 manager: %v", err)
			return
		}
		cgroup1Manager = cgroup
	}
}

func isCgroupV2() bool {
	return cgroups.Mode() == cgroups.Unified
}

func getCGroup1() (cgroup1.Cgroup, error) {
	path := cgroup1.NestedPath("")
	cgroup, err := cgroup1.Load(path)
	if err != nil {
		return nil, fmt.Errorf("cgroup1 manager is nil")
	}
	return cgroup, nil
}

func getCGroup2() (*cgroup2.Manager, error) {
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

func getCgroupCpuUsage(interval time.Duration) (float64, error) {
	if isCgroupV2() {
		return getCgroup2CpuUsage(interval)
	} else {
		return getCgroup1CpuUsage(interval)
	}
}

func getCgroupMemoryUsage() (float64, error) {
	if isCgroupV2() {
		return getCgroup2MemoryUsage()
	} else {
		return getCgroup1MemoryUsage()
	}
}

func getCgroup1CpuUsage(interval time.Duration) (float64, error) {
	stat1, err := cgroup1Manager.Stat()
	if err != nil {
		return -1, fmt.Errorf("failed to get initial CPU stat: %w", err)
	}
	usage1 := stat1.CPU.Usage.Total

	time.Sleep(interval)

	stat2, err := cgroup1Manager.Stat()
	if err != nil {
		return -1, fmt.Errorf("failed to get second CPU stat: %w", err)
	}
	usage2 := stat2.CPU.Usage.Total

	return getCpuUsageFromSamples(usage1, usage2, interval)
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
	if limit == 0 || limit == ^uint64(0) {
		return -1, fmt.Errorf("Failed to compute memory usage with invalid limit: %d", limit)
	}
	return float64(usage) / float64(limit), nil
}

func getCgroup2CpuUsage(interval time.Duration) (float64, error) {
	stat1, err := cgroup2Manager.Stat()
	if err != nil {
		return -1, fmt.Errorf("failed to get initial CPU stat: %w", err)
	}
	usage1 := stat1.CPU.UsageUsec

	time.Sleep(interval)

	stat2, err := cgroup2Manager.Stat()
	if err != nil {
		return -1, fmt.Errorf("failed to get second CPU stat: %w", err)
	}
	usage2 := stat2.CPU.UsageUsec

	return getCpuUsageFromSamples(usage1, usage2, interval)
}

func getCgroup2MemoryUsage() (float64, error) {
	stats, err := cgroup2Manager.Stat()
	if err != nil {
		return -1, fmt.Errorf("failed to get cgroup2 stats: %w", err)
	}
	usage := stats.Memory.Usage
	limit := stats.Memory.UsageLimit
	if limit == 0 || limit == ^uint64(0) {
		return -1, fmt.Errorf("Failed to compute memory usage with invalid limit: %d", limit)
	}
	return float64(usage) / float64(limit), nil
}
