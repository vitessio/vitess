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
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetCGroupCpuUsageMetrics(t *testing.T) {
	sleepBeforeCpuSample()
	cpu, err := getCgroupCpuUsage()
	validateCpu(t, cpu, err)
	t.Logf("cpu %.5f", cpu)
}

func TestGetCgroupMemoryUsageMetrics(t *testing.T) {
	mem, err := getCgroupMemoryUsage()
	validateMem(t, mem, err)
	t.Logf("mem %.5f", mem)
}

func TestErrHandlingWithCgroups(t *testing.T) {
	origCgroupManager := cgroupManager
	defer func() {
		cgroupManager = origCgroupManager
	}()

	cpu, err := getCgroupCpuUsage()
	validateCpu(t, cpu, err)
	mem, err := getCgroupMemoryUsage()
	validateMem(t, mem, err)

	cgroupManager = nil
	require.Nil(t, cgroupManager)

	cpu, err = getCgroupCpuUsage()
	require.ErrorContains(t, err, errCgroupMetricsNotAvailable.Error())
	require.Equal(t, -1, int(cpu))
	mem, err = getCgroupMemoryUsage()
	require.ErrorContains(t, err, errCgroupMetricsNotAvailable.Error())
	require.Equal(t, -1, int(mem))
}

func TestCgroupCpuCount(t *testing.T) {
	numCPU := float64(runtime.NumCPU())

	tests := []struct {
		name   string
		cpuMax map[string]string
		want   float64
	}{
		{name: "no cpu.max files", want: numCPU},
		{name: "unlimited", cpuMax: map[string]string{"/pod/ctr": "max 100000\n"}, want: numCPU},
		{name: "limit on the group", cpuMax: map[string]string{"/pod/ctr": "200000 100000\n"}, want: 2},
		{name: "limit on an ancestor", cpuMax: map[string]string{"/pod": "50000 100000\n", "/pod/ctr": "max 100000\n"}, want: 0.5},
		{name: "smallest limit wins", cpuMax: map[string]string{"/pod": "300000 100000\n", "/pod/ctr": "150000 100000\n"}, want: 1.5},
		{name: "limit above the host CPU count", cpuMax: map[string]string{"/pod/ctr": "100000000 100000\n"}, want: numCPU},
		{name: "malformed", cpuMax: map[string]string{"/pod/ctr": "garbage\n"}, want: numCPU},
		{name: "invalid quota", cpuMax: map[string]string{"/pod/ctr": "abc 100000\n"}, want: numCPU},
		{name: "zero period", cpuMax: map[string]string{"/pod/ctr": "100000 0\n"}, want: numCPU},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mountpoint := t.TempDir()
			require.NoError(t, os.MkdirAll(filepath.Join(mountpoint, "pod", "ctr"), 0o755))
			for group, content := range tt.cpuMax {
				require.NoError(t, os.WriteFile(filepath.Join(mountpoint, group, "cpu.max"), []byte(content), 0o644))
			}
			require.InDelta(t, tt.want, cgroupCpuCount(mountpoint, "/pod/ctr"), 1e-9)
		})
	}
}
