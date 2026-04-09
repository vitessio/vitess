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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
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
	origCgroupMemoryPath := cgroupMemoryPath
	origCgroupMemoryLimit := cgroupMemoryLimit.Load()
	defer func() {
		cgroupManager = origCgroupManager
		cgroupMemoryPath = origCgroupMemoryPath
		cgroupMemoryLimit.Store(origCgroupMemoryLimit)
	}()

	cpu, err := getCgroupCpuUsage()
	validateCpu(t, cpu, err)
	mem, err := getCgroupMemoryUsage()
	validateMem(t, mem, err)

	cgroupManager = nil
	cgroupMemoryPath = ""
	cgroupMemoryLimit.Store(0)
	require.Nil(t, cgroupManager)

	cpu, err = getCgroupCpuUsage()
	require.ErrorContains(t, err, errCgroupMetricsNotAvailable.Error())
	require.Equal(t, int(cpu), -1)
	mem, err = getCgroupMemoryUsage()
	require.ErrorContains(t, err, errCgroupMetricsNotAvailable.Error())
	require.Equal(t, int(mem), -1)
}

func TestGetCgroupMemoryUsageAtPath(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.current"), []byte("100\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.max"), []byte("200\n"), 0o644))

	var cachedLimit atomic.Uint64
	usage, err := getCgroupMemoryUsageAtPath(dir, &cachedLimit)
	require.NoError(t, err)
	assert.InDelta(t, 0.5, usage, 0.000001)
	require.Equal(t, uint64(200), cachedLimit.Load())
}

func TestGetCgroupMemoryLimitAtPathCachesLimit(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.max"), []byte("200\n"), 0o644))

	var cachedLimit atomic.Uint64
	limit, err := getCgroupMemoryLimitAtPath(dir, &cachedLimit)
	require.NoError(t, err)
	require.Equal(t, uint64(200), limit)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.max"), []byte("400\n"), 0o644))
	limit, err = getCgroupMemoryLimitAtPath(dir, &cachedLimit)
	require.NoError(t, err)
	require.Equal(t, uint64(200), limit)
}

func TestReadCgroupMemoryValue(t *testing.T) {
	dir := t.TempDir()

	t.Run("numeric", func(t *testing.T) {
		path := filepath.Join(dir, "memory.current")
		require.NoError(t, os.WriteFile(path, []byte("123\n"), 0o644))
		value, err := readCgroupMemoryValue(path)
		require.NoError(t, err)
		require.Equal(t, uint64(123), value)
	})

	t.Run("max", func(t *testing.T) {
		path := filepath.Join(dir, "memory.max")
		require.NoError(t, os.WriteFile(path, []byte("max\n"), 0o644))
		value, err := readCgroupMemoryValue(path)
		require.NoError(t, err)
		require.Equal(t, ^uint64(0), value)
	})
}
