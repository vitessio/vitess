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
	"testing"
)

func TestGetCpuMetrics(t *testing.T) {
	sleepBeforeCpuSample()
	cpuUsage, err := getHostCpuUsage()
	validateCpu(t, cpuUsage, err)
	t.Logf("CPU Utilization is %.2f", cpuUsage)
}

func TestGetMemoryMetrics(t *testing.T) {
	memoryUsage, err := getHostMemoryUsage()
	validateMem(t, memoryUsage, err)
	t.Logf("Memory Utilization is %.2f", memoryUsage)
}
