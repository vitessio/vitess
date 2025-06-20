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
	"runtime"
	"testing"
	"time"
)

func sleepBeforeCpuSample() {
	time.Sleep(750 * time.Millisecond)
}

func validateCpu(t *testing.T, cpu float64, err error) {
	if err != nil {
		t.Errorf("Error reading CPU: %v, value %.10f", err, cpu)
	}
	if cpu <= 0 || cpu > float64(runtime.NumCPU()) {
		t.Errorf("CPU value out of range %5.f", cpu)
	}
}

func validateMem(t *testing.T, mem float64, err error) {
	if err != nil {
		t.Errorf("Error reading memory: %v, value %.10f", err, mem)
	}
	if mem <= 0 || mem > 1 {
		t.Errorf("Mem value out of range %5.f", mem)
	}
}

func TestGetCpuUsageMetrics(t *testing.T) {
	sleepBeforeCpuSample()
	value := getCpuUsage()
	t.Logf("CPU usage %v", value)
	validateCpu(t, value, nil)
}

func TestGetMemoryUsageMetrics(t *testing.T) {
	value := getMemoryUsage()
	t.Logf("Memory usage %v", value)
	validateMem(t, value, nil)
}
