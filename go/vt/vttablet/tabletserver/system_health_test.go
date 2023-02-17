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
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

func TestSystemHealthCollector(t *testing.T) {
	monitor := &systemHealthCollector{
		config: &tabletenv.TabletConfig{
			EnableSystemHealthMonitor: true,
		},
		cpuSampleWindow: time.Millisecond * 50,
		interval:        time.Millisecond * 100,
	}

	// open
	assert.Nil(t, monitor.Open())

	// generate cpu load to measure
	stopLoadGeneration := make(chan bool, 1)
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			for {
				select {
				case <-stopLoadGeneration:
					return
				default: // nolint:staticcheck
				}
			}
		}()
	}
	time.Sleep(monitor.interval * 2)

	// try 10 times in case CPU usage is still 0.00%
	var tries int
	cpuUsage := monitor.GetCPUUsage()
	for cpuUsage == 0 && tries < 10 {
		cpuUsage = monitor.GetCPUUsage()
		tries++
		time.Sleep(monitor.interval)
	}
	stopLoadGeneration <- true
	close(stopLoadGeneration)
	assert.Less(t, tries, 10)

	assert.True(t, monitor.started)
	assert.NotZero(t, cpuUsage)

	// test close
	monitor.Close()
	assert.False(t, monitor.started)

}
