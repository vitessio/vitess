/*
   Copyright 2014 Outbrain Inc.

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

package metrics

import (
	"time"

	"vitess.io/vitess/go/vt/orchestrator/config"
)

var matricTickCallbacks [](func())

// InitMetrics is called once in the lifetime of the app, after config has been loaded
func InitMetrics() error {
	go func() {
		metricsCallbackTick := time.Tick(time.Duration(config.DebugMetricsIntervalSeconds) * time.Second)
		for range metricsCallbackTick {
			for _, f := range matricTickCallbacks {
				go f()
			}
		}
	}()

	return nil
}

func OnMetricsTick(f func()) {
	matricTickCallbacks = append(matricTickCallbacks, f)
}
