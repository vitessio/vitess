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

package engine

import (
	"sync"

	"vitess.io/vitess/go/stats"
)

type Metrics struct {
	optimizedQueryExec *stats.CountersWithSingleLabel
}

// TODO (fix it): This is a temporary solution to avoid multiple registry of metric counter in test.
var defaultMetric = &Metrics{}
var once sync.Once

func InitializeMetrics() *Metrics {
	once.Do(func() {
		defaultMetric.optimizedQueryExec = stats.NewCountersWithSingleLabel("OptimizedQueryExecutions", "Counts optimized queries executed at VTGate by plan type.", "Plan")
	})
	return defaultMetric
}
