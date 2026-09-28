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

package workflow

import (
	"fmt"
	"sort"
)

// LogRecorder is used to collect logs for a specific purpose.
// Not thread-safe since it is expected to be generated in repeatable sequence
type LogRecorder struct {
	logs []string
}

// NewLogRecorder creates a new instance of LogRecorder
func NewLogRecorder() *LogRecorder {
	lr := LogRecorder{}
	return &lr
}

// Log records a new log message
func (lr *LogRecorder) Log(log string) {
	lr.logs = append(lr.logs, log)
}

// Logf records a new log message with interpolation parameters using fmt.Sprintf.
func (lr *LogRecorder) Logf(log string, args ...any) {
	lr.logs = append(lr.logs, fmt.Sprintf(log, args...))
}

// LogSlice sorts a given slice using natural sort, so that the result is predictable.
// Useful when logging arrays or maps where order of objects can vary
func (lr *LogRecorder) LogSlice(logs []string) {
	sort.Strings(logs)
	for _, log := range logs {
		lr.Log(log)
	}
}

// GetLogs returns all recorded logs in sequence
func (lr *LogRecorder) GetLogs() []string {
	return lr.logs
}
