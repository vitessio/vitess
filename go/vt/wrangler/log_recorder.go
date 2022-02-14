package wrangler

import (
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
	//fmt.Printf("DR: %s\n", log)
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
