package stats

// PullBackend should be implemented to export pull metrics from Vitess.
// Note that there are no functions implemented to export:
// String/StringMap as Prometheus doesn't have support for exporting metrics of those types. Future implementations could add support for that, though, if relevant to the backend.
type PullBackend interface {
	NewMetric(*Counter, string, ValueType)
	NewMetricWithLabels(*Counters, string, string, ValueType)
	NewCountersWithMultiLabels(*CountersWithMultiLabels, string)
	NewCountersFuncWithMultiLabels(*CountersFuncWithMultiLabels, string) // MultiCounterFuncs are always exported as Gauges? Should they just be gauges then.
	NewGaugesWithMultiLabels(*GaugesWithMultiLabels, string)
	NewGaugeFunc(*GaugeFunc, string)
	NewTiming(*Timings, string)
	NewMultiTiming(*MultiTimings, string)
}

// ValueType specifies whether the value of a metric goes up monotonically
// or if it goes up and down. This is useful for exporting to backends that
// differentiate between counters and gauges.
type ValueType int

const (
	// CounterValue is used to specify a value that only goes up (but can be reset to 0).
	CounterValue = iota
	// GaugeValue is used to specify a value that goes both up adn down.
	GaugeValue
)
