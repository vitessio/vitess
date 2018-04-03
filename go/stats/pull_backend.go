package stats

import (
	"expvar"
	"sync"

	log "github.com/golang/glog"
)

// PullBackend should be implemented to export pull metrics from Vitess.
// Note that there are no functions implemented to export:
// String/StringMap as Prometheus doesn't have support for exporting metrics of those types. Future implementations could add support for that, though, if relevant to the backend.
type PullBackend interface {
	NewMetric(*Counter, string, ValueType)
	NewMetricWithLabels(*Counters, string, ValueType)
	NewCountersWithMultiLabels(*MultiCounters, string)
	NewCountersFuncWithMultiLabels(*MultiCountersFunc, string) // MultiCounterFuncs are always exported as Gauges
	NewGaugesWithMultiLabels(*GaugesWithLabels, string)
	NewGaugeFunc(*GaugeFunc, string)
	NewTiming(*Timings, string)
	NewMultiTiming(*MultiTimings, string)
}

var (
	mu sync.Mutex
	be = make(map[string]PullBackend)
)

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

// PublishPullMetric publishes the given metric on the registered backends.
func PublishPullMetric(name string, v expvar.Var) {
	mu.Lock()
	defer mu.Unlock()
	for _, backend := range be {
		switch st := v.(type) {
		case *Counter:
			backend.NewMetric(st, name, CounterValue)
		case *Gauge:
			backend.NewMetric(&st.Counter, name, GaugeValue)
		case *GaugeFunc:
			backend.NewGaugeFunc(st, name)
		case *CountersWithLabels:
			backend.NewMetricWithLabels(&st.Counters, name, st.labelName, CounterValue)
		case *CountersWithMultiLabels:
			backend.NewCountersWithMultiLabels(st, name)
		case *CountersFuncWithMultiLabels:
			backend.NewCountersFuncWithMultiLabels(st, name)
		case *GaugesWithLabels:
			backend.NewMetricWithLabels(&st.Counters, name, st.labelName, GaugeValue)
		case *GaugesWithMultiLabels:
			backend.NewGaugesWithMultiLabels(st, name)
		case *Timings:
			backend.NewTiming(st, name)
		case *MultiTimings:
			backend.NewMultiTiming(st, name)
		default:
			log.Warningf("Unsupported type for %s: %T", name, st)
		}
	}
}

// RegisterPullBackendImpl registers an implementation of PullBackend
func RegisterPullBackendImpl(name string, pb PullBackend) {
	mu.Lock()
	defer mu.Unlock()
	if _, ok := be[name]; ok {
		log.Fatalf("PullBackend named %v already exists", name)
	}
	be[name] = pb
}
