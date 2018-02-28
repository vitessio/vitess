package stats

import (
	"sync"

	log "github.com/golang/glog"
)

// PullBackend should be implemented to export pull metrics from Vitess.
// Note that there are no functions implemented to export:
// String/StringMap as Prometheus doesn't have support for exporting metrics of those types. Future implementations could add support for that, though, if relevant to the backend.
type PullBackend interface {
	NewMetric(*Counters, string, ValueType)
	NewMultiCounter(*MultiCounters, string)
	NewMultiGauge(*MultiGauges, string)
	NewMultiCounterFunc(*MultiCountersFunc, string) // MultiCounterFuncs are always exported as Gauges
	NewInt(*Int, string, ValueType)
	NewIntFunc(*IntFunc, string) // IntFuncs are always exported as Gauges
	NewTiming(*Timings, string)
	NewMultiTiming(*MultiTimings, string)
}

var (
	mu sync.Mutex
	be = make(map[string]PullBackend)

	pullCounters          = make(map[string]*Counters)
	pullGauges            = make(map[string]*Gauges)
	pullMultiGauges       = make(map[string]*MultiGauges)
	pullMultiCounters     = make(map[string]*MultiCounters)
	pullMultiCountersFunc = make(map[string]*MultiCountersFunc)
	pullTimings           = make(map[string]*Timings)
	pullMultiTimings      = make(map[string]*MultiTimings)
	pullInts              = make(map[string]*Int)
	pullIntGauges         = make(map[string]*IntGauge)
	pullIntsFunc          = make(map[string]*IntFunc)
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

func publishPullMultiCounters(mc *MultiCounters, name string) {
	mu.Lock()
	defer mu.Unlock()
	pullMultiCounters[name] = mc

	for _, backend := range be {
		backend.NewMultiCounter(mc, name)
	}
}

func publishPullCounters(c *Counters, name string) {
	mu.Lock()
	defer mu.Unlock()
	pullCounters[name] = c

	for _, backend := range be {
		backend.NewMetric(c, name, CounterValue)
	}
}

func publishPullGauges(g *Gauges, name string) {
	mu.Lock()
	defer mu.Unlock()
	pullGauges[name] = g
	for _, backend := range be {
		backend.NewMetric(&g.Counters, name, GaugeValue)
	}
}

func publishPullMultiGauges(mg *MultiGauges, name string) {
	mu.Lock()
	defer mu.Unlock()
	pullMultiGauges[name] = mg
	for _, backend := range be {
		backend.NewMultiGauge(mg, name)
	}
}

func publishPullMultiCountersFunc(mcf *MultiCountersFunc, name string) {
	mu.Lock()
	defer mu.Unlock()
	pullMultiCountersFunc[name] = mcf

	for _, backend := range be {
		backend.NewMultiCounterFunc(mcf, name)
	}
}

func publishPullTimings(t *Timings, name string) {
	mu.Lock()
	defer mu.Unlock()
	pullTimings[name] = t

	for _, backend := range be {
		backend.NewTiming(t, name)
	}
}

func publishPullMultiTimings(mt *MultiTimings, name string) {
	mu.Lock()
	defer mu.Unlock()
	pullMultiTimings[name] = mt
	for _, backend := range be {
		backend.NewMultiTiming(mt, name)
	}
}

func publishPullInt(i *Int, name string) {
	mu.Lock()
	defer mu.Unlock()
	pullInts[name] = i
	for _, backend := range be {
		backend.NewInt(i, name, CounterValue)
	}
}

func publishPullIntGauge(i *IntGauge, name string) {
	mu.Lock()
	defer mu.Unlock()
	pullIntGauges[name] = i
	for _, backend := range be {
		backend.NewInt(&i.Int, name, GaugeValue)
	}
}

func publishPullIntFunc(i *IntFunc, name string) {
	mu.Lock()
	defer mu.Unlock()
	pullIntsFunc[name] = i
	for _, backend := range be {
		backend.NewIntFunc(i, name)
	}
}

// RegisterPullBackendImpl registers an implementation of PullBackend
func RegisterPullBackendImpl(name string, pullBackend PullBackend) {
	mu.Lock()
	defer mu.Unlock()
	if _, ok := be[name]; ok {
		log.Fatalf("PullBackend named %v already exists", name)
	}

	// Flush pre-existing metrics
	for mcName, mc := range pullMultiCounters {
		pullBackend.NewMultiCounter(mc, mcName)
	}

	for mgName, mg := range pullMultiGauges {
		pullBackend.NewMultiGauge(mg, mgName)
	}

	for cName, c := range pullCounters {
		pullBackend.NewMetric(c, cName, CounterValue)
	}

	for gName, g := range pullGauges {
		pullBackend.NewMetric(&g.Counters, gName, GaugeValue)
	}

	for mcfName, mcf := range pullMultiCountersFunc {
		pullBackend.NewMultiCounterFunc(mcf, mcfName)
	}

	for tName, t := range pullTimings {
		pullBackend.NewTiming(t, tName)
	}

	for mtName, mt := range pullMultiTimings {
		pullBackend.NewMultiTiming(mt, mtName)
	}

	for iName, i := range pullInts {
		pullBackend.NewInt(i, iName, CounterValue)
	}

	for ifName, intFunc := range pullIntsFunc {
		pullBackend.NewIntFunc(intFunc, ifName)
	}

	for iGaugeName, iGauge := range pullIntGauges {
		pullBackend.NewInt(&iGauge.Int, iGaugeName, GaugeValue)
	}

	be[name] = pullBackend
}
