package prombackend

import (
	"strings"

	prom "github.com/prometheus/client_golang/prometheus"
	"vitess.io/vitess/go/stats"
)

// metricsCollector collects both stats.Counters and stats.Gauges
type metricsCollector struct {
	counters map[*stats.Counters]*prom.Desc
	vt       stats.ValueType
}

// Describe implements Collector.
func (c *metricsCollector) Describe(ch chan<- *prom.Desc) {
	for _, desc := range c.counters {
		ch <- desc
	}
}

// Collect implements Collector.
func (c *metricsCollector) Collect(ch chan<- prom.Metric) {
	for counter, desc := range c.counters {
		for tag, val := range counter.Counts() {
			ch <- prom.MustNewConstMetric(
				desc,
				toPromValueType(c.vt),
				float64(val),
				tag)
		}
	}
}

type multiCountersCollector struct {
	multiCounters map[*stats.MultiCounters]*prom.Desc
}

// Describe implements Collector.
func (c *multiCountersCollector) Describe(ch chan<- *prom.Desc) {
	for _, desc := range c.multiCounters {
		ch <- desc
	}
}

// Collect implements Collector.
func (c *multiCountersCollector) Collect(ch chan<- prom.Metric) {
	for mc, desc := range c.multiCounters {
		for lvs, val := range mc.Counters.Counts() {
			labelValues := strings.Split(lvs, ".")
			value := float64(val)
			ch <- prom.MustNewConstMetric(desc, prom.CounterValue, value, labelValues...)
		}
	}
}

type multiGaugesCollector struct {
	multiGauges map[*stats.MultiGauges]*prom.Desc
}

// Describe implements Collector.
func (c *multiGaugesCollector) Describe(ch chan<- *prom.Desc) {
	for _, desc := range c.multiGauges {
		ch <- desc
	}
}

// Collect implements Collector.
func (c *multiGaugesCollector) Collect(ch chan<- prom.Metric) {
	for mc, desc := range c.multiGauges {
		for lvs, val := range mc.Gauges.Counters.Counts() {
			labelValues := strings.Split(lvs, ".")
			value := float64(val)
			ch <- prom.MustNewConstMetric(desc, prom.GaugeValue, value, labelValues...)
		}
	}
}

type multiCountersFuncCollector struct {
	multiCountersFunc map[*stats.MultiCountersFunc]*prom.Desc
}

// Describe implements Collector.
func (c *multiCountersFuncCollector) Describe(ch chan<- *prom.Desc) {
	for _, desc := range c.multiCountersFunc {
		ch <- desc
	}
}

// Collect implements Collector.
func (c *multiCountersFuncCollector) Collect(ch chan<- prom.Metric) {
	for mcf, desc := range c.multiCountersFunc {
		for lvs, val := range mcf.CountersFunc.Counts() {
			labelValues := strings.Split(lvs, ".")
			value := float64(val)
			ch <- prom.MustNewConstMetric(desc, prom.CounterValue, value, labelValues...)
		}
	}
}

type intCollector struct {
	intMetrics map[*stats.Int]*prom.Desc
	vt         stats.ValueType
}

// Describe implements Collector.
func (c *intCollector) Describe(ch chan<- *prom.Desc) {
	for _, desc := range c.intMetrics {
		ch <- desc
	}
}

// Collect implements Collector.
func (c *intCollector) Collect(ch chan<- prom.Metric) {
	for i, desc := range c.intMetrics {
		val := i.Get()
		if i.Get() == 0 {
			val = 1
		}
		ch <- prom.MustNewConstMetric(desc, toPromValueType(c.vt), float64(val))
	}
}

type timingsCollector struct {
	timings map[*stats.Timings]*prom.Desc
}

// Describe implements Collector.
func (c *timingsCollector) Describe(ch chan<- *prom.Desc) {
	for _, desc := range c.timings {
		ch <- desc
	}
}

// Collect implements Collector.
func (c *timingsCollector) Collect(ch chan<- prom.Metric) {
	for t, desc := range c.timings {
		for cat, his := range t.Histograms() {
			ch <- prom.MustNewConstHistogram(
				desc,
				uint64(his.Count()),
				float64(his.Total()),
				makePromBucket(his.Cutoffs(), his.Buckets()),
				cat)
		}
	}
}

func makePromBucket(cutoffs []int64, buckets []int64) map[float64]uint64 {
	output := make(map[float64]uint64)
	last := uint64(0)
	for i := range cutoffs {
		//TODO(zmagg): ewwwww gross type conversion. error if it overflows?
		output[float64(cutoffs[i])] = uint64(buckets[i]) + last
		last = output[float64(cutoffs[i])]
	}
	return output
}

type multiTimingsCollector struct {
	multiTimings map[*stats.MultiTimings]*prom.Desc
}

// Describe implements Collector.
func (c *multiTimingsCollector) Describe(ch chan<- *prom.Desc) {
	for _, desc := range c.multiTimings {
		ch <- desc
	}
}

// Collect implements Collector.
func (c *multiTimingsCollector) Collect(ch chan<- prom.Metric) {
	for t, desc := range c.multiTimings {
		for cat, his := range t.Timings.Histograms() {
			labelValues := strings.Split(cat, ".")
			ch <- prom.MustNewConstHistogram(
				desc,
				uint64(his.Count()),
				float64(his.Total()),
				makePromBucket(his.Cutoffs(), his.Buckets()),
				labelValues...)
		}
	}
}

type intFuncCollector struct {
	intFuncs map[*stats.IntFunc]*prom.Desc
}

// Describe implements Collector.
func (c *intFuncCollector) Describe(ch chan<- *prom.Desc) {
	for _, desc := range c.intFuncs {
		ch <- desc
	}
}

// Collect implements Collector.
func (c *intFuncCollector) Collect(ch chan<- prom.Metric) {
	for i, desc := range c.intFuncs {
		ch <- prom.MustNewConstMetric(desc, prom.GaugeValue, float64(i.F()))
	}
}

func toPromValueType(vt stats.ValueType) prom.ValueType {
	if vt == stats.CounterValue {
		return prom.CounterValue
	} else if vt == stats.GaugeValue {
		return prom.GaugeValue
	} else {
		return prom.UntypedValue
	}
}
