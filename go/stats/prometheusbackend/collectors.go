package prometheusbackend

import (
	"strings"

	prom "github.com/prometheus/client_golang/prometheus"
	"vitess.io/vitess/go/stats"
)

type metricCollector struct {
	counter *stats.Counter
	desc    *prom.Desc
	vt      ValueType
}

// Describe implements Collector.
func (c *metricCollector) Describe(ch chan<- *prom.Desc) {
	ch <- c.desc
}

// Collect implements Collector.
func (c *metricCollector) Collect(ch chan<- prom.Metric) {
	ch <- prom.MustNewConstMetric(c.desc, toPromValueType(c.vt), float64(c.counter.Get()))
}

// metricWithLabelsCollector collects both stats.Counters and stats.Gauges
type metricWithLabelsCollector struct {
	counter *stats.Counters
	desc    *prom.Desc
	vt      ValueType
}

// Describe implements Collector.
func (c *metricWithLabelsCollector) Describe(ch chan<- *prom.Desc) {
	ch <- c.desc
}

// Collect implements Collector.
func (c *metricWithLabelsCollector) Collect(ch chan<- prom.Metric) {
	for tag, val := range c.counter.Counts() {
		ch <- prom.MustNewConstMetric(
			c.desc,
			toPromValueType(c.vt),
			float64(val),
			tag)
	}
}

type metricWithMultiLabelsCollector struct {
	cml  *stats.CountersWithMultiLabels
	desc *prom.Desc
}

// Describe implements Collector.
func (c *metricWithMultiLabelsCollector) Describe(ch chan<- *prom.Desc) {
	ch <- c.desc
}

// Collect implements Collector.
func (c *metricWithMultiLabelsCollector) Collect(ch chan<- prom.Metric) {
	for lvs, val := range c.cml.Counts() {
		labelValues := strings.Split(lvs, ".")
		value := float64(val)
		ch <- prom.MustNewConstMetric(c.desc, prom.CounterValue, value, labelValues...)
	}
}

type multiGaugesCollector struct {
	gml  *stats.GaugesWithMultiLabels
	desc *prom.Desc
}

// Describe implements Collector.
func (c *multiGaugesCollector) Describe(ch chan<- *prom.Desc) {
	ch <- c.desc
}

// Collect implements Collector.
func (c *multiGaugesCollector) Collect(ch chan<- prom.Metric) {
	for lvs, val := range c.gml.Counts() {
		labelValues := strings.Split(lvs, ".")
		value := float64(val)
		ch <- prom.MustNewConstMetric(c.desc, prom.GaugeValue, value, labelValues...)
	}
}

type metricsFuncWithMultiLabelsCollector struct {
	cfml *stats.CountersFuncWithMultiLabels
	desc *prom.Desc
	vt   ValueType
}

// Describe implements Collector.
func (c *metricsFuncWithMultiLabelsCollector) Describe(ch chan<- *prom.Desc) {
	ch <- c.desc
}

// Collect implements Collector.
func (c *metricsFuncWithMultiLabelsCollector) Collect(ch chan<- prom.Metric) {
	for lvs, val := range c.cfml.Counts() {
		labelValues := strings.Split(lvs, ".")
		value := float64(val)
		ch <- prom.MustNewConstMetric(c.desc, toPromValueType(c.vt), value, labelValues...)
	}
}

type timingsCollector struct {
	t    *stats.Timings
	desc *prom.Desc
}

// Describe implements Collector.
func (c *timingsCollector) Describe(ch chan<- *prom.Desc) {
	ch <- c.desc
}

// Collect implements Collector.
func (c *timingsCollector) Collect(ch chan<- prom.Metric) {
	for cat, his := range c.t.Histograms() {
		ch <- prom.MustNewConstHistogram(
			c.desc,
			uint64(his.Count()),
			float64(his.Total()),
			makePromBucket(his.Cutoffs(), his.Buckets()),
			cat)
	}
}

func makePromBucket(cutoffs []int64, buckets []int64) map[float64]uint64 {
	output := make(map[float64]uint64)
	last := uint64(0)
	for i := range cutoffs {
		key := float64(cutoffs[i]) / 1000000000
		//TODO(zmagg): int64 => uint64 conversion. error if it overflows?
		output[key] = uint64(buckets[i]) + last
		last = output[key]
	}
	return output
}

type multiTimingsCollector struct {
	mt   *stats.MultiTimings
	desc *prom.Desc
}

// Describe implements Collector.
func (c *multiTimingsCollector) Describe(ch chan<- *prom.Desc) {
	ch <- c.desc
}

// Collect implements Collector.
func (c *multiTimingsCollector) Collect(ch chan<- prom.Metric) {
	for cat, his := range c.mt.Timings.Histograms() {
		labelValues := strings.Split(cat, ".")
		ch <- prom.MustNewConstHistogram(
			c.desc,
			uint64(his.Count()),
			float64(his.Total()),
			makePromBucket(his.Cutoffs(), his.Buckets()),
			labelValues...)
	}
}

type metricFuncCollector struct {
	cf   *stats.CounterFunc
	desc *prom.Desc
	vt   ValueType
}

// Describe implements Collector.
func (c *metricFuncCollector) Describe(ch chan<- *prom.Desc) {
	ch <- c.desc
}

// Collect implements Collector.
func (c *metricFuncCollector) Collect(ch chan<- prom.Metric) {
	ch <- prom.MustNewConstMetric(c.desc, toPromValueType(c.vt), float64(c.cf.F()))
}

func toPromValueType(vt ValueType) prom.ValueType {
	if vt == CounterValue {
		return prom.CounterValue
	} else if vt == GaugeValue {
		return prom.GaugeValue
	} else {
		return prom.UntypedValue
	}
}
