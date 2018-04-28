package prometheusbackend

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"vitess.io/vitess/go/stats"
)

type metricFuncCollector struct {
	// f returns the floating point value of the metric.
	f    func() float64
	desc *prometheus.Desc
	vt   prometheus.ValueType
}

// Describe implements Collector.
func (mc *metricFuncCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- mc.desc
}

// Collect implements Collector.
func (mc *metricFuncCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(mc.desc, mc.vt, float64(mc.f()))
}

// countersWithLabelsCollector collects stats.CountersWithLabels
type countersWithLabelsCollector struct {
	counters *stats.CountersWithLabels
	desc     *prometheus.Desc
	vt       prometheus.ValueType
}

// Describe implements Collector.
func (c *countersWithLabelsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect implements Collector.
func (c *countersWithLabelsCollector) Collect(ch chan<- prometheus.Metric) {
	for tag, val := range c.counters.Counts() {
		ch <- prometheus.MustNewConstMetric(
			c.desc,
			c.vt,
			float64(val),
			tag)
	}
}

// gaugesWithLabelsCollector collects stats.GaugesWithLabels
type gaugesWithLabelsCollector struct {
	gauges *stats.GaugesWithLabels
	desc   *prometheus.Desc
	vt     prometheus.ValueType
}

// Describe implements Collector.
func (g *gaugesWithLabelsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- g.desc
}

// Collect implements Collector.
func (g *gaugesWithLabelsCollector) Collect(ch chan<- prometheus.Metric) {
	for tag, val := range g.gauges.Counts() {
		ch <- prometheus.MustNewConstMetric(
			g.desc,
			g.vt,
			float64(val),
			tag)
	}
}

type metricWithMultiLabelsCollector struct {
	cml  *stats.CountersWithMultiLabels
	desc *prometheus.Desc
}

// Describe implements Collector.
func (c *metricWithMultiLabelsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect implements Collector.
func (c *metricWithMultiLabelsCollector) Collect(ch chan<- prometheus.Metric) {
	for lvs, val := range c.cml.Counts() {
		labelValues := strings.Split(lvs, ".")
		value := float64(val)
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.CounterValue, value, labelValues...)
	}
}

type multiGaugesCollector struct {
	gml  *stats.GaugesWithMultiLabels
	desc *prometheus.Desc
}

// Describe implements Collector.
func (c *multiGaugesCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect implements Collector.
func (c *multiGaugesCollector) Collect(ch chan<- prometheus.Metric) {
	for lvs, val := range c.gml.Counts() {
		labelValues := strings.Split(lvs, ".")
		value := float64(val)
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, value, labelValues...)
	}
}

type metricsFuncWithMultiLabelsCollector struct {
	cfml *stats.CountersFuncWithMultiLabels
	desc *prometheus.Desc
	vt   prometheus.ValueType
}

// Describe implements Collector.
func (c *metricsFuncWithMultiLabelsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect implements Collector.
func (c *metricsFuncWithMultiLabelsCollector) Collect(ch chan<- prometheus.Metric) {
	for lvs, val := range c.cfml.Counts() {
		labelValues := strings.Split(lvs, ".")
		value := float64(val)
		ch <- prometheus.MustNewConstMetric(c.desc, c.vt, value, labelValues...)
	}
}

type timingsCollector struct {
	t    *stats.Timings
	desc *prometheus.Desc
}

// Describe implements Collector.
func (c *timingsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect implements Collector.
func (c *timingsCollector) Collect(ch chan<- prometheus.Metric) {
	for cat, his := range c.t.Histograms() {
		ch <- prometheus.MustNewConstHistogram(
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
	desc *prometheus.Desc
}

// Describe implements Collector.
func (c *multiTimingsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect implements Collector.
func (c *multiTimingsCollector) Collect(ch chan<- prometheus.Metric) {
	for cat, his := range c.mt.Timings.Histograms() {
		labelValues := strings.Split(cat, ".")
		ch <- prometheus.MustNewConstHistogram(
			c.desc,
			uint64(his.Count()),
			float64(his.Total()),
			makePromBucket(his.Cutoffs(), his.Buckets()),
			labelValues...)
	}
}
