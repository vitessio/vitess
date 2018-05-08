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

func newMetricFuncCollector(v stats.Variable, name string, vt prometheus.ValueType, f func() float64) {
	collector := &metricFuncCollector{
		f: f,
		desc: prometheus.NewDesc(
			name,
			v.Help(),
			nil,
			nil),
		vt: vt}

	prometheus.MustRegister(collector)
}

// Describe implements Collector.
func (mc *metricFuncCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- mc.desc
}

// Collect implements Collector.
func (mc *metricFuncCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(mc.desc, mc.vt, float64(mc.f()))
}

// countersWithSingleLabelCollector collects stats.CountersWithSingleLabel.
type countersWithSingleLabelCollector struct {
	counters *stats.CountersWithSingleLabel
	desc     *prometheus.Desc
	vt       prometheus.ValueType
}

func newCountersWithSingleLabelCollector(c *stats.CountersWithSingleLabel, name string, labelName string, vt prometheus.ValueType) {
	collector := &countersWithSingleLabelCollector{
		counters: c,
		desc: prometheus.NewDesc(
			name,
			c.Help(),
			[]string{labelName},
			nil),
		vt: vt}

	prometheus.MustRegister(collector)
}

// Describe implements Collector.
func (c *countersWithSingleLabelCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect implements Collector.
func (c *countersWithSingleLabelCollector) Collect(ch chan<- prometheus.Metric) {
	for tag, val := range c.counters.Counts() {
		ch <- prometheus.MustNewConstMetric(
			c.desc,
			c.vt,
			float64(val),
			tag)
	}
}

// gaugesWithSingleLabelCollector collects stats.GaugesWithSingleLabel.
type gaugesWithSingleLabelCollector struct {
	gauges *stats.GaugesWithSingleLabel
	desc   *prometheus.Desc
	vt     prometheus.ValueType
}

func newGaugesWithSingleLabelCollector(g *stats.GaugesWithSingleLabel, name string, labelName string, vt prometheus.ValueType) {
	collector := &gaugesWithSingleLabelCollector{
		gauges: g,
		desc: prometheus.NewDesc(
			name,
			g.Help(),
			[]string{labelName},
			nil),
		vt: vt}

	prometheus.MustRegister(collector)
}

// Describe implements Collector.
func (g *gaugesWithSingleLabelCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- g.desc
}

// Collect implements Collector.
func (g *gaugesWithSingleLabelCollector) Collect(ch chan<- prometheus.Metric) {
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

func newMetricWithMultiLabelsCollector(cml *stats.CountersWithMultiLabels, name string) {
	c := &metricWithMultiLabelsCollector{
		cml: cml,
		desc: prometheus.NewDesc(
			name,
			cml.Help(),
			labelsToSnake(cml.Labels()),
			nil),
	}

	prometheus.MustRegister(c)
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

type gaugesWithMultiLabelsCollector struct {
	gml  *stats.GaugesWithMultiLabels
	desc *prometheus.Desc
}

func newGaugesWithMultiLabelsCollector(gml *stats.GaugesWithMultiLabels, name string) {
	c := &gaugesWithMultiLabelsCollector{
		gml: gml,
		desc: prometheus.NewDesc(
			name,
			gml.Help(),
			labelsToSnake(gml.Labels()),
			nil),
	}

	prometheus.MustRegister(c)
}

// Describe implements Collector.
func (c *gaugesWithMultiLabelsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect implements Collector.
func (c *gaugesWithMultiLabelsCollector) Collect(ch chan<- prometheus.Metric) {
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

func newMetricsFuncWithMultiLabelsCollector(cfml *stats.CountersFuncWithMultiLabels, name string, vt prometheus.ValueType) {
	collector := &metricsFuncWithMultiLabelsCollector{
		cfml: cfml,
		desc: prometheus.NewDesc(
			name,
			cfml.Help(),
			labelsToSnake(cfml.Labels()),
			nil),
		vt: vt,
	}

	prometheus.MustRegister(collector)
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
	t       *stats.Timings
	cutoffs []float64
	desc    *prometheus.Desc
}

func newTimingsCollector(t *stats.Timings, name string) {
	cutoffs := make([]float64, len(t.Cutoffs()))
	for i, val := range t.Cutoffs() {
		cutoffs[i] = float64(val) / 1000000000
	}

	collector := &timingsCollector{
		t:       t,
		cutoffs: cutoffs,
		desc: prometheus.NewDesc(
			name,
			t.Help(),
			[]string{t.Label()},
			nil),
	}

	prometheus.MustRegister(collector)
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
			makeCumulativeBuckets(c.cutoffs, his.Buckets()),
			cat)
	}
}

func makeCumulativeBuckets(cutoffs []float64, buckets []int64) map[float64]uint64 {
	output := make(map[float64]uint64)
	last := uint64(0)
	for i, key := range cutoffs {
		//TODO(zmagg): int64 => uint64 conversion. error if it overflows?
		output[key] = uint64(buckets[i]) + last
		last = output[key]
	}
	return output
}

type multiTimingsCollector struct {
	mt      *stats.MultiTimings
	cutoffs []float64
	desc    *prometheus.Desc
}

func newMultiTimingsCollector(mt *stats.MultiTimings, name string) {
	cutoffs := make([]float64, len(mt.Cutoffs()))
	for i, val := range mt.Cutoffs() {
		cutoffs[i] = float64(val) / 1000000000
	}

	collector := &multiTimingsCollector{
		mt:      mt,
		cutoffs: cutoffs,
		desc: prometheus.NewDesc(
			name,
			mt.Help(),
			labelsToSnake(mt.Labels()),
			nil),
	}

	prometheus.MustRegister(collector)
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
			makeCumulativeBuckets(c.cutoffs, his.Buckets()),
			labelValues...)
	}
}

type histogramCollector struct {
	h       *stats.Histogram
	cutoffs []float64
	desc    *prometheus.Desc
}

func newHistogramCollector(h *stats.Histogram, name string) {
	cutoffs := make([]float64, len(h.Cutoffs()))
	for i, val := range h.Cutoffs() {
		cutoffs[i] = float64(val)
	}

	collector := &histogramCollector{
		h:       h,
		cutoffs: cutoffs,
		desc: prometheus.NewDesc(
			name,
			h.Help(),
			[]string{},
			nil),
	}

	prometheus.MustRegister(collector)
}

// Describe implements Collector.
func (c *histogramCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

// Collect implements Collector.
func (c *histogramCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstHistogram(
		c.desc,
		uint64(c.h.Count()),
		float64(c.h.Total()),
		makeCumulativeBuckets(c.cutoffs, c.h.Buckets()),
	)
}
