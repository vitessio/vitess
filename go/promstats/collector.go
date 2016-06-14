/*
Package promstats contains adapters to publish stats variables to prometheus (http://prometheus.io)
*/
package promstats

import (
	"expvar"
	"fmt"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/youtube/vitess/go/stats"
)

// NewCollector returns a prometheus.Collector for a given stats var.
// It supports all stats var types except String, StringFunc and Rates.
// The returned collector still needs to be registered with prometheus registry.
func NewCollector(opts prometheus.Opts, v expvar.Var) prometheus.Collector {
	switch st := v.(type) {
	case *stats.Int:
		return prometheus.NewGaugeFunc(prometheus.GaugeOpts(opts), func() float64 {
			return float64(st.Get())
		})
	case stats.IntFunc:
		return prometheus.NewGaugeFunc(prometheus.GaugeOpts(opts), func() float64 {
			return float64(st())
		})
	case *stats.Duration:
		return prometheus.NewGaugeFunc(prometheus.GaugeOpts(opts), func() float64 {
			return st.Get().Seconds()
		})
	case stats.DurationFunc:
		return prometheus.NewGaugeFunc(prometheus.GaugeOpts(opts), func() float64 {
			return st().Seconds()
		})
	case *stats.Float:
		return prometheus.NewGaugeFunc(prometheus.GaugeOpts(opts), st.Get)
	case stats.FloatFunc:
		return prometheus.NewGaugeFunc(prometheus.GaugeOpts(opts), st)
	case *stats.Counters:
		return newCountersCollector(opts, st, "tag")
	case stats.CountersFunc:
		return newCountersCollector(opts, st, "tag")
	case *stats.MultiCounters:
		return newCountersCollector(opts, st, st.Labels()...)
	case *stats.MultiCountersFunc:
		return newCountersCollector(opts, st, st.Labels()...)
	case *stats.Histogram:
		return newHistogramCollector(opts, st)
	case *stats.Timings:
		return newTimingsCollector(opts, st, "category")
	case *stats.MultiTimings:
		return newTimingsCollector(opts, &st.Timings, st.Labels()...)
	case *stats.String:
		// prometheus can't collect string values
		return nil
	case stats.StringFunc:
		// prometheus can't collect string values
		return nil
	case *stats.Rates:
		// Ignore these, because monitoring tools will calculate
		// rates for us.
		return nil
	default:
		glog.Warningf("Unsupported type for %s: %T", opts.Name, v)
		return nil
	}
}

type countersCollector struct {
	desc    *prometheus.Desc
	c       stats.CountTracker
	nLabels int
}

func newCountersCollector(opts prometheus.Opts, c stats.CountTracker, labels ...string) prometheus.Collector {
	desc := prometheus.NewDesc(
		prometheus.BuildFQName(opts.Namespace, opts.Subsystem, opts.Name),
		opts.Help,
		labels,
		opts.ConstLabels,
	)
	return countersCollector{
		desc:    desc,
		c:       c,
		nLabels: len(labels),
	}
}

func (c countersCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

var replacer = strings.NewReplacer(`\\`, `\`, `\.`, `.`, `.`, "\000")

func split(key string) []string {
	return strings.Split(replacer.Replace(key), "\000")
}

func (c countersCollector) Collect(ch chan<- prometheus.Metric) {
	for k, n := range c.c.Counts() {
		if c.nLabels > 1 {
			labels := split(k)
			if len(labels) != c.nLabels {
				err := fmt.Errorf("wrong number of labels in MultiCounters key: %d != %d (key=%q)", len(labels), c.nLabels, k)
				ch <- prometheus.NewInvalidMetric(c.desc, err)
				continue
			}
			ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(n), labels...)
			continue
		}
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(n), k)
	}
}

func newHistogram(opts prometheus.Opts, cutoffs []int64) (prometheus.Histogram, func(int64)) {
	buckets := make([]float64, len(cutoffs))
	for i := range cutoffs {
		buckets[i] = float64(cutoffs[i])
	}
	hOpts := prometheus.HistogramOpts{
		Namespace:   opts.Namespace,
		Subsystem:   opts.Subsystem,
		Name:        opts.Name,
		Help:        opts.Help,
		ConstLabels: opts.ConstLabels,
		Buckets:     buckets,
	}
	m := prometheus.NewHistogram(hOpts)
	return m, func(n int64) {
		m.Observe(float64(n))
	}
}

type histogramCollector struct {
	desc *prometheus.Desc
	h    *stats.Histogram
}

func newHistogramCollector(opts prometheus.Opts, h *stats.Histogram) histogramCollector {
	desc := prometheus.NewDesc(
		prometheus.BuildFQName(opts.Namespace, opts.Subsystem, opts.Name),
		opts.Help,
		nil,
		opts.ConstLabels,
	)
	return histogramCollector{
		desc: desc,
		h:    h,
	}
}

func histogramMetric(desc *prometheus.Desc, h *stats.Histogram, scale float64, labels ...string) prometheus.Metric {
	count := uint64(0)
	sum := float64(h.Total()) * scale
	cutoffs := h.Cutoffs()
	statBuckets := h.Buckets()
	promBuckets := make(map[float64]uint64, len(cutoffs))
	for i, cutoff := range cutoffs {
		upperBound := float64(cutoff) * scale
		count += uint64(statBuckets[i])
		promBuckets[upperBound] = count
	}
	count += uint64(statBuckets[len(statBuckets)-1])
	return prometheus.MustNewConstHistogram(desc, count, sum, promBuckets, labels...)
}

func (h histogramCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- h.desc
}

func (h histogramCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- histogramMetric(h.desc, h.h, 1)
}

type timingsCollector struct {
	desc    *prometheus.Desc
	t       *stats.Timings
	nLabels int
}

func newTimingsCollector(opts prometheus.Opts, t *stats.Timings, labels ...string) prometheus.Collector {
	desc := prometheus.NewDesc(
		prometheus.BuildFQName(opts.Namespace, opts.Subsystem, opts.Name),
		opts.Help,
		labels,
		opts.ConstLabels,
	)
	return timingsCollector{
		desc:    desc,
		t:       t,
		nLabels: len(labels),
	}
}

func (c timingsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

func (c timingsCollector) Collect(ch chan<- prometheus.Metric) {
	for k, h := range c.t.Histograms() {
		if c.nLabels > 1 {
			labels := split(k)
			if len(labels) != c.nLabels {
				err := fmt.Errorf("wrong number of labels in MultiTimings key: %d != %d (key=%q)", len(labels), c.nLabels, k)
				ch <- prometheus.NewInvalidMetric(c.desc, err)
				continue
			}
			ch <- histogramMetric(c.desc, h, 1/float64(time.Second), labels...)
			continue
		}
		ch <- histogramMetric(c.desc, h, 1/float64(time.Second), k)
	}
}
