package prometheusbackend

import (
	"expvar"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/logutil"
)

// PromBackend implements PullBackend using Prometheus as the backing metrics storage.
type PromBackend struct {
	namespace string
}

var (
	be             *PromBackend
	logUnsupported *logutil.ThrottledLogger
)

// Init initializes the Prometheus be with the given namespace.
func Init(namespace string) {
	http.Handle("/metrics", promhttp.Handler())
	be := &PromBackend{namespace: namespace}
	logUnsupported = logutil.NewThrottledLogger("PrometheusUnsupportedMetricType", 1*time.Minute)
	stats.Register(be.publishPrometheusMetric)
}

// PublishPromMetric is used to publish the metric to Prometheus.
func (be *PromBackend) publishPrometheusMetric(name string, v expvar.Var) {
	switch st := v.(type) {
	case *stats.Counter:
		be.newMetric(st, name, prometheus.CounterValue, func() float64 { return float64(st.Get()) })
	case *stats.CounterFunc:
		be.newMetric(st, name, prometheus.CounterValue, func() float64 { return float64(st.F()) })
	case *stats.Gauge:
		be.newMetric(st, name, prometheus.GaugeValue, func() float64 { return float64(st.Get()) })
	case *stats.GaugeFunc:
		be.newMetric(st, name, prometheus.GaugeValue, func() float64 { return float64(st.F()) })
	case *stats.CountersWithLabels:
		be.newCountersWithLabels(st, name, st.LabelName(), prometheus.CounterValue)
	case *stats.CountersWithMultiLabels:
		be.newCountersWithMultiLabels(st, name)
	case *stats.CountersFuncWithMultiLabels:
		be.newMetricsFuncWithMultiLabels(st, name, prometheus.CounterValue)
	case *stats.GaugesFuncWithMultiLabels:
		be.newMetricsFuncWithMultiLabels(&st.CountersFuncWithMultiLabels, name, prometheus.GaugeValue)
	case *stats.GaugesWithLabels:
		be.newGaugesWithLabels(st, name, st.LabelName(), prometheus.GaugeValue)
	case *stats.GaugesWithMultiLabels:
		be.newGaugesWithMultiLabels(st, name)
	case *stats.CounterDuration:
		be.newMetric(st, name, prometheus.CounterValue, func() float64 { return st.Get().Seconds() })
	case *stats.CounterDurationFunc:
		be.newMetric(st, name, prometheus.CounterValue, func() float64 { return st.F().Seconds() })
	case *stats.GaugeDuration:
		be.newMetric(st, name, prometheus.GaugeValue, func() float64 { return st.Get().Seconds() })
	case *stats.GaugeDurationFunc:
		be.newMetric(st, name, prometheus.GaugeValue, func() float64 { return st.F().Seconds() })
	case *stats.Timings:
		be.newTiming(st, name)
	case *stats.MultiTimings:
		be.newMultiTiming(st, name)
	default:
		logUnsupported.Infof("Not exporting to Prometheus an unsupported metric type of %T: %s", st, name)
	}
}

func (be *PromBackend) newCountersWithLabels(c *stats.CountersWithLabels, name string, labelName string, vt prometheus.ValueType) {
	collector := &countersWithLabelsCollector{
		counters: c,
		desc: prometheus.NewDesc(
			be.buildPromName(name),
			c.Help(),
			[]string{labelName},
			nil),
		vt: vt}

	prometheus.MustRegister(collector)
}

func (be *PromBackend) newGaugesWithLabels(g *stats.GaugesWithLabels, name string, labelName string, vt prometheus.ValueType) {
	collector := &gaugesWithLabelsCollector{
		gauges: g,
		desc: prometheus.NewDesc(
			be.buildPromName(name),
			g.Help(),
			[]string{labelName},
			nil),
		vt: vt}

	prometheus.MustRegister(collector)
}

func (be *PromBackend) newCountersWithMultiLabels(cml *stats.CountersWithMultiLabels, name string) {
	c := &metricWithMultiLabelsCollector{
		cml: cml,
		desc: prometheus.NewDesc(
			be.buildPromName(name),
			cml.Help(),
			labelsToSnake(cml.Labels()),
			nil),
	}

	prometheus.MustRegister(c)
}

func (be *PromBackend) newGaugesWithMultiLabels(gml *stats.GaugesWithMultiLabels, name string) {
	c := &multiGaugesCollector{
		gml: gml,
		desc: prometheus.NewDesc(
			be.buildPromName(name),
			gml.Help(),
			labelsToSnake(gml.Labels()),
			nil),
	}

	prometheus.MustRegister(c)
}

func (be *PromBackend) newMetricsFuncWithMultiLabels(cfml *stats.CountersFuncWithMultiLabels, name string, vt prometheus.ValueType) {
	collector := &metricsFuncWithMultiLabelsCollector{
		cfml: cfml,
		desc: prometheus.NewDesc(
			be.buildPromName(name),
			cfml.Help(),
			labelsToSnake(cfml.Labels()),
			nil),
		vt: vt,
	}

	prometheus.MustRegister(collector)
}

func (be *PromBackend) newTiming(t *stats.Timings, name string) {
	collector := &timingsCollector{
		t: t,
		desc: prometheus.NewDesc(
			be.buildPromName(name),
			t.Help(),
			[]string{"Histograms"}, // hard coded label key
			nil),
	}

	prometheus.MustRegister(collector)
}

func (be *PromBackend) newMultiTiming(mt *stats.MultiTimings, name string) {
	collector := &multiTimingsCollector{
		mt: mt,
		desc: prometheus.NewDesc(
			be.buildPromName(name),
			mt.Help(),
			labelsToSnake(mt.Labels()),
			nil),
	}

	prometheus.MustRegister(collector)
}

func (be *PromBackend) newMetric(v stats.Variable, name string, vt prometheus.ValueType, f func() float64) {
	collector := &metricFuncCollector{
		f: f,
		desc: prometheus.NewDesc(
			be.buildPromName(name),
			v.Help(),
			nil,
			nil),
		vt: vt}

	prometheus.MustRegister(collector)
}

// buildPromName specifies the namespace as a prefix to the metric name
func (be *PromBackend) buildPromName(name string) string {
	s := strings.TrimPrefix(normalizeMetric(name), be.namespace+"_")
	return prometheus.BuildFQName("", be.namespace, s)
}

func labelsToSnake(labels []string) []string {
	output := make([]string, len(labels))
	for i, l := range labels {
		output[i] = normalizeMetric(l)
	}
	return output
}

// normalizeMetricForPrometheus produces a compliant name by applying
// special case conversions and then applying a camel case to snake case converter.
func normalizeMetric(name string) string {
	// Special cases
	r := strings.NewReplacer("VSchema", "vschema", "VtGate", "vtgate")
	name = r.Replace(name)

	return stats.GetSnakeName(name)
}
