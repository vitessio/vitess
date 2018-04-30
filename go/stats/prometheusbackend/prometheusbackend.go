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
	be             PromBackend
	logUnsupported *logutil.ThrottledLogger
)

// Init initializes the Prometheus be with the given namespace.
func Init(namespace string) {
	http.Handle("/metrics", promhttp.Handler())
	be.namespace = namespace
	logUnsupported = logutil.NewThrottledLogger("PrometheusUnsupportedMetricType", 1*time.Minute)
	stats.Register(be.publishPrometheusMetric)
}

// PublishPromMetric is used to publish the metric to Prometheus.
func (be PromBackend) publishPrometheusMetric(name string, v expvar.Var) {
	switch st := v.(type) {
	case *stats.Counter:
		newMetricFuncCollector(st, be.buildPromName(name), prometheus.CounterValue, func() float64 { return float64(st.Get()) })
	case *stats.CounterFunc:
		newMetricFuncCollector(st, be.buildPromName(name), prometheus.CounterValue, func() float64 { return float64(st.F()) })
	case *stats.Gauge:
		newMetricFuncCollector(st, be.buildPromName(name), prometheus.GaugeValue, func() float64 { return float64(st.Get()) })
	case *stats.GaugeFunc:
		newMetricFuncCollector(st, be.buildPromName(name), prometheus.GaugeValue, func() float64 { return float64(st.F()) })
	case *stats.CountersWithSingleLabel:
		newCountersWithSingleLabelCollector(st, be.buildPromName(name), st.Label(), prometheus.CounterValue)
	case *stats.CountersWithMultiLabels:
		newMetricWithMultiLabelsCollector(st, be.buildPromName(name))
	case *stats.CountersFuncWithMultiLabels:
		newMetricsFuncWithMultiLabelsCollector(st, be.buildPromName(name), prometheus.CounterValue)
	case *stats.GaugesFuncWithMultiLabels:
		newMetricsFuncWithMultiLabelsCollector(&st.CountersFuncWithMultiLabels, be.buildPromName(name), prometheus.GaugeValue)
	case *stats.GaugesWithSingleLabel:
		newGaugesWithSingleLabelCollector(st, be.buildPromName(name), st.Label(), prometheus.GaugeValue)
	case *stats.GaugesWithMultiLabels:
		newGaugesWithMultiLabelsCollector(st, be.buildPromName(name))
	case *stats.CounterDuration:
		newMetricFuncCollector(st, be.buildPromName(name), prometheus.CounterValue, func() float64 { return st.Get().Seconds() })
	case *stats.CounterDurationFunc:
		newMetricFuncCollector(st, be.buildPromName(name), prometheus.CounterValue, func() float64 { return st.F().Seconds() })
	case *stats.GaugeDuration:
		newMetricFuncCollector(st, be.buildPromName(name), prometheus.GaugeValue, func() float64 { return st.Get().Seconds() })
	case *stats.GaugeDurationFunc:
		newMetricFuncCollector(st, be.buildPromName(name), prometheus.GaugeValue, func() float64 { return st.F().Seconds() })
	case *stats.Timings:
		newTimingsCollector(st, be.buildPromName(name))
	case *stats.MultiTimings:
		newMultiTimingsCollector(st, be.buildPromName(name))
	case *stats.Histogram:
		newHistogramCollector(st, be.buildPromName(name))
	default:
		logUnsupported.Infof("Not exporting to Prometheus an unsupported metric type of %T: %s", st, name)
	}
}

// buildPromName specifies the namespace as a prefix to the metric name
func (be PromBackend) buildPromName(name string) string {
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
