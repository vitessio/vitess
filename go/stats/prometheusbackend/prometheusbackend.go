package prometheusbackend

import (
	"expvar"
	"net/http"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
)

// PromBackend implements PullBackend using Prometheus as the backing metrics storage.
type PromBackend struct {
	namespace string
}

var (
	be PromBackend
)

// Init initializes the Prometheus be with the given namespace.
func Init(namespace string) {
	http.Handle("/metrics", promhttp.Handler())
	be.namespace = namespace
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
	case stats.FloatFunc:
		newMetricFuncCollector(st, be.buildPromName(name), prometheus.GaugeValue, func() float64 { return (st)() })
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
	case *stats.String, stats.StringFunc, stats.StringMapFunc, *stats.Rates, stats.JSONFunc:
		// Silently ignore these types since they don't make sense to
		// export to Prometheus' data model.
	default:
		log.Fatalf("prometheus: Metric type %T (seen for variable: %s) is not covered by type switch. Add it there and to all other plugins which register a NewVarHook.", st, name)
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
