package prometheusbackend

import (
	"expvar"
	"net/http"
	"strings"
	"unicode"

	log "github.com/golang/glog"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"vitess.io/vitess/go/stats"
)

// PromBackend implements PullBackend using Prometheus as the backing metrics storage.
type PromBackend struct {
	namespace string
}

var (
	be *PromBackend
)

// Init initializes the Prometheus be with the given namespace.
func Init(namespace string) {
	http.Handle("/metrics", promhttp.Handler())
	be := &PromBackend{namespace: namespace}
	stats.Register(be.publishPrometheusMetric)
}

// PublishPromMetric is used to publish the metric to Prometheus.
func (be *PromBackend) publishPrometheusMetric(name string, v expvar.Var) {
	switch st := v.(type) {
	case *stats.Counter:
		be.newMetric(st, name, prometheus.CounterValue)
	case *stats.Gauge:
		be.newMetric(&st.Counter, name, prometheus.GaugeValue)
	case *stats.CounterFunc:
		be.newMetricFunc(st, name, prometheus.CounterValue)
	case *stats.GaugeFunc:
		be.newMetricFunc(&st.CounterFunc, name, prometheus.GaugeValue)
	case *stats.CountersWithLabels:
		be.newMetricWithLabels(&st.Counters, name, st.LabelName(), prometheus.CounterValue)
	case *stats.CountersWithMultiLabels:
		be.newCountersWithMultiLabels(st, name)
	case *stats.CountersFuncWithMultiLabels:
		be.newMetricsFuncWithMultiLabels(st, name, prometheus.CounterValue)
	case *stats.GaugesFuncWithMultiLabels:
		be.newMetricsFuncWithMultiLabels(&st.CountersFuncWithMultiLabels, name, prometheus.GaugeValue)
	case *stats.GaugesWithLabels:
		be.newMetricWithLabels(&st.Counters, name, st.LabelName(), prometheus.GaugeValue)
	case *stats.GaugesWithMultiLabels:
		be.newGaugesWithMultiLabels(st, name)
	case *stats.Timings:
		be.newTiming(st, name)
	case *stats.MultiTimings:
		be.newMultiTiming(st, name)
	//case *stats.DurationFunc:
	//		be.newDurationFunc(st, name)
	default:
		log.Warningf("Unsupported type for %s: %T", name, st)
	}
}

func (be *PromBackend) newMetricWithLabels(c *stats.Counters, name string, labelName string, vt prometheus.ValueType) {
	collector := &metricWithLabelsCollector{
		counter: c,
		desc: prometheus.NewDesc(
			be.buildPromName(name),
			c.Help(),
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
			cml.Counters.Help(),
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

func (be *PromBackend) newMetric(c *stats.Counter, name string, vt prometheus.ValueType) {
	collector := &metricCollector{
		counter: c,
		desc: prometheus.NewDesc(
			be.buildPromName(name),
			c.Help(),
			nil,
			nil),
		vt: vt}

	prometheus.MustRegister(collector)
}

func (be *PromBackend) newMetricFunc(cf *stats.CounterFunc, name string, vt prometheus.ValueType) {
	collector := &metricFuncCollector{
		cf: cf,
		desc: prometheus.NewDesc(
			be.buildPromName(name),
			cf.Help(),
			nil,
			nil),
		vt: vt}

	prometheus.MustRegister(collector)
}

func (be *PromBackend) buildPromName(name string) string {
	s := strings.TrimPrefix(toSnake(name), be.namespace)
	return prometheus.BuildFQName("", be.namespace, s)
}

func labelsToSnake(labels []string) []string {
	output := make([]string, len(labels))
	for i, l := range labels {
		output[i] = toSnake(l)
	}
	return output
}

func toSnake(name string) string {
	// Special cases
	r := strings.NewReplacer("VSchema", "Vschema", "VtGate", "Vtgate")
	name = r.Replace(name)
	// Camel case => snake case
	runes := []rune(name)

	var out []rune
	for i := 0; i < len(runes); i++ {
		if i > 0 && unicode.IsUpper(runes[i]) && ((i+1 < len(runes) && unicode.IsLower(runes[i+1])) || unicode.IsLower(runes[i-1])) {
			out = append(out, '_')
		}
		out = append(out, unicode.ToLower(runes[i]))
	}

	return string(out)
}
