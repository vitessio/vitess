package prombackend

import (
	"expvar"
	"net/http"
	"strings"
	"unicode"

	log "github.com/golang/glog"

	prom "github.com/prometheus/client_golang/prometheus"
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
	stats.Register(be.PublishPromMetric)
}

// PublishPromMetric is used to publish the metric to Prometheus.
func (be *PromBackend) PublishPromMetric(name string, v expvar.Var) {
	switch st := v.(type) {
	case *stats.Counter:
		be.NewMetric(st, name, stats.CounterValue)
	case *stats.Gauge:
		be.NewMetric(&st.Counter, name, stats.GaugeValue)
	case *stats.GaugeFunc:
		be.NewGaugeFunc(st, name)
	case *stats.CountersWithLabels:
		be.NewMetricWithLabels(&st.Counters, name, st.LabelName(), stats.CounterValue)
	case *stats.CountersWithMultiLabels:
		be.NewCountersWithMultiLabels(st, name)
	case *stats.CountersFuncWithMultiLabels:
		be.NewCountersFuncWithMultiLabels(st, name)
	case *stats.GaugesWithLabels:
		be.NewMetricWithLabels(&st.Counters, name, st.LabelName(), stats.GaugeValue)
	case *stats.GaugesWithMultiLabels:
		be.NewGaugesWithMultiLabels(st, name)
	case *stats.Timings:
		be.NewTiming(st, name)
	case *stats.MultiTimings:
		be.NewMultiTiming(st, name)
	default:
		log.Warningf("Unsupported type for %s: %T", name, st)
	}
}

// NewMetricWithLabels is part of the PullBackend interface.
func (be *PromBackend) NewMetricWithLabels(c *stats.Counters, name string, labelName string, vt stats.ValueType) {
	collector := &metricsCollector{
		counters: map[*stats.Counters]*prom.Desc{
			c: prom.NewDesc(
				prom.BuildFQName("", be.namespace, toSnake(name)),
				c.Help(),
				[]string{labelName},
				nil),
		}, vt: vt}

	prom.MustRegister(collector)
}

// NewCountersWithMultiLabels is part of the PullBackend interface.
func (be *PromBackend) NewCountersWithMultiLabels(mc *stats.CountersWithMultiLabels, name string) {
	c := &multiCountersCollector{
		multiCounters: map[*stats.CountersWithMultiLabels]*prom.Desc{
			mc: prom.NewDesc(
				prom.BuildFQName("", be.namespace, toSnake(name)),
				mc.Counters.Help(),
				labelsToSnake(mc.Labels()),
				nil),
		}}

	prom.MustRegister(c)
}

// NewGaugesWithMultiLabels is part of the PullBackend interface.
func (be *PromBackend) NewGaugesWithMultiLabels(mg *stats.GaugesWithMultiLabels, name string) {
	c := &multiGaugesCollector{
		multiGauges: map[*stats.GaugesWithMultiLabels]*prom.Desc{
			mg: prom.NewDesc(
				prom.BuildFQName("", be.namespace, toSnake(name)),
				mg.Help(),
				labelsToSnake(mg.Labels()),
				nil),
		}}

	prom.MustRegister(c)
}

// NewCountersFuncWithMultiLabels is part of the PullBackend interface.
func (be *PromBackend) NewCountersFuncWithMultiLabels(mcf *stats.CountersFuncWithMultiLabels, name string) {
	collector := &multiCountersFuncCollector{
		multiCountersFunc: map[*stats.CountersFuncWithMultiLabels]*prom.Desc{
			mcf: prom.NewDesc(
				prom.BuildFQName("", be.namespace, toSnake(name)),
				mcf.Help(),
				labelsToSnake(mcf.Labels()),
				nil),
		}}

	prom.MustRegister(collector)
}

// NewTiming is part of the PullBackend interface
func (be *PromBackend) NewTiming(t *stats.Timings, name string) {
	collector := &timingsCollector{
		timings: map[*stats.Timings]*prom.Desc{
			t: prom.NewDesc(
				prom.BuildFQName("", be.namespace, toSnake(name)),
				t.Help(),
				[]string{"Histograms"}, // hard coded label key
				nil),
		}}

	prom.MustRegister(collector)
}

// NewMultiTiming is part of the PullBackend interface
func (be *PromBackend) NewMultiTiming(mt *stats.MultiTimings, name string) {
	collector := &multiTimingsCollector{
		multiTimings: map[*stats.MultiTimings]*prom.Desc{
			mt: prom.NewDesc(
				prom.BuildFQName("", be.namespace, toSnake(name)),
				mt.Help(),
				labelsToSnake(mt.Labels()),
				nil),
		}}

	prom.MustRegister(collector)
}

// NewMetric is part of the PullBackend interface
func (be *PromBackend) NewMetric(c *stats.Counter, name string, vt stats.ValueType) {
	collector := &metricCollector{
		m: map[*stats.Counter]*prom.Desc{
			c: prom.NewDesc(
				prom.BuildFQName("", be.namespace, toSnake(name)),
				c.Help(),
				nil,
				nil),
		}, vt: vt}
	prom.MustRegister(collector)
}

// NewGaugeFunc is part of the PullBackend interface
func (be *PromBackend) NewGaugeFunc(gf *stats.GaugeFunc, name string) {
	collector := &gaugeFuncCollector{
		gfm: map[*stats.GaugeFunc]*prom.Desc{
			gf: prom.NewDesc(
				prom.BuildFQName("", be.namespace, toSnake(name)),
				gf.Help(),
				nil,
				nil),
		}}

	prom.MustRegister(collector)
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
	r := strings.NewReplacer("VSchema", "Vschema")
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
