package prombackend

import (
	"net/http"
	"strings"
	"unicode"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"vitess.io/vitess/go/stats"
)

// PromBackend implements PullBackend using Prometheus as the backing metrics storage.
type PromBackend struct {
	namespace string
}

// Init initializes the Prometheus backend with the given namespace.
func Init(namespace string) {
	http.Handle("/metrics", promhttp.Handler())
	promBackend := &PromBackend{namespace: namespace}
	stats.RegisterPullBackendImpl("prom", promBackend)
}

// NewMetric is part of the PullBackend interface.
func (be *PromBackend) NewMetric(c *stats.Counters, name string, vt stats.ValueType) {
	collector := &metricsCollector{
		counters: map[*stats.Counters]*prom.Desc{
			c: prom.NewDesc(
				prom.BuildFQName("", be.namespace, toSnake(name)),
				c.Help(),
				[]string{"type"}, // Hard-coded label key for Counters
				nil),
		}, vt: vt}

	prom.MustRegister(collector)
}

// NewMultiCounter is part of the PullBackend interface.
func (be *PromBackend) NewMultiCounter(mc *stats.MultiCounters, name string) {
	c := &multiCountersCollector{
		multiCounters: map[*stats.MultiCounters]*prom.Desc{
			mc: prom.NewDesc(
				prom.BuildFQName("", be.namespace, toSnake(name)),
				mc.Counters.Help(),
				labelsToSnake(mc.Labels()),
				nil),
		}}

	prom.MustRegister(c)
}

// NewMultiGauge is part of the PullBackend interface.
func (be *PromBackend) NewMultiGauge(mg *stats.MultiGauges, name string) {
	c := &multiGaugesCollector{
		multiGauges: map[*stats.MultiGauges]*prom.Desc{
			mg: prom.NewDesc(
				prom.BuildFQName("", be.namespace, toSnake(name)),
				mg.Gauges.Counters.Help(),
				labelsToSnake(mg.Labels()),
				nil),
		}}

	prom.MustRegister(c)
}

// NewMultiCounterFunc is part of the PullBackend interface.
func (be *PromBackend) NewMultiCounterFunc(mcf *stats.MultiCountersFunc, name string) {
	collector := &multiCountersFuncCollector{
		multiCountersFunc: map[*stats.MultiCountersFunc]*prom.Desc{
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

// NewInt is part of the PullBackend interface
func (be *PromBackend) NewInt(i *stats.Int, name string, vt stats.ValueType) {
	collector := &intCollector{
		intMetrics: map[*stats.Int]*prom.Desc{
			i: prom.NewDesc(
				prom.BuildFQName("", be.namespace, toSnake(name)),
				i.Help(),
				nil,
				nil),
		}, vt: vt}
	prom.MustRegister(collector)
}

// NewIntFunc is part of the PullBackend interface
func (be *PromBackend) NewIntFunc(intFunc *stats.IntFunc, name string) {
	collector := &intFuncCollector{
		intFuncs: map[*stats.IntFunc]*prom.Desc{
			intFunc: prom.NewDesc(
				prom.BuildFQName("", be.namespace, toSnake(name)),
				intFunc.Help(),
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
