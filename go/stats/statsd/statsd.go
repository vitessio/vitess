package statsd

import (
	"encoding/json"
	"expvar"
	"flag"
	"fmt"
	"strings"

	"github.com/DataDog/datadog-go/statsd"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
)

var (
	statsdAddress    = flag.String("statsd_address", "", "Address for statsd client")
	statsdSampleRate = flag.Float64("statsd_sample_rate", 1.0, "")
)

// StatsBackend implements PullBackend using statsd
type StatsBackend struct {
	namespace    string
	statsdClient *statsd.Client
	sampleRate   float64
}

var (
	sb StatsBackend
)

// makeLabel builds a tag list with a single label + value.
func makeLabel(labelName string, labelVal string) []string {
	return []string{fmt.Sprintf("%s:%s", labelName, labelVal)}
}

// makeLabels takes the vitess stat representation of label values ("."-separated list) and breaks it
// apart into a map of label name -> label value.
func makeLabels(labelNames []string, labelValsCombined string) []string {
	tags := make([]string, len(labelNames))
	labelVals := strings.Split(labelValsCombined, ".")
	for i, v := range labelVals {
		tags[i] = fmt.Sprintf("%s:%s", labelNames[i], v)
	}
	return tags
}

func (sb StatsBackend) addHistogram(name string, h *stats.Histogram, tags []string) {
	labels := h.Labels()
	buckets := h.Buckets()
	for i := range labels {
		name := fmt.Sprintf("%s.%s", name, labels[i])
		sb.statsdClient.Gauge(name, float64(buckets[i]), tags, sb.sampleRate)
	}
	sb.statsdClient.Gauge(fmt.Sprintf("%s.%s", name, h.CountLabel()),
		(float64)(h.Count()),
		tags,
		sb.sampleRate,
	)
	sb.statsdClient.Gauge(fmt.Sprintf("%s.%s", name, h.TotalLabel()),
		(float64)(h.Total()),
		tags,
		sb.sampleRate,
	)
}

// Init initializes the statsd with the given namespace.
func Init(namespace string) {
	servenv.OnRun(func() {
		if *statsdAddress == "" {
			return
		}
		statsdC, err := statsd.NewBuffered(*statsdAddress, 100)
		if err != nil {
			log.Errorf("Failed to create statsd client %v", err)
			return
		}
		statsdC.Namespace = namespace + "."
		sb.namespace = namespace
		sb.statsdClient = statsdC
		sb.sampleRate = *statsdSampleRate
		stats.RegisterPushBackend("statsd", sb)
	})
}

func (sb StatsBackend) addExpVar(kv expvar.KeyValue) {
	k := kv.Key
	switch v := kv.Value.(type) {
	case *stats.String:
		if err := sb.statsdClient.Set(k, v.Get(), nil, sb.sampleRate); err != nil {
			log.Errorf("Failed to add String %v for key %v", v, k)
		}
	case *stats.Counter:
		if err := sb.statsdClient.Count(k, v.Get(), nil, sb.sampleRate); err != nil {
			log.Errorf("Failed to add Counter %v for key %v", v, k)
		}
	case *stats.Gauge:
		if err := sb.statsdClient.Gauge(k, float64(v.Get()), nil, sb.sampleRate); err != nil {
			log.Errorf("Failed to add Gauge %v for key %v", v, k)
		}
	case *stats.GaugeFunc:
		if err := sb.statsdClient.Gauge(k, float64(v.F()), nil, sb.sampleRate); err != nil {
			log.Errorf("Failed to add GaugeFunc %v for key %v", v, k)
		}
	case *stats.CounterFunc:
		if err := sb.statsdClient.Gauge(k, float64(v.F()), nil, sb.sampleRate); err != nil {
			log.Errorf("Failed to add CounterFunc %v for key %v", v, k)
		}
	case *stats.CounterDuration:
		if err := sb.statsdClient.TimeInMilliseconds(k, float64(v.Get().Milliseconds()), nil, sb.sampleRate); err != nil {
			log.Errorf("Failed to add CounterDuration %v for key %v", v, k)
		}
	case *stats.CounterDurationFunc:
		if err := sb.statsdClient.TimeInMilliseconds(k, float64(v.F().Milliseconds()), nil, sb.sampleRate); err != nil {
			log.Errorf("Failed to add CounterDuration %v for key %v", v, k)
		}
	case *stats.GaugeDuration:
		if err := sb.statsdClient.TimeInMilliseconds(k, float64(v.Get().Milliseconds()), nil, sb.sampleRate); err != nil {
			log.Errorf("Failed to add GaugeDuration %v for key %v", v, k)
		}
	case *stats.GaugeDurationFunc:
		if err := sb.statsdClient.TimeInMilliseconds(k, float64(v.F().Milliseconds()), nil, sb.sampleRate); err != nil {
			log.Errorf("Failed to add GaugeDuration %v for key %v", v, k)
		}
	case *stats.CountersWithSingleLabel:
		for labelVal, val := range v.Counts() {
			if err := sb.statsdClient.Count(k, val, makeLabel(v.Label(), labelVal), sb.sampleRate); err != nil {
				log.Errorf("Failed to add CountersWithSingleLabel %v for key %v", v, k)
			}
		}
	case *stats.CountersWithMultiLabels:
		for labelVals, val := range v.Counts() {
			if err := sb.statsdClient.Count(k, val, makeLabels(v.Labels(), labelVals), sb.sampleRate); err != nil {
				log.Errorf("Failed to add CountersFuncWithMultiLabels %v for key %v", v, k)
			}
		}
	case *stats.CountersFuncWithMultiLabels:
		for labelVals, val := range v.Counts() {
			if err := sb.statsdClient.Count(k, val, makeLabels(v.Labels(), labelVals), sb.sampleRate); err != nil {
				log.Errorf("Failed to add CountersFuncWithMultiLabels %v for key %v", v, k)
			}
		}
	case *stats.GaugesWithMultiLabels:
		for labelVals, val := range v.Counts() {
			if err := sb.statsdClient.Gauge(k, float64(val), makeLabels(v.Labels(), labelVals), sb.sampleRate); err != nil {
				log.Errorf("Failed to add GaugesWithMultiLabels %v for key %v", v, k)
			}
		}
	case *stats.GaugesFuncWithMultiLabels:
		for labelVals, val := range v.Counts() {
			if err := sb.statsdClient.Gauge(k, float64(val), makeLabels(v.Labels(), labelVals), sb.sampleRate); err != nil {
				log.Errorf("Failed to add GaugesFuncWithMultiLabels %v for key %v", v, k)
			}
		}
	case *stats.GaugesWithSingleLabel:
		for labelVal, val := range v.Counts() {
			if err := sb.statsdClient.Gauge(k, float64(val), makeLabel(v.Label(), labelVal), sb.sampleRate); err != nil {
				log.Errorf("Failed to add GaugesWithSingleLabel %v for key %v", v, k)
			}
		}
	case *stats.MultiTimings:
		labels := v.Labels()
		hists := v.Histograms()
		for labelValsCombined, histogram := range hists {
			sb.addHistogram(k, histogram, makeLabels(labels, labelValsCombined))
		}
	case *stats.Timings:
		// TODO: for statsd.timing metrics, there is no good way to transfer the histogram to it
		// If we store a in memory buffer for stats.Timings and flush it here it's hard to make the stats
		// thread safe.
		// Instead, we export the timings stats as histogram here. We won't have the percentile breakdown
		// for the metrics, but we can still get the average from total and count
		labels := []string{v.Label()}
		hists := v.Histograms()
		for labelValsCombined, histogram := range hists {
			sb.addHistogram(k, histogram, makeLabels(labels, labelValsCombined))
		}
	case *stats.Histogram:
		sb.addHistogram(k, v, []string{})
	case expvar.Func:
		// Export memstats as gauge so that we don't need to call extra ReadMemStats
		if k == "memstats" {
			var obj map[string]interface{}
			if err := json.Unmarshal([]byte(v.String()), &obj); err != nil {
				return
			}
			for k, v := range obj {
				if k == "NumGC" {
					if err := sb.statsdClient.Gauge("NumGC", v.(float64), []string{}, sb.sampleRate); err != nil {
						log.Errorf("Failed to export NumGC %v", v)
					}
				} else if k == "Frees" {
					if err := sb.statsdClient.Gauge("Frees", v.(float64), []string{}, sb.sampleRate); err != nil {
						log.Errorf("Failed to export Frees %v", v)
					}
				} else if k == "GCCPUFraction" {
					if err := sb.statsdClient.Gauge("GCCPUFraction", v.(float64), []string{}, sb.sampleRate); err != nil {
						log.Errorf("Failed to export GCCPUFraction %v", v)
					}
				} else if k == "PauseTotalNs" {
					if err := sb.statsdClient.Gauge("PauseTotalNs", v.(float64), []string{}, sb.sampleRate); err != nil {
						log.Errorf("Failed to export PauseTotalNs %v", v)
					}
				} else if k == "HeapAlloc" {
					if err := sb.statsdClient.Gauge("HeapAlloc", v.(float64), []string{}, sb.sampleRate); err != nil {
						log.Errorf("Failed to export HeapAlloc %v", v)
					}
				}
			}
		}
	case *stats.StringMapFunc, *stats.Rates, *stats.RatesFunc:
		// Silently ignore metrics that does not make sense to be exported to statsd
	default:
		log.Warningf("Silently ignore metrics with key %v [%T]", k, kv.Value)
	}
}

// PushAll flush out the pending metrics
func (sb StatsBackend) PushAll() error {
	expvar.Do(func(kv expvar.KeyValue) {
		sb.addExpVar(kv)
	})
	if err := sb.statsdClient.Flush(); err != nil {
		return err
	}
	return nil
}
