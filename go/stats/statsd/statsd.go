package statsd

import (
	"encoding/json"
	"expvar"
	"fmt"
	"hash/crc32"
	"strings"
	"sync"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
)

var (
	statsdAddress    string
	statsdSampleRate = 1.0
)

func registerFlags(fs *pflag.FlagSet) {
	fs.StringVar(&statsdAddress, "statsd_address", statsdAddress, "Address for statsd client")
	fs.Float64Var(&statsdSampleRate, "statsd_sample_rate", statsdSampleRate, "Sample rate for statsd metrics")
}

func init() {
	servenv.OnParseFor("vtgate", registerFlags)
	servenv.OnParseFor("vttablet", registerFlags)
}

// StatsBackend implements PullBackend using statsd
type StatsBackend struct {
	namespace    string
	statsdClient *statsd.Client
	sampleRate   float64
}

var (
	sb              StatsBackend
	buildGitRecOnce sync.Once
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

func makeCommonTags(tags map[string]string) []string {
	var commonTags []string
	for k, v := range tags {
		commonTag := fmt.Sprintf("%s:%s", k, v)
		commonTags = append(commonTags, commonTag)
	}
	return commonTags
}

// Init initializes the statsd with the given namespace.
func Init(namespace string) {
	servenv.OnRun(func() {
		InitWithoutServenv(namespace)
	})
}

// InitWithoutServenv initializes the statsd using the namespace but without servenv
func InitWithoutServenv(namespace string) {
	if statsdAddress == "" {
		log.Info("statsdAddress is empty")
		return
	}
	statsdC, err := statsd.NewBuffered(statsdAddress, 100)
	if err != nil {
		log.Errorf("Failed to create statsd client %v", err)
		return
	}
	statsdC.Namespace = namespace + "."
	if tags := stats.ParseCommonTags(*stats.CommonTags); len(tags) > 0 {
		statsdC.Tags = makeCommonTags(tags)
	}
	sb.namespace = namespace
	sb.statsdClient = statsdC
	sb.sampleRate = statsdSampleRate
	stats.RegisterPushBackend("statsd", sb)
	stats.RegisterTimerHook(func(statsName, name string, value int64, timings *stats.Timings) {
		tags := makeLabels(strings.Split(timings.Label(), "."), name)
		if err := statsdC.TimeInMilliseconds(statsName, float64(value), tags, sb.sampleRate); err != nil {
			log.Errorf("Fail to TimeInMilliseconds %v: %v", statsName, err)
		}
	})
	stats.RegisterHistogramHook(func(statsName string, val int64) {
		if err := statsdC.Histogram(statsName, float64(val), []string{}, sb.sampleRate); err != nil {
			log.Errorf("Fail to Histogram for %v: %v", statsName, err)
		}
	})
}

func (sb StatsBackend) addExpVar(kv expvar.KeyValue) {
	k := kv.Key
	switch v := kv.Value.(type) {
	case *stats.Counter:
		if err := sb.statsdClient.Count(k, v.Get(), nil, sb.sampleRate); err != nil {
			log.Errorf("Failed to add Counter %v for key %v", v, k)
		}
	case *stats.Gauge:
		if err := sb.statsdClient.Gauge(k, float64(v.Get()), nil, sb.sampleRate); err != nil {
			log.Errorf("Failed to add Gauge %v for key %v", v, k)
		}
	case *stats.GaugeFloat64:
		if err := sb.statsdClient.Gauge(k, v.Get(), nil, sb.sampleRate); err != nil {
			log.Errorf("Failed to add GaugeFloat64 %v for key %v", v, k)
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
	case *stats.Timings, *stats.MultiTimings, *stats.Histogram:
		// it does not make sense to export static expvar to statsd,
		// instead we rely on hooks to integrate with statsd' timing and histogram api directly
	case expvar.Func:
		// Export memstats as gauge so that we don't need to call extra ReadMemStats
		if k == "memstats" {
			var obj map[string]any
			if err := json.Unmarshal([]byte(v.String()), &obj); err != nil {
				return
			}
			for k, v := range obj {
				memstatsVal, ok := v.(float64)
				if ok {
					memstatsKey := "memstats." + k
					if err := sb.statsdClient.Gauge(memstatsKey, memstatsVal, []string{}, sb.sampleRate); err != nil {
						log.Errorf("Failed to export %v %v", k, v)
					}
				}
			}
		}
	case *stats.String:
		if k == "BuildGitRev" {
			buildGitRecOnce.Do(func() {
				checksum := crc32.ChecksumIEEE([]byte(v.Get()))
				if err := sb.statsdClient.Gauge(k, float64(checksum), []string{}, sb.sampleRate); err != nil {
					log.Errorf("Failed to export %v %v", k, v)
				}
			})
		}
	case *stats.Rates, *stats.RatesFunc, *stats.StringFunc, *stats.StringMapFunc,
		stats.StringFunc, stats.StringMapFunc:
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
