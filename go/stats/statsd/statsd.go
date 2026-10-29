/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package statsd

import (
	"encoding/json"
	"expvar"
	"fmt"
	"hash/crc32"
	"log/slog"
	"strings"
	"sync"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/utils"
)

var (
	statsdAddress    string
	statsdSampleRate = 1.0
)

func registerFlags(fs *pflag.FlagSet) {
	utils.SetFlagStringVar(fs, &statsdAddress, "statsd-address", statsdAddress, "Address for statsd client")
	utils.SetFlagFloat64Var(fs, &statsdSampleRate, "statsd-sample-rate", statsdSampleRate, "Sample rate for statsd metrics")
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
	commonTags := make([]string, 0, len(tags))
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
	opts := []statsd.Option{
		statsd.WithMaxMessagesPerPayload(100),
		statsd.WithNamespace(namespace),
	}
	if tags := stats.ParseCommonTags(stats.CommonTags); len(tags) > 0 {
		opts = append(opts, statsd.WithTags(makeCommonTags(tags)))
	}
	statsdC, err := statsd.New(statsdAddress, opts...)
	if err != nil {
		log.Error("Failed to create statsd client", slog.Any("error", err))
		return
	}
	sb.namespace = namespace
	sb.statsdClient = statsdC
	sb.sampleRate = statsdSampleRate
	stats.RegisterPushBackend("statsd", sb)
	stats.RegisterTimerHook(func(statsName, name string, value int64, timings *stats.Timings) {
		tags := makeLabels(strings.Split(timings.Label(), "."), name)
		if err := statsdC.TimeInMilliseconds(statsName, float64(value), tags, sb.sampleRate); err != nil {
			log.Error("Fail to TimeInMilliseconds", slog.String("stats_name", statsName), slog.Any("error", err))
		}
	})
	stats.RegisterHistogramHook(func(statsName string, val int64) {
		if err := statsdC.Histogram(statsName, float64(val), []string{}, sb.sampleRate); err != nil {
			log.Error("Fail to Histogram", slog.String("stats_name", statsName), slog.Any("error", err))
		}
	})
}

func (sb StatsBackend) addExpVar(kv expvar.KeyValue) {
	k := kv.Key
	switch v := kv.Value.(type) {
	case *stats.Counter:
		if err := sb.statsdClient.Count(k, v.Get(), nil, sb.sampleRate); err != nil {
			log.Error("Failed to add Counter", slog.String("stats_name", k), slog.String("stats_value", fmt.Sprint(v)), slog.Any("error", err))
		}
	case *stats.Gauge:
		if err := sb.statsdClient.Gauge(k, float64(v.Get()), nil, sb.sampleRate); err != nil {
			log.Error("Failed to add Gauge", slog.String("stats_name", k), slog.String("stats_value", fmt.Sprint(v)), slog.Any("error", err))
		}
	case *stats.GaugeFloat64:
		if err := sb.statsdClient.Gauge(k, v.Get(), nil, sb.sampleRate); err != nil {
			log.Error("Failed to add GaugeFloat64", slog.String("stats_name", k), slog.String("stats_value", fmt.Sprint(v)), slog.Any("error", err))
		}
	case *stats.GaugeFunc:
		if err := sb.statsdClient.Gauge(k, float64(v.F()), nil, sb.sampleRate); err != nil {
			log.Error("Failed to add GaugeFunc", slog.String("stats_name", k), slog.String("stats_value", fmt.Sprint(v)), slog.Any("error", err))
		}
	case *stats.CounterFunc:
		if err := sb.statsdClient.Gauge(k, float64(v.F()), nil, sb.sampleRate); err != nil {
			log.Error("Failed to add CounterFunc", slog.String("stats_name", k), slog.String("stats_value", fmt.Sprint(v)), slog.Any("error", err))
		}
	case *stats.CounterDuration:
		if err := sb.statsdClient.TimeInMilliseconds(k, float64(v.Get().Milliseconds()), nil, sb.sampleRate); err != nil {
			log.Error("Failed to add CounterDuration", slog.String("stats_name", k), slog.String("stats_value", fmt.Sprint(v)), slog.Any("error", err))
		}
	case *stats.CounterDurationFunc:
		if err := sb.statsdClient.TimeInMilliseconds(k, float64(v.F().Milliseconds()), nil, sb.sampleRate); err != nil {
			log.Error("Failed to add CounterDurationFunc", slog.String("stats_name", k), slog.String("stats_value", fmt.Sprint(v)), slog.Any("error", err))
		}
	case *stats.GaugeDuration:
		if err := sb.statsdClient.TimeInMilliseconds(k, float64(v.Get().Milliseconds()), nil, sb.sampleRate); err != nil {
			log.Error("Failed to add GaugeDuration", slog.String("stats_name", k), slog.String("stats_value", fmt.Sprint(v)), slog.Any("error", err))
		}
	case *stats.GaugeDurationFunc:
		if err := sb.statsdClient.TimeInMilliseconds(k, float64(v.F().Milliseconds()), nil, sb.sampleRate); err != nil {
			log.Error("Failed to add GaugeDurationFunc", slog.String("stats_name", k), slog.String("stats_value", fmt.Sprint(v)), slog.Any("error", err))
		}
	case *stats.CountersWithSingleLabel:
		for labelVal, val := range v.Counts() {
			if err := sb.statsdClient.Count(k, val, makeLabel(v.Label(), labelVal), sb.sampleRate); err != nil {
				log.Error("Failed to add CountersWithSingleLabel", slog.String("stats_name", k), slog.String("stats_value", fmt.Sprint(v)), slog.Any("error", err))
			}
		}
	case *stats.CountersWithMultiLabels:
		for labelVals, val := range v.Counts() {
			if err := sb.statsdClient.Count(k, val, makeLabels(v.Labels(), labelVals), sb.sampleRate); err != nil {
				log.Error("Failed to add CountersWithMultiLabels", slog.String("stats_name", k), slog.String("stats_value", fmt.Sprint(v)), slog.Any("error", err))
			}
		}
	case *stats.CountersFuncWithMultiLabels:
		for labelVals, val := range v.Counts() {
			if err := sb.statsdClient.Count(k, val, makeLabels(v.Labels(), labelVals), sb.sampleRate); err != nil {
				log.Error("Failed to add CountersFuncWithMultiLabels", slog.String("stats_name", k), slog.String("stats_value", fmt.Sprint(v)), slog.Any("error", err))
			}
		}
	case *stats.GaugesWithMultiLabels:
		for labelVals, val := range v.Counts() {
			if err := sb.statsdClient.Gauge(k, float64(val), makeLabels(v.Labels(), labelVals), sb.sampleRate); err != nil {
				log.Error("Failed to add GaugesWithMultiLabels", slog.String("stats_name", k), slog.String("stats_value", fmt.Sprint(v)), slog.Any("error", err))
			}
		}
	case *stats.GaugesFuncWithMultiLabels:
		for labelVals, val := range v.Counts() {
			if err := sb.statsdClient.Gauge(k, float64(val), makeLabels(v.Labels(), labelVals), sb.sampleRate); err != nil {
				log.Error("Failed to add GaugesFuncWithMultiLabels", slog.String("stats_name", k), slog.String("stats_value", fmt.Sprint(v)), slog.Any("error", err))
			}
		}
	case *stats.GaugesWithSingleLabel:
		for labelVal, val := range v.Counts() {
			if err := sb.statsdClient.Gauge(k, float64(val), makeLabel(v.Label(), labelVal), sb.sampleRate); err != nil {
				log.Error("Failed to add GaugesWithSingleLabel", slog.String("stats_name", k), slog.String("stats_value", fmt.Sprint(v)), slog.Any("error", err))
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
						log.Error("Failed to export", slog.String("stats_name", memstatsKey), slog.String("stats_value", fmt.Sprint(v)), slog.Any("error", err))
					}
				}
			}
		}
	case *stats.String:
		if k == "BuildGitRev" {
			buildGitRecOnce.Do(func() {
				checksum := crc32.ChecksumIEEE([]byte(v.Get()))
				if err := sb.statsdClient.Gauge(k, float64(checksum), []string{}, sb.sampleRate); err != nil {
					log.Error("Failed to export", slog.String("stats_name", k), slog.String("stats_value", fmt.Sprint(v)), slog.Any("error", err))
				}
			})
		}
	case *stats.Rates, *stats.RatesFunc, *stats.StringFunc, *stats.StringMapFunc,
		stats.StringFunc, stats.StringMapFunc:
		// Silently ignore metrics that does not make sense to be exported to statsd
	default:
		log.Warn("Silently ignore metrics", slog.String("stats_name", k), slog.String("type", fmt.Sprintf("%T", kv.Value)))
	}
}

// PushAll flushes out the pending metrics
func (sb StatsBackend) PushAll() error {
	expvar.Do(func(kv expvar.KeyValue) {
		sb.addExpVar(kv)
	})
	if err := sb.statsdClient.Flush(); err != nil {
		return err
	}
	return nil
}

// PushOne pushes the single provided metric.
func (sb StatsBackend) PushOne(name string, v stats.Variable) error {
	sb.addExpVar(expvar.KeyValue{
		Key:   name,
		Value: v,
	})
	if err := sb.statsdClient.Flush(); err != nil {
		return err
	}
	return nil
}
