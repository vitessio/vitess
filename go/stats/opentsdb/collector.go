/*
Copyright 2019 The Vitess Authors.

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

package opentsdb

import (
	"encoding/json"
	"expvar"
	"strings"
	"unicode"

	"vitess.io/vitess/go/stats"
)

// collector tracks state for a single pass of stats reporting / data collection.
type collector struct {
	commonTags map[string]string
	data       []*DataPoint
	prefix     string
	timestamp  int64
}

func (dc *collector) collectAll() {
	expvar.Do(func(kv expvar.KeyValue) {
		dc.addExpVar(kv)
	})
}

func (dc *collector) collectOne(name string, v expvar.Var) {
	dc.addExpVar(expvar.KeyValue{
		Key:   name,
		Value: v,
	})
}

func (dc *collector) addInt(metric string, val int64, tags map[string]string) {
	dc.addFloat(metric, float64(val), tags)
}

func (dc *collector) addFloat(metric string, val float64, tags map[string]string) {
	var fullMetric string
	if len(dc.prefix) > 0 {
		fullMetric = combineMetricName(dc.prefix, metric)
	} else {
		fullMetric = metric
	}

	// Restrict metric and tag name/values to legal characters:
	// http://opentsdb.net/docs/build/html/user_guide/writing.html#metrics-and-tags
	//
	// Also make everything lowercase, since opentsdb is case sensitive and lowercase
	// simplifies the convention.
	sanitize := func(text string) string {
		var b strings.Builder
		for _, r := range text {
			if unicode.IsDigit(r) || unicode.IsLetter(r) || r == '-' || r == '_' || r == '/' || r == '.' {
				b.WriteRune(r)
			} else {
				// For characters that would cause errors, write underscore instead
				b.WriteRune('_')
			}
		}
		return strings.ToLower(b.String())
	}

	fullTags := make(map[string]string)
	for k, v := range dc.commonTags {
		fullTags[sanitize(k)] = sanitize(v)
	}
	for k, v := range tags {
		fullTags[sanitize(k)] = sanitize(v)
	}

	dp := &DataPoint{
		Metric:    sanitize(fullMetric),
		Value:     val,
		Timestamp: float64(dc.timestamp),
		Tags:      fullTags,
	}
	dc.data = append(dc.data, dp)
}

// addExpVar adds all the data points associated with a particular expvar to the list of
// opentsdb data points. How an expvar is translated depends on its type.
//
// Well-known metric types like histograms and integers are directly converted (saving labels
// as tags).
//
// Generic unrecognized expvars are serialized to json and their int/float values are exported.
// Strings and lists in expvars are not exported.
func (dc *collector) addExpVar(kv expvar.KeyValue) {
	k := kv.Key
	switch v := kv.Value.(type) {
	case stats.FloatFunc:
		dc.addFloat(k, v(), nil)
	case *stats.Counter:
		dc.addInt(k, v.Get(), nil)
	case *stats.CounterFunc:
		dc.addInt(k, v.F(), nil)
	case *stats.Gauge:
		dc.addInt(k, v.Get(), nil)
	case *stats.GaugeFloat64:
		dc.addFloat(k, v.Get(), nil)
	case *stats.GaugeFunc:
		dc.addInt(k, v.F(), nil)
	case *stats.CounterDuration:
		dc.addInt(k, int64(v.Get()), nil)
	case *stats.CounterDurationFunc:
		dc.addInt(k, int64(v.F()), nil)
	case *stats.MultiTimings:
		dc.addTimings(v.Labels(), &v.Timings, k)
	case *stats.Timings:
		dc.addTimings([]string{v.Label()}, v, k)
	case *stats.Histogram:
		dc.addHistogram(v, 1, k, make(map[string]string))
	case *stats.CountersWithSingleLabel:
		for labelVal, val := range v.Counts() {
			dc.addInt(k, val, makeLabel(v.Label(), labelVal))
		}
	case *stats.CountersWithMultiLabels:
		for labelVals, val := range v.Counts() {
			dc.addInt(k, val, makeLabels(v.Labels(), labelVals))
		}
	case *stats.CountersFuncWithMultiLabels:
		for labelVals, val := range v.Counts() {
			dc.addInt(k, val, makeLabels(v.Labels(), labelVals))
		}
	case *stats.GaugesWithMultiLabels:
		for labelVals, val := range v.Counts() {
			dc.addInt(k, val, makeLabels(v.Labels(), labelVals))
		}
	case *stats.GaugesFuncWithMultiLabels:
		for labelVals, val := range v.Counts() {
			dc.addInt(k, val, makeLabels(v.Labels(), labelVals))
		}
	case *stats.GaugesWithSingleLabel:
		for labelVal, val := range v.Counts() {
			dc.addInt(k, val, makeLabel(v.Label(), labelVal))
		}
	default:
		// Deal with generic expvars by converting them to JSON and pulling out
		// all the floats. Strings and lists will not be exported to opentsdb.
		var obj map[string]any
		if err := json.Unmarshal([]byte(v.String()), &obj); err != nil {
			return
		}

		// Recursive helper function.
		dc.addUnrecognizedExpvars(combineMetricName("expvar", k), obj)
	}
}

// addUnrecognizedExpvars recurses into a json object to pull out float64 variables to report.
func (dc *collector) addUnrecognizedExpvars(prefix string, obj map[string]any) {
	for k, v := range obj {
		prefix := combineMetricName(prefix, k)
		switch v := v.(type) {
		case map[string]any:
			dc.addUnrecognizedExpvars(prefix, v)
		case float64:
			dc.addFloat(prefix, v, nil)
		}
	}
}

// addTimings converts a vitess Timings stat to something opentsdb can deal with.
func (dc *collector) addTimings(labels []string, timings *stats.Timings, prefix string) {
	histograms := timings.Histograms()
	for labelValsCombined, histogram := range histograms {
		// If you prefer millisecond timings over nanoseconds you can pass 1000000 here instead of 1.
		dc.addHistogram(histogram, 1, prefix, makeLabels(labels, labelValsCombined))
	}
}

func (dc *collector) addHistogram(histogram *stats.Histogram, divideBy int64, prefix string, tags map[string]string) {
	// TODO: OpenTSDB 2.3 doesn't have histogram support, although it's forthcoming.
	// For simplicity we report each bucket as a different metric.
	//
	// An alternative approach if you don't mind changing the code is to add a hook to Histogram creation that
	// associates each histogram with a shadow type that can track percentiles (like Timer from rcrowley/go-metrics).

	labels := histogram.Labels()
	buckets := histogram.Buckets()
	for i := range labels {
		dc.addInt(
			combineMetricName(prefix, labels[i]),
			buckets[i],
			tags,
		)
	}

	dc.addInt(
		combineMetricName(prefix, histogram.CountLabel()),
		(*histogram).Count(),
		tags,
	)
	dc.addInt(
		combineMetricName(prefix, histogram.TotalLabel()),
		(*histogram).Total()/divideBy,
		tags,
	)
}

// combineMetricName joins parts of a hierarchical name with a "."
func combineMetricName(parts ...string) string {
	return strings.Join(parts, ".")
}

// makeLabel builds a tag list with a single label + value.
func makeLabel(labelName string, labelVal string) map[string]string {
	return map[string]string{labelName: labelVal}
}

// makeLabels takes the vitess stat representation of label values ("."-separated list) and breaks it
// apart into a map of label name -> label value.
func makeLabels(labelNames []string, labelValsCombined string) map[string]string {
	tags := make(map[string]string)
	labelVals := strings.Split(labelValsCombined, ".")
	for i, v := range labelVals {
		tags[labelNames[i]] = v
	}
	return tags
}
