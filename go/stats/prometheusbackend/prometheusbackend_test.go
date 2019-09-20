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
package prometheusbackend

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/stats"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const namespace = "namespace"

func TestPrometheusCounter(t *testing.T) {
	name := "blah"
	c := stats.NewCounter(name, "blah")
	c.Add(1)
	checkHandlerForMetrics(t, name, 1)
	//TODO: ban this? And for other counter types too?
	// c.Add(-1)
	c.Reset()
	checkHandlerForMetrics(t, name, 0)
}

func TestPrometheusGauge(t *testing.T) {
	name := "blah_gauge"
	c := stats.NewGauge(name, "help")
	c.Add(1)
	checkHandlerForMetrics(t, name, 1)
	c.Add(-1)
	checkHandlerForMetrics(t, name, 0)
	c.Set(-5)
	checkHandlerForMetrics(t, name, -5)
	c.Reset()
	checkHandlerForMetrics(t, name, 0)
}

func TestPrometheusCounterFunc(t *testing.T) {
	name := "blah_counterfunc"
	stats.NewCounterFunc(name, "help", func() int64 {
		return 2
	})

	checkHandlerForMetrics(t, name, 2)
}

func TestPrometheusGaugeFunc(t *testing.T) {
	name := "blah_gaugefunc"

	stats.NewGaugeFunc(name, "help", func() int64 {
		return -3
	})

	checkHandlerForMetrics(t, name, -3)
}

func TestPrometheusFloatFunc(t *testing.T) {
	name := "blah_floatfunc"

	stats.Publish(name, stats.FloatFunc(func() float64 { return -4 }))
	checkHandlerForMetrics(t, name, -4)
}

func TestPrometheusCounterDuration(t *testing.T) {
	name := "blah_counterduration"

	d := stats.NewCounterDuration(name, "help")
	d.Add(1 * time.Second)

	checkHandlerForMetrics(t, name, 1)
}

func TestPrometheusCounterDurationFunc(t *testing.T) {
	name := "blah_counterdurationfunc"

	stats.NewCounterDurationFunc(name, "help", func() time.Duration { return 1 * time.Second })

	checkHandlerForMetrics(t, name, 1)
}

func TestPrometheusGaugeDuration(t *testing.T) {
	name := "blah_gaugeduration"

	d := stats.NewGaugeDuration(name, "help")
	d.Set(1 * time.Second)

	checkHandlerForMetrics(t, name, 1)
}

func TestPrometheusGaugeDurationFunc(t *testing.T) {
	name := "blah_gaugedurationfunc"

	stats.NewGaugeDurationFunc(name, "help", func() time.Duration { return 1 * time.Second })

	checkHandlerForMetrics(t, name, 1)
}

func checkHandlerForMetrics(t *testing.T, metric string, value int) {
	response := testMetricsHandler(t)

	expected := fmt.Sprintf("%s_%s %d", namespace, metric, value)

	if !strings.Contains(response.Body.String(), expected) {
		t.Fatalf("Expected %s got %s", expected, response.Body.String())
	}
}

func TestPrometheusCountersWithSingleLabel(t *testing.T) {
	name := "blah_counterswithsinglelabel"
	c := stats.NewCountersWithSingleLabel(name, "help", "label", "tag1", "tag2")
	c.Add("tag1", 1)
	checkHandlerForMetricWithSingleLabel(t, name, "label", "tag1", 1)
	checkHandlerForMetricWithSingleLabel(t, name, "label", "tag2", 0)
	c.Add("tag2", 41)
	checkHandlerForMetricWithSingleLabel(t, name, "label", "tag1", 1)
	checkHandlerForMetricWithSingleLabel(t, name, "label", "tag2", 41)
	c.Reset("tag2")
	checkHandlerForMetricWithSingleLabel(t, name, "label", "tag1", 1)
	checkHandlerForMetricWithSingleLabel(t, name, "label", "tag2", 0)
}

func TestPrometheusGaugesWithSingleLabel(t *testing.T) {
	name := "blah_gaugeswithsinglelabel"
	c := stats.NewGaugesWithSingleLabel(name, "help", "label", "tag1", "tag2")
	c.Add("tag1", 1)
	checkHandlerForMetricWithSingleLabel(t, name, "label", "tag1", 1)

	c.Add("tag2", 1)
	checkHandlerForMetricWithSingleLabel(t, name, "label", "tag2", 1)

	c.Set("tag1", -1)
	checkHandlerForMetricWithSingleLabel(t, name, "label", "tag1", -1)

	c.Reset("tag2")
	checkHandlerForMetricWithSingleLabel(t, name, "label", "tag1", -1)
	checkHandlerForMetricWithSingleLabel(t, name, "label", "tag2", 0)
}

func checkHandlerForMetricWithSingleLabel(t *testing.T, metric, label, tag string, value int) {
	response := testMetricsHandler(t)

	expected := fmt.Sprintf("%s_%s{%s=\"%s\"} %d", namespace, metric, label, tag, value)

	if !strings.Contains(response.Body.String(), expected) {
		t.Fatalf("Expected %s got %s", expected, response.Body.String())
	}
}

func TestPrometheusCountersWithMultiLabels(t *testing.T) {
	name := "blah_counterswithmultilabels"
	labels := []string{"label1", "label2"}
	labelValues := []string{"foo", "bar"}
	c := stats.NewCountersWithMultiLabels(name, "help", labels)
	c.Add(labelValues, 1)
	checkHandlerForMetricWithMultiLabels(t, name, labels, labelValues, 1)
	labelValues2 := []string{"baz", "bazbar"}
	c.Add(labelValues2, 1)
	checkHandlerForMetricWithMultiLabels(t, name, labels, labelValues, 1)
	checkHandlerForMetricWithMultiLabels(t, name, labels, labelValues2, 1)
	c.Reset(labelValues)
	checkHandlerForMetricWithMultiLabels(t, name, labels, labelValues, 0)
	checkHandlerForMetricWithMultiLabels(t, name, labels, labelValues2, 1)
}

func TestPrometheusGaugesWithMultiLabels(t *testing.T) {
	name := "blah_gaugeswithmultilabels"
	labels := []string{"label1", "label2"}
	labelValues := []string{"foo", "bar"}
	c := stats.NewGaugesWithMultiLabels(name, "help", labels)
	c.Add(labelValues, 1)
	checkHandlerForMetricWithMultiLabels(t, name, labels, labelValues, 1)

	c.Set(labelValues, -1)
	checkHandlerForMetricWithMultiLabels(t, name, labels, labelValues, -1)

	labelValues2 := []string{"baz", "bazbar"}
	c.Add(labelValues2, 1)
	checkHandlerForMetricWithMultiLabels(t, name, labels, labelValues, -1)
	checkHandlerForMetricWithMultiLabels(t, name, labels, labelValues2, 1)

	c.Reset(labelValues)
	checkHandlerForMetricWithMultiLabels(t, name, labels, labelValues, 0)
	checkHandlerForMetricWithMultiLabels(t, name, labels, labelValues2, 1)
}

func TestPrometheusCountersWithMultiLabels_AddPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic when adding to inequal label lengths")
		}
	}()

	name := "blah_counterswithmultilabels_inequallength"
	c := stats.NewCountersWithMultiLabels(name, "help", []string{"label1", "label2"})
	c.Add([]string{"label1"}, 1)
}

func TestPrometheusCountersFuncWithMultiLabels(t *testing.T) {
	name := "blah_countersfuncwithmultilabels"
	labels := []string{"label1", "label2"}

	stats.NewCountersFuncWithMultiLabels(name, "help", labels, func() map[string]int64 {
		m := make(map[string]int64)
		m["foo.bar"] = 1
		m["bar.baz"] = 1
		return m
	})

	checkHandlerForMetricWithMultiLabels(t, name, labels, []string{"foo", "bar"}, 1)
	checkHandlerForMetricWithMultiLabels(t, name, labels, []string{"bar", "baz"}, 1)
}

func checkHandlerForMetricWithMultiLabels(t *testing.T, metric string, labels []string, labelValues []string, value int64) {
	response := testMetricsHandler(t)

	expected := fmt.Sprintf("%s_%s{%s=\"%s\",%s=\"%s\"} %d", namespace, metric, labels[0], labelValues[0], labels[1], labelValues[1], value)

	if !strings.Contains(response.Body.String(), expected) {
		t.Fatalf("Expected %s got %s", expected, response.Body.String())
	}
}

func TestPrometheusTimings(t *testing.T) {
	name := "blah_timings"
	cats := []string{"cat1", "cat2"}
	timing := stats.NewTimings(name, "help", "category", cats...)
	timing.Add("cat1", time.Duration(30*time.Millisecond))
	timing.Add("cat1", time.Duration(200*time.Millisecond))
	timing.Add("cat1", time.Duration(1*time.Second))

	response := testMetricsHandler(t)
	var s []string

	s = append(s, fmt.Sprintf("%s_%s_bucket{category=\"%s\",le=\"0.0005\"} %d", namespace, name, cats[0], 0))
	s = append(s, fmt.Sprintf("%s_%s_bucket{category=\"%s\",le=\"0.001\"} %d", namespace, name, cats[0], 0))
	s = append(s, fmt.Sprintf("%s_%s_bucket{category=\"%s\",le=\"0.005\"} %d", namespace, name, cats[0], 0))
	s = append(s, fmt.Sprintf("%s_%s_bucket{category=\"%s\",le=\"0.01\"} %d", namespace, name, cats[0], 0))
	s = append(s, fmt.Sprintf("%s_%s_bucket{category=\"%s\",le=\"0.05\"} %d", namespace, name, cats[0], 1))
	s = append(s, fmt.Sprintf("%s_%s_bucket{category=\"%s\",le=\"0.1\"} %d", namespace, name, cats[0], 1))
	s = append(s, fmt.Sprintf("%s_%s_bucket{category=\"%s\",le=\"0.5\"} %d", namespace, name, cats[0], 2))
	s = append(s, fmt.Sprintf("%s_%s_bucket{category=\"%s\",le=\"1\"} %d", namespace, name, cats[0], 3))
	s = append(s, fmt.Sprintf("%s_%s_bucket{category=\"%s\",le=\"5\"} %d", namespace, name, cats[0], 3))
	s = append(s, fmt.Sprintf("%s_%s_bucket{category=\"%s\",le=\"10\"} %d", namespace, name, cats[0], 3))
	s = append(s, fmt.Sprintf("%s_%s_bucket{category=\"%s\",le=\"+Inf\"} %d", namespace, name, cats[0], 3))
	s = append(s, fmt.Sprintf("%s_%s_sum{category=\"%s\"} %s", namespace, name, cats[0], "1.23"))
	s = append(s, fmt.Sprintf("%s_%s_count{category=\"%s\"} %d", namespace, name, cats[0], 3))

	for _, line := range s {
		if !strings.Contains(response.Body.String(), line) {
			t.Fatalf("Expected result to contain %s, got %s", line, response.Body.String())
		}
	}
}

func TestPrometheusMultiTimings(t *testing.T) {
	name := "blah_multitimings"
	cats := []string{"cat1", "cat2"}
	catLabels := []string{"foo", "bar"}
	timing := stats.NewMultiTimings(name, "help", cats)
	timing.Add(catLabels, time.Duration(30*time.Millisecond))
	timing.Add(catLabels, time.Duration(200*time.Millisecond))
	timing.Add(catLabels, time.Duration(1*time.Second))

	response := testMetricsHandler(t)
	var s []string

	s = append(s, fmt.Sprintf("%s_%s_bucket{%s=\"%s\",%s=\"%s\",le=\"0.0005\"} %d", namespace, name, cats[0], catLabels[0], cats[1], catLabels[1], 0))
	s = append(s, fmt.Sprintf("%s_%s_bucket{%s=\"%s\",%s=\"%s\",le=\"0.001\"} %d", namespace, name, cats[0], catLabels[0], cats[1], catLabels[1], 0))
	s = append(s, fmt.Sprintf("%s_%s_bucket{%s=\"%s\",%s=\"%s\",le=\"0.005\"} %d", namespace, name, cats[0], catLabels[0], cats[1], catLabels[1], 0))
	s = append(s, fmt.Sprintf("%s_%s_bucket{%s=\"%s\",%s=\"%s\",le=\"0.01\"} %d", namespace, name, cats[0], catLabels[0], cats[1], catLabels[1], 0))
	s = append(s, fmt.Sprintf("%s_%s_bucket{%s=\"%s\",%s=\"%s\",le=\"0.05\"} %d", namespace, name, cats[0], catLabels[0], cats[1], catLabels[1], 1))
	s = append(s, fmt.Sprintf("%s_%s_bucket{%s=\"%s\",%s=\"%s\",le=\"0.1\"} %d", namespace, name, cats[0], catLabels[0], cats[1], catLabels[1], 1))
	s = append(s, fmt.Sprintf("%s_%s_bucket{%s=\"%s\",%s=\"%s\",le=\"0.5\"} %d", namespace, name, cats[0], catLabels[0], cats[1], catLabels[1], 2))
	s = append(s, fmt.Sprintf("%s_%s_bucket{%s=\"%s\",%s=\"%s\",le=\"1\"} %d", namespace, name, cats[0], catLabels[0], cats[1], catLabels[1], 3))
	s = append(s, fmt.Sprintf("%s_%s_bucket{%s=\"%s\",%s=\"%s\",le=\"5\"} %d", namespace, name, cats[0], catLabels[0], cats[1], catLabels[1], 3))
	s = append(s, fmt.Sprintf("%s_%s_bucket{%s=\"%s\",%s=\"%s\",le=\"10\"} %d", namespace, name, cats[0], catLabels[0], cats[1], catLabels[1], 3))
	s = append(s, fmt.Sprintf("%s_%s_bucket{%s=\"%s\",%s=\"%s\",le=\"+Inf\"} %d", namespace, name, cats[0], catLabels[0], cats[1], catLabels[1], 3))
	s = append(s, fmt.Sprintf("%s_%s_sum{%s=\"%s\",%s=\"%s\"} %s", namespace, name, cats[0], catLabels[0], cats[1], catLabels[1], "1.23"))
	s = append(s, fmt.Sprintf("%s_%s_count{%s=\"%s\",%s=\"%s\"} %d", namespace, name, cats[0], catLabels[0], cats[1], catLabels[1], 3))

	for _, line := range s {
		if !strings.Contains(response.Body.String(), line) {
			t.Fatalf("Expected result to contain %s, got %s", line, response.Body.String())
		}
	}
}

func TestPrometheusMultiTimings_PanicWrongLength(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic when adding to inequal label lengths")
		}
	}()

	c := stats.NewMultiTimings("name", "help", []string{"label1", "label2"})
	c.Add([]string{"label1"}, time.Duration(100000000))
}

func TestPrometheusHistogram(t *testing.T) {
	name := "blah_hist"
	hist := stats.NewHistogram(name, "help", []int64{1, 5, 10})
	hist.Add(2)
	hist.Add(3)
	hist.Add(6)

	response := testMetricsHandler(t)
	var s []string

	s = append(s, fmt.Sprintf("%s_%s_bucket{le=\"1\"} %d", namespace, name, 0))
	s = append(s, fmt.Sprintf("%s_%s_bucket{le=\"5\"} %d", namespace, name, 2))
	s = append(s, fmt.Sprintf("%s_%s_bucket{le=\"10\"} %d", namespace, name, 3))
	s = append(s, fmt.Sprintf("%s_%s_sum %d", namespace, name, 1))
	s = append(s, fmt.Sprintf("%s_%s_count %d", namespace, name, 3))

	for _, line := range s {
		if !strings.Contains(response.Body.String(), line) {
			t.Fatalf("Expected result to contain %s, got %s", line, response.Body.String())
		}
	}
}

func testMetricsHandler(t *testing.T) *httptest.ResponseRecorder {
	req, _ := http.NewRequest("GET", "/metrics", nil)
	response := httptest.NewRecorder()

	promhttp.Handler().ServeHTTP(response, req)
	return response
}

func TestMain(m *testing.M) {
	Init(namespace)
	os.Exit(m.Run())
}
