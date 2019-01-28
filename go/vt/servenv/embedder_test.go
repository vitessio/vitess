/*
Copyright 2018 The Vitess Authors.

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

package servenv

import (
	"expvar"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"vitess.io/vitess/go/stats"
)

func TestURLPrefix(t *testing.T) {
	if got, want := URLPrefix(""), ""; got != want {
		t.Errorf("URLPrefix(''): %v, want %v", got, want)
	}
	if got, want := URLPrefix("a"), "/a"; got != want {
		t.Errorf("URLPrefix('a'): %v, want %v", got, want)
	}
}

func TestHandleFunc(t *testing.T) {
	// Listen on a random port
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port
	go http.Serve(listener, nil)

	HandleFunc("", "/path", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("1"))
	})
	if got, want := httpGet(t, fmt.Sprintf("http://localhost:%d/path", port)), "1"; got != want {
		t.Errorf("httpGet: %s, want %s", got, want)
	}
	if got, want := httpGet(t, fmt.Sprintf("http://localhost:%d/debug/status", port)), "Status for"; !strings.Contains(got, want) {
		t.Errorf("httpGet: %s, must contain %s", got, want)
	}

	AssignPrefix("a", "")
	HandleFunc("a", "/path", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("2"))
	})
	if got, want := httpGet(t, fmt.Sprintf("http://localhost:%d/a/path", port)), "2"; got != want {
		t.Errorf("httpGet: %s, want %s", got, want)
	}
	if got, want := httpGet(t, fmt.Sprintf("http://localhost:%d/a/debug/status", port)), "Status for"; !strings.Contains(got, want) {
		t.Errorf("httpGet: %s, must contain %s", got, want)
	}

	HandleFunc("a", "/path", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("3"))
	})
	if got, want := httpGet(t, fmt.Sprintf("http://localhost:%d/a/path", port)), "3"; got != want {
		t.Errorf("httpGet: %s, want %s", got, want)
	}
	if got, want := httpGet(t, fmt.Sprintf("http://localhost:%d/a/debug/status", port)), "Status for"; !strings.Contains(got, want) {
		t.Errorf("httpGet: %s, must contain %s", got, want)
	}

	AssignPrefix("a", "")
	HandleFunc("a", "/path", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("4"))
	})
	if got, want := httpGet(t, fmt.Sprintf("http://localhost:%d/a/path", port)), "4"; got != want {
		t.Errorf("httpGet: %s, want %s", got, want)
	}

	AssignPrefix("b", "")
	HandleFunc("b", "/path", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("5"))
	})
	if got, want := httpGet(t, fmt.Sprintf("http://localhost:%d/b/path", port)), "5"; got != want {
		t.Errorf("httpGet: %s, want %s", got, want)
	}
	if got, want := httpGet(t, fmt.Sprintf("http://localhost:%d/b/debug/status", port)), "Status for"; !strings.Contains(got, want) {
		t.Errorf("httpGet: %s, must contain %s", got, want)
	}
	// Ensure "a" is still the same.
	if got, want := httpGet(t, fmt.Sprintf("http://localhost:%d/a/path", port)), "4"; got != want {
		t.Errorf("httpGet: %s, want %s", got, want)
	}
	if got, want := httpGet(t, fmt.Sprintf("http://localhost:%d/a/debug/status", port)), "Status for"; !strings.Contains(got, want) {
		t.Errorf("httpGet: %s, must contain %s", got, want)
	}
}

func httpGet(t *testing.T, url string) string {
	t.Helper()

	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestCountersFuncWithMultiLabels(t *testing.T) {
	NewCountersFuncWithMultiLabels("", "gcfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 1} })
	if got, want := expvar.Get("gcfwml").String(), `{"a": 1}`; got != want {
		t.Errorf("CountersFuncWithMultiLabels get: %s, want %s", got, want)
	}

	AssignPrefix("i1", "label")

	// Ensure anonymous vars don't cause panics.
	NewCountersFuncWithMultiLabels("i1", "", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 2} })
	NewCountersFuncWithMultiLabels("i1", "", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 3} })

	NewCountersFuncWithMultiLabels("i1", "lcfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 4} })
	if got, want := expvar.Get("labellcfwml").String(), `{"i1.a": 4}`; got != want {
		t.Errorf("CountersFuncWithMultiLabels get: %s, want %s", got, want)
	}

	// Ensure var gets replaced.
	NewCountersFuncWithMultiLabels("i1", "lcfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 5} })
	if got, want := expvar.Get("labellcfwml").String(), `{"i1.a": 5}`; got != want {
		t.Errorf("CountersFuncWithMultiLabels get: %s, want %s", got, want)
	}

	AssignPrefix("i1", "label")
	// Ensure gauge gets reset on re-instantiation.
	if got, want := expvar.Get("labellcfwml").String(), "{}"; got != want {
		t.Errorf("CountersFuncWithMultiLabels get: %s, want %s", got, want)
	}
	// Ensure new value is returned after var gets added.
	NewCountersFuncWithMultiLabels("i1", "lcfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 6} })
	if got, want := expvar.Get("labellcfwml").String(), `{"i1.a": 6}`; got != want {
		t.Errorf("CountersFuncWithMultiLabels get: %s, want %s", got, want)
	}

	AssignPrefix("i2", "label")
	NewCountersFuncWithMultiLabels("i2", "lcfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 7} })
	want1 := `{"i1.a": 6, "i2.a": 7}`
	want2 := `{"i2.a": 7, "i1.a": 6}`
	if got := expvar.Get("labellcfwml").String(); got != want1 && got != want2 {
		t.Errorf("GaugeDuration get: %s, want %s or %s", got, want1, want2)
	}
}

func TestGaugesFuncWithMultiLabels(t *testing.T) {
	NewGaugesFuncWithMultiLabels("", "ggfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 1} })
	if got, want := expvar.Get("ggfwml").String(), `{"a": 1}`; got != want {
		t.Errorf("GaugesFuncWithMultiLabels get: %s, want %s", got, want)
	}

	AssignPrefix("i1", "label")

	// Ensure anonymous vars don't cause panics.
	NewGaugesFuncWithMultiLabels("i1", "", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 2} })
	NewGaugesFuncWithMultiLabels("i1", "", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 3} })

	NewGaugesFuncWithMultiLabels("i1", "lgfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 4} })
	if got, want := expvar.Get("labellgfwml").String(), `{"i1.a": 4}`; got != want {
		t.Errorf("GaugesFuncWithMultiLabels get: %s, want %s", got, want)
	}

	// Ensure var gets replaced.
	NewGaugesFuncWithMultiLabels("i1", "lgfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 5} })
	if got, want := expvar.Get("labellgfwml").String(), `{"i1.a": 5}`; got != want {
		t.Errorf("GaugesFuncWithMultiLabels get: %s, want %s", got, want)
	}

	AssignPrefix("i1", "label")
	// Ensure gauge gets reset on re-instantiation.
	if got, want := expvar.Get("labellgfwml").String(), "{}"; got != want {
		t.Errorf("GaugesFuncWithMultiLabels get: %s, want %s", got, want)
	}
	// Ensure new value is returned after var gets added.
	NewGaugesFuncWithMultiLabels("i1", "lgfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 6} })
	if got, want := expvar.Get("labellgfwml").String(), `{"i1.a": 6}`; got != want {
		t.Errorf("GaugesFuncWithMultiLabels get: %s, want %s", got, want)
	}

	AssignPrefix("i2", "label")
	NewGaugesFuncWithMultiLabels("i2", "lgfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 7} })
	want1 := `{"i1.a": 6, "i2.a": 7}`
	want2 := `{"i2.a": 7, "i1.a": 6}`
	if got := expvar.Get("labellgfwml").String(); got != want1 && got != want2 {
		t.Errorf("GaugeDuration get: %s, want %s or %s", got, want1, want2)
	}
}

func TestCounter(t *testing.T) {
	c := NewCounter("", "gcounter", "")
	c.Add(1)
	if got, want := expvar.Get("gcounter").String(), "1"; got != want {
		t.Errorf("Counter get: %s, want %s", got, want)
	}

	AssignPrefix("i1", "label")

	// Ensure anonymous vars don't cause panics.
	NewCounter("i1", "", "")
	NewCounter("i1", "", "")

	c = NewCounter("i1", "lcounter", "")
	c.Add(4)
	if got, want := expvar.Get("labellcounter").String(), `{"i1": 4}`; got != want {
		t.Errorf("Counter get: %s, want %s", got, want)
	}

	// Ensure var gets replaced.
	c = NewCounter("i1", "lcounter", "")
	c.Add(5)
	if got, want := expvar.Get("labellcounter").String(), `{"i1": 5}`; got != want {
		t.Errorf("Counter get: %s, want %s", got, want)
	}

	AssignPrefix("i1", "label")
	// Ensure gauge gets reset on re-instantiation.
	if got, want := expvar.Get("labellcounter").String(), "{}"; got != want {
		t.Errorf("Counter get: %s, want %s", got, want)
	}
	// Ensure new value is returned after var gets added.
	c = NewCounter("i1", "lcounter", "")
	c.Add(5)
	if got, want := expvar.Get("labellcounter").String(), `{"i1": 5}`; got != want {
		t.Errorf("Counter get: %s, want %s", got, want)
	}

	AssignPrefix("i2", "label")
	c = NewCounter("i2", "lcounter", "")
	c.Add(6)
	want1 := `{"i1": 5, "i2": 6}`
	want2 := `{"i2": 6, "i1": 5}`
	if got := expvar.Get("labellcounter").String(); got != want1 && got != want2 {
		t.Errorf("Counter get: %s, want %s or %s", got, want1, want2)
	}
}

func TestGauge(t *testing.T) {
	c := NewGauge("", "ggauge", "")
	c.Set(1)
	if got, want := expvar.Get("ggauge").String(), "1"; got != want {
		t.Errorf("Gauge get: %s, want %s", got, want)
	}

	AssignPrefix("i1", "label")

	// Ensure anonymous vars don't cause panics.
	NewGauge("i1", "", "")
	NewGauge("i1", "", "")

	c = NewGauge("i1", "lgauge", "")
	c.Set(4)
	if got, want := expvar.Get("labellgauge").String(), `{"i1": 4}`; got != want {
		t.Errorf("Gauge get: %s, want %s", got, want)
	}

	// Ensure var gets replaced.
	c = NewGauge("i1", "lgauge", "")
	c.Set(5)
	if got, want := expvar.Get("labellgauge").String(), `{"i1": 5}`; got != want {
		t.Errorf("Gauge get: %s, want %s", got, want)
	}

	AssignPrefix("i1", "label")
	// Ensure gauge gets reset on re-instantiation.
	if got, want := expvar.Get("labellgauge").String(), "{}"; got != want {
		t.Errorf("Gauge get: %s, want %s", got, want)
	}
	// Ensure new value is returned after var gets added.
	c = NewGauge("i1", "lgauge", "")
	c.Set(5)
	if got, want := expvar.Get("labellgauge").String(), `{"i1": 5}`; got != want {
		t.Errorf("Gauge get: %s, want %s", got, want)
	}

	AssignPrefix("i2", "label")
	c = NewGauge("i2", "lgauge", "")
	c.Set(6)
	want1 := `{"i1": 5, "i2": 6}`
	want2 := `{"i2": 6, "i1": 5}`
	if got := expvar.Get("labellgauge").String(); got != want1 && got != want2 {
		t.Errorf("Gauge get: %s, want %s or %s", got, want1, want2)
	}
}

func TestCounterFunc(t *testing.T) {
	NewCounterFunc("", "gcf", "", func() int64 { return 1 })
	if got, want := expvar.Get("gcf").String(), "1"; got != want {
		t.Errorf("Counter get: %s, want %s", got, want)
	}

	AssignPrefix("i1", "label")

	// Ensure anonymous vars don't cause panics.
	NewCounterFunc("i1", "", "", func() int64 { return 2 })
	NewCounterFunc("i1", "", "", func() int64 { return 3 })

	NewCounterFunc("i1", "lcf", "", func() int64 { return 4 })
	if got, want := expvar.Get("labellcf").String(), `{"i1": 4}`; got != want {
		t.Errorf("Counter get: %s, want %s", got, want)
	}

	// Ensure var gets replaced.
	NewCounterFunc("i1", "lcf", "", func() int64 { return 5 })
	if got, want := expvar.Get("labellcf").String(), `{"i1": 5}`; got != want {
		t.Errorf("Counter get: %s, want %s", got, want)
	}

	AssignPrefix("i1", "label")
	// Ensure gauge gets reset on re-instantiation.
	if got, want := expvar.Get("labellcf").String(), "{}"; got != want {
		t.Errorf("Counter get: %s, want %s", got, want)
	}
	// Ensure new value is returned after var gets added.
	NewCounterFunc("i1", "lcf", "", func() int64 { return 5 })
	if got, want := expvar.Get("labellcf").String(), `{"i1": 5}`; got != want {
		t.Errorf("Counter get: %s, want %s", got, want)
	}

	AssignPrefix("i2", "label")
	NewCounterFunc("i2", "lcf", "", func() int64 { return 6 })
	want1 := `{"i1": 5, "i2": 6}`
	want2 := `{"i2": 6, "i1": 5}`
	if got := expvar.Get("labellcf").String(); got != want1 && got != want2 {
		t.Errorf("Counter get: %s, want %s or %s", got, want1, want2)
	}
}

func TestGaugeFunc(t *testing.T) {
	NewGaugeFunc("", "ggf", "", func() int64 { return 1 })
	if got, want := expvar.Get("ggf").String(), "1"; got != want {
		t.Errorf("Gauge get: %s, want %s", got, want)
	}

	AssignPrefix("i1", "label")

	// Ensure anonymous vars don't cause panics.
	NewGaugeFunc("i1", "", "", func() int64 { return 2 })
	NewGaugeFunc("i1", "", "", func() int64 { return 3 })

	NewGaugeFunc("i1", "lgf", "", func() int64 { return 4 })
	if got, want := expvar.Get("labellgf").String(), `{"i1": 4}`; got != want {
		t.Errorf("Gauge get: %s, want %s", got, want)
	}

	// Ensure var gets replaced.
	NewGaugeFunc("i1", "lgf", "", func() int64 { return 5 })
	if got, want := expvar.Get("labellgf").String(), `{"i1": 5}`; got != want {
		t.Errorf("Gauge get: %s, want %s", got, want)
	}

	AssignPrefix("i1", "label")
	// Ensure gauge gets reset on re-instantiation.
	if got, want := expvar.Get("labellgf").String(), "{}"; got != want {
		t.Errorf("Gauge get: %s, want %s", got, want)
	}
	// Ensure new value is returned after var gets added.
	NewGaugeFunc("i1", "lgf", "", func() int64 { return 5 })
	if got, want := expvar.Get("labellgf").String(), `{"i1": 5}`; got != want {
		t.Errorf("Gauge get: %s, want %s", got, want)
	}

	AssignPrefix("i2", "label")
	NewGaugeFunc("i2", "lgf", "", func() int64 { return 6 })
	want1 := `{"i1": 5, "i2": 6}`
	want2 := `{"i2": 6, "i1": 5}`
	if got := expvar.Get("labellgf").String(); got != want1 && got != want2 {
		t.Errorf("Gauge get: %s, want %s or %s", got, want1, want2)
	}
}

func TestCounterDurationFunc(t *testing.T) {
	NewCounterDurationFunc("", "gcduration", "", func() time.Duration { return 1 })
	if got, want := expvar.Get("gcduration").String(), "1"; got != want {
		t.Errorf("CounterDuration get: %s, want %s", got, want)
	}

	AssignPrefix("i1", "label")

	// Ensure anonymous vars don't cause panics.
	NewCounterDurationFunc("i1", "", "", func() time.Duration { return 2 })
	NewCounterDurationFunc("i1", "", "", func() time.Duration { return 3 })

	NewCounterDurationFunc("i1", "lcduration", "", func() time.Duration { return 4 })
	if got, want := expvar.Get("labellcduration").String(), `{"i1": 4}`; got != want {
		t.Errorf("CounterDuration get: %s, want %s", got, want)
	}

	// Ensure var gets replaced.
	NewCounterDurationFunc("i1", "lcduration", "", func() time.Duration { return 5 })
	if got, want := expvar.Get("labellcduration").String(), `{"i1": 5}`; got != want {
		t.Errorf("CounterDuration get: %s, want %s", got, want)
	}

	AssignPrefix("i1", "label")
	// Ensure gauge gets reset on re-instantiation.
	if got, want := expvar.Get("labellcduration").String(), "{}"; got != want {
		t.Errorf("CounterDuration get: %s, want %s", got, want)
	}
	// Ensure new value is returned after var gets added.
	NewCounterDurationFunc("i1", "lcduration", "", func() time.Duration { return 5 })
	if got, want := expvar.Get("labellcduration").String(), `{"i1": 5}`; got != want {
		t.Errorf("CounterDuration get: %s, want %s", got, want)
	}

	AssignPrefix("i2", "label")
	NewCounterDurationFunc("i2", "lcduration", "", func() time.Duration { return 6 })
	want1 := `{"i1": 5, "i2": 6}`
	want2 := `{"i2": 6, "i1": 5}`
	if got := expvar.Get("labellcduration").String(); got != want1 && got != want2 {
		t.Errorf("CounterDuration get: %s, want %s or %s", got, want1, want2)
	}
}

func TestGaugeDurationFunc(t *testing.T) {
	NewGaugeDurationFunc("", "ggduration", "", func() time.Duration { return 1 })
	if got, want := expvar.Get("ggduration").String(), "1"; got != want {
		t.Errorf("GaugeDuration get: %s, want %s", got, want)
	}

	AssignPrefix("i1", "label")

	// Ensure anonymous vars don't cause panics.
	NewGaugeDurationFunc("i1", "", "", func() time.Duration { return 2 })
	NewGaugeDurationFunc("i1", "", "", func() time.Duration { return 3 })

	NewGaugeDurationFunc("i1", "lgduration", "", func() time.Duration { return 4 })
	if got, want := expvar.Get("labellgduration").String(), `{"i1": 4}`; got != want {
		t.Errorf("GaugeDuration get: %s, want %s", got, want)
	}

	// Ensure var gets replaced.
	NewGaugeDurationFunc("i1", "lgduration", "", func() time.Duration { return 5 })
	if got, want := expvar.Get("labellgduration").String(), `{"i1": 5}`; got != want {
		t.Errorf("GaugeDuration get: %s, want %s", got, want)
	}

	AssignPrefix("i1", "label")
	// Ensure gauge gets reset on re-instantiation.
	if got, want := expvar.Get("labellgduration").String(), "{}"; got != want {
		t.Errorf("GaugeDuration get: %s, want %s", got, want)
	}
	// Ensure new value is returned after var gets added.
	NewGaugeDurationFunc("i1", "lgduration", "", func() time.Duration { return 6 })
	if got, want := expvar.Get("labellgduration").String(), `{"i1": 6}`; got != want {
		t.Errorf("GaugeDuration get: %s, want %s", got, want)
	}

	AssignPrefix("i2", "label")
	NewGaugeDurationFunc("i2", "lgduration", "", func() time.Duration { return 7 })
	want1 := `{"i1": 6, "i2": 7}`
	want2 := `{"i2": 7, "i1": 6}`
	if got := expvar.Get("labellgduration").String(); got != want1 && got != want2 {
		t.Errorf("GaugeDuration get: %s, want %s or %s", got, want1, want2)
	}
}

func TestCountersWithSingleLabel(t *testing.T) {
	g := NewCountersWithSingleLabel("", "gcwsl", "", "l")
	g.Add("a", 1)
	if got, want := expvar.Get("gcwsl").String(), `{"a": 1}`; got != want {
		t.Errorf("CountersWithSingleLabel get: %s, want %s", got, want)
	}

	AssignPrefix("i1", "label")

	// Ensure anonymous vars don't cause panics.
	NewCountersWithSingleLabel("i1", "", "", "l")
	NewCountersWithSingleLabel("i1", "", "", "l")

	g = NewCountersWithSingleLabel("i1", "lcwsl", "", "l")
	g.Add("a", 4)
	if got, want := expvar.Get("labellcwsl").String(), `{"i1.a": 4}`; got != want {
		t.Errorf("CountersWithSingleLabel get: %s, want %s", got, want)
	}

	// Ensure var gets replaced.
	g = NewCountersWithSingleLabel("i1", "lcwsl", "", "l")
	g.Add("a", 5)
	if got, want := expvar.Get("labellcwsl").String(), `{"i1.a": 5}`; got != want {
		t.Errorf("CountersWithSingleLabel get: %s, want %s", got, want)
	}

	AssignPrefix("i1", "label")
	// Ensure gauge gets reset on re-instantiation.
	if got, want := expvar.Get("labellcwsl").String(), "{}"; got != want {
		t.Errorf("CountersWithSingleLabel get: %s, want %s", got, want)
	}
	// Ensure new value is returned after var gets added.
	g = NewCountersWithSingleLabel("i1", "lcwsl", "", "l")
	g.Add("a", 6)
	if got, want := expvar.Get("labellcwsl").String(), `{"i1.a": 6}`; got != want {
		t.Errorf("CountersWithSingleLabel get: %s, want %s", got, want)
	}

	AssignPrefix("i2", "label")
	g = NewCountersWithSingleLabel("i2", "lcwsl", "", "l")
	g.Add("a", 7)
	want1 := `{"i1.a": 6, "i2.a": 7}`
	want2 := `{"i2.a": 7, "i1.a": 6}`
	if got := expvar.Get("labellcwsl").String(); got != want1 && got != want2 {
		t.Errorf("CountersWithSingleLabel get: %s, want %s or %s", got, want1, want2)
	}
}

func TestGaugesWithSingleLabel(t *testing.T) {
	g := NewGaugesWithSingleLabel("", "ggwsl", "", "l")
	g.Set("a", 1)
	if got, want := expvar.Get("ggwsl").String(), `{"a": 1}`; got != want {
		t.Errorf("GaugesWithSingleLabel get: %s, want %s", got, want)
	}

	AssignPrefix("i1", "label")

	// Ensure anonymous vars don't cause panics.
	NewGaugesWithSingleLabel("i1", "", "", "l")
	NewGaugesWithSingleLabel("i1", "", "", "l")

	g = NewGaugesWithSingleLabel("i1", "lgwsl", "", "l")
	g.Set("a", 4)
	if got, want := expvar.Get("labellgwsl").String(), `{"i1.a": 4}`; got != want {
		t.Errorf("GaugesWithSingleLabel get: %s, want %s", got, want)
	}

	// Ensure var gets replaced.
	g = NewGaugesWithSingleLabel("i1", "lgwsl", "", "l")
	g.Set("a", 5)
	if got, want := expvar.Get("labellgwsl").String(), `{"i1.a": 5}`; got != want {
		t.Errorf("GaugesWithSingleLabel get: %s, want %s", got, want)
	}

	AssignPrefix("i1", "label")
	// Ensure gauge gets reset on re-instantiation.
	if got, want := expvar.Get("labellgwsl").String(), "{}"; got != want {
		t.Errorf("GaugesWithSingleLabel get: %s, want %s", got, want)
	}
	// Ensure new value is returned after var gets added.
	g = NewGaugesWithSingleLabel("i1", "lgwsl", "", "l")
	g.Set("a", 6)
	if got, want := expvar.Get("labellgwsl").String(), `{"i1.a": 6}`; got != want {
		t.Errorf("GaugesWithSingleLabel get: %s, want %s", got, want)
	}

	AssignPrefix("i2", "label")
	g = NewGaugesWithSingleLabel("i2", "lgwsl", "", "l")
	g.Set("a", 7)
	want1 := `{"i1.a": 6, "i2.a": 7}`
	want2 := `{"i2.a": 7, "i1.a": 6}`
	if got := expvar.Get("labellgwsl").String(); got != want1 && got != want2 {
		t.Errorf("GaugesWithSingleLabel get: %s, want %s or %s", got, want1, want2)
	}
}

func TestCountersWithMultiLabels(t *testing.T) {
	g := NewCountersWithMultiLabels("", "gcwml", "", []string{"l"})
	g.Add([]string{"a"}, 1)
	if got, want := expvar.Get("gcwml").String(), `{"a": 1}`; got != want {
		t.Errorf("CountersWithMultiLabels get: %s, want %s", got, want)
	}

	AssignPrefix("i1", "label")

	// Ensure anonymous vars don't cause panics.
	NewCountersWithMultiLabels("i1", "", "", []string{"l"})
	NewCountersWithMultiLabels("i1", "", "", []string{"l"})

	g = NewCountersWithMultiLabels("i1", "lcwml", "", []string{"l"})
	g.Add([]string{"a"}, 4)
	if got, want := expvar.Get("labellcwml").String(), `{"i1.a": 4}`; got != want {
		t.Errorf("CountersWithMultiLabels get: %s, want %s", got, want)
	}

	// Ensure var gets replaced.
	g = NewCountersWithMultiLabels("i1", "lcwml", "", []string{"l"})
	g.Add([]string{"a"}, 5)
	if got, want := expvar.Get("labellcwml").String(), `{"i1.a": 5}`; got != want {
		t.Errorf("CountersWithMultiLabels get: %s, want %s", got, want)
	}

	AssignPrefix("i1", "label")
	// Ensure gauge gets reset on re-instantiation.
	if got, want := expvar.Get("labellcwml").String(), "{}"; got != want {
		t.Errorf("CountersWithMultiLabels get: %s, want %s", got, want)
	}
	// Ensure new value is returned after var gets added.
	g = NewCountersWithMultiLabels("i1", "lcwml", "", []string{"l"})
	g.Add([]string{"a"}, 6)
	if got, want := expvar.Get("labellcwml").String(), `{"i1.a": 6}`; got != want {
		t.Errorf("CountersWithMultiLabels get: %s, want %s", got, want)
	}

	AssignPrefix("i2", "label")
	g = NewCountersWithMultiLabels("i2", "lcwml", "", []string{"l"})
	g.Add([]string{"a"}, 7)
	want1 := `{"i1.a": 6, "i2.a": 7}`
	want2 := `{"i2.a": 7, "i1.a": 6}`
	if got := expvar.Get("labellcwml").String(); got != want1 && got != want2 {
		t.Errorf("CountersWithMultiLabels get: %s, want %s or %s", got, want1, want2)
	}
}

func TestGaugesWithMultiLabels(t *testing.T) {
	g := NewGaugesWithMultiLabels("", "ggwml", "", []string{"l"})
	g.Set([]string{"a"}, 1)
	if got, want := expvar.Get("ggwml").String(), `{"a": 1}`; got != want {
		t.Errorf("GaugesWithMultiLabels get: %s, want %s", got, want)
	}

	AssignPrefix("i1", "label")

	// Ensure anonymous vars don't cause panics.
	NewGaugesWithMultiLabels("i1", "", "", []string{"l"})
	NewGaugesWithMultiLabels("i1", "", "", []string{"l"})

	g = NewGaugesWithMultiLabels("i1", "lgwml", "", []string{"l"})
	g.Set([]string{"a"}, 4)
	if got, want := expvar.Get("labellgwml").String(), `{"i1.a": 4}`; got != want {
		t.Errorf("GaugesWithMultiLabels get: %s, want %s", got, want)
	}

	// Ensure var gets replaced.
	g = NewGaugesWithMultiLabels("i1", "lgwml", "", []string{"l"})
	g.Set([]string{"a"}, 5)
	if got, want := expvar.Get("labellgwml").String(), `{"i1.a": 5}`; got != want {
		t.Errorf("GaugesWithMultiLabels get: %s, want %s", got, want)
	}

	AssignPrefix("i1", "label")
	// Ensure gauge gets reset on re-instantiation.
	if got, want := expvar.Get("labellgwml").String(), "{}"; got != want {
		t.Errorf("GaugesWithMultiLabels get: %s, want %s", got, want)
	}
	// Ensure new value is returned after var gets added.
	g = NewGaugesWithMultiLabels("i1", "lgwml", "", []string{"l"})
	g.Set([]string{"a"}, 6)
	if got, want := expvar.Get("labellgwml").String(), `{"i1.a": 6}`; got != want {
		t.Errorf("GaugesWithMultiLabels get: %s, want %s", got, want)
	}

	AssignPrefix("i2", "label")
	g = NewGaugesWithMultiLabels("i2", "lgwml", "", []string{"l"})
	g.Set([]string{"a"}, 7)
	want1 := `{"i1.a": 6, "i2.a": 7}`
	want2 := `{"i2.a": 7, "i1.a": 6}`
	if got := expvar.Get("labellgwml").String(); got != want1 && got != want2 {
		t.Errorf("GaugeDuration get: %s, want %s or %s", got, want1, want2)
	}
}

func TestTimings(t *testing.T) {
	g := NewTimings("", "gtimings", "", "l")
	g.Add("a", 1)
	if got, want := expvar.Get("gtimings").String(), "TotalCount"; !strings.Contains(got, want) {
		t.Errorf("CountersFuncWithLabels get: %s, must contain %s", got, want)
	}

	AssignPrefix("i1", "label")

	// Ensure anonymous vars don't cause panics.
	NewTimings("i1", "", "", "l")
	NewTimings("i1", "", "", "l")

	// Ensure non-anonymous vars also don't cause panics
	NewTimings("i1", "ltimings", "", "l")
	NewTimings("i1", "ltimings", "", "l")
}

func TestMultiTimings(t *testing.T) {
	g := NewMultiTimings("", "gmtimings", "", []string{"l"})
	g.Add([]string{"a"}, 1)
	if got, want := expvar.Get("gmtimings").String(), "TotalCount"; !strings.Contains(got, want) {
		t.Errorf("CountersFuncWithMultiLabels get: %s, must contain %s", got, want)
	}

	AssignPrefix("i1", "label")

	// Ensure anonymous vars don't cause panics.
	NewMultiTimings("i1", "", "", []string{"l"})
	NewMultiTimings("i1", "", "", []string{"l"})

	// Ensure non-anonymous vars also don't cause panics
	NewMultiTimings("i1", "lmtimings", "", []string{"l"})
	NewMultiTimings("i1", "lmtimings", "", []string{"l"})
}

func TestRates(t *testing.T) {
	tm := NewMultiTimings("", "gratetimings", "", []string{"l"})
	NewRates("", "grates", tm, 15*60/5, 5*time.Second)
	if got, want := expvar.Get("grates").String(), "{}"; got != want {
		t.Errorf("CountersFuncWithMultiLabels get: %s, want %s", got, want)
	}

	AssignPrefix("i1", "label")

	// Ensure anonymous vars don't cause panics.
	NewRates("i1", "", tm, 15*60/5, 5*time.Second)
	NewRates("i1", "", tm, 15*60/5, 5*time.Second)

	// Ensure non-anonymous vars also don't cause panics
	NewRates("i1", "lrates", tm, 15*60/5, 5*time.Second)
	NewRates("i1", "lrates", tm, 15*60/5, 5*time.Second)
}

func TestHistogram(t *testing.T) {
	g := NewHistogram("", "ghebdogram", "", []int64{10})
	g.Add(1)
	if got, want := expvar.Get("ghebdogram").String(), `{"10": 1, "inf": 1, "Count": 1, "Total": 1}`; !strings.Contains(got, want) {
		t.Errorf("CountersFuncWithMultiLabels get: %s, must contain %s", got, want)
	}

	AssignPrefix("i1", "label")

	// Ensure anonymous vars don't cause panics.
	NewHistogram("i1", "", "", []int64{10})
	NewHistogram("i1", "", "", []int64{10})

	// Ensure non-anonymous vars also don't cause panics
	NewHistogram("i1", "lhebdogram", "", []int64{10})
	NewHistogram("i1", "lhebdogram", "", []int64{10})
}

func TestPublish(t *testing.T) {
	s := stats.NewString("")
	Publish("", "gpub", s)
	s.Set("1")
	if got, want := expvar.Get("gpub").String(), `"1"`; got != want {
		t.Errorf("Publish get: %s, want %s", got, want)
	}

	// This should not crash.
	AssignPrefix("i1", "label")
	Publish("i1", "lpub", s)
	Publish("i1", "lpub", s)
}
