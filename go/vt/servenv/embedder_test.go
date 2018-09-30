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
	if got, want := NewEmbedder("", "").URLPrefix(), ""; got != want {
		t.Errorf("URLPrefix(''): %v, want %v", got, want)
	}
	if got, want := NewEmbedder("a", "").URLPrefix(), "/a"; got != want {
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

	ebd := NewEmbedder("", "")
	ebd.HandleFunc("/path", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("1"))
	})
	if got, want := httpGet(t, fmt.Sprintf("http://localhost:%d/path", port)), "1"; got != want {
		t.Errorf("httpGet: %s, want %s", got, want)
	}
	if got, want := httpGet(t, fmt.Sprintf("http://localhost:%d/debug/status", port)), "Status for"; !strings.Contains(got, want) {
		t.Errorf("httpGet: %s, must contain %s", got, want)
	}

	ebd = NewEmbedder("a", "")
	ebd.HandleFunc("/path", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("2"))
	})
	if got, want := httpGet(t, fmt.Sprintf("http://localhost:%d/a/path", port)), "2"; got != want {
		t.Errorf("httpGet: %s, want %s", got, want)
	}
	if got, want := httpGet(t, fmt.Sprintf("http://localhost:%d/a/debug/status", port)), "Status for"; !strings.Contains(got, want) {
		t.Errorf("httpGet: %s, must contain %s", got, want)
	}

	ebd.HandleFunc("/path", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("3"))
	})
	if got, want := httpGet(t, fmt.Sprintf("http://localhost:%d/a/path", port)), "3"; got != want {
		t.Errorf("httpGet: %s, want %s", got, want)
	}
	if got, want := httpGet(t, fmt.Sprintf("http://localhost:%d/a/debug/status", port)), "Status for"; !strings.Contains(got, want) {
		t.Errorf("httpGet: %s, must contain %s", got, want)
	}

	ebd = NewEmbedder("a", "")
	ebd.HandleFunc("/path", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("4"))
	})
	if got, want := httpGet(t, fmt.Sprintf("http://localhost:%d/a/path", port)), "4"; got != want {
		t.Errorf("httpGet: %s, want %s", got, want)
	}

	ebd = NewEmbedder("b", "")
	ebd.HandleFunc("/path", func(w http.ResponseWriter, r *http.Request) {
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
	ebd := NewEmbedder("", "")
	ebd.NewCountersFuncWithMultiLabels("gcfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 1} })
	if got, want := expvar.Get("gcfwml").String(), `{"a": 1}`; got != want {
		t.Errorf("CountersFuncWithMultiLabels get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewCountersFuncWithMultiLabels("", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 2} })
	ebd.NewCountersFuncWithMultiLabels("", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 3} })

	ebd.NewCountersFuncWithMultiLabels("lcfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 4} })
	if got, want := expvar.Get("labellcfwml").String(), `{"i1.a": 4}`; got != want {
		t.Errorf("CountersFuncWithMultiLabels get: %s, want %s", got, want)
	}

	// Ensure var gets replaced.
	ebd.NewCountersFuncWithMultiLabels("lcfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 5} })
	if got, want := expvar.Get("labellcfwml").String(), `{"i1.a": 5}`; got != want {
		t.Errorf("CountersFuncWithMultiLabels get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i1", "label")
	// Ensure gauge gets reset on re-instantiation.
	if got, want := expvar.Get("labellcfwml").String(), "{}"; got != want {
		t.Errorf("CountersFuncWithMultiLabels get: %s, want %s", got, want)
	}
	// Ensure new value is returned after var gets added.
	ebd.NewCountersFuncWithMultiLabels("lcfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 6} })
	if got, want := expvar.Get("labellcfwml").String(), `{"i1.a": 6}`; got != want {
		t.Errorf("CountersFuncWithMultiLabels get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i2", "label")
	ebd.NewCountersFuncWithMultiLabels("lcfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 7} })
	want1 := `{"i1.a": 6, "i2.a": 7}`
	want2 := `{"i2.a": 7, "i1.a": 6}`
	if got := expvar.Get("labellcfwml").String(); got != want1 && got != want2 {
		t.Errorf("GaugeDuration get: %s, want %s or %s", got, want1, want2)
	}
}

func TestGaugesFuncWithMultiLabels(t *testing.T) {
	ebd := NewEmbedder("", "")
	ebd.NewGaugesFuncWithMultiLabels("ggfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 1} })
	if got, want := expvar.Get("ggfwml").String(), `{"a": 1}`; got != want {
		t.Errorf("GaugesFuncWithMultiLabels get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewGaugesFuncWithMultiLabels("", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 2} })
	ebd.NewGaugesFuncWithMultiLabels("", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 3} })

	ebd.NewGaugesFuncWithMultiLabels("lgfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 4} })
	if got, want := expvar.Get("labellgfwml").String(), `{"i1.a": 4}`; got != want {
		t.Errorf("GaugesFuncWithMultiLabels get: %s, want %s", got, want)
	}

	// Ensure var gets replaced.
	ebd.NewGaugesFuncWithMultiLabels("lgfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 5} })
	if got, want := expvar.Get("labellgfwml").String(), `{"i1.a": 5}`; got != want {
		t.Errorf("GaugesFuncWithMultiLabels get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i1", "label")
	// Ensure gauge gets reset on re-instantiation.
	if got, want := expvar.Get("labellgfwml").String(), "{}"; got != want {
		t.Errorf("GaugesFuncWithMultiLabels get: %s, want %s", got, want)
	}
	// Ensure new value is returned after var gets added.
	ebd.NewGaugesFuncWithMultiLabels("lgfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 6} })
	if got, want := expvar.Get("labellgfwml").String(), `{"i1.a": 6}`; got != want {
		t.Errorf("GaugesFuncWithMultiLabels get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i2", "label")
	ebd.NewGaugesFuncWithMultiLabels("lgfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 7} })
	want1 := `{"i1.a": 6, "i2.a": 7}`
	want2 := `{"i2.a": 7, "i1.a": 6}`
	if got := expvar.Get("labellgfwml").String(); got != want1 && got != want2 {
		t.Errorf("GaugeDuration get: %s, want %s or %s", got, want1, want2)
	}
}

func TestCounter(t *testing.T) {
	ebd := NewEmbedder("", "")
	c := ebd.NewCounter("gcounter", "")
	c.Add(1)
	if got, want := expvar.Get("gcounter").String(), "1"; got != want {
		t.Errorf("Counter get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewCounter("", "")
	ebd.NewCounter("", "")

	c = ebd.NewCounter("lcounter", "")
	c.Add(4)
	if got, want := expvar.Get("labellcounter").String(), `{"i1": 4}`; got != want {
		t.Errorf("Counter get: %s, want %s", got, want)
	}

	// Ensure var gets replaced.
	c = ebd.NewCounter("lcounter", "")
	c.Add(5)
	if got, want := expvar.Get("labellcounter").String(), `{"i1": 5}`; got != want {
		t.Errorf("Counter get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i1", "label")
	// Ensure gauge gets reset on re-instantiation.
	if got, want := expvar.Get("labellcounter").String(), "{}"; got != want {
		t.Errorf("Counter get: %s, want %s", got, want)
	}
	// Ensure new value is returned after var gets added.
	c = ebd.NewCounter("lcounter", "")
	c.Add(5)
	if got, want := expvar.Get("labellcounter").String(), `{"i1": 5}`; got != want {
		t.Errorf("Counter get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i2", "label")
	c = ebd.NewCounter("lcounter", "")
	c.Add(6)
	want1 := `{"i1": 5, "i2": 6}`
	want2 := `{"i2": 6, "i1": 5}`
	if got := expvar.Get("labellcounter").String(); got != want1 && got != want2 {
		t.Errorf("Counter get: %s, want %s or %s", got, want1, want2)
	}
}

func TestGauge(t *testing.T) {
	ebd := NewEmbedder("", "")
	c := ebd.NewGauge("ggauge", "")
	c.Set(1)
	if got, want := expvar.Get("ggauge").String(), "1"; got != want {
		t.Errorf("Gauge get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewGauge("", "")
	ebd.NewGauge("", "")

	c = ebd.NewGauge("lgauge", "")
	c.Set(4)
	if got, want := expvar.Get("labellgauge").String(), `{"i1": 4}`; got != want {
		t.Errorf("Gauge get: %s, want %s", got, want)
	}

	// Ensure var gets replaced.
	c = ebd.NewGauge("lgauge", "")
	c.Set(5)
	if got, want := expvar.Get("labellgauge").String(), `{"i1": 5}`; got != want {
		t.Errorf("Gauge get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i1", "label")
	// Ensure gauge gets reset on re-instantiation.
	if got, want := expvar.Get("labellgauge").String(), "{}"; got != want {
		t.Errorf("Gauge get: %s, want %s", got, want)
	}
	// Ensure new value is returned after var gets added.
	c = ebd.NewGauge("lgauge", "")
	c.Set(5)
	if got, want := expvar.Get("labellgauge").String(), `{"i1": 5}`; got != want {
		t.Errorf("Gauge get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i2", "label")
	c = ebd.NewGauge("lgauge", "")
	c.Set(6)
	want1 := `{"i1": 5, "i2": 6}`
	want2 := `{"i2": 6, "i1": 5}`
	if got := expvar.Get("labellgauge").String(); got != want1 && got != want2 {
		t.Errorf("Gauge get: %s, want %s or %s", got, want1, want2)
	}
}

func TestCounterFunc(t *testing.T) {
	ebd := NewEmbedder("", "")
	ebd.NewCounterFunc("gcf", "", func() int64 { return 1 })
	if got, want := expvar.Get("gcf").String(), "1"; got != want {
		t.Errorf("Counter get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewCounterFunc("", "", func() int64 { return 2 })
	ebd.NewCounterFunc("", "", func() int64 { return 3 })

	ebd.NewCounterFunc("lcf", "", func() int64 { return 4 })
	if got, want := expvar.Get("labellcf").String(), `{"i1": 4}`; got != want {
		t.Errorf("Counter get: %s, want %s", got, want)
	}

	// Ensure var gets replaced.
	ebd.NewCounterFunc("lcf", "", func() int64 { return 5 })
	if got, want := expvar.Get("labellcf").String(), `{"i1": 5}`; got != want {
		t.Errorf("Counter get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i1", "label")
	// Ensure gauge gets reset on re-instantiation.
	if got, want := expvar.Get("labellcf").String(), "{}"; got != want {
		t.Errorf("Counter get: %s, want %s", got, want)
	}
	// Ensure new value is returned after var gets added.
	ebd.NewCounterFunc("lcf", "", func() int64 { return 5 })
	if got, want := expvar.Get("labellcf").String(), `{"i1": 5}`; got != want {
		t.Errorf("Counter get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i2", "label")
	ebd.NewCounterFunc("lcf", "", func() int64 { return 6 })
	want1 := `{"i1": 5, "i2": 6}`
	want2 := `{"i2": 6, "i1": 5}`
	if got := expvar.Get("labellcf").String(); got != want1 && got != want2 {
		t.Errorf("Counter get: %s, want %s or %s", got, want1, want2)
	}
}

func TestGaugeFunc(t *testing.T) {
	ebd := NewEmbedder("", "")
	ebd.NewGaugeFunc("ggf", "", func() int64 { return 1 })
	if got, want := expvar.Get("ggf").String(), "1"; got != want {
		t.Errorf("Gauge get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewGaugeFunc("", "", func() int64 { return 2 })
	ebd.NewGaugeFunc("", "", func() int64 { return 3 })

	ebd.NewGaugeFunc("lgf", "", func() int64 { return 4 })
	if got, want := expvar.Get("labellgf").String(), `{"i1": 4}`; got != want {
		t.Errorf("Gauge get: %s, want %s", got, want)
	}

	// Ensure var gets replaced.
	ebd.NewGaugeFunc("lgf", "", func() int64 { return 5 })
	if got, want := expvar.Get("labellgf").String(), `{"i1": 5}`; got != want {
		t.Errorf("Gauge get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i1", "label")
	// Ensure gauge gets reset on re-instantiation.
	if got, want := expvar.Get("labellgf").String(), "{}"; got != want {
		t.Errorf("Gauge get: %s, want %s", got, want)
	}
	// Ensure new value is returned after var gets added.
	ebd.NewGaugeFunc("lgf", "", func() int64 { return 5 })
	if got, want := expvar.Get("labellgf").String(), `{"i1": 5}`; got != want {
		t.Errorf("Gauge get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i2", "label")
	ebd.NewGaugeFunc("lgf", "", func() int64 { return 6 })
	want1 := `{"i1": 5, "i2": 6}`
	want2 := `{"i2": 6, "i1": 5}`
	if got := expvar.Get("labellgf").String(); got != want1 && got != want2 {
		t.Errorf("Gauge get: %s, want %s or %s", got, want1, want2)
	}
}

func TestCounterDurationFunc(t *testing.T) {
	ebd := NewEmbedder("", "")
	ebd.NewCounterDurationFunc("gcduration", "", func() time.Duration { return 1 })
	if got, want := expvar.Get("gcduration").String(), "1"; got != want {
		t.Errorf("CounterDuration get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewCounterDurationFunc("", "", func() time.Duration { return 2 })
	ebd.NewCounterDurationFunc("", "", func() time.Duration { return 3 })

	ebd.NewCounterDurationFunc("lcduration", "", func() time.Duration { return 4 })
	if got, want := expvar.Get("labellcduration").String(), `{"i1": 4}`; got != want {
		t.Errorf("CounterDuration get: %s, want %s", got, want)
	}

	// Ensure var gets replaced.
	ebd.NewCounterDurationFunc("lcduration", "", func() time.Duration { return 5 })
	if got, want := expvar.Get("labellcduration").String(), `{"i1": 5}`; got != want {
		t.Errorf("CounterDuration get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i1", "label")
	// Ensure gauge gets reset on re-instantiation.
	if got, want := expvar.Get("labellcduration").String(), "{}"; got != want {
		t.Errorf("CounterDuration get: %s, want %s", got, want)
	}
	// Ensure new value is returned after var gets added.
	ebd.NewCounterDurationFunc("lcduration", "", func() time.Duration { return 5 })
	if got, want := expvar.Get("labellcduration").String(), `{"i1": 5}`; got != want {
		t.Errorf("CounterDuration get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i2", "label")
	ebd.NewCounterDurationFunc("lcduration", "", func() time.Duration { return 6 })
	want1 := `{"i1": 5, "i2": 6}`
	want2 := `{"i2": 6, "i1": 5}`
	if got := expvar.Get("labellcduration").String(); got != want1 && got != want2 {
		t.Errorf("CounterDuration get: %s, want %s or %s", got, want1, want2)
	}
}

func TestGaugeDurationFunc(t *testing.T) {
	ebd := NewEmbedder("", "")
	ebd.NewGaugeDurationFunc("ggduration", "", func() time.Duration { return 1 })
	if got, want := expvar.Get("ggduration").String(), "1"; got != want {
		t.Errorf("GaugeDuration get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewGaugeDurationFunc("", "", func() time.Duration { return 2 })
	ebd.NewGaugeDurationFunc("", "", func() time.Duration { return 3 })

	ebd.NewGaugeDurationFunc("lgduration", "", func() time.Duration { return 4 })
	if got, want := expvar.Get("labellgduration").String(), `{"i1": 4}`; got != want {
		t.Errorf("GaugeDuration get: %s, want %s", got, want)
	}

	// Ensure var gets replaced.
	ebd.NewGaugeDurationFunc("lgduration", "", func() time.Duration { return 5 })
	if got, want := expvar.Get("labellgduration").String(), `{"i1": 5}`; got != want {
		t.Errorf("GaugeDuration get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i1", "label")
	// Ensure gauge gets reset on re-instantiation.
	if got, want := expvar.Get("labellgduration").String(), "{}"; got != want {
		t.Errorf("GaugeDuration get: %s, want %s", got, want)
	}
	// Ensure new value is returned after var gets added.
	ebd.NewGaugeDurationFunc("lgduration", "", func() time.Duration { return 6 })
	if got, want := expvar.Get("labellgduration").String(), `{"i1": 6}`; got != want {
		t.Errorf("GaugeDuration get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i2", "label")
	ebd.NewGaugeDurationFunc("lgduration", "", func() time.Duration { return 7 })
	want1 := `{"i1": 6, "i2": 7}`
	want2 := `{"i2": 7, "i1": 6}`
	if got := expvar.Get("labellgduration").String(); got != want1 && got != want2 {
		t.Errorf("GaugeDuration get: %s, want %s or %s", got, want1, want2)
	}
}

func TestCountersWithSingleLabel(t *testing.T) {
	ebd := NewEmbedder("", "")
	g := ebd.NewCountersWithSingleLabel("gcwsl", "", "l")
	g.Add("a", 1)
	if got, want := expvar.Get("gcwsl").String(), `{"a": 1}`; got != want {
		t.Errorf("CountersWithSingleLabel get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewCountersWithSingleLabel("", "", "l")
	ebd.NewCountersWithSingleLabel("", "", "l")

	g = ebd.NewCountersWithSingleLabel("lcwsl", "", "l")
	g.Add("a", 4)
	if got, want := expvar.Get("labellcwsl").String(), `{"i1.a": 4}`; got != want {
		t.Errorf("CountersWithSingleLabel get: %s, want %s", got, want)
	}

	// Ensure var gets replaced.
	g = ebd.NewCountersWithSingleLabel("lcwsl", "", "l")
	g.Add("a", 5)
	if got, want := expvar.Get("labellcwsl").String(), `{"i1.a": 5}`; got != want {
		t.Errorf("CountersWithSingleLabel get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i1", "label")
	// Ensure gauge gets reset on re-instantiation.
	if got, want := expvar.Get("labellcwsl").String(), "{}"; got != want {
		t.Errorf("CountersWithSingleLabel get: %s, want %s", got, want)
	}
	// Ensure new value is returned after var gets added.
	g = ebd.NewCountersWithSingleLabel("lcwsl", "", "l")
	g.Add("a", 6)
	if got, want := expvar.Get("labellcwsl").String(), `{"i1.a": 6}`; got != want {
		t.Errorf("CountersWithSingleLabel get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i2", "label")
	g = ebd.NewCountersWithSingleLabel("lcwsl", "", "l")
	g.Add("a", 7)
	want1 := `{"i1.a": 6, "i2.a": 7}`
	want2 := `{"i2.a": 7, "i1.a": 6}`
	if got := expvar.Get("labellcwsl").String(); got != want1 && got != want2 {
		t.Errorf("CountersWithSingleLabel get: %s, want %s or %s", got, want1, want2)
	}
}

func TestGaugesWithSingleLabel(t *testing.T) {
	ebd := NewEmbedder("", "")
	g := ebd.NewGaugesWithSingleLabel("ggwsl", "", "l")
	g.Set("a", 1)
	if got, want := expvar.Get("ggwsl").String(), `{"a": 1}`; got != want {
		t.Errorf("GaugesWithSingleLabel get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewGaugesWithSingleLabel("", "", "l")
	ebd.NewGaugesWithSingleLabel("", "", "l")

	g = ebd.NewGaugesWithSingleLabel("lgwsl", "", "l")
	g.Set("a", 4)
	if got, want := expvar.Get("labellgwsl").String(), `{"i1.a": 4}`; got != want {
		t.Errorf("GaugesWithSingleLabel get: %s, want %s", got, want)
	}

	// Ensure var gets replaced.
	g = ebd.NewGaugesWithSingleLabel("lgwsl", "", "l")
	g.Set("a", 5)
	if got, want := expvar.Get("labellgwsl").String(), `{"i1.a": 5}`; got != want {
		t.Errorf("GaugesWithSingleLabel get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i1", "label")
	// Ensure gauge gets reset on re-instantiation.
	if got, want := expvar.Get("labellgwsl").String(), "{}"; got != want {
		t.Errorf("GaugesWithSingleLabel get: %s, want %s", got, want)
	}
	// Ensure new value is returned after var gets added.
	g = ebd.NewGaugesWithSingleLabel("lgwsl", "", "l")
	g.Set("a", 6)
	if got, want := expvar.Get("labellgwsl").String(), `{"i1.a": 6}`; got != want {
		t.Errorf("GaugesWithSingleLabel get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i2", "label")
	g = ebd.NewGaugesWithSingleLabel("lgwsl", "", "l")
	g.Set("a", 7)
	want1 := `{"i1.a": 6, "i2.a": 7}`
	want2 := `{"i2.a": 7, "i1.a": 6}`
	if got := expvar.Get("labellgwsl").String(); got != want1 && got != want2 {
		t.Errorf("GaugesWithSingleLabel get: %s, want %s or %s", got, want1, want2)
	}
}

func TestCountersWithMultiLabels(t *testing.T) {
	ebd := NewEmbedder("", "")
	g := ebd.NewCountersWithMultiLabels("gcwml", "", []string{"l"})
	g.Add([]string{"a"}, 1)
	if got, want := expvar.Get("gcwml").String(), `{"a": 1}`; got != want {
		t.Errorf("CountersWithMultiLabels get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewCountersWithMultiLabels("", "", []string{"l"})
	ebd.NewCountersWithMultiLabels("", "", []string{"l"})

	g = ebd.NewCountersWithMultiLabels("lcwml", "", []string{"l"})
	g.Add([]string{"a"}, 4)
	if got, want := expvar.Get("labellcwml").String(), `{"i1.a": 4}`; got != want {
		t.Errorf("CountersWithMultiLabels get: %s, want %s", got, want)
	}

	// Ensure var gets replaced.
	g = ebd.NewCountersWithMultiLabels("lcwml", "", []string{"l"})
	g.Add([]string{"a"}, 5)
	if got, want := expvar.Get("labellcwml").String(), `{"i1.a": 5}`; got != want {
		t.Errorf("CountersWithMultiLabels get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i1", "label")
	// Ensure gauge gets reset on re-instantiation.
	if got, want := expvar.Get("labellcwml").String(), "{}"; got != want {
		t.Errorf("CountersWithMultiLabels get: %s, want %s", got, want)
	}
	// Ensure new value is returned after var gets added.
	g = ebd.NewCountersWithMultiLabels("lcwml", "", []string{"l"})
	g.Add([]string{"a"}, 6)
	if got, want := expvar.Get("labellcwml").String(), `{"i1.a": 6}`; got != want {
		t.Errorf("CountersWithMultiLabels get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i2", "label")
	g = ebd.NewCountersWithMultiLabels("lcwml", "", []string{"l"})
	g.Add([]string{"a"}, 7)
	want1 := `{"i1.a": 6, "i2.a": 7}`
	want2 := `{"i2.a": 7, "i1.a": 6}`
	if got := expvar.Get("labellcwml").String(); got != want1 && got != want2 {
		t.Errorf("CountersWithMultiLabels get: %s, want %s or %s", got, want1, want2)
	}
}

func TestGaugesWithMultiLabels(t *testing.T) {
	ebd := NewEmbedder("", "")
	g := ebd.NewGaugesWithMultiLabels("ggwml", "", []string{"l"})
	g.Set([]string{"a"}, 1)
	if got, want := expvar.Get("ggwml").String(), `{"a": 1}`; got != want {
		t.Errorf("GaugesWithMultiLabels get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewGaugesWithMultiLabels("", "", []string{"l"})
	ebd.NewGaugesWithMultiLabels("", "", []string{"l"})

	g = ebd.NewGaugesWithMultiLabels("lgwml", "", []string{"l"})
	g.Set([]string{"a"}, 4)
	if got, want := expvar.Get("labellgwml").String(), `{"i1.a": 4}`; got != want {
		t.Errorf("GaugesWithMultiLabels get: %s, want %s", got, want)
	}

	// Ensure var gets replaced.
	g = ebd.NewGaugesWithMultiLabels("lgwml", "", []string{"l"})
	g.Set([]string{"a"}, 5)
	if got, want := expvar.Get("labellgwml").String(), `{"i1.a": 5}`; got != want {
		t.Errorf("GaugesWithMultiLabels get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i1", "label")
	// Ensure gauge gets reset on re-instantiation.
	if got, want := expvar.Get("labellgwml").String(), "{}"; got != want {
		t.Errorf("GaugesWithMultiLabels get: %s, want %s", got, want)
	}
	// Ensure new value is returned after var gets added.
	g = ebd.NewGaugesWithMultiLabels("lgwml", "", []string{"l"})
	g.Set([]string{"a"}, 6)
	if got, want := expvar.Get("labellgwml").String(), `{"i1.a": 6}`; got != want {
		t.Errorf("GaugesWithMultiLabels get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i2", "label")
	g = ebd.NewGaugesWithMultiLabels("lgwml", "", []string{"l"})
	g.Set([]string{"a"}, 7)
	want1 := `{"i1.a": 6, "i2.a": 7}`
	want2 := `{"i2.a": 7, "i1.a": 6}`
	if got := expvar.Get("labellgwml").String(); got != want1 && got != want2 {
		t.Errorf("GaugeDuration get: %s, want %s or %s", got, want1, want2)
	}
}

func TestTimings(t *testing.T) {
	ebd := NewEmbedder("", "")
	g := ebd.NewTimings("gtimings", "", "l")
	g.Add("a", 1)
	if got, want := expvar.Get("gtimings").String(), "TotalCount"; !strings.Contains(got, want) {
		t.Errorf("CountersFuncWithLabels get: %s, must contain %s", got, want)
	}

	ebd = NewEmbedder("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewTimings("", "", "l")
	ebd.NewTimings("", "", "l")

	// Ensure non-anonymous vars also don't cause panics
	ebd.NewTimings("ltimings", "", "l")
	ebd.NewTimings("ltimings", "", "l")
}

func TestMultiTimings(t *testing.T) {
	ebd := NewEmbedder("", "")
	g := ebd.NewMultiTimings("gmtimings", "", []string{"l"})
	g.Add([]string{"a"}, 1)
	if got, want := expvar.Get("gmtimings").String(), "TotalCount"; !strings.Contains(got, want) {
		t.Errorf("CountersFuncWithMultiLabels get: %s, must contain %s", got, want)
	}

	ebd = NewEmbedder("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewMultiTimings("", "", []string{"l"})
	ebd.NewMultiTimings("", "", []string{"l"})

	// Ensure non-anonymous vars also don't cause panics
	ebd.NewMultiTimings("lmtimings", "", []string{"l"})
	ebd.NewMultiTimings("lmtimings", "", []string{"l"})
}

func TestRates(t *testing.T) {
	ebd := NewEmbedder("", "")
	tm := ebd.NewMultiTimings("gratetimings", "", []string{"l"})
	ebd.NewRates("grates", tm, 15*60/5, 5*time.Second)
	if got, want := expvar.Get("grates").String(), "{}"; got != want {
		t.Errorf("CountersFuncWithMultiLabels get: %s, want %s", got, want)
	}

	ebd = NewEmbedder("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewRates("", tm, 15*60/5, 5*time.Second)
	ebd.NewRates("", tm, 15*60/5, 5*time.Second)

	// Ensure non-anonymous vars also don't cause panics
	ebd.NewRates("lrates", tm, 15*60/5, 5*time.Second)
	ebd.NewRates("lrates", tm, 15*60/5, 5*time.Second)
}

func TestHistogram(t *testing.T) {
	ebd := NewEmbedder("", "")
	g := ebd.NewHistogram("ghebdogram", "", []int64{10})
	g.Add(1)
	if got, want := expvar.Get("ghebdogram").String(), `{"10": 1, "inf": 1, "Count": 1, "Total": 1}`; !strings.Contains(got, want) {
		t.Errorf("CountersFuncWithMultiLabels get: %s, must contain %s", got, want)
	}

	ebd = NewEmbedder("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewHistogram("", "", []int64{10})
	ebd.NewHistogram("", "", []int64{10})

	// Ensure non-anonymous vars also don't cause panics
	ebd.NewHistogram("lhebdogram", "", []int64{10})
	ebd.NewHistogram("lhebdogram", "", []int64{10})
}

func TestPublish(t *testing.T) {
	ebd := NewEmbedder("", "")
	s := stats.NewString("")
	ebd.Publish("gpub", s)
	s.Set("1")
	if got, want := expvar.Get("gpub").String(), `"1"`; got != want {
		t.Errorf("Publish get: %s, want %s", got, want)
	}

	// This should not crash.
	ebd = NewEmbedder("i1", "label")
	ebd.Publish("lpub", s)
	ebd.Publish("lpub", s)
}
