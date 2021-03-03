/*
Copyright 2020 The Vitess Authors.

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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/stats"
)

func TestURLPrefix(t *testing.T) {
	assert.Equal(t, "", NewExporter("", "").URLPrefix())
	assert.Equal(t, "/a", NewExporter("a", "").URLPrefix())
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

	ebd := NewExporter("", "")
	ebd.HandleFunc("/path", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("1"))
	})
	assert.Equal(t, "1", httpGet(t, fmt.Sprintf("http://localhost:%d/path", port)))
	assert.Contains(t, httpGet(t, fmt.Sprintf("http://localhost:%d/debug/status", port)), "Status for")

	ebd = NewExporter("a", "")
	ebd.HandleFunc("/path", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("2"))
	})
	assert.Equal(t, "2", httpGet(t, fmt.Sprintf("http://localhost:%d/a/path", port)))
	assert.Contains(t, httpGet(t, fmt.Sprintf("http://localhost:%d/debug/status", port)), "Status for")

	ebd.HandleFunc("/path", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("3"))
	})
	assert.Equal(t, "3", httpGet(t, fmt.Sprintf("http://localhost:%d/a/path", port)))
	assert.Contains(t, httpGet(t, fmt.Sprintf("http://localhost:%d/a/debug/status", port)), "Status for")

	ebd = NewExporter("a", "")
	ebd.HandleFunc("/path", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("4"))
	})
	assert.Equal(t, "4", httpGet(t, fmt.Sprintf("http://localhost:%d/a/path", port)))

	ebd = NewExporter("b", "")
	ebd.HandleFunc("/path", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("5"))
	})
	assert.Equal(t, "5", httpGet(t, fmt.Sprintf("http://localhost:%d/b/path", port)))
	assert.Contains(t, httpGet(t, fmt.Sprintf("http://localhost:%d/b/debug/status", port)), "Status for")
	// Ensure "a" is still the same.
	assert.Equal(t, "4", httpGet(t, fmt.Sprintf("http://localhost:%d/a/path", port)))
	assert.Contains(t, httpGet(t, fmt.Sprintf("http://localhost:%d/a/debug/status", port)), "Status for")
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
	ebd := NewExporter("", "")
	ebd.NewCountersFuncWithMultiLabels("gcfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 1} })
	assert.Equal(t, `{"a": 1}`, expvar.Get("gcfwml").String())

	ebd = NewExporter("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewCountersFuncWithMultiLabels("", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 2} })
	ebd.NewCountersFuncWithMultiLabels("", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 3} })

	// Ensure reuse of global var is ignored.
	ebd.NewCountersFuncWithMultiLabels("gcfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 2} })
	assert.Equal(t, `{"a": 1}`, expvar.Get("gcfwml").String())

	ebd.NewCountersFuncWithMultiLabels("lcfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 4} })
	assert.Equal(t, `{"i1.a": 4}`, expvar.Get("lcfwml").String())

	// Ensure var gets replaced.
	ebd.NewCountersFuncWithMultiLabels("lcfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 5} })
	assert.Equal(t, `{"i1.a": 5}`, expvar.Get("lcfwml").String())

	ebd = NewExporter("i2", "label")
	ebd.NewCountersFuncWithMultiLabels("lcfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 6} })
	assert.Contains(t, expvar.Get("lcfwml").String(), `"i1.a": 5`)
	assert.Contains(t, expvar.Get("lcfwml").String(), `"i2.a": 6`)
}

func TestGaugesFuncWithMultiLabels(t *testing.T) {
	ebd := NewExporter("", "")
	ebd.NewGaugesFuncWithMultiLabels("ggfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 1} })
	assert.Equal(t, `{"a": 1}`, expvar.Get("ggfwml").String())

	ebd = NewExporter("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewGaugesFuncWithMultiLabels("", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 2} })
	ebd.NewGaugesFuncWithMultiLabels("", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 3} })

	// Ensure reuse of global var is ignored.
	ebd.NewGaugesFuncWithMultiLabels("ggfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 1} })
	assert.Equal(t, `{"a": 1}`, expvar.Get("ggfwml").String())

	ebd.NewGaugesFuncWithMultiLabels("lgfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 4} })
	assert.Equal(t, `{"i1.a": 4}`, expvar.Get("lgfwml").String())

	// Ensure var gets replaced.
	ebd.NewGaugesFuncWithMultiLabels("lgfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 5} })
	assert.Equal(t, `{"i1.a": 5}`, expvar.Get("lgfwml").String())

	ebd = NewExporter("i2", "label")
	ebd.NewGaugesFuncWithMultiLabels("lgfwml", "", []string{"l"}, func() map[string]int64 { return map[string]int64{"a": 6} })
	assert.Contains(t, expvar.Get("lgfwml").String(), `"i1.a": 5`)
	assert.Contains(t, expvar.Get("lgfwml").String(), `"i2.a": 6`)
}

func TestCounter(t *testing.T) {
	ebd := NewExporter("", "")
	c := ebd.NewCounter("gcounter", "")
	c.Add(1)
	assert.Equal(t, "1", expvar.Get("gcounter").String())

	ebd = NewExporter("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewCounter("", "")
	ebd.NewCounter("", "")

	// Ensure global var gets reused.
	c = ebd.NewCounter("gcounter", "")
	c.Add(1)
	assert.Equal(t, "2", expvar.Get("gcounter").String())

	c = ebd.NewCounter("lcounter", "")
	c.Add(4)
	assert.Equal(t, `{"i1": 4}`, expvar.Get("lcounter").String())

	// Ensure var gets reused.
	c = ebd.NewCounter("lcounter", "")
	c.Add(5)
	assert.Equal(t, `{"i1": 9}`, expvar.Get("lcounter").String())

	ebd = NewExporter("i2", "label")
	c = ebd.NewCounter("lcounter", "")
	c.Add(6)
	assert.Contains(t, expvar.Get("lcounter").String(), `"i1": 9`)
	assert.Contains(t, expvar.Get("lcounter").String(), `"i2": 6`)
}

func TestGauge(t *testing.T) {
	ebd := NewExporter("", "")
	c := ebd.NewGauge("ggauge", "")
	c.Set(1)
	assert.Equal(t, "1", expvar.Get("ggauge").String())

	ebd = NewExporter("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewGauge("", "")
	ebd.NewGauge("", "")

	// Ensure global var gets reused.
	c = ebd.NewGauge("ggauge", "")
	c.Set(2)
	assert.Equal(t, "2", expvar.Get("ggauge").String())

	c = ebd.NewGauge("lgauge", "")
	c.Set(4)
	assert.Equal(t, `{"i1": 4}`, expvar.Get("lgauge").String())

	// Ensure var gets reused.
	c = ebd.NewGauge("lgauge", "")
	assert.Equal(t, `{"i1": 4}`, expvar.Get("lgauge").String())
	c.Set(5)
	assert.Equal(t, `{"i1": 5}`, expvar.Get("lgauge").String())

	ebd = NewExporter("i2", "label")
	c = ebd.NewGauge("lgauge", "")
	c.Set(6)
	assert.Contains(t, expvar.Get("lgauge").String(), `"i1": 5`)
	assert.Contains(t, expvar.Get("lgauge").String(), `"i2": 6`)
}

func TestCounterFunc(t *testing.T) {
	ebd := NewExporter("", "")
	ebd.NewCounterFunc("gcf", "", func() int64 { return 1 })
	assert.Equal(t, "1", expvar.Get("gcf").String())

	ebd = NewExporter("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewCounterFunc("", "", func() int64 { return 2 })
	ebd.NewCounterFunc("", "", func() int64 { return 3 })

	// Ensure reuse of global var is ignored.
	ebd.NewCounterFunc("gcf", "", func() int64 { return 2 })
	assert.Equal(t, "1", expvar.Get("gcf").String())

	ebd.NewCounterFunc("lcf", "", func() int64 { return 4 })
	assert.Equal(t, `{"i1": 4}`, expvar.Get("lcf").String())

	// Ensure var gets replaced.
	ebd.NewCounterFunc("lcf", "", func() int64 { return 5 })
	assert.Equal(t, `{"i1": 5}`, expvar.Get("lcf").String())

	ebd = NewExporter("i2", "label")
	ebd.NewCounterFunc("lcf", "", func() int64 { return 6 })
	assert.Contains(t, expvar.Get("lcf").String(), `"i1": 5`)
	assert.Contains(t, expvar.Get("lcf").String(), `"i2": 6`)
}

func TestGaugeFunc(t *testing.T) {
	ebd := NewExporter("", "")
	ebd.NewGaugeFunc("ggf", "", func() int64 { return 1 })
	assert.Equal(t, "1", expvar.Get("ggf").String())

	ebd = NewExporter("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewGaugeFunc("", "", func() int64 { return 2 })
	ebd.NewGaugeFunc("", "", func() int64 { return 3 })

	// Ensure reuse of global var is ignored.
	ebd.NewGaugeFunc("ggf", "", func() int64 { return 2 })
	assert.Equal(t, "1", expvar.Get("ggf").String())

	ebd.NewGaugeFunc("lgf", "", func() int64 { return 4 })
	assert.Equal(t, `{"i1": 4}`, expvar.Get("lgf").String())

	// Ensure var gets replaced.
	ebd.NewGaugeFunc("lgf", "", func() int64 { return 5 })
	assert.Equal(t, `{"i1": 5}`, expvar.Get("lgf").String())

	ebd = NewExporter("i2", "label")
	ebd.NewGaugeFunc("lgf", "", func() int64 { return 6 })
	assert.Contains(t, expvar.Get("lgf").String(), `"i1": 5`)
	assert.Contains(t, expvar.Get("lgf").String(), `"i2": 6`)
}

func TestCounterDurationFunc(t *testing.T) {
	ebd := NewExporter("", "")
	ebd.NewCounterDurationFunc("gcduration", "", func() time.Duration { return 1 })
	assert.Equal(t, "1", expvar.Get("gcduration").String())

	ebd = NewExporter("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewCounterDurationFunc("", "", func() time.Duration { return 2 })
	ebd.NewCounterDurationFunc("", "", func() time.Duration { return 3 })

	// Ensure reuse of global var is ignored.
	ebd.NewCounterDurationFunc("gcduration", "", func() time.Duration { return 2 })
	assert.Equal(t, "1", expvar.Get("gcduration").String())

	ebd.NewCounterDurationFunc("lcduration", "", func() time.Duration { return 4 })
	assert.Equal(t, `{"i1": 4}`, expvar.Get("lcduration").String())

	// Ensure var gets replaced.
	ebd.NewCounterDurationFunc("lcduration", "", func() time.Duration { return 5 })
	assert.Equal(t, `{"i1": 5}`, expvar.Get("lcduration").String())

	ebd = NewExporter("i2", "label")
	ebd.NewCounterDurationFunc("lcduration", "", func() time.Duration { return 6 })
	assert.Contains(t, expvar.Get("lcduration").String(), `"i1": 5`)
	assert.Contains(t, expvar.Get("lcduration").String(), `"i2": 6`)
}

func TestGaugeDurationFunc(t *testing.T) {
	ebd := NewExporter("", "")
	ebd.NewGaugeDurationFunc("ggduration", "", func() time.Duration { return 1 })
	assert.Equal(t, "1", expvar.Get("ggduration").String())

	ebd = NewExporter("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewGaugeDurationFunc("", "", func() time.Duration { return 2 })
	ebd.NewGaugeDurationFunc("", "", func() time.Duration { return 3 })

	// Ensure reuse of global var is ignored.
	ebd.NewGaugeDurationFunc("ggduration", "", func() time.Duration { return 2 })
	assert.Equal(t, "1", expvar.Get("ggduration").String())

	ebd.NewGaugeDurationFunc("lgduration", "", func() time.Duration { return 4 })
	assert.Equal(t, `{"i1": 4}`, expvar.Get("lgduration").String())

	// Ensure var gets replaced.
	ebd.NewGaugeDurationFunc("lgduration", "", func() time.Duration { return 5 })
	assert.Equal(t, `{"i1": 5}`, expvar.Get("lgduration").String())

	ebd = NewExporter("i2", "label")
	ebd.NewGaugeDurationFunc("lgduration", "", func() time.Duration { return 6 })
	assert.Contains(t, expvar.Get("lgduration").String(), `"i1": 5`)
	assert.Contains(t, expvar.Get("lgduration").String(), `"i2": 6`)
}

func TestCountersWithSingleLabel(t *testing.T) {
	ebd := NewExporter("", "")
	g := ebd.NewCountersWithSingleLabel("gcwsl", "", "l")
	g.Add("a", 1)
	assert.Equal(t, `{"a": 1}`, expvar.Get("gcwsl").String())

	ebd = NewExporter("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewCountersWithSingleLabel("", "", "l")
	ebd.NewCountersWithSingleLabel("", "", "l")

	// Ensure global var gets reused.
	g = ebd.NewCountersWithSingleLabel("gcwsl", "", "l")
	g.Add("a", 1)
	assert.Equal(t, `{"a": 2}`, expvar.Get("gcwsl").String())

	g = ebd.NewCountersWithSingleLabel("lcwsl", "", "l")
	g.Add("a", 4)
	assert.Equal(t, `{"i1.a": 4}`, expvar.Get("lcwsl").String())

	// Ensure var gets reused.
	g = ebd.NewCountersWithSingleLabel("lcwsl", "", "l")
	g.Add("a", 5)
	assert.Equal(t, `{"i1.a": 9}`, expvar.Get("lcwsl").String())

	ebd = NewExporter("i2", "label")
	g = ebd.NewCountersWithSingleLabel("lcwsl", "", "l")
	g.Add("a", 6)
	assert.Contains(t, expvar.Get("lcwsl").String(), `"i1.a": 9`)
	assert.Contains(t, expvar.Get("lcwsl").String(), `"i2.a": 6`)
}

func TestGaugesWithSingleLabel(t *testing.T) {
	ebd := NewExporter("", "")
	g := ebd.NewGaugesWithSingleLabel("ggwsl", "", "l")
	g.Set("a", 1)
	assert.Equal(t, `{"a": 1}`, expvar.Get("ggwsl").String())

	ebd = NewExporter("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewGaugesWithSingleLabel("", "", "l")
	ebd.NewGaugesWithSingleLabel("", "", "l")

	// Ensure reuse of global var is ignored.
	g = ebd.NewGaugesWithSingleLabel("ggwsl", "", "l")
	g.Set("a", 2)
	assert.Equal(t, `{"a": 1}`, expvar.Get("ggwsl").String())

	g = ebd.NewGaugesWithSingleLabel("lgwsl", "", "l")
	g.Set("a", 4)
	assert.Equal(t, `{"i1.a": 4}`, expvar.Get("lgwsl").String())

	// Ensure var gets reused.
	g = ebd.NewGaugesWithSingleLabel("lgwsl", "", "l")
	assert.Equal(t, `{"i1.a": 4}`, expvar.Get("lgwsl").String())
	g.Set("a", 5)
	assert.Equal(t, `{"i1.a": 5}`, expvar.Get("lgwsl").String())

	ebd = NewExporter("i2", "label")
	g = ebd.NewGaugesWithSingleLabel("lgwsl", "", "l")
	g.Set("a", 6)
	assert.Contains(t, expvar.Get("lgwsl").String(), `"i1.a": 5`)
	assert.Contains(t, expvar.Get("lgwsl").String(), `"i2.a": 6`)
}

func TestCountersWithMultiLabels(t *testing.T) {
	ebd := NewExporter("", "")
	g := ebd.NewCountersWithMultiLabels("gcwml", "", []string{"l"})
	g.Add([]string{"a"}, 1)
	assert.Equal(t, `{"a": 1}`, expvar.Get("gcwml").String())

	ebd = NewExporter("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewCountersWithMultiLabels("", "", []string{"l"})
	ebd.NewCountersWithMultiLabels("", "", []string{"l"})

	// Ensure global var gets reused.
	g = ebd.NewCountersWithMultiLabels("gcwml", "", []string{"l"})
	g.Add([]string{"a"}, 1)
	assert.Equal(t, `{"a": 2}`, expvar.Get("gcwml").String())

	g = ebd.NewCountersWithMultiLabels("lcwml", "", []string{"l"})
	g.Add([]string{"a"}, 4)
	assert.Equal(t, `{"i1.a": 4}`, expvar.Get("lcwml").String())

	// Ensure var gets reused.
	g = ebd.NewCountersWithMultiLabels("lcwml", "", []string{"l"})
	g.Add([]string{"a"}, 5)
	assert.Equal(t, `{"i1.a": 9}`, expvar.Get("lcwml").String())

	ebd = NewExporter("i2", "label")
	g = ebd.NewCountersWithMultiLabels("lcwml", "", []string{"l"})
	g.Add([]string{"a"}, 6)
	assert.Contains(t, expvar.Get("lcwml").String(), `"i1.a": 9`)
	assert.Contains(t, expvar.Get("lcwml").String(), `"i2.a": 6`)
}

func TestGaugesWithMultiLabels(t *testing.T) {
	ebd := NewExporter("", "")
	g := ebd.NewGaugesWithMultiLabels("ggwml", "", []string{"l"})
	g.Set([]string{"a"}, 1)
	assert.Equal(t, `{"a": 1}`, expvar.Get("ggwml").String())

	ebd = NewExporter("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewGaugesWithMultiLabels("", "", []string{"l"})
	ebd.NewGaugesWithMultiLabels("", "", []string{"l"})

	// Ensure reuse of global var is ignored.
	g = ebd.NewGaugesWithMultiLabels("ggwml", "", []string{"l"})
	g.Set([]string{"a"}, 2)
	assert.Equal(t, `{"a": 1}`, expvar.Get("ggwml").String())

	g = ebd.NewGaugesWithMultiLabels("lgwml", "", []string{"l"})
	g.Set([]string{"a"}, 4)
	assert.Equal(t, `{"i1.a": 4}`, expvar.Get("lgwml").String())

	// Ensure var gets reused.
	g = ebd.NewGaugesWithMultiLabels("lgwml", "", []string{"l"})
	assert.Equal(t, `{"i1.a": 4}`, expvar.Get("lgwml").String())
	g.Set([]string{"a"}, 5)
	assert.Equal(t, `{"i1.a": 5}`, expvar.Get("lgwml").String())

	ebd = NewExporter("i2", "label")
	g = ebd.NewGaugesWithMultiLabels("lgwml", "", []string{"l"})
	g.Set([]string{"a"}, 6)
	assert.Contains(t, expvar.Get("lgwml").String(), `"i1.a": 5`)
	assert.Contains(t, expvar.Get("lgwml").String(), `"i2.a": 6`)
}

func TestTimings(t *testing.T) {
	ebd := NewExporter("", "")
	g := ebd.NewTimings("gtimings", "", "l")
	g.Add("a", 1)
	assert.Contains(t, expvar.Get("gtimings").String(), `"TotalCount":1`)
	g.Record("a", time.Now())
	assert.Contains(t, expvar.Get("gtimings").String(), `"TotalCount":2`)

	ebd = NewExporter("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewTimings("", "", "l")
	ebd.NewTimings("", "", "l")

	// Ensure global var gets reused.
	g = ebd.NewTimings("gtimings", "", "l")
	g.Add("a", 1)
	assert.Contains(t, expvar.Get("gtimings").String(), `"TotalCount":3`)

	g = ebd.NewTimings("ltimings", "", "l")
	g.Add("a", 1)
	g.Add("a", 1)
	assert.Contains(t, expvar.Get("ltimings").String(), `i1.a`)
	assert.Contains(t, expvar.Get("ltimings").String(), `"TotalCount":2`)

	// Ensure var gets reused.
	g = ebd.NewTimings("ltimings", "", "l")
	g.Add("a", 1)
	assert.Contains(t, expvar.Get("ltimings").String(), `i1.a`)
	assert.Contains(t, expvar.Get("ltimings").String(), `"TotalCount":3`)
	g.Record("a", time.Now())
	assert.Contains(t, expvar.Get("ltimings").String(), `"TotalCount":4`)

	ebd = NewExporter("i2", "label")
	g = ebd.NewTimings("ltimings", "", "l")
	g.Add("a", 1)
	assert.Contains(t, expvar.Get("ltimings").String(), `i1.a`)
	assert.Contains(t, expvar.Get("ltimings").String(), `i2.a`)
	assert.Contains(t, expvar.Get("ltimings").String(), `"TotalCount":5`)

	want := map[string]int64{
		"All":  5,
		"i1.a": 4,
		"i2.a": 1,
	}
	assert.Equal(t, want, g.Counts())
}

func TestMultiTimings(t *testing.T) {
	ebd := NewExporter("", "")
	g := ebd.NewMultiTimings("gmtimings", "", []string{"l"})
	g.Add([]string{"a"}, 1)
	assert.Contains(t, expvar.Get("gmtimings").String(), `"TotalCount":1`)
	g.Record([]string{"a"}, time.Now())
	assert.Contains(t, expvar.Get("gmtimings").String(), `"TotalCount":2`)

	ebd = NewExporter("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewMultiTimings("", "", []string{"l"})
	ebd.NewMultiTimings("", "", []string{"l"})

	// Ensure global var gets reused.
	g = ebd.NewMultiTimings("gmtimings", "", []string{"l"})
	g.Add([]string{"a"}, 1)
	assert.Contains(t, expvar.Get("gmtimings").String(), `"TotalCount":3`)

	g = ebd.NewMultiTimings("lmtimings", "", []string{"l"})
	g.Add([]string{"a"}, 1)
	g.Add([]string{"a"}, 1)
	assert.Contains(t, expvar.Get("lmtimings").String(), `i1.a`)
	assert.Contains(t, expvar.Get("lmtimings").String(), `"TotalCount":2`)
	g.Record([]string{"a"}, time.Now())
	assert.Contains(t, expvar.Get("lmtimings").String(), `"TotalCount":3`)

	// Ensure var gets reused.
	g = ebd.NewMultiTimings("lmtimings", "", []string{"l"})
	g.Add([]string{"a"}, 1)
	assert.Contains(t, expvar.Get("lmtimings").String(), `i1.a`)
	assert.Contains(t, expvar.Get("lmtimings").String(), `"TotalCount":4`)

	ebd = NewExporter("i2", "label")
	g = ebd.NewMultiTimings("lmtimings", "", []string{"l"})
	g.Add([]string{"a"}, 1)
	assert.Contains(t, expvar.Get("lmtimings").String(), `i1.a`)
	assert.Contains(t, expvar.Get("lmtimings").String(), `i2.a`)
	assert.Contains(t, expvar.Get("lmtimings").String(), `"TotalCount":5`)

	want := map[string]int64{
		"All":  5,
		"i1.a": 4,
		"i2.a": 1,
	}
	assert.Equal(t, want, g.Counts())
}

func TestRates(t *testing.T) {
	ebd := NewExporter("", "")
	tm := ebd.NewMultiTimings("gratetimings", "", []string{"l"})
	ebd.NewRates("grates", tm, 15*60/5, 5*time.Second)
	assert.Equal(t, "{}", expvar.Get("grates").String())

	ebd = NewExporter("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewRates("", tm, 15*60/5, 5*time.Second)
	ebd.NewRates("", tm, 15*60/5, 5*time.Second)

	// Ensure global var gets reused.
	ebd.NewRates("grates", tm, 15*60/5, 5*time.Second)
	assert.Equal(t, "{}", expvar.Get("grates").String())

	// Ensure var gets reused.
	rates1 := ebd.NewRates("lrates", tm, 15*60/5, 5*time.Second)
	rates2 := ebd.NewRates("lrates", tm, 15*60/5, 5*time.Second)
	assert.True(t, rates2 == rates1)

	ebd = NewExporter("i2", "label")
	rates3 := ebd.NewRates("lrates", tm, 15*60/5, 5*time.Second)
	assert.True(t, rates3 != rates1)
}

func TestHistogram(t *testing.T) {
	ebd := NewExporter("", "")
	g := ebd.NewHistogram("ghistogram", "", []int64{10})
	g.Add(1)
	assert.Contains(t, expvar.Get("ghistogram").String(), `{"10": 1, "inf": 0, "Count": 1, "Total": 1}`)

	ebd = NewExporter("i1", "label")

	// Ensure anonymous vars don't cause panics.
	ebd.NewHistogram("", "", []int64{10})
	ebd.NewHistogram("", "", []int64{10})

	// Ensure reuse of global var doesn't panic.
	_ = ebd.NewHistogram("ghistogram", "", []int64{10})

	g = ebd.NewHistogram("lhistogram", "", []int64{10})
	g.Add(1)
	g.Add(1)
	assert.Contains(t, expvar.Get("lmtimings").String(), `i1`)
	assert.Contains(t, expvar.Get("lhistogram").String(), `{"10": 2, "inf": 0, "Count": 2, "Total": 2}`)

	// Ensure var gets replaced.
	g = ebd.NewHistogram("lhistogram", "", []int64{10})
	g.Add(1)
	assert.Contains(t, expvar.Get("lmtimings").String(), `i1`)
	assert.Contains(t, expvar.Get("lhistogram").String(), `{"10": 1, "inf": 0, "Count": 1, "Total": 1}`)
}

func TestPublish(t *testing.T) {
	ebd := NewExporter("", "")
	s := stats.NewString("")
	ebd.Publish("gpub", s)
	s.Set("1")
	assert.Equal(t, `"1"`, expvar.Get("gpub").String())

	ebd = NewExporter("i1", "label")
	ebd.Publish("lpub", s)
	assert.Equal(t, `{"i1": "1"}`, expvar.Get("lpub").String())

	// Ensure reuse of global var doesn't panic.
	ebd.Publish("gpub", s)

	ebd = NewExporter("i2", "label")
	ebd.Publish("lpub", s)
	assert.Contains(t, expvar.Get("lpub").String(), `"i1": "1"`)
	assert.Contains(t, expvar.Get("lpub").String(), `"i2": "1"`)
}
