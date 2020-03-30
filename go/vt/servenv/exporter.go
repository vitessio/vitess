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
	"net/http"
	"sync"
	"time"

	"vitess.io/vitess/go/stats"
)

var (
	// exporterMu protects exporters and globalStatVars, timingsVars and otherStatsVars.
	// varMap and handleFunc have their own mutexes.
	exporterMu sync.Mutex

	// exporters contains the full list of exporters. Entries can only be
	// added. The creation of a new Exporter with a previously existing
	// name causes that Exporter to be reused.
	exporters = make(map[string]*Exporter)

	// globalStatVars contains the merged stats vars created for the exporters.
	globalStatVars = make(map[string]*varMap)

	// timingsVars contains all the timings vars.
	timingsVars = make(map[string]*stats.MultiTimings)

	// otherStatsVars contains Rates, Histograms and Publish vars.
	otherStatsVars = make(map[string]*expvar.Map)
)

//-----------------------------------------------------------------

// Exporter remaps http and stats end-points to distinct namespaces.
//
// Unnamed exporters are treated as unscoped, and requests are passed
// through to the underlying functions.
//
// For named exporters, http handle requests of the form /path will
// be remapped to /name/path. In the case of stats variables, a new
// dimension will be added. For example, a Counter of value 1
// will be changed to a map {"name": 1}. A multi-counter like
// { "a.b": 1, "c.d": 2} will be mapped to {"name.a.b": 1, "name.c.d": 2}.
// Stats vars of the same name are merged onto a single map. For example,
// if exporters name1 and name2 independently create a stats Counter
// named foo and export values 1 and 2, the result is a merged stats var
// named foo with the following content: {"name1": 1, "name2": 2}.
type Exporter struct {
	name, label string
	handleFuncs map[string]*handleFunc
	sp          *statusPage
}

// NewExporter creates a new Exporter with name as namespace.
// label is the name of the additonial dimension for the stats vars.
func NewExporter(name, label string) *Exporter {
	if name == "" {
		return &Exporter{}
	}

	exporterMu.Lock()
	defer exporterMu.Unlock()

	if e, ok := exporters[name]; ok {
		e.resetLocked()
		return e
	}
	e := &Exporter{
		name:        name,
		label:       label,
		handleFuncs: make(map[string]*handleFunc),
		sp:          newStatusPage(name),
	}
	exporters[name] = e
	return e
}

func (e *Exporter) resetLocked() {
	for _, hf := range e.handleFuncs {
		hf.Set(nil)
	}
	for _, vmap := range globalStatVars {
		vmap.Unset(e.name)
	}
	e.sp.reset()
}

// URLPrefix returns the URL prefix for the exporter.
func (e *Exporter) URLPrefix() string {
	// There are two other places where this logic is duplicated:
	// status.go and go/vt/vtgate/discovery/healthcheck.go.
	if e.name == "" {
		return e.name
	}
	return "/" + e.name
}

// HandleFunc sets or overwrites the handler for url. If Exporter has a name,
// url remapped from /path to /name/path. If name is empty, the request
// is passed through to http.HandleFunc.
func (e *Exporter) HandleFunc(url string, f func(w http.ResponseWriter, r *http.Request)) {
	if e.name == "" {
		http.HandleFunc(url, f)
		return
	}

	if hf, ok := e.handleFuncs[url]; ok {
		hf.Set(f)
		return
	}
	hf := &handleFunc{f: f}
	e.handleFuncs[url] = hf

	http.HandleFunc(e.URLPrefix()+url, func(w http.ResponseWriter, r *http.Request) {
		if f := hf.Get(); f != nil {
			f(w, r)
		}
	})
}

// AddStatusPart adds a status part to the status page. If Exporter has a name,
// the part is added to a url named /name/debug/status. Otherwise, it's /debug/status.
func (e *Exporter) AddStatusPart(banner, frag string, f func() interface{}) {
	if e.name == "" {
		AddStatusPart(banner, frag, f)
		return
	}
	e.sp.addStatusPart(banner, frag, f)
}

// NewCountersFuncWithMultiLabels creates a name-spaced equivalent for stats.NewCountersFuncWithMultiLabels.
func (e *Exporter) NewCountersFuncWithMultiLabels(name, help string, labels []string, f func() map[string]int64) *stats.CountersFuncWithMultiLabels {
	// If e.name is empty, it's a pass-through.
	// If name is empty, it's an unexported var.
	if e.name == "" || name == "" {
		return stats.NewCountersFuncWithMultiLabels(name, help, labels, f)
	}

	exporterMu.Lock()
	defer exporterMu.Unlock()

	if vmap, ok := globalStatVars[name]; ok {
		vmap.Set(e.name, f)
		return stats.NewCountersFuncWithMultiLabels("", help, labels, f)
	}
	vmap := &varMap{vars: map[string]func() map[string]int64{e.name: f}}
	globalStatVars[name] = vmap

	newlabels := combineLabels(e.label, labels)
	_ = stats.NewCountersFuncWithMultiLabels(name, help, newlabels, vmap.Fetch)
	return stats.NewCountersFuncWithMultiLabels("", help, labels, f)
}

// NewGaugesFuncWithMultiLabels creates a name-spaced equivalent for stats.NewGaugesFuncWithMultiLabels.
func (e *Exporter) NewGaugesFuncWithMultiLabels(name, help string, labels []string, f func() map[string]int64) *stats.GaugesFuncWithMultiLabels {
	// This implementation is identical to NewCountersFuncWithMultiLabels, except it's for Gauges.
	if e.name == "" || name == "" {
		return stats.NewGaugesFuncWithMultiLabels(name, help, labels, f)
	}

	exporterMu.Lock()
	defer exporterMu.Unlock()

	if vmap, ok := globalStatVars[name]; ok {
		vmap.Set(e.name, f)
		return stats.NewGaugesFuncWithMultiLabels("", help, labels, f)
	}
	vmap := &varMap{vars: map[string]func() map[string]int64{e.name: f}}
	globalStatVars[name] = vmap

	newlabels := combineLabels(e.label, labels)
	_ = stats.NewGaugesFuncWithMultiLabels(name, help, newlabels, vmap.Fetch)
	return stats.NewGaugesFuncWithMultiLabels("", help, labels, f)
}

// NewCounter creates a name-spaced equivalent for stats.NewCounter.
func (e *Exporter) NewCounter(name string, help string) *stats.Counter {
	if e.name == "" || name == "" {
		return stats.NewCounter(name, help)
	}
	v := stats.NewCounter("", help)
	_ = e.NewCounterFunc(name, help, v.Get)
	return v
}

// NewGauge creates a name-spaced equivalent for stats.NewGauge.
func (e *Exporter) NewGauge(name string, help string) *stats.Gauge {
	if e.name == "" || name == "" {
		return stats.NewGauge(name, help)
	}
	v := stats.NewGauge("", help)
	_ = e.NewGaugeFunc(name, help, v.Get)
	return v
}

// NewCounterFunc creates a name-spaced equivalent for stats.NewCounterFunc.
func (e *Exporter) NewCounterFunc(name string, help string, f func() int64) *stats.CounterFunc {
	if e.name == "" || name == "" {
		return stats.NewCounterFunc(name, help, f)
	}
	_ = e.NewCountersFuncWithMultiLabels(name, help, nil, func() map[string]int64 {
		return map[string]int64{"": f()}
	})
	return stats.NewCounterFunc("", help, f)
}

// NewGaugeFunc creates a name-spaced equivalent for stats.NewGaugeFunc.
func (e *Exporter) NewGaugeFunc(name string, help string, f func() int64) *stats.GaugeFunc {
	if e.name == "" || name == "" {
		return stats.NewGaugeFunc(name, help, f)
	}
	_ = e.NewGaugesFuncWithMultiLabels(name, help, nil, func() map[string]int64 {
		return map[string]int64{"": f()}
	})
	return stats.NewGaugeFunc("", help, f)
}

// NewCounterDurationFunc creates a name-spaced equivalent for stats.NewCounterDurationFunc.
func (e *Exporter) NewCounterDurationFunc(name string, help string, f func() time.Duration) *stats.CounterDurationFunc {
	if e.name == "" || name == "" {
		return stats.NewCounterDurationFunc(name, help, f)
	}
	_ = e.NewCounterFunc(name, help, func() int64 { return int64(f()) })
	return stats.NewCounterDurationFunc("", help, f)
}

// NewGaugeDurationFunc creates a name-spaced equivalent for stats.NewGaugeDurationFunc.
func (e *Exporter) NewGaugeDurationFunc(name string, help string, f func() time.Duration) *stats.GaugeDurationFunc {
	if e.name == "" || name == "" {
		return stats.NewGaugeDurationFunc(name, help, f)
	}
	_ = e.NewGaugeFunc(name, help, func() int64 { return int64(f()) })
	return stats.NewGaugeDurationFunc("", help, f)
}

// NewCountersWithSingleLabel creates a name-spaced equivalent for stats.NewCountersWithSingleLabel.
// Tags are ignored if the exporter is named.
func (e *Exporter) NewCountersWithSingleLabel(name, help string, label string, tags ...string) *stats.CountersWithSingleLabel {
	if e.name == "" || name == "" {
		return stats.NewCountersWithSingleLabel(name, help, label, tags...)
	}

	v := stats.NewCountersWithSingleLabel("", help, label)
	_ = e.NewCountersFuncWithMultiLabels(name, help, []string{label}, v.Counts)
	return v
}

// NewGaugesWithSingleLabel creates a name-spaced equivalent for stats.NewGaugesWithSingleLabel.
// Tags are ignored if the exporter is named.
func (e *Exporter) NewGaugesWithSingleLabel(name, help string, label string, tags ...string) *stats.GaugesWithSingleLabel {
	if e.name == "" || name == "" {
		return stats.NewGaugesWithSingleLabel(name, help, label, tags...)
	}

	v := stats.NewGaugesWithSingleLabel("", help, label)
	_ = e.NewGaugesFuncWithMultiLabels(name, help, []string{label}, v.Counts)
	return v
}

// NewCountersWithMultiLabels creates a name-spaced equivalent for stats.NewCountersWithMultiLabels.
func (e *Exporter) NewCountersWithMultiLabels(name, help string, labels []string) *stats.CountersWithMultiLabels {
	if e.name == "" || name == "" {
		return stats.NewCountersWithMultiLabels(name, help, labels)
	}

	v := stats.NewCountersWithMultiLabels("", help, labels)
	_ = e.NewCountersFuncWithMultiLabels(name, help, labels, v.Counts)
	return v
}

// NewGaugesWithMultiLabels creates a name-spaced equivalent for stats.NewGaugesWithMultiLabels.
func (e *Exporter) NewGaugesWithMultiLabels(name, help string, labels []string) *stats.GaugesWithMultiLabels {
	if e.name == "" || name == "" {
		return stats.NewGaugesWithMultiLabels(name, help, labels)
	}

	v := stats.NewGaugesWithMultiLabels("", help, labels)
	_ = e.NewGaugesFuncWithMultiLabels(name, help, labels, v.Counts)
	return v
}

// NewTimings creates a name-spaced equivalent for stats.NewTimings.
// The function currently just returns an unexported variable.
func (e *Exporter) NewTimings(name string, help string, label string) *TimingsWrapper {
	if e.name == "" || name == "" {
		return &TimingsWrapper{
			timings: stats.NewMultiTimings(name, help, []string{label}),
		}
	}

	exporterMu.Lock()
	defer exporterMu.Unlock()

	if tv, ok := timingsVars[name]; ok {
		return &TimingsWrapper{
			name:    e.name,
			timings: tv,
		}
	}
	mt := stats.NewMultiTimings(name, help, []string{e.label, label})
	timingsVars[name] = mt
	return &TimingsWrapper{
		name:    e.name,
		timings: mt,
	}
}

// NewMultiTimings creates a name-spaced equivalent for stats.NewMultiTimings.
// The function currently just returns an unexported variable.
func (e *Exporter) NewMultiTimings(name string, help string, labels []string) *MultiTimingsWrapper {
	if e.name == "" || name == "" {
		return &MultiTimingsWrapper{
			timings: stats.NewMultiTimings(name, help, labels),
		}
	}

	exporterMu.Lock()
	defer exporterMu.Unlock()

	if tv, ok := timingsVars[name]; ok {
		return &MultiTimingsWrapper{
			name:    e.name,
			timings: tv,
		}
	}
	mt := stats.NewMultiTimings(name, help, combineLabels(e.label, labels))
	timingsVars[name] = mt
	return &MultiTimingsWrapper{
		name:    e.name,
		timings: mt,
	}
}

// NewRates creates a name-spaced equivalent for stats.NewRates.
// The function currently just returns an unexported variable.
func (e *Exporter) NewRates(name string, countTracker stats.CountTracker, samples int, interval time.Duration) *stats.Rates {
	if e.name == "" || name == "" {
		return stats.NewRates(name, countTracker, samples, interval)
	}

	exporterMu.Lock()
	defer exporterMu.Unlock()

	ov, ok := otherStatsVars[name]
	if !ok {
		ov = expvar.NewMap(name)
		otherStatsVars[name] = ov
	}
	rates := stats.NewRates("", countTracker, samples, interval)
	ov.Set(e.name, rates)
	return rates
}

// NewHistogram creates a name-spaced equivalent for stats.NewHistogram.
// The function currently just returns an unexported variable.
func (e *Exporter) NewHistogram(name, help string, cutoffs []int64) *stats.Histogram {
	if e.name == "" || name == "" {
		return stats.NewHistogram(name, help, cutoffs)
	}

	exporterMu.Lock()
	defer exporterMu.Unlock()

	ov, ok := otherStatsVars[name]
	if !ok {
		ov = expvar.NewMap(name)
		otherStatsVars[name] = ov
	}
	hist := stats.NewHistogram("", help, cutoffs)
	ov.Set(e.name, hist)
	return hist
}

// Publish creates a name-spaced equivalent for stats.Publish.
// The function just passes through if the Exporter name is empty.
func (e *Exporter) Publish(name string, v expvar.Var) {
	if e.name == "" || name == "" {
		stats.Publish(name, v)
		return
	}

	exporterMu.Lock()
	defer exporterMu.Unlock()

	ov, ok := otherStatsVars[name]
	if !ok {
		ov = expvar.NewMap(name)
		otherStatsVars[name] = ov
	}
	ov.Set(e.name, v)
}

//-----------------------------------------------------------------

// varMap contains the metadata for a merged stats var. It supports
// gauges and counters. For every Exporter, it stores a function
// that yields the counter or gauge values for that Exporter.
type varMap struct {
	mu   sync.Mutex
	vars map[string]func() map[string]int64
}

// Set adds or updates the func for an Exporter.
func (vmap *varMap) Set(name string, f func() map[string]int64) {
	vmap.mu.Lock()
	defer vmap.mu.Unlock()
	vmap.vars[name] = f
}

// Unset removes the Exporter's entry from varMap.
func (vmap *varMap) Unset(name string) {
	vmap.mu.Lock()
	defer vmap.mu.Unlock()
	delete(vmap.vars, name)
}

// Fetch returns the consolidated stats value for all exporters.
func (vmap *varMap) Fetch() map[string]int64 {
	result := make(map[string]int64)
	vmap.mu.Lock()
	defer vmap.mu.Unlock()
	for k, f := range vmap.vars {
		for innerk, innerv := range f() {
			if innerk == "" {
				result[k] = innerv
			} else {
				result[k+"."+innerk] = innerv
			}
		}
	}
	return result
}

//-----------------------------------------------------------------

// TimingsWrapper provides a namespaced version of stats.Timings.
type TimingsWrapper struct {
	name    string
	timings *stats.MultiTimings
}

// Add behaves like Timings.Add.
func (tw *TimingsWrapper) Add(name string, elapsed time.Duration) {
	if tw.name == "" {
		tw.timings.Add([]string{name}, elapsed)
		return
	}
	tw.timings.Add([]string{tw.name, name}, elapsed)
}

// Record behaves like Timings.Record.
func (tw *TimingsWrapper) Record(name string, startTime time.Time) {
	if tw.name == "" {
		tw.timings.Record([]string{name}, startTime)
		return
	}
	tw.timings.Record([]string{tw.name, name}, startTime)
}

// Counts behaves lie Timings.Counts.
func (tw *TimingsWrapper) Counts() map[string]int64 {
	return tw.timings.Counts()
}

//-----------------------------------------------------------------

// MultiTimingsWrapper provides a namespaced version of stats.MultiTimings.
type MultiTimingsWrapper struct {
	name    string
	timings *stats.MultiTimings
}

// Add behaves like MultiTimings.Add.
func (tw *MultiTimingsWrapper) Add(names []string, elapsed time.Duration) {
	if tw.name == "" {
		tw.timings.Add(names, elapsed)
		return
	}
	newlabels := combineLabels(tw.name, names)
	tw.timings.Add(newlabels, elapsed)
}

// Record behaves like MultiTimings.Record.
func (tw *MultiTimingsWrapper) Record(names []string, startTime time.Time) {
	if tw.name == "" {
		tw.timings.Record(names, startTime)
		return
	}
	newlabels := combineLabels(tw.name, names)
	tw.timings.Record(newlabels, startTime)
}

// Counts behaves lie MultiTimings.Counts.
func (tw *MultiTimingsWrapper) Counts() map[string]int64 {
	return tw.timings.Counts()
}

//-----------------------------------------------------------------

// handleFunc stores the http Handler for an Exporter. This function can
// be replaced as needed.
type handleFunc struct {
	mu sync.Mutex
	f  func(w http.ResponseWriter, r *http.Request)
}

// Set replaces the existing handler with a new one.
func (hf *handleFunc) Set(f func(w http.ResponseWriter, r *http.Request)) {
	hf.mu.Lock()
	defer hf.mu.Unlock()
	hf.f = f
}

// Get returns the current handler.
func (hf *handleFunc) Get() func(w http.ResponseWriter, r *http.Request) {
	hf.mu.Lock()
	defer hf.mu.Unlock()
	return hf.f
}

//-----------------------------------------------------------------

func combineLabels(label string, labels []string) []string {
	return append(append(make([]string, 0, len(labels)+1), label), labels...)
}
