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
	"net/http"
	"sync"
	"time"

	"vitess.io/vitess/go/stats"
)

// varType is used to specify what type of var to create.
type varType int

const (
	typeCounter = varType(iota)
	typeGauge
)

// onDup is used to specify how to handle duplicates when creating vars.
type onDup int

const (
	replaceOnDup = onDup(iota)
	reuseOnDup
)

var (
	// exporterMu is for all variables below.
	exporterMu sync.Mutex

	// exporters contains the full list of exporters. Entries can only be
	// added. The creation of a new Exporter with a previously existing
	// name causes that Exporter to be reused.
	exporters = make(map[string]*Exporter)

	// unnamedExports contain variables that were exported using
	// an unnamed exporter. If there is a name collision here, we
	// just reuse the unnamed variable.
	unnamedExports = make(map[string]expvar.Var)

	// exportedMultiCountVars contains the merged stats vars created for the vars that support Counts.
	exportedMultiCountVars = make(map[string]*multiCountVars)

	// exportedSingleCountVars contains the merged stats vars created for vars that support Count.
	exportedSingleCountVars = make(map[string]*singleCountVars)

	// exportedTimingsVars contains all the timings vars.
	exportedTimingsVars = make(map[string]*stats.MultiTimings)

	// exportedOtherStatsVars contains Rates, Histograms and Publish vars.
	exportedOtherStatsVars = make(map[string]*expvar.Map)
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
//
// The functions that create the stat vars don't always return
// the actual exported variable. Instead they return a variable that
// only affects the dimension that was assigned to the exporter.
// The exported variables are named evar, and the variables returned
// to the caller are named lvar (local var).
//
// If there are duplicates, "Func" vars will be changed to invoke
// the latest callback function. Non-Func vars will be reused. For
// counters, the adds will continue to add on top of existing values.
// For gauges, this is less material because a new "Set" will overwrite
// the previous value. This behavior of reusing counters is necessary
// because we build derived variables like Rates, which need to continue
// referencing the original variable that was created.
type Exporter struct {
	name, label string
	handleFuncs map[string]*handleFunc
	sp          *statusPage
}

// NewExporter creates a new Exporter with name as namespace.
// label is the name of the additional dimension for the stats vars.
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
	e.sp.reset()
}

// Name returns the name of the exporter.
func (e *Exporter) Name() string {
	return e.name
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
		v := stats.NewCountersFuncWithMultiLabels(name, help, labels, f)
		addUnnamedExport(name, v)
		return v
	}
	lvar := stats.NewCountersFuncWithMultiLabels("", help, labels, f)
	_ = e.createCountsTracker(name, help, labels, lvar, replaceOnDup, typeCounter)
	return lvar
}

func (e *Exporter) createCountsTracker(name, help string, labels []string, lvar multiCountVar, ondup onDup, typ varType) multiCountVar {
	exporterMu.Lock()
	defer exporterMu.Unlock()

	if c, ok := unnamedExports[name]; ok {
		if typ == typeCounter {
			return c.(multiCountVar)
		}
		return nil
	}

	if evar, ok := exportedMultiCountVars[name]; ok {
		evar.mu.Lock()
		defer evar.mu.Unlock()

		if ondup == reuseOnDup {
			if c, ok := evar.vars[e.name]; ok {
				return c
			}
		}
		evar.vars[e.name] = lvar
		return nil
	}
	evar := &multiCountVars{vars: map[string]multiCountVar{e.name: lvar}}
	exportedMultiCountVars[name] = evar

	newlabels := combineLabels(e.label, labels)
	if typ == typeCounter {
		_ = stats.NewCountersFuncWithMultiLabels(name, help, newlabels, evar.Fetch)
	} else {
		_ = stats.NewGaugesFuncWithMultiLabels(name, help, newlabels, evar.Fetch)
	}
	return nil
}

// NewGaugesFuncWithMultiLabels creates a name-spaced equivalent for stats.NewGaugesFuncWithMultiLabels.
func (e *Exporter) NewGaugesFuncWithMultiLabels(name, help string, labels []string, f func() map[string]int64) *stats.GaugesFuncWithMultiLabels {
	if e.name == "" || name == "" {
		v := stats.NewGaugesFuncWithMultiLabels(name, help, labels, f)
		addUnnamedExport(name, v)
		return v
	}
	lvar := stats.NewGaugesFuncWithMultiLabels("", help, labels, f)
	_ = e.createCountsTracker(name, help, labels, lvar, replaceOnDup, typeGauge)
	return lvar
}

// NewCounter creates a name-spaced equivalent for stats.NewCounter.
func (e *Exporter) NewCounter(name string, help string) *stats.Counter {
	if e.name == "" || name == "" {
		v := stats.NewCounter(name, help)
		addUnnamedExport(name, v)
		return v
	}
	lvar := stats.NewCounter("", help)
	if exists := e.createCountTracker(name, help, lvar, reuseOnDup, typeCounter); exists != nil {
		return exists.(*stats.Counter)
	}
	return lvar
}

func (e *Exporter) createCountTracker(name, help string, lvar singleCountVar, ondup onDup, typ varType) singleCountVar {
	exporterMu.Lock()
	defer exporterMu.Unlock()

	if c, ok := unnamedExports[name]; ok {
		if typ == typeCounter {
			return c.(singleCountVar)
		}
		return nil
	}

	if evar, ok := exportedSingleCountVars[name]; ok {
		evar.mu.Lock()
		defer evar.mu.Unlock()

		if ondup == reuseOnDup {
			c, ok := evar.vars[e.name]
			if ok {
				return c
			}
		}
		evar.vars[e.name] = lvar
		return nil
	}
	evar := &singleCountVars{vars: map[string]singleCountVar{e.name: lvar}}
	exportedSingleCountVars[name] = evar

	if typ == typeCounter {
		_ = stats.NewCountersFuncWithMultiLabels(name, help, []string{e.label}, evar.Fetch)
	} else {
		_ = stats.NewGaugesFuncWithMultiLabels(name, help, []string{e.label}, evar.Fetch)
	}
	return nil
}

// NewGauge creates a name-spaced equivalent for stats.NewGauge.
func (e *Exporter) NewGauge(name string, help string) *stats.Gauge {
	if e.name == "" || name == "" {
		v := stats.NewGauge(name, help)
		addUnnamedExport(name, v)
		return v
	}
	lvar := stats.NewGauge("", help)
	if exists := e.createCountTracker(name, help, lvar, reuseOnDup, typeCounter); exists != nil {
		return exists.(*stats.Gauge)
	}
	return lvar
}

// NewCounterFunc creates a name-spaced equivalent for stats.NewCounterFunc.
func (e *Exporter) NewCounterFunc(name string, help string, f func() int64) *stats.CounterFunc {
	if e.name == "" || name == "" {
		v := stats.NewCounterFunc(name, help, f)
		addUnnamedExport(name, v)
		return v
	}
	lvar := stats.NewCounterFunc("", help, f)
	_ = e.createCountTracker(name, help, lvar, replaceOnDup, typeCounter)
	return lvar
}

// NewGaugeFunc creates a name-spaced equivalent for stats.NewGaugeFunc.
func (e *Exporter) NewGaugeFunc(name string, help string, f func() int64) *stats.GaugeFunc {
	if e.name == "" || name == "" {
		v := stats.NewGaugeFunc(name, help, f)
		addUnnamedExport(name, v)
		return v
	}
	lvar := stats.NewGaugeFunc("", help, f)
	_ = e.createCountTracker(name, help, lvar, replaceOnDup, typeGauge)
	return lvar
}

// NewCounterDurationFunc creates a name-spaced equivalent for stats.NewCounterDurationFunc.
func (e *Exporter) NewCounterDurationFunc(name string, help string, f func() time.Duration) *stats.CounterDurationFunc {
	if e.name == "" || name == "" {
		v := stats.NewCounterDurationFunc(name, help, f)
		addUnnamedExport(name, v)
		return v
	}
	lvar := stats.NewCounterDurationFunc("", help, f)
	_ = e.createCountTracker(name, help, lvar, replaceOnDup, typeCounter)
	return lvar
}

// NewGaugeDurationFunc creates a name-spaced equivalent for stats.NewGaugeDurationFunc.
func (e *Exporter) NewGaugeDurationFunc(name string, help string, f func() time.Duration) *stats.GaugeDurationFunc {
	if e.name == "" || name == "" {
		v := stats.NewGaugeDurationFunc(name, help, f)
		addUnnamedExport(name, v)
		return v
	}
	lvar := stats.NewGaugeDurationFunc("", help, f)
	_ = e.createCountTracker(name, help, lvar, replaceOnDup, typeGauge)
	return lvar
}

// NewCountersWithSingleLabel creates a name-spaced equivalent for stats.NewCountersWithSingleLabel.
// Tags are ignored if the exporter is named.
func (e *Exporter) NewCountersWithSingleLabel(name, help string, label string, tags ...string) *stats.CountersWithSingleLabel {
	if e.name == "" || name == "" {
		v := stats.NewCountersWithSingleLabel(name, help, label, tags...)
		addUnnamedExport(name, v)
		return v
	}
	lvar := stats.NewCountersWithSingleLabel("", help, label)
	if exists := e.createCountsTracker(name, help, []string{label}, lvar, reuseOnDup, typeCounter); exists != nil {
		return exists.(*stats.CountersWithSingleLabel)
	}
	return lvar
}

// NewGaugesWithSingleLabel creates a name-spaced equivalent for stats.NewGaugesWithSingleLabel.
// Tags are ignored if the exporter is named.
func (e *Exporter) NewGaugesWithSingleLabel(name, help string, label string, tags ...string) *stats.GaugesWithSingleLabel {
	if e.name == "" || name == "" {
		v := stats.NewGaugesWithSingleLabel(name, help, label, tags...)
		addUnnamedExport(name, v)
		return v
	}

	lvar := stats.NewGaugesWithSingleLabel("", help, label)
	if exists := e.createCountsTracker(name, help, []string{label}, lvar, reuseOnDup, typeGauge); exists != nil {
		return exists.(*stats.GaugesWithSingleLabel)
	}
	return lvar
}

// NewCountersWithMultiLabels creates a name-spaced equivalent for stats.NewCountersWithMultiLabels.
func (e *Exporter) NewCountersWithMultiLabels(name, help string, labels []string) *stats.CountersWithMultiLabels {
	if e.name == "" || name == "" {
		v := stats.NewCountersWithMultiLabels(name, help, labels)
		addUnnamedExport(name, v)
		return v
	}

	lvar := stats.NewCountersWithMultiLabels("", help, labels)
	if exists := e.createCountsTracker(name, help, labels, lvar, reuseOnDup, typeCounter); exists != nil {
		return exists.(*stats.CountersWithMultiLabels)
	}
	return lvar
}

// NewGaugesWithMultiLabels creates a name-spaced equivalent for stats.NewGaugesWithMultiLabels.
func (e *Exporter) NewGaugesWithMultiLabels(name, help string, labels []string) *stats.GaugesWithMultiLabels {
	if e.name == "" || name == "" {
		v := stats.NewGaugesWithMultiLabels(name, help, labels)
		addUnnamedExport(name, v)
		return v
	}

	lvar := stats.NewGaugesWithMultiLabels("", help, labels)
	if exists := e.createCountsTracker(name, help, labels, lvar, reuseOnDup, typeGauge); exists != nil {
		return exists.(*stats.GaugesWithMultiLabels)
	}
	return lvar
}

// NewTimings creates a name-spaced equivalent for stats.NewTimings.
// The function currently just returns an unexported variable.
func (e *Exporter) NewTimings(name string, help string, label string) *TimingsWrapper {
	if e.name == "" || name == "" {
		v := &TimingsWrapper{
			timings: stats.NewMultiTimings(name, help, []string{label}),
		}
		addUnnamedExport(name, v.timings)
		return v
	}

	exporterMu.Lock()
	defer exporterMu.Unlock()

	if v, ok := unnamedExports[name]; ok {
		return &TimingsWrapper{
			timings: v.(*stats.MultiTimings),
		}
	}

	if tv, ok := exportedTimingsVars[name]; ok {
		return &TimingsWrapper{
			name:    e.name,
			timings: tv,
		}
	}
	mt := stats.NewMultiTimings(name, help, []string{e.label, label})
	exportedTimingsVars[name] = mt
	return &TimingsWrapper{
		name:    e.name,
		timings: mt,
	}
}

// NewMultiTimings creates a name-spaced equivalent for stats.NewMultiTimings.
// The function currently just returns an unexported variable.
func (e *Exporter) NewMultiTimings(name string, help string, labels []string) *MultiTimingsWrapper {
	if e.name == "" || name == "" {
		v := &MultiTimingsWrapper{
			timings: stats.NewMultiTimings(name, help, labels),
		}
		addUnnamedExport(name, v.timings)
		return v
	}

	exporterMu.Lock()
	defer exporterMu.Unlock()

	if v, ok := unnamedExports[name]; ok {
		return &MultiTimingsWrapper{
			timings: v.(*stats.MultiTimings),
		}
	}

	if tv, ok := exportedTimingsVars[name]; ok {
		return &MultiTimingsWrapper{
			name:    e.name,
			timings: tv,
		}
	}
	mt := stats.NewMultiTimings(name, help, combineLabels(e.label, labels))
	exportedTimingsVars[name] = mt
	return &MultiTimingsWrapper{
		name:    e.name,
		timings: mt,
	}
}

// NewRates creates a name-spaced equivalent for stats.NewRates.
// The function currently just returns an unexported variable.
func (e *Exporter) NewRates(name string, singleCountVar multiCountVar, samples int, interval time.Duration) *stats.Rates {
	if e.name == "" || name == "" {
		v := stats.NewRates(name, singleCountVar, samples, interval)
		addUnnamedExport(name, v)
		return v
	}

	exporterMu.Lock()
	defer exporterMu.Unlock()

	if v, ok := unnamedExports[name]; ok {
		return v.(*stats.Rates)
	}

	ov, ok := exportedOtherStatsVars[name]
	if !ok {
		ov = expvar.NewMap(name)
		exportedOtherStatsVars[name] = ov
	}
	if lvar := ov.Get(e.name); lvar != nil {
		return lvar.(*stats.Rates)
	}

	rates := stats.NewRates("", singleCountVar, samples, interval)
	ov.Set(e.name, rates)
	return rates
}

// NewHistogram creates a name-spaced equivalent for stats.NewHistogram.
// The function currently just returns an unexported variable.
func (e *Exporter) NewHistogram(name, help string, cutoffs []int64) *stats.Histogram {
	if e.name == "" || name == "" {
		v := stats.NewHistogram(name, help, cutoffs)
		addUnnamedExport(name, v)
		return v
	}
	hist := stats.NewHistogram("", help, cutoffs)
	e.addToOtherVars(name, hist)
	return hist
}

// Publish creates a name-spaced equivalent for stats.Publish.
// The function just passes through if the Exporter name is empty.
func (e *Exporter) Publish(name string, v expvar.Var) {
	if e.name == "" || name == "" {
		addUnnamedExport(name, v)
		stats.Publish(name, v)
		return
	}
	e.addToOtherVars(name, v)
}

func (e *Exporter) addToOtherVars(name string, v expvar.Var) {
	exporterMu.Lock()
	defer exporterMu.Unlock()

	if _, ok := unnamedExports[name]; ok {
		return
	}

	ov, ok := exportedOtherStatsVars[name]
	if !ok {
		ov = expvar.NewMap(name)
		exportedOtherStatsVars[name] = ov
	}
	ov.Set(e.name, v)
}

//-----------------------------------------------------------------

// singleCountVar is any stats that support Get.
type singleCountVar interface {
	Get() int64
}

// singleCountVars contains all stats that support Get, like *stats.Counter.
type singleCountVars struct {
	mu   sync.Mutex
	vars map[string]singleCountVar
}

// Fetch returns the consolidated stats value for all exporters, like stats.CountersWithSingleLabel.
func (evar *singleCountVars) Fetch() map[string]int64 {
	result := make(map[string]int64)
	evar.mu.Lock()
	defer evar.mu.Unlock()
	for k, c := range evar.vars {
		result[k] = c.Get()
	}
	return result
}

//-----------------------------------------------------------------

// multiCountVar is any stats that support Counts.
type multiCountVar interface {
	Counts() map[string]int64
}

// multiCountVars contains all stats that support Counts.
type multiCountVars struct {
	mu   sync.Mutex
	vars map[string]multiCountVar
}

// Fetch returns the consolidated stats value for all exporters.
func (evar *multiCountVars) Fetch() map[string]int64 {
	result := make(map[string]int64)
	evar.mu.Lock()
	defer evar.mu.Unlock()
	for k, c := range evar.vars {
		for innerk, innerv := range c.Counts() {
			result[k+"."+innerk] = innerv
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

// Counts behaves like Timings.Counts.
func (tw *TimingsWrapper) Counts() map[string]int64 {
	return tw.timings.Counts()
}

// Reset will clear histograms: used during testing
func (tw *TimingsWrapper) Reset() {
	tw.timings.Reset()
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

// Reset will clear histograms: used during testing
func (tw *MultiTimingsWrapper) Reset() {
	tw.timings.Reset()
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

func addUnnamedExport(name string, v expvar.Var) {
	if name == "" {
		return
	}
	exporterMu.Lock()
	unnamedExports[name] = v
	exporterMu.Unlock()
}

func combineLabels(label string, labels []string) []string {
	return append(append(make([]string, 0, len(labels)+1), label), labels...)
}
