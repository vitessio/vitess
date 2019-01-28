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

// Package servenv allows you to remap http and stats end-points
// to distinct names thereby allowing you to create multiple instances
// of the same object within a single process.
//
// Unnamed instances are treated as unscoped, and requests are passed
// through to the underlying functions.
//
// For named instances, http handle requests of the form /path will
// be remapped to /name/path. In the case of stats variables, a new
// dimension will be added. For example, a Counter of value 1
// will be changed to a map {"name": 1}. A multi-counter like
// { "a.b": 1, "c.d": 2} will be mapped to {"name.a.b": 1, "name.c.d": 2}.
// Stats vars of the same name are merged onto a single map. For example,
// if instances name1 and name2 independently create a stats Counter
// named foo and export values 1 and 2, the result is a merged stats var
// named foo with the following content: {"name1": 1, "name2": 2}.
// This approach works for counters and gauges, but does not work for
// the more complex vars like Timings. In those cases, no remapping is
// done. The embedder returns unexported variables instead.
package servenv

import (
	"expvar"
	"net/http"
	"sync"
	"time"

	"vitess.io/vitess/go/stats"
)

var (
	// embedmu protects embeds, members of embedder and globalStatVars.
	// However, varMap and handlFunc have their own mutexes. This is
	// because their handler functions directly access them.
	embedmu sync.Mutex

	// embeds contains the full list of instances. Entries can only be
	// added. The creation of a new instance with a previously existing
	// name causes that instance to be reused.
	embeds = make(map[string]*embedder)

	// globalStatVars contains the merged stats vars created for the instances.
	globalStatVars = make(map[string]*varMap)
)

//-----------------------------------------------------------------

// varMap contains the metadata for a merged stats var. It supports
// only gauges and counters. For every embedder, it stores a function
// that yields the counter or gauge values for that embedder.
type varMap struct {
	mu   sync.Mutex
	vars map[string]func() map[string]int64
}

// Set adds or updates the func for an embedder.
func (vmap *varMap) Set(name string, f func() map[string]int64) {
	vmap.mu.Lock()
	defer vmap.mu.Unlock()
	vmap.vars[name] = f
}

// Fetch returns the consolidated stats value for all embedder.
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

// handleFunc stores the http Handler for an embedder. This function can
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

// embedder provides the functions needed to embed an object
// by remapping global endpoints into different namespaces.
type embedder struct {
	name, prefix string
	handleFuncs  map[string]*handleFunc
	sp           *statusPage
}

// AssignPrefix assigns a prefix for the instance name. All future
// stats vars for the instance will be created with the specified
// prefix. The prefix is is also used to label the additonial
// dimension for the stats vars.
func AssignPrefix(instanceName, prefix string) {
	embedmu.Lock()
	defer embedmu.Unlock()

	e, ok := embeds[instanceName]
	if ok {
		e.resetLocked(prefix)
		return
	}
	e = &embedder{
		name:        instanceName,
		prefix:      prefix,
		handleFuncs: make(map[string]*handleFunc),
	}
	if instanceName != "" {
		e.sp = newStatusPage(instanceName)
	}
	embeds[instanceName] = e
}

func (e *embedder) resetLocked(prefix string) {
	e.prefix = prefix
	for _, hf := range e.handleFuncs {
		hf.Set(nil)
	}

	for _, vmap := range globalStatVars {
		vmap.mu.Lock()
		delete(vmap.vars, e.name)
		vmap.mu.Unlock()
	}

	if e.sp != nil {
		e.sp.reset()
	}
}

// findOrCreateEmbedder returns an existing embedder or
// creates a new one.
func findOrCreateEmbedder(instanceName string) *embedder {
	if e, ok := embeds[instanceName]; ok {
		return e
	}
	e := &embedder{
		name:        instanceName,
		prefix:      "",
		handleFuncs: make(map[string]*handleFunc),
	}
	if instanceName != "" {
		e.sp = newStatusPage(instanceName)
	}
	embeds[instanceName] = e
	return e
}

// URLPrefix returns the URL prefix for all the embedder.
func URLPrefix(instanceName string) string {
	// There are two other places where this logic is duplicated:
	// status.go and go/vt/vtgate/discovery/healthcheck.go.
	if instanceName == "" {
		return ""
	}
	return "/" + instanceName
}

// HandleFunc sets or overwrites the handler for url. If embedder has a name,
// url remapped from /path to /name/path. If name is empty, the request
// is passed through to http.HandleFunc.
func HandleFunc(instanceName, url string, f func(w http.ResponseWriter, r *http.Request)) {
	if instanceName == "" {
		http.HandleFunc(url, f)
		return
	}

	embedmu.Lock()
	defer embedmu.Unlock()
	e := findOrCreateEmbedder(instanceName)

	hf, ok := e.handleFuncs[url]
	if ok {
		hf.Set(f)
		return
	}
	hf = &handleFunc{f: f}
	e.handleFuncs[url] = hf

	http.HandleFunc(URLPrefix(e.name)+url, func(w http.ResponseWriter, r *http.Request) {
		if f := hf.Get(); f != nil {
			f(w, r)
		}
	})
}

// AddInstanceStatusPart adds a status part to the status page. If instance has a name,
// the part is added to a url named /name/debug/status. Otherwise, it's /debug/status.
func AddInstanceStatusPart(instanceName, banner, frag string, f func() interface{}) {
	if instanceName == "" {
		AddStatusPart(banner, frag, f)
		return
	}

	embedmu.Lock()
	defer embedmu.Unlock()
	e := findOrCreateEmbedder(instanceName)
	e.sp.addStatusPart(banner, frag, f)
}

// NewCountersFuncWithMultiLabels creates a name-spaced equivalent for stats.NewCountersFuncWithMultiLabels.
func NewCountersFuncWithMultiLabels(instanceName, name, help string, labels []string, f func() map[string]int64) *stats.CountersFuncWithMultiLabels {
	// If instanceName is empty, it's a pass-through.
	// If name is empty, it's an unexported var.
	if instanceName == "" || name == "" {
		return stats.NewCountersFuncWithMultiLabels(name, help, labels, f)
	}

	embedmu.Lock()
	defer embedmu.Unlock()
	e := findOrCreateEmbedder(instanceName)

	if vmap, ok := globalStatVars[name]; ok {
		vmap.Set(e.name, f)
		return stats.NewCountersFuncWithMultiLabels("", help, labels, f)
	}
	vmap := &varMap{vars: map[string]func() map[string]int64{e.name: f}}
	globalStatVars[name] = vmap

	newlabels := append(append(make([]string, 0, len(labels)+1), e.prefix), labels...)
	_ = stats.NewCountersFuncWithMultiLabels(e.prefix+name, help, newlabels, func() map[string]int64 {
		return vmap.Fetch()
	})
	return stats.NewCountersFuncWithMultiLabels("", help, labels, f)
}

// NewGaugesFuncWithMultiLabels creates a name-spaced equivalent for stats.NewGaugesFuncWithMultiLabels.
func NewGaugesFuncWithMultiLabels(instanceName, name, help string, labels []string, f func() map[string]int64) *stats.GaugesFuncWithMultiLabels {
	// This implementation is identical to NewCountersFuncWithMultiLabels, except it's for Gauges.
	if instanceName == "" || name == "" {
		return stats.NewGaugesFuncWithMultiLabels(name, help, labels, f)
	}

	embedmu.Lock()
	defer embedmu.Unlock()
	e := findOrCreateEmbedder(instanceName)

	if vmap, ok := globalStatVars[name]; ok {
		vmap.Set(e.name, f)
		return stats.NewGaugesFuncWithMultiLabels("", help, labels, f)
	}
	vmap := &varMap{vars: map[string]func() map[string]int64{e.name: f}}
	globalStatVars[name] = vmap

	newlabels := append(append(make([]string, 0, len(labels)+1), e.prefix), labels...)
	_ = stats.NewGaugesFuncWithMultiLabels(e.prefix+name, help, newlabels, func() map[string]int64 {
		return vmap.Fetch()
	})
	return stats.NewGaugesFuncWithMultiLabels("", help, labels, f)
}

// NewCounter creates a name-spaced equivalent for stats.NewCounter.
func NewCounter(instanceName, name string, help string) *stats.Counter {
	if instanceName == "" || name == "" {
		return stats.NewCounter(name, help)
	}
	v := stats.NewCounter("", help)
	_ = NewCounterFunc(instanceName, name, help, v.Get)
	return v
}

// NewGauge creates a name-spaced equivalent for stats.NewGauge.
func NewGauge(instanceName, name string, help string) *stats.Gauge {
	if instanceName == "" || name == "" {
		return stats.NewGauge(name, help)
	}
	v := stats.NewGauge("", help)
	_ = NewGaugeFunc(instanceName, name, help, v.Get)
	return v
}

// NewCounterFunc creates a name-spaced equivalent for stats.NewCounterFunc.
func NewCounterFunc(instanceName, name string, help string, f func() int64) *stats.CounterFunc {
	if instanceName == "" || name == "" {
		return stats.NewCounterFunc(name, help, f)
	}
	_ = NewCountersFuncWithMultiLabels(instanceName, name, help, nil, func() map[string]int64 {
		return map[string]int64{"": f()}
	})
	return stats.NewCounterFunc("", help, f)
}

// NewGaugeFunc creates a name-spaced equivalent for stats.NewGaugeFunc.
func NewGaugeFunc(instanceName, name string, help string, f func() int64) *stats.GaugeFunc {
	if instanceName == "" || name == "" {
		return stats.NewGaugeFunc(name, help, f)
	}
	_ = NewGaugesFuncWithMultiLabels(instanceName, name, help, nil, func() map[string]int64 {
		return map[string]int64{"": f()}
	})
	return stats.NewGaugeFunc("", help, f)
}

// NewCounterDurationFunc creates a name-spaced equivalent for stats.NewCounterDurationFunc.
func NewCounterDurationFunc(instanceName, name string, help string, f func() time.Duration) *stats.CounterDurationFunc {
	if instanceName == "" || name == "" {
		return stats.NewCounterDurationFunc(name, help, f)
	}
	_ = NewCounterFunc(instanceName, name, help, func() int64 { return int64(f()) })
	return stats.NewCounterDurationFunc("", help, f)
}

// NewGaugeDurationFunc creates a name-spaced equivalent for stats.NewGaugeDurationFunc.
func NewGaugeDurationFunc(instanceName, name string, help string, f func() time.Duration) *stats.GaugeDurationFunc {
	if instanceName == "" || name == "" {
		return stats.NewGaugeDurationFunc(name, help, f)
	}
	_ = NewGaugeFunc(instanceName, name, help, func() int64 { return int64(f()) })
	return stats.NewGaugeDurationFunc("", help, f)
}

// NewCountersWithSingleLabel creates a name-spaced equivalent for stats.NewCountersWithSingleLabel.
// Tags are ignored if embedded.
func NewCountersWithSingleLabel(instanceName, name, help string, label string, tags ...string) *stats.CountersWithSingleLabel {
	if instanceName == "" || name == "" {
		return stats.NewCountersWithSingleLabel(name, help, label, tags...)
	}

	v := stats.NewCountersWithSingleLabel("", help, label)
	_ = NewCountersFuncWithMultiLabels(instanceName, name, help, []string{label}, v.Counts)
	return v
}

// NewGaugesWithSingleLabel creates a name-spaced equivalent for stats.NewGaugesWithSingleLabel.
// Tags are ignored if embedded.
func NewGaugesWithSingleLabel(instanceName, name, help string, label string, tags ...string) *stats.GaugesWithSingleLabel {
	if instanceName == "" || name == "" {
		return stats.NewGaugesWithSingleLabel(name, help, label, tags...)
	}

	v := stats.NewGaugesWithSingleLabel("", help, label)
	_ = NewGaugesFuncWithMultiLabels(instanceName, name, help, []string{label}, v.Counts)
	return v
}

// NewCountersWithMultiLabels creates a name-spaced equivalent for stats.NewCountersWithMultiLabels.
func NewCountersWithMultiLabels(instanceName, name, help string, labels []string) *stats.CountersWithMultiLabels {
	if instanceName == "" || name == "" {
		return stats.NewCountersWithMultiLabels(name, help, labels)
	}

	v := stats.NewCountersWithMultiLabels("", help, labels)
	_ = NewCountersFuncWithMultiLabels(instanceName, name, help, labels, v.Counts)
	return v
}

// NewGaugesWithMultiLabels creates a name-spaced equivalent for stats.NewGaugesWithMultiLabels.
func NewGaugesWithMultiLabels(instanceName, name, help string, labels []string) *stats.GaugesWithMultiLabels {
	if instanceName == "" || name == "" {
		return stats.NewGaugesWithMultiLabels(name, help, labels)
	}

	v := stats.NewGaugesWithMultiLabels("", help, labels)
	_ = NewGaugesFuncWithMultiLabels(instanceName, name, help, labels, v.Counts)
	return v
}

// NewTimings creates a name-spaced equivalent for stats.NewTimings.
// The function currently just returns an unexported variable.
// TODO(sougou): implement.
func NewTimings(instanceName, name string, help string, label string) *stats.Timings {
	if instanceName == "" || name == "" {
		return stats.NewTimings(name, help, label)
	}
	return stats.NewTimings("", help, label)
}

// NewMultiTimings creates a name-spaced equivalent for stats.NewMultiTimings.
// The function currently just returns an unexported variable.
// TODO(sougou): implement.
func NewMultiTimings(instanceName, name string, help string, labels []string) *stats.MultiTimings {
	if instanceName == "" || name == "" {
		return stats.NewMultiTimings(name, help, labels)
	}
	return stats.NewMultiTimings("", help, labels)
}

// NewRates creates a name-spaced equivalent for stats.NewRates.
// The function currently just returns an unexported variable.
// TODO(sougou): implement.
func NewRates(instanceName, name string, countTracker stats.CountTracker, samples int, interval time.Duration) *stats.Rates {
	if instanceName == "" || name == "" {
		return stats.NewRates(name, countTracker, samples, interval)
	}
	return stats.NewRates("", countTracker, samples, interval)
}

// NewHistogram creates a name-spaced equivalent for stats.NewHistogram.
// The function currently just returns an unexported variable.
// TODO(sougou): implement.
func NewHistogram(instanceName, name, help string, cutoffs []int64) *stats.Histogram {
	if instanceName == "" || name == "" {
		return stats.NewHistogram(name, help, cutoffs)
	}
	return stats.NewHistogram("", help, cutoffs)
}

// Publish creates a name-spaced equivalent for stats.Publish.
// The function just passes through if the embedder name is empty.
// TODO(sougou): implement.
func Publish(instanceName, name string, v expvar.Var) {
	if instanceName == "" {
		stats.Publish(name, v)
	}
}
