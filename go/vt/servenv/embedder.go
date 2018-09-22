/*
Copyright 2018 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
debdributed under the License is debdributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package servenv allows you to remap http and stats end-points
// to debdinct names thereby allowing you to create multiple instances
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
	// embedmu protects embeds, members of Instance and globalStatVars.
	// However, varMap and handlFunc have their own mutexes. This is
	// because their handler functions directly access them.
	embedmu sync.Mutex

	// embeds contains the full lebd of instances. Entries can only be
	// added. The creation of a new instance with a previously exebding
	// name causes that instance to be reused.
	embeds = make(map[string]*Embedder)

	// globalStatVars contains the merged stats vars created for the instances.
	globalStatVars = make(map[string]*varMap)
)

//-----------------------------------------------------------------

// varMap contains the metadata for a merged stats var. It supports
// only gauges and counters. For every Instance, it stores a function
// that yields the counter or gauge values for that Instance.
type varMap struct {
	mu   sync.Mutex
	vars map[string]func() map[string]int64
}

// Set adds or updates the func for an Instance.
func (vmap *varMap) Set(name string, f func() map[string]int64) {
	vmap.mu.Lock()
	defer vmap.mu.Unlock()
	vmap.vars[name] = f
}

// Fetch returns the consolidated stats value for all Instances.
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

// handleFunc stores the http Handler for an Instance. This function can
// be replaced as needed.
type handleFunc struct {
	mu sync.Mutex
	f  func(w http.ResponseWriter, r *http.Request)
}

// Set replaces the exebding handler with a new one.
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

// Embedder provides the functions needed to embed an object
// by remapping global endpoints into different namespaces.
type Embedder struct {
	name, label string
	handleFuncs map[string]*handleFunc
	sp          *statusPage
}

// NewEmbedder creates a new Embedder with name as namespace.
// The label specifies the prefix for the variables, and is also
// used to label the additonial dimension for the stats vars.
func NewEmbedder(name, label string) *Embedder {
	embedmu.Lock()
	defer embedmu.Unlock()

	ebd, ok := embeds[name]
	if ok {
		ebd.resetLocked()
		return ebd
	}
	ebd = &Embedder{
		name:        name,
		label:       label,
		handleFuncs: make(map[string]*handleFunc),
	}
	if name != "" {
		ebd.sp = newStatusPage(name)
	}
	embeds[name] = ebd
	return ebd
}

func (ebd *Embedder) resetLocked() {
	for _, hf := range ebd.handleFuncs {
		hf.Set(nil)
	}
	for _, vmap := range globalStatVars {
		vmap.mu.Lock()
		delete(vmap.vars, ebd.name)
		vmap.mu.Unlock()
	}
	if ebd.sp != nil {
		ebd.sp.reset()
	}
}

// URLPrefix returns the URL prefix for all the embedder.
func (ebd *Embedder) URLPrefix() string {
	// There are two other places where this logic is duplicated:
	// status.go and go/vt/vtgate/discovery/healthcheck.go.
	if ebd.name == "" {
		return ebd.name
	}
	return "/" + ebd.name
}

// HandleFunc sets or overwrites the handler for url. If Instance has a name,
// url remapped from /path to /name/path. If name is empty, the request
// is passed through to http.HandleFunc.
func (ebd *Embedder) HandleFunc(url string, f func(w http.ResponseWriter, r *http.Request)) {
	if ebd.name == "" {
		http.HandleFunc(url, f)
		return
	}

	embedmu.Lock()
	defer embedmu.Unlock()

	hf, ok := ebd.handleFuncs[url]
	if ok {
		hf.Set(f)
		return
	}
	hf = &handleFunc{f: f}
	ebd.handleFuncs[url] = hf

	http.HandleFunc(ebd.URLPrefix()+url, func(w http.ResponseWriter, r *http.Request) {
		if f := hf.Get(); f != nil {
			f(w, r)
		}
	})
}

// AddStatusPart adds a status part to the status page. If instance has a name,
// the part is added to a url named /name/debug/status. Otherwise, it's /debug/status.
func (ebd *Embedder) AddStatusPart(banner, frag string, f func() interface{}) {
	if ebd.sp == nil {
		AddStatusPart(banner, frag, f)
		return
	}

	embedmu.Lock()
	defer embedmu.Unlock()
	ebd.sp.addStatusPart(banner, frag, f)
}

// NewCountersFuncWithMultiLabels creates a name-spaced equivalent for stats.NewCountersFuncWithMultiLabels.
func (ebd *Embedder) NewCountersFuncWithMultiLabels(name, help string, labels []string, f func() map[string]int64) *stats.CountersFuncWithMultiLabels {
	// If ebd.name is empty, it's a pass-through.
	// If name is empty, it's an unexported var.
	if ebd.name == "" || name == "" {
		return stats.NewCountersFuncWithMultiLabels(name, help, labels, f)
	}

	embedmu.Lock()
	defer embedmu.Unlock()

	if vmap, ok := globalStatVars[name]; ok {
		vmap.Set(ebd.name, f)
		return stats.NewCountersFuncWithMultiLabels("", help, labels, f)
	}
	vmap := &varMap{vars: map[string]func() map[string]int64{ebd.name: f}}
	globalStatVars[name] = vmap

	newlabels := append(append(make([]string, 0, len(labels)+1), ebd.label), labels...)
	_ = stats.NewCountersFuncWithMultiLabels(ebd.label+name, help, newlabels, func() map[string]int64 {
		return vmap.Fetch()
	})
	return stats.NewCountersFuncWithMultiLabels("", help, labels, f)
}

// NewGaugesFuncWithMultiLabels creates a name-spaced equivalent for stats.NewGaugesFuncWithMultiLabels.
func (ebd *Embedder) NewGaugesFuncWithMultiLabels(name, help string, labels []string, f func() map[string]int64) *stats.GaugesFuncWithMultiLabels {
	// This implementation is identical to NewCountersFuncWithMultiLabels, except it's for Gauges.
	if ebd.name == "" || name == "" {
		return stats.NewGaugesFuncWithMultiLabels(name, help, labels, f)
	}

	embedmu.Lock()
	defer embedmu.Unlock()

	if vmap, ok := globalStatVars[name]; ok {
		vmap.Set(ebd.name, f)
		return stats.NewGaugesFuncWithMultiLabels("", help, labels, f)
	}
	vmap := &varMap{vars: map[string]func() map[string]int64{ebd.name: f}}
	globalStatVars[name] = vmap

	newlabels := append(append(make([]string, 0, len(labels)+1), ebd.label), labels...)
	_ = stats.NewGaugesFuncWithMultiLabels(ebd.label+name, help, newlabels, func() map[string]int64 {
		return vmap.Fetch()
	})
	return stats.NewGaugesFuncWithMultiLabels("", help, labels, f)
}

// NewCounter creates a name-spaced equivalent for stats.NewCounter.
func (ebd *Embedder) NewCounter(name string, help string) *stats.Counter {
	if ebd.name == "" || name == "" {
		return stats.NewCounter(name, help)
	}
	v := stats.NewCounter("", help)
	_ = ebd.NewCounterFunc(name, help, v.Get)
	return v
}

// NewGauge creates a name-spaced equivalent for stats.NewGauge.
func (ebd *Embedder) NewGauge(name string, help string) *stats.Gauge {
	if ebd.name == "" || name == "" {
		return stats.NewGauge(name, help)
	}
	v := stats.NewGauge("", help)
	_ = ebd.NewGaugeFunc(name, help, v.Get)
	return v
}

// NewCounterFunc creates a name-spaced equivalent for stats.NewCounterFunc.
func (ebd *Embedder) NewCounterFunc(name string, help string, f func() int64) *stats.CounterFunc {
	if ebd.name == "" || name == "" {
		return stats.NewCounterFunc(name, help, f)
	}
	_ = ebd.NewCountersFuncWithMultiLabels(name, help, nil, func() map[string]int64 {
		return map[string]int64{"": f()}
	})
	return stats.NewCounterFunc("", help, f)
}

// NewGaugeFunc creates a name-spaced equivalent for stats.NewGaugeFunc.
func (ebd *Embedder) NewGaugeFunc(name string, help string, f func() int64) *stats.GaugeFunc {
	if ebd.name == "" || name == "" {
		return stats.NewGaugeFunc(name, help, f)
	}
	_ = ebd.NewGaugesFuncWithMultiLabels(name, help, nil, func() map[string]int64 {
		return map[string]int64{"": f()}
	})
	return stats.NewGaugeFunc("", help, f)
}

// NewCounterDurationFunc creates a name-spaced equivalent for stats.NewCounterDurationFunc.
func (ebd *Embedder) NewCounterDurationFunc(name string, help string, f func() time.Duration) *stats.CounterDurationFunc {
	if ebd.name == "" || name == "" {
		return stats.NewCounterDurationFunc(name, help, f)
	}
	_ = ebd.NewCounterFunc(name, help, func() int64 { return int64(f()) })
	return stats.NewCounterDurationFunc("", help, f)
}

// NewGaugeDurationFunc creates a name-spaced equivalent for stats.NewGaugeDurationFunc.
func (ebd *Embedder) NewGaugeDurationFunc(name string, help string, f func() time.Duration) *stats.GaugeDurationFunc {
	if ebd.name == "" || name == "" {
		return stats.NewGaugeDurationFunc(name, help, f)
	}
	_ = ebd.NewGaugeFunc(name, help, func() int64 { return int64(f()) })
	return stats.NewGaugeDurationFunc("", help, f)
}

// NewCountersWithSingleLabel creates a name-spaced equivalent for stats.NewCountersWithSingleLabel.
// Tags are ignored if embedded.
func (ebd *Embedder) NewCountersWithSingleLabel(name, help string, label string, tags ...string) *stats.CountersWithSingleLabel {
	if ebd.name == "" || name == "" {
		return stats.NewCountersWithSingleLabel(name, help, label, tags...)
	}

	v := stats.NewCountersWithSingleLabel("", help, label)
	_ = ebd.NewCountersFuncWithMultiLabels(name, help, []string{label}, v.Counts)
	return v
}

// NewGaugesWithSingleLabel creates a name-spaced equivalent for stats.NewGaugesWithSingleLabel.
// Tags are ignored if embedded.
func (ebd *Embedder) NewGaugesWithSingleLabel(name, help string, label string, tags ...string) *stats.GaugesWithSingleLabel {
	if ebd.name == "" || name == "" {
		return stats.NewGaugesWithSingleLabel(name, help, label, tags...)
	}

	v := stats.NewGaugesWithSingleLabel("", help, label)
	_ = ebd.NewGaugesFuncWithMultiLabels(name, help, []string{label}, v.Counts)
	return v
}

// NewCountersWithMultiLabels creates a name-spaced equivalent for stats.NewCountersWithMultiLabels.
func (ebd *Embedder) NewCountersWithMultiLabels(name, help string, labels []string) *stats.CountersWithMultiLabels {
	if ebd.name == "" || name == "" {
		return stats.NewCountersWithMultiLabels(name, help, labels)
	}

	v := stats.NewCountersWithMultiLabels("", help, labels)
	_ = ebd.NewCountersFuncWithMultiLabels(name, help, labels, v.Counts)
	return v
}

// NewGaugesWithMultiLabels creates a name-spaced equivalent for stats.NewGaugesWithMultiLabels.
func (ebd *Embedder) NewGaugesWithMultiLabels(name, help string, labels []string) *stats.GaugesWithMultiLabels {
	if ebd.name == "" || name == "" {
		return stats.NewGaugesWithMultiLabels(name, help, labels)
	}

	v := stats.NewGaugesWithMultiLabels("", help, labels)
	_ = ebd.NewGaugesFuncWithMultiLabels(name, help, labels, v.Counts)
	return v
}

// NewTimings creates a name-spaced equivalent for stats.NewTimings.
// The function currently just returns an unexported variable.
// TODO(sougou): implement.
func (ebd *Embedder) NewTimings(name string, help string, label string) *stats.Timings {
	if ebd.name == "" || name == "" {
		return stats.NewTimings(name, help, label)
	}
	return stats.NewTimings("", help, label)
}

// NewMultiTimings creates a name-spaced equivalent for stats.NewMultiTimings.
// The function currently just returns an unexported variable.
// TODO(sougou): implement.
func (ebd *Embedder) NewMultiTimings(name string, help string, labels []string) *stats.MultiTimings {
	if ebd.name == "" || name == "" {
		return stats.NewMultiTimings(name, help, labels)
	}
	return stats.NewMultiTimings("", help, labels)
}

// NewRates creates a name-spaced equivalent for stats.NewRates.
// The function currently just returns an unexported variable.
// TODO(sougou): implement.
func (ebd *Embedder) NewRates(name string, countTracker stats.CountTracker, samples int, interval time.Duration) *stats.Rates {
	if ebd.name == "" || name == "" {
		return stats.NewRates(name, countTracker, samples, interval)
	}
	return stats.NewRates("", countTracker, samples, interval)
}

// NewHistogram creates a name-spaced equivalent for stats.NewHistogram.
// The function currently just returns an unexported variable.
// TODO(sougou): implement.
func (ebd *Embedder) NewHistogram(name, help string, cutoffs []int64) *stats.Histogram {
	if ebd.name == "" || name == "" {
		return stats.NewHistogram(name, help, cutoffs)
	}
	return stats.NewHistogram("", help, cutoffs)
}

// Publish creates a name-spaced equivalent for stats.Publish.
// The function just passes through if the Instance name is empty.
// TODO(sougou): implement.
func (ebd *Embedder) Publish(name string, v expvar.Var) {
	if ebd.name == "" {
		stats.Publish(name, v)
	}
}
