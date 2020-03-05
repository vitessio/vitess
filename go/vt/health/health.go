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

package health

import (
	"errors"
	"fmt"
	"html/template"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	// DefaultAggregator is the global aggregator to use for real
	// programs. Use a custom one for tests.
	DefaultAggregator *Aggregator

	// ErrSlaveNotRunning is returned by health plugins when replication
	// is not running and we can't figure out the replication delay.
	// Note everything else should be operational, and the underlying
	// MySQL instance should be capable of answering queries.
	ErrSlaveNotRunning = errors.New("slave is not running")
)

func init() {
	DefaultAggregator = NewAggregator()
}

// Reporter reports the health status of a tablet.
type Reporter interface {
	// Report returns the replication delay gathered by this
	// module (or 0 if it thinks it's not behind), assuming that
	// it is a slave type or not, and that its query service
	// should be running or not. If Report returns an error it
	// implies that the tablet is in a bad shape and not able to
	// handle queries.
	Report(isSlaveType, shouldQueryServiceBeRunning bool) (replicationDelay time.Duration, err error)

	// HTMLName returns a displayable name for the module.
	// Can be used to be displayed in the status page.
	HTMLName() template.HTML
}

// FunctionReporter is a function that may act as a Reporter.
type FunctionReporter func(bool, bool) (time.Duration, error)

// Report implements Reporter.Report
func (fc FunctionReporter) Report(isSlaveType, shouldQueryServiceBeRunning bool) (time.Duration, error) {
	return fc(isSlaveType, shouldQueryServiceBeRunning)
}

// HTMLName implements Reporter.HTMLName
func (fc FunctionReporter) HTMLName() template.HTML {
	return template.HTML("FunctionReporter")
}

// Aggregator aggregates the results of many Reporters.
// It also implements the Reporter interface.
type Aggregator struct {
	// mu protects all fields below its declaration.
	mu        sync.Mutex
	reporters map[string]Reporter
}

// NewAggregator returns a new empty Aggregator
func NewAggregator() *Aggregator {
	return &Aggregator{
		reporters: make(map[string]Reporter),
	}
}

type singleResult struct {
	name  string
	delay time.Duration
	err   error
}

// Report aggregates health statuses from all the reporters. If any
// errors occur during the reporting, they will be logged, but only
// the first error will be returned.
// The returned replication delay will be the highest of all the replication
// delays returned by the Reporter implementations (although typically
// only one implementation will actually return a meaningful one).
func (ag *Aggregator) Report(isSlaveType, shouldQueryServiceBeRunning bool) (time.Duration, error) {
	wg := sync.WaitGroup{}
	results := make([]singleResult, len(ag.reporters))
	index := 0
	ag.mu.Lock()
	for name, rep := range ag.reporters {
		wg.Add(1)
		go func(index int, name string, rep Reporter) {
			defer wg.Done()
			results[index].name = name
			results[index].delay, results[index].err = rep.Report(isSlaveType, shouldQueryServiceBeRunning)
		}(index, name, rep)
		index++
	}
	ag.mu.Unlock()
	wg.Wait()

	// merge and return the results
	var result time.Duration
	var err error
	for _, s := range results {
		switch s.err {
		case ErrSlaveNotRunning:
			// Return the ErrSlaveNotRunning sentinel
			// value, only if there are no other errors.
			err = ErrSlaveNotRunning
		case nil:
			if s.delay > result {
				result = s.delay
			}
		default:
			return 0, fmt.Errorf("%v: %v", s.name, s.err)
		}
	}
	return result, err
}

// Register registers rep with ag.
func (ag *Aggregator) Register(name string, rep Reporter) {
	ag.mu.Lock()
	defer ag.mu.Unlock()
	if _, ok := ag.reporters[name]; ok {
		panic("reporter named " + name + " is already registered")
	}
	ag.reporters[name] = rep
}

// RegisterSimpleCheck registers a simple health check function.
func (ag *Aggregator) RegisterSimpleCheck(name string, check func() error) {
	ag.Register(name, simpleReporter{html: template.HTML(name), check: check})
}

// HTMLName returns an aggregate name for all the reporters
func (ag *Aggregator) HTMLName() template.HTML {
	ag.mu.Lock()
	defer ag.mu.Unlock()
	result := make([]string, 0, len(ag.reporters))
	for _, rep := range ag.reporters {
		result = append(result, string(rep.HTMLName()))
	}
	sort.Strings(result)
	return template.HTML(strings.Join(result, "&nbsp; + &nbsp;"))
}

type simpleReporter struct {
	html  template.HTML
	check func() error
}

func (s simpleReporter) HTMLName() template.HTML {
	return s.html
}

func (s simpleReporter) Report(bool, bool) (time.Duration, error) {
	return 0, s.check()
}
