package health

import (
	"fmt"
	"html/template"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/topo"
)

var (
	defaultAggregator *Aggregator
)

func init() {
	defaultAggregator = NewAggregator()
}

// Reporter reports the health status of a tablet.
type Reporter interface {
	// Report returns the replication delay gathered by this
	// module (or 0 if it thinks it's not behind), assuming that
	// its tablet type is TabletType, and that its query service
	// should be running or not. If Report returns an error it
	// implies that the tablet is in a bad shape and not able to
	// handle queries.
	Report(tabletType topo.TabletType, shouldQueryServiceBeRunning bool) (replicationDelay time.Duration, err error)

	// HTMLName returns a displayable name for the module.
	// Can be used to be displayed in the status page.
	HTMLName() template.HTML
}

// FunctionReporter is a function that may act as a Reporter.
type FunctionReporter func(topo.TabletType, bool) (time.Duration, error)

// Report implements Reporter.Report
func (fc FunctionReporter) Report(tabletType topo.TabletType, shouldQueryServiceBeRunning bool) (time.Duration, error) {
	return fc(tabletType, shouldQueryServiceBeRunning)
}

// HTMLName implements Reporter.HTMLName
func (fc FunctionReporter) HTMLName() template.HTML {
	return template.HTML("FunctionReporter")
}

// Aggregator aggregates the results of many Reporters.
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

// Run aggregates health statuses from all the reporters. If any
// errors occur during the reporting, they will be logged, but only
// the first error will be returned.
// The returned replication delay will be the highest of all the replication
// delays returned by the Reporter implementations (although typically
// only one implementation will actually return a meaningful one).
func (ag *Aggregator) Run(tabletType topo.TabletType, shouldQueryServiceBeRunning bool) (time.Duration, error) {
	var (
		wg  sync.WaitGroup
		rec concurrency.AllErrorRecorder
	)

	results := make(chan time.Duration, len(ag.reporters))
	ag.mu.Lock()
	for name, rep := range ag.reporters {
		wg.Add(1)
		go func(name string, rep Reporter) {
			defer wg.Done()
			replicationDelay, err := rep.Report(tabletType, shouldQueryServiceBeRunning)
			if err != nil {
				rec.RecordError(fmt.Errorf("%v: %v", name, err))
				return
			}
			results <- replicationDelay
		}(name, rep)
	}
	ag.mu.Unlock()
	wg.Wait()
	close(results)
	if err := rec.Error(); err != nil {
		return 0, err
	}

	// merge and return the results
	var result time.Duration
	for replicationDelay := range results {
		if replicationDelay > result {
			result = replicationDelay
		}
	}
	return result, nil
}

// Register registers rep with ag. Only keys specified in keys will be
// aggregated from this particular Reporter.
func (ag *Aggregator) Register(name string, rep Reporter) {
	ag.mu.Lock()
	defer ag.mu.Unlock()
	if _, ok := ag.reporters[name]; ok {
		panic("reporter named " + name + " is already registered")
	}
	ag.reporters[name] = rep

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

// Run collects all the health statuses from the default health
// aggregator.
func Run(tabletType topo.TabletType, shouldQueryServiceBeRunning bool) (time.Duration, error) {
	return defaultAggregator.Run(tabletType, shouldQueryServiceBeRunning)
}

// Register registers rep under name with the default health
// aggregator. Only keys specified in keys will be aggregated from
// this particular Reporter.
func Register(name string, rep Reporter) {
	defaultAggregator.Register(name, rep)
}

// HTMLName returns an aggregate name for the default reporter
func HTMLName() template.HTML {
	return defaultAggregator.HTMLName()
}
