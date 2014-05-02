package health

import (
	"fmt"
	"strings"
	"sync"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/topo"
)

var (
	defaultAggregator *Aggregator
)

const (
	// ReplicationLag should be the key for any reporters
	// reporting MySQL repliaction lag.
	ReplicationLag = "replication_lag"

	// ReplicationLagHigh should be the value for any reporters
	// indicating that the replication lag is too high.
	ReplicationLagHigh = "high"
)

func init() {
	defaultAggregator = NewAggregator()
}

// Reporter reports the health status of a tablet.
type Reporter interface {
	// Report returns a map of health states for the tablet
	// assuming that its tablet type is typ. If Report returns an
	// error it implies that the tablet is in a bad shape and not
	// able to handle queries.
	Report(typ topo.TabletType) (status map[string]string, err error)

	// HTMLName returns a displayable name for the module.
	// Can be used to be displayed in the status page.
	HTMLName() string
}

// FunctionReporter is a function that may act as a Reporter.
type FunctionReporter func(typ topo.TabletType) (map[string]string, error)

// Report implements Reporter.Report
func (fc FunctionReporter) Report(typ topo.TabletType) (status map[string]string, err error) {
	return fc(typ)
}

// HTMLName implements Reporter.HTMLName
func (fc FunctionReporter) HTMLName() string {
	return "FunctionReporter"
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

// Run runs aggregates health statuses from all the reporters. If any
// errors occur during the reporting, they will be logged, but only
// the first error will be returned.
// It may return an empty map if no health condition is detected. Note
// it will not return nil, but an empty map.
func (ag *Aggregator) Run(typ topo.TabletType) (map[string]string, error) {
	var (
		wg  sync.WaitGroup
		rec concurrency.AllErrorRecorder
	)

	results := make(chan map[string]string, len(ag.reporters))
	ag.mu.Lock()
	for name, rep := range ag.reporters {
		name, rep := name, rep
		wg.Add(1)

		go func() {
			defer wg.Done()
			status, err := rep.Report(typ)
			if err != nil {
				log.Errorf("reporter %v: %v", name, err)
				rec.RecordError(err)
				return
			}
			results <- status
		}()
	}
	ag.mu.Unlock()
	wg.Wait()
	close(results)
	if err := rec.Error(); err != nil {
		return nil, err
	}

	// merge and return the results
	result := make(map[string]string)
	for part := range results {
		for k, v := range part {
			if _, ok := result[k]; ok {
				return nil, fmt.Errorf("duplicate key: %v", k)
			}
			result[k] = v
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
func (ag *Aggregator) HTMLName() string {
	ag.mu.Lock()
	defer ag.mu.Unlock()
	result := make([]string, 0, len(ag.reporters))
	for _, rep := range ag.reporters {
		result = append(result, rep.HTMLName())
	}
	return strings.Join(result, "&nbsp; + &nbsp;")
}

// Run collects all the health statuses from the default health
// aggregator.
func Run(typ topo.TabletType) (map[string]string, error) {
	return defaultAggregator.Run(typ)
}

// Register registers rep under name with the default health
// aggregator. Only keys specified in keys will be aggregated from
// this particular Reporter.
func Register(name string, rep Reporter) {
	defaultAggregator.Register(name, rep)
}

// HTMLName returns an aggregate name for the default reporter
func HTMLName() string {
	return defaultAggregator.HTMLName()
}
