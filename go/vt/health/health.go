package health

import (
	"fmt"
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
}

// FunctionReporter is a function that may act as a Reporter.
type FunctionReporter func(typ topo.TabletType) (map[string]string, error)

func (fc FunctionReporter) Report(typ topo.TabletType) (status map[string]string, err error) {
	return fc(typ)
}

// Aggregator aggregates the results of many Reporters.
type Aggregator struct {
	mu        sync.Mutex
	reporters map[string]Reporter
}

func NewAggregator() *Aggregator {
	return &Aggregator{reporters: make(map[string]Reporter)}
}

// Run runs aggregates health statuses from all the reporters. If any
// errors occur during the reporting, they will be logged, but only
// the first error will be returned.
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
