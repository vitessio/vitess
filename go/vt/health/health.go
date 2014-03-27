package health

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/history"
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

	historyLength = 16
)

const (
	Healthy   = "healthy"
	Unhappy   = "unhappy"
	Unhealthy = "unhealthy"
	Unknown   = "unknown"
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
	History *history.History

	// mu protects all fields below its declaration.
	mu        sync.Mutex
	reporters map[string]Reporter
}

func NewAggregator() *Aggregator {
	return &Aggregator{
		History:   history.New(historyLength),
		reporters: make(map[string]Reporter),
	}
}

// Record records one run of an aggregator.
type Record struct {
	Error  error
	Result map[string]string
	Time   time.Time
}

func (r Record) Class() string {
	switch {
	case r.Error != nil:
		return Unhealthy
	case len(r.Result) == 0:
		return Unhappy
	default:
		return Healthy
	}
}

func (r Record) IsDuplicate(other interface{}) bool {
	rother, ok := other.(Record)
	if !ok {
		return false
	}
	return reflect.DeepEqual(r.Error, rother.Error) && reflect.DeepEqual(r.Result, rother.Result)
}

// Run runs aggregates health statuses from all the reporters. If any
// errors occur during the reporting, they will be logged, but only
// the first error will be returned.
func (ag *Aggregator) Run(typ topo.TabletType) (map[string]string, error) {
	var (
		wg  sync.WaitGroup
		rec concurrency.AllErrorRecorder
	)

	record := Record{
		Time:   time.Now(),
		Result: make(map[string]string),
	}

	defer ag.History.Add(record)

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
	if record.Error = rec.Error(); record.Error != nil {
		return nil, record.Error
	}
	for part := range results {
		for k, v := range part {
			if _, ok := record.Result[k]; ok {
				return nil, fmt.Errorf("duplicate key: %v", k)
			}
			record.Result[k] = v
		}
	}
	return record.Result, nil
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

// History returns the health records from the default health
// aggregator.
func History() []interface{} {
	return defaultAggregator.History.Records()
}
