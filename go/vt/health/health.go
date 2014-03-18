package health

import (
	"sync"

	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/topo"
)

var (
	defaultAggregator *Aggregator
)

func init() {
	defaultAggregator = NewAggregator()
}

type Aggregator struct {
	mu        sync.Mutex
	reporters map[string]Reporter
}

func NewAggregator() *Aggregator {
	return &Aggregator{reporters: make(map[string]Reporter)}
}

func (ag *Aggregator) Run(typ topo.TabletType) (Status, error) {
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
				rec.RecordError(err)
			}
			res := make(map[string]string)
			for k, v := range status {
				res[name+"."+k] = v
			}
			results <- res
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
			result[k] = v
		}
	}
	return result, nil
}

func (ag *Aggregator) Register(name string, rep Reporter) {
	ag.reporters[name] = rep
}

func Run(typ topo.TabletType) (Status, error) {
	return defaultAggregator.Run(typ)
}
func Register(name string, rep Reporter) {
	defaultAggregator.Register(name, string)
}

type Status map[string]string

type Reporter interface {
	// Report returns a map of health states for the tablet
	// assuming that its tablet type is typ.
	Report(typ topo.TabletType) (status Status, err error)
}

func Register(name string, rep Reporter) {
}

type FunctionReporter func(typ topo.TabletType) (Status, error)

func (fc FunctionReporter) Report(typ topo.TabletType) (Status, error) {
	return fc(typ)
}
