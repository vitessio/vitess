package janitor

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
)

var janitorRepository = make(map[string]Janitor)

func AvailableModules() (modules []string) {
	for name := range janitorRepository {
		modules = append(modules, name)
	}
	return modules
}

func Register(name string, janitor Janitor) {
	if _, ok := janitorRepository[name]; ok {
		panic("janitor with name " + name + " is already registered")
	}
	janitorRepository[name] = janitor
}

type JanitorWithStatus interface {
	// StatusTemplate should return a string containing the
	// template that should be used to display the status of the
	// janitor. The template should expect to get a JanitorInfo
	// instance wrapping the Janitor itself.
	StatusTemplate() string
}

// Janitor is a module that fixes the topology. If janitor implements
// http.Handle, it will have a page added to /janitorz.
type Janitor interface {
	// Run performs one round of fixes. If active is true, the
	// janitor can perform destructive changes to the
	// topology. Otherwise, it is considered to be in dry run
	// mode, and it should only log what it would have done if it
	// were active.
	Run(active bool) error

	// Initialize initializes the Janitor.
	Configure(ts topo.Server, wr *wrangler.Wrangler, keyspace, shard string) error
}

type Scheduler struct {
	Keyspace string
	Shard    string

	// How long to sleep between janitor runs.
	sleepTime time.Duration
	mux       *http.ServeMux
	janitors  map[string]*JanitorInfo
	ts        topo.Server
	wrangler  *wrangler.Wrangler
}

func New(keyspace, shard string, ts topo.Server, wr *wrangler.Wrangler, sleepTime time.Duration) (*Scheduler, error) {
	if keyspace == "" || shard == "" {
		return nil, errors.New("keyspace and shard cannot be empty")
	}
	return &Scheduler{
		Keyspace:  keyspace,
		Shard:     shard,
		ts:        ts,
		wrangler:  wr,
		sleepTime: sleepTime,
		janitors:  make(map[string]*JanitorInfo),
		mux:       http.NewServeMux(),
	}, nil
}

type JanitorInfo struct {
	Janitor
	Active       bool
	mu           sync.RWMutex
	errorCount   int
	runs         int
	totalRuntime time.Duration
	// todo: history
}

func newJanitorInfo(janitor Janitor) *JanitorInfo {
	return &JanitorInfo{Janitor: janitor}
}

func (ji *JanitorInfo) AverageRuntime() time.Duration {
	ji.mu.RLock()
	defer ji.mu.RUnlock()
	if ji.runs == 0 {
		return 0
	}
	return time.Duration(int(ji.totalRuntime) / ji.runs)
}

func (ji *JanitorInfo) ErrorCount() int {
	ji.mu.RLock()
	defer ji.mu.RUnlock()
	return ji.errorCount
}

func (ji *JanitorInfo) Runs() int {
	ji.mu.RLock()
	defer ji.mu.RUnlock()
	return ji.runs
}

func (ji *JanitorInfo) RecordError(err error) {
	ji.mu.Lock()
	defer ji.mu.Unlock()
	ji.errorCount++
	// todo: add record to history
}

func (ji *JanitorInfo) RecordSuccess(start time.Time) {
	ji.mu.Lock()
	defer ji.mu.Unlock()
	ji.totalRuntime += time.Since(start)
	ji.runs++
	ji.errorCount = 0

	// todo: add record to history
}

func (scheduler *Scheduler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	scheduler.mux.ServeHTTP(w, r)
}

func (scheduler *Scheduler) enable(name string, active bool) error {
	janitor, ok := janitorRepository[name]
	if !ok {
		return fmt.Errorf("janitor not registered: %q", name)
	}
	scheduler.janitors[name] = &JanitorInfo{Janitor: janitor, Active: active}
	return janitor.Configure(scheduler.ts, scheduler.wrangler, scheduler.Keyspace, scheduler.Shard)
}

func (scheduler *Scheduler) Enable(janitorNames []string) error {
	for _, name := range janitorNames {
		if err := scheduler.enable(name, true); err != nil {
			return err
		}
	}
	return nil
}

func (scheduler *Scheduler) EnableDryRun(janitorNames []string) error {
	for _, name := range janitorNames {
		if err := scheduler.enable(name, false); err != nil {
			return err
		}
	}
	return nil
}

func (scheduler *Scheduler) runJanitor(name string) {
	janitor, ok := scheduler.janitors[name]
	if !ok {
		panic("janitor " + name + " not enabled")
	}
	active := janitor.Active && scheduler.IsMaster()
	log.Infof("running janitor %v (active: %v)", name, active)
	start := time.Now()
	if err := janitor.Run(active); err != nil {
		// TODO(szopa): Add some exponential
		// backoff if an error occurs.
		log.Errorf("janitor %v run: %v", name, err)
		janitor.RecordError(err)
		return
	}
	log.Infof("janitor %v run successfully", name)
	janitor.RecordSuccess(start)
}

func (scheduler *Scheduler) Run() {
	// TODO(szopa): Be smarter about how often each janitor should
	// be run (the ones that take longer should run less often).
	for {
		for name := range scheduler.janitors {
			scheduler.runJanitor(name)
			log.Infof("pausing for %v", scheduler.sleepTime)
			time.Sleep(scheduler.sleepTime)
		}
	}
}
