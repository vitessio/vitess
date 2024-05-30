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

package txthrottler

import (
	"context"
	"math/rand/v2"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/throttler"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	throttlerdatapb "vitess.io/vitess/go/vt/proto/throttlerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// These vars store the functions used to create the topo server, healthcheck,
// and go/vt/throttler. These are provided here so that they can be overridden
// in tests to generate mocks.
type healthCheckFactoryFunc func(topoServer *topo.Server, cell string, cellsToWatch []string) discovery.HealthCheck
type throttlerFactoryFunc func(name, unit string, threadCount int, maxRate int64, maxReplicationLagConfig throttler.MaxReplicationLagModuleConfig) (ThrottlerInterface, error)

var (
	healthCheckFactory healthCheckFactoryFunc
	throttlerFactory   throttlerFactoryFunc
)

func resetTxThrottlerFactories() {
	healthCheckFactory = func(topoServer *topo.Server, cell string, cellsToWatch []string) discovery.HealthCheck {
		return discovery.NewHealthCheck(context.Background(), discovery.DefaultHealthCheckRetryDelay, discovery.DefaultHealthCheckTimeout, topoServer, cell, strings.Join(cellsToWatch, ","))
	}
	throttlerFactory = func(name, unit string, threadCount int, maxRate int64, maxReplicationLagConfig throttler.MaxReplicationLagModuleConfig) (ThrottlerInterface, error) {
		return throttler.NewThrottlerFromConfig(name, unit, threadCount, maxRate, maxReplicationLagConfig, time.Now)
	}
}

func init() {
	resetTxThrottlerFactories()
}

// TxThrottler defines the interface for the transaction throttler.
type TxThrottler interface {
	InitDBConfig(target *querypb.Target)
	Open() (err error)
	Close()
	Throttle(priority int, workload string) (result bool)
}

// ThrottlerInterface defines the public interface that is implemented by go/vt/throttler.Throttler
// It is only used here to allow mocking out a throttler object.
type ThrottlerInterface interface {
	Throttle(threadID int) time.Duration
	ThreadFinished(threadID int)
	Close()
	MaxRate() int64
	SetMaxRate(rate int64)
	RecordReplicationLag(time time.Time, th *discovery.TabletHealth)
	GetConfiguration() *throttlerdatapb.Configuration
	UpdateConfiguration(configuration *throttlerdatapb.Configuration, copyZeroValues bool) error
	ResetConfiguration()
	MaxLag(tabletType topodatapb.TabletType) uint32
}

// TxThrottlerName is the name the wrapped go/vt/throttler object will be registered with
// go/vt/throttler.GlobalManager.
const TxThrottlerName = "TransactionThrottler"

// fetchKnownCells gathers a list of known cells from the topology. On error,
// the cell of the local tablet will be used and an error is logged.
func fetchKnownCells(ctx context.Context, topoServer *topo.Server, target *querypb.Target) []string {
	cells, err := topoServer.GetKnownCells(ctx)
	if err != nil {
		log.Errorf("txThrottler: falling back to local cell due to error fetching cells from topology: %+v", err)
		cells = []string{target.Cell}
	}
	return cells
}

// txThrottler implements TxThrottle for throttling transactions based on replication lag.
// It's a thin wrapper around the throttler found in vitess/go/vt/throttler.
// It uses a discovery.HealthCheck to send replication-lag updates to the wrapped throttler.
//
// Intended Usage:
//
//	// Assuming topoServer is a topo.Server variable pointing to a Vitess topology server.
//	t := NewTxThrottler(config, topoServer)
//
//	// A transaction throttler must be opened before its first use:
//	if err := t.Open(keyspace, shard); err != nil {
//	  return err
//	}
//
//	// Checking whether to throttle can be done as follows before starting a transaction.
//	if t.Throttle() {
//	  return fmt.Errorf("Transaction throttled!")
//	} else {
//	  // execute transaction.
//	}
//
//	// To release the resources used by the throttler the caller should call Close().
//	t.Close()
//
// A txThrottler object is generally not thread-safe: at any given time at most one goroutine should
// be executing a method. The only exception is the 'Throttle' method where multiple goroutines are
// allowed to execute it concurrently.
type txThrottler struct {
	config *tabletenv.TabletConfig

	// state holds an open transaction throttler state. It is nil
	// if the TransactionThrottler is closed.
	state txThrottlerState

	target     *querypb.Target
	topoServer *topo.Server

	// stats
	throttlerRunning          *stats.Gauge
	healthChecksReadTotal     *stats.CountersWithMultiLabels
	healthChecksRecordedTotal *stats.CountersWithMultiLabels
	requestsTotal             *stats.CountersWithSingleLabel
	requestsThrottled         *stats.CountersWithSingleLabel
}

type txThrottlerState interface {
	deallocateResources()
	StatsUpdate(tabletStats *discovery.TabletHealth)
	throttle() bool
}

// txThrottlerStateImpl holds the state of an open TxThrottler object.
type txThrottlerStateImpl struct {
	config      *tabletenv.TabletConfig
	txThrottler *txThrottler

	// throttleMu serializes calls to throttler.Throttler.Throttle(threadId).
	// That method is required to be called in serial for each threadId.
	throttleMu      sync.Mutex
	throttler       ThrottlerInterface
	stopHealthCheck context.CancelFunc

	healthCheck      discovery.HealthCheck
	healthCheckChan  chan *discovery.TabletHealth
	healthCheckCells []string
	cellsFromTopo    bool

	// tabletTypes stores the tablet types for throttling
	tabletTypes map[topodatapb.TabletType]bool

	maxLag             int64
	done               chan bool
	waitForTermination sync.WaitGroup
}

// NewTxThrottler tries to construct a txThrottler from the relevant
// fields in the tabletenv.Env and topo.Server objects.
func NewTxThrottler(env tabletenv.Env, topoServer *topo.Server) TxThrottler {
	config := env.Config()
	if config.EnableTxThrottler {
		if len(config.TxThrottlerHealthCheckCells) == 0 {
			defer log.Infof("Initialized transaction throttler using tabletTypes: %+v, cellsFromTopo: true, topoRefreshInterval: %s, throttlerConfig: %q",
				config.TxThrottlerTabletTypes, config.TxThrottlerTopoRefreshInterval, config.TxThrottlerConfig.Get(),
			)
		} else {
			defer log.Infof("Initialized transaction throttler using tabletTypes: %+v, healthCheckCells: %+v, throttlerConfig: %q",
				config.TxThrottlerTabletTypes, config.TxThrottlerHealthCheckCells, config.TxThrottlerConfig.Get(),
			)
		}
	}

	return &txThrottler{
		config:           config,
		topoServer:       topoServer,
		throttlerRunning: env.Exporter().NewGauge(TxThrottlerName+"Running", "transaction throttler running state"),
		healthChecksReadTotal: env.Exporter().NewCountersWithMultiLabels(TxThrottlerName+"HealthchecksRead", "transaction throttler healthchecks read",
			[]string{"cell", "DbType"}),
		healthChecksRecordedTotal: env.Exporter().NewCountersWithMultiLabels(TxThrottlerName+"HealthchecksRecorded", "transaction throttler healthchecks recorded",
			[]string{"cell", "DbType"}),
		requestsTotal:     env.Exporter().NewCountersWithSingleLabel(TxThrottlerName+"Requests", "transaction throttler requests", "workload"),
		requestsThrottled: env.Exporter().NewCountersWithSingleLabel(TxThrottlerName+"Throttled", "transaction throttler requests throttled", "workload"),
	}
}

// InitDBConfig initializes the target parameters for the throttler.
func (t *txThrottler) InitDBConfig(target *querypb.Target) { t.target = target.CloneVT() }

// Open opens the transaction throttler. It must be called prior to 'Throttle'.
func (t *txThrottler) Open() (err error) {
	if !t.config.EnableTxThrottler {
		return nil
	}
	if t.state != nil {
		return nil
	}
	log.Info("txThrottler: opening")
	t.throttlerRunning.Set(1)
	t.state, err = newTxThrottlerState(t, t.config, t.target)
	return err
}

// Close closes the txThrottler object and releases resources.
// It should be called after the throttler is no longer needed.
// It's ok to call this method on a closed throttler--in which case the method does nothing.
func (t *txThrottler) Close() {
	if !t.config.EnableTxThrottler {
		return
	}
	if t.state == nil {
		return
	}
	t.state.deallocateResources()
	t.state = nil
	t.throttlerRunning.Set(0)
	log.Info("txThrottler: closed")
}

// Throttle should be called before a new transaction is started.
// It returns true if the transaction should not proceed (the caller
// should back off). Throttle requires that Open() was previously called
// successfully.
func (t *txThrottler) Throttle(priority int, workload string) (result bool) {
	if !t.config.EnableTxThrottler {
		return false
	}
	if t.state == nil {
		return false
	}

	// Throttle according to both what the throttler state says and the priority. Workloads with lower priority value
	// are less likely to be throttled.
	result = rand.IntN(sqlparser.MaxPriorityValue) < priority && t.state.throttle()

	t.requestsTotal.Add(workload, 1)
	if result {
		t.requestsThrottled.Add(workload, 1)
	}

	return result && !t.config.TxThrottlerDryRun
}

func newTxThrottlerState(txThrottler *txThrottler, config *tabletenv.TabletConfig, target *querypb.Target) (txThrottlerState, error) {
	maxReplicationLagModuleConfig := throttler.MaxReplicationLagModuleConfig{Configuration: config.TxThrottlerConfig.Get()}

	t, err := throttlerFactory(
		TxThrottlerName,
		"TPS",                           /* unit */
		1,                               /* threadCount */
		throttler.MaxRateModuleDisabled, /* maxRate */
		maxReplicationLagModuleConfig,
	)
	if err != nil {
		return nil, err
	}
	if err := t.UpdateConfiguration(config.TxThrottlerConfig.Get(), true /* copyZeroValues */); err != nil {
		t.Close()
		return nil, err
	}

	tabletTypes := make(map[topodatapb.TabletType]bool, len(*config.TxThrottlerTabletTypes))
	for _, tabletType := range *config.TxThrottlerTabletTypes {
		tabletTypes[tabletType] = true
	}

	state := &txThrottlerStateImpl{
		config:           config,
		healthCheckCells: config.TxThrottlerHealthCheckCells,
		tabletTypes:      tabletTypes,
		throttler:        t,
		txThrottler:      txThrottler,
		done:             make(chan bool, 1),
	}

	// get cells from topo if none defined in tabletenv config
	if len(state.healthCheckCells) == 0 {
		ctx, cancel := context.WithTimeout(context.Background(), topo.RemoteOperationTimeout)
		defer cancel()
		state.healthCheckCells = fetchKnownCells(ctx, txThrottler.topoServer, target)
		state.cellsFromTopo = true
	}

	ctx, cancel := context.WithCancel(context.Background())
	state.stopHealthCheck = cancel
	state.initHealthCheckStream(txThrottler.topoServer, target)
	go state.healthChecksProcessor(ctx, txThrottler.topoServer, target)
	state.waitForTermination.Add(1)
	go state.updateMaxLag()

	return state, nil
}

func (ts *txThrottlerStateImpl) initHealthCheckStream(topoServer *topo.Server, target *querypb.Target) {
	ts.healthCheck = healthCheckFactory(topoServer, target.Cell, ts.healthCheckCells)
	ts.healthCheckChan = ts.healthCheck.Subscribe()

}

func (ts *txThrottlerStateImpl) closeHealthCheckStream() {
	if ts.healthCheck == nil {
		return
	}
	ts.stopHealthCheck()
	ts.healthCheck.Close()
}

func (ts *txThrottlerStateImpl) updateHealthCheckCells(ctx context.Context, topoServer *topo.Server, target *querypb.Target) {
	fetchCtx, cancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer cancel()

	knownCells := fetchKnownCells(fetchCtx, topoServer, target)
	if !reflect.DeepEqual(knownCells, ts.healthCheckCells) {
		log.Info("txThrottler: restarting healthcheck stream due to topology cells update")
		ts.healthCheckCells = knownCells
		ts.closeHealthCheckStream()
		ts.initHealthCheckStream(topoServer, target)
	}
}

func (ts *txThrottlerStateImpl) healthChecksProcessor(ctx context.Context, topoServer *topo.Server, target *querypb.Target) {
	var cellsUpdateTicks <-chan time.Time
	if ts.cellsFromTopo {
		ticker := time.NewTicker(ts.config.TxThrottlerTopoRefreshInterval)
		cellsUpdateTicks = ticker.C
		defer ticker.Stop()
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-cellsUpdateTicks:
			ts.updateHealthCheckCells(ctx, topoServer, target)
		case th := <-ts.healthCheckChan:
			ts.StatsUpdate(th)
		}
	}
}

func (ts *txThrottlerStateImpl) throttle() bool {
	if ts.throttler == nil {
		log.Error("txThrottler: throttle called after deallocateResources was called")
		return false
	}
	// Serialize calls to ts.throttle.Throttle()
	ts.throttleMu.Lock()
	defer ts.throttleMu.Unlock()

	maxLag := atomic.LoadInt64(&ts.maxLag)

	return maxLag > ts.config.TxThrottlerConfig.TargetReplicationLagSec &&
		ts.throttler.Throttle(0 /* threadId */) > 0
}

func (ts *txThrottlerStateImpl) updateMaxLag() {
	defer ts.waitForTermination.Done()
	// We use half of the target lag to ensure we have enough resolution to see changes in lag below that value
	ticker := time.NewTicker(time.Duration(ts.config.TxThrottlerConfig.TargetReplicationLagSec/2) * time.Second)
	defer ticker.Stop()
outerloop:
	for {
		select {
		case <-ticker.C:
			var maxLag uint32

			for tabletType := range ts.tabletTypes {
				maxLagPerTabletType := ts.throttler.MaxLag(tabletType)
				if maxLagPerTabletType > maxLag {
					maxLag = maxLagPerTabletType
				}
			}
			atomic.StoreInt64(&ts.maxLag, int64(maxLag))
		case <-ts.done:
			break outerloop
		}
	}
}

func (ts *txThrottlerStateImpl) deallocateResources() {
	// Close healthcheck and topo watchers
	ts.closeHealthCheckStream()
	ts.healthCheck = nil

	ts.done <- true
	ts.waitForTermination.Wait()
	// After ts.healthCheck is closed txThrottlerStateImpl.StatsUpdate() is guaranteed not
	// to be executing, so we can safely close the throttler.
	ts.throttler.Close()
	ts.throttler = nil
}

// StatsUpdate updates the health of a tablet with the given healthcheck.
func (ts *txThrottlerStateImpl) StatsUpdate(tabletStats *discovery.TabletHealth) {
	if len(ts.tabletTypes) == 0 {
		return
	}

	tabletType := tabletStats.Target.TabletType
	metricLabels := []string{tabletStats.Target.Cell, tabletType.String()}
	ts.txThrottler.healthChecksReadTotal.Add(metricLabels, 1)

	// Monitor tablets for replication lag if they have a tablet
	// type specified by the --tx-throttler-tablet-types flag.
	if ts.tabletTypes[tabletType] {
		ts.throttler.RecordReplicationLag(time.Now(), tabletStats)
		ts.txThrottler.healthChecksRecordedTotal.Add(metricLabels, 1)
	}
}
