/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package txthrottler

import (
	"context"
	"math/rand"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

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
// topology watchers and go/vt/throttler. These are provided here so that they can be overridden
// in tests to generate mocks.
type healthCheckFactoryFunc func(topoServer *topo.Server, cell string, cellsToWatch []string) discovery.HealthCheck
type topologyWatcherFactoryFunc func(topoServer *topo.Server, hc discovery.HealthCheck, cell, keyspace, shard string, refreshInterval time.Duration, topoReadConcurrency int) TopologyWatcherInterface
type throttlerFactoryFunc func(name, unit string, threadCount int, maxRate int64, maxReplicationLagConfig throttler.MaxReplicationLagModuleConfig) (ThrottlerInterface, error)

var (
	healthCheckFactory     healthCheckFactoryFunc
	topologyWatcherFactory topologyWatcherFactoryFunc
	throttlerFactory       throttlerFactoryFunc
)

func resetTxThrottlerFactories() {
	healthCheckFactory = func(topoServer *topo.Server, cell string, cellsToWatch []string) discovery.HealthCheck {
		return discovery.NewHealthCheck(context.Background(), discovery.DefaultHealthCheckRetryDelay, discovery.DefaultHealthCheckTimeout, topoServer, cell, strings.Join(cellsToWatch, ","))
	}
	topologyWatcherFactory = func(topoServer *topo.Server, hc discovery.HealthCheck, cell, keyspace, shard string, refreshInterval time.Duration, topoReadConcurrency int) TopologyWatcherInterface {
		return discovery.NewCellTabletsWatcher(context.Background(), topoServer, hc, discovery.NewFilterByKeyspace([]string{keyspace}), cell, refreshInterval, true, topoReadConcurrency)
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
}

// TopologyWatcherInterface defines the public interface that is implemented by
// discovery.LegacyTopologyWatcher. It is only used here to allow mocking out
// go/vt/discovery.LegacyTopologyWatcher.
type TopologyWatcherInterface interface {
	Start()
	Stop()
}

// TxThrottlerName is the name the wrapped go/vt/throttler object will be registered with
// go/vt/throttler.GlobalManager.
const TxThrottlerName = "TransactionThrottler"

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
	// config stores the transaction throttler's configuration.
	// It is populated in NewTxThrottler and is not modified
	// since.
	config *txThrottlerConfig

	// state holds an open transaction throttler state. It is nil
	// if the TransactionThrottler is closed.
	state *txThrottlerState

	target     *querypb.Target
	topoServer *topo.Server

	// stats
	throttlerRunning          *stats.Gauge
	topoWatchers              *stats.GaugesWithSingleLabel
	healthChecksReadTotal     *stats.CountersWithMultiLabels
	healthChecksRecordedTotal *stats.CountersWithMultiLabels
	requestsTotal             *stats.CountersWithSingleLabel
	requestsThrottled         *stats.CountersWithSingleLabel
}

// txThrottlerConfig holds the parameters that need to be
// passed when constructing a TxThrottler object.
type txThrottlerConfig struct {
	// enabled is true if the transaction throttler is enabled. All methods
	// of a disabled transaction throttler do nothing and Throttle() always
	// returns false.
	enabled bool

	throttlerConfig *throttlerdatapb.Configuration
	// healthCheckCells stores the cell names in which running vttablets will be monitored for
	// replication lag.
	healthCheckCells []string

	// tabletTypes stores the tablet types for throttling
	tabletTypes map[topodatapb.TabletType]bool
}

// txThrottlerState holds the state of an open TxThrottler object.
type txThrottlerState struct {
	config      *txThrottlerConfig
	txThrottler *txThrottler

	// throttleMu serializes calls to throttler.Throttler.Throttle(threadId).
	// That method is required to be called in serial for each threadId.
	throttleMu      sync.Mutex
	throttler       ThrottlerInterface
	stopHealthCheck context.CancelFunc

	healthCheck      discovery.HealthCheck
	topologyWatchers map[string]TopologyWatcherInterface
}

// NewTxThrottler tries to construct a txThrottler from the
// relevant fields in the tabletenv.Config object. It returns a disabled TxThrottler if
// any error occurs.
// This function calls tryCreateTxThrottler that does the actual creation work
// and returns an error if one occurred.
func NewTxThrottler(env tabletenv.Env, topoServer *topo.Server) TxThrottler {
	throttlerConfig := &txThrottlerConfig{enabled: false}

	if env.Config().EnableTxThrottler {
		// Clone tsv.TxThrottlerHealthCheckCells so that we don't assume tsv.TxThrottlerHealthCheckCells
		// is immutable.
		healthCheckCells := env.Config().TxThrottlerHealthCheckCells

		tabletTypes := make(map[topodatapb.TabletType]bool, len(*env.Config().TxThrottlerTabletTypes))
		for _, tabletType := range *env.Config().TxThrottlerTabletTypes {
			tabletTypes[tabletType] = true
		}

		throttlerConfig = &txThrottlerConfig{
			enabled:          true,
			tabletTypes:      tabletTypes,
			throttlerConfig:  env.Config().TxThrottlerConfig.Get(),
			healthCheckCells: healthCheckCells,
		}

		defer log.Infof("Initialized transaction throttler with config: %+v", throttlerConfig)
	}

	return &txThrottler{
		config:           throttlerConfig,
		topoServer:       topoServer,
		throttlerRunning: env.Exporter().NewGauge("TransactionThrottlerRunning", "transaction throttler running state"),
		topoWatchers:     env.Exporter().NewGaugesWithSingleLabel("TransactionThrottlerTopoWatchers", "transaction throttler topology watchers", "cell"),
		healthChecksReadTotal: env.Exporter().NewCountersWithMultiLabels("TransactionThrottlerHealthchecksRead", "transaction throttler healthchecks read",
			[]string{"cell", "DbType"}),
		healthChecksRecordedTotal: env.Exporter().NewCountersWithMultiLabels("TransactionThrottlerHealthchecksRecorded", "transaction throttler healthchecks recorded",
			[]string{"cell", "DbType"}),
		requestsTotal:     env.Exporter().NewCountersWithSingleLabel("TransactionThrottlerRequests", "transaction throttler requests", "workload"),
		requestsThrottled: env.Exporter().NewCountersWithSingleLabel("TransactionThrottlerThrottled", "transaction throttler requests throttled", "workload"),
	}
}

// InitDBConfig initializes the target parameters for the throttler.
func (t *txThrottler) InitDBConfig(target *querypb.Target) {
	t.target = proto.Clone(target).(*querypb.Target)
}

// Open opens the transaction throttler. It must be called prior to 'Throttle'.
func (t *txThrottler) Open() (err error) {
	if !t.config.enabled {
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
	if !t.config.enabled {
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
	if !t.config.enabled {
		return false
	}
	if t.state == nil {
		return false
	}

	// Throttle according to both what the throttler state says and the priority. Workloads with lower priority value
	// are less likely to be throttled.
	result = t.state.throttle() && rand.Intn(sqlparser.MaxPriorityValue) < priority

	t.requestsTotal.Add(workload, 1)
	if result {
		t.requestsThrottled.Add(workload, 1)
	}

	return result
}

func newTxThrottlerState(txThrottler *txThrottler, config *txThrottlerConfig, target *querypb.Target) (*txThrottlerState, error) {
	maxReplicationLagModuleConfig := throttler.MaxReplicationLagModuleConfig{Configuration: config.throttlerConfig}

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
	if err := t.UpdateConfiguration(config.throttlerConfig, true /* copyZeroValues */); err != nil {
		t.Close()
		return nil, err
	}
	state := &txThrottlerState{
		config:      config,
		throttler:   t,
		txThrottler: txThrottler,
	}
	createTxThrottlerHealthCheck(txThrottler.topoServer, config, state, target.Cell)

	state.topologyWatchers = make(
		map[string]TopologyWatcherInterface, len(config.healthCheckCells))
	for _, cell := range config.healthCheckCells {
		state.topologyWatchers[cell] = topologyWatcherFactory(
			txThrottler.topoServer,
			state.healthCheck,
			cell,
			target.Keyspace,
			target.Shard,
			discovery.DefaultTopologyWatcherRefreshInterval,
			discovery.DefaultTopoReadConcurrency,
		)
		txThrottler.topoWatchers.Add(cell, 1)
	}
	return state, nil
}

func createTxThrottlerHealthCheck(topoServer *topo.Server, config *txThrottlerConfig, result *txThrottlerState, cell string) {
	ctx, cancel := context.WithCancel(context.Background())
	result.stopHealthCheck = cancel
	result.healthCheck = healthCheckFactory(topoServer, cell, config.healthCheckCells)
	ch := result.healthCheck.Subscribe()
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case th := <-ch:
				result.StatsUpdate(th)
			}
		}
	}(ctx)
}

func (ts *txThrottlerState) throttle() bool {
	if ts.throttler == nil {
		log.Error("throttle called after deallocateResources was called")
		return false
	}
	// Serialize calls to ts.throttle.Throttle()
	ts.throttleMu.Lock()
	defer ts.throttleMu.Unlock()
	return ts.throttler.Throttle(0 /* threadId */) > 0
}

func (ts *txThrottlerState) deallocateResources() {
	// We don't really need to nil out the fields here
	// as deallocateResources is not expected to be called
	// more than once, but it doesn't hurt to do so.
	for cell, watcher := range ts.topologyWatchers {
		watcher.Stop()
		ts.txThrottler.topoWatchers.Reset(cell)
	}
	ts.topologyWatchers = nil

	ts.healthCheck.Close()
	ts.healthCheck = nil

	// After ts.healthCheck is closed txThrottlerState.StatsUpdate() is guaranteed not
	// to be executing, so we can safely close the throttler.
	ts.throttler.Close()
	ts.throttler = nil
}

// StatsUpdate updates the health of a tablet with the given healthcheck.
func (ts *txThrottlerState) StatsUpdate(tabletStats *discovery.TabletHealth) {
	if ts.config.tabletTypes == nil {
		return
	}

	tabletType := tabletStats.Target.TabletType
	metricLabels := []string{tabletStats.Target.Cell, tabletType.String()}
	ts.txThrottler.healthChecksReadTotal.Add(metricLabels, 1)

	// Monitor tablets for replication lag if they have a tablet
	// type specified by the --tx_throttler_tablet_types flag.
	if ts.config.tabletTypes[tabletType] {
		ts.throttler.RecordReplicationLag(time.Now(), tabletStats)
		ts.txThrottler.healthChecksRecordedTotal.Add(metricLabels, 1)
	}
}
