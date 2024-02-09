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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"google.golang.org/protobuf/encoding/prototext"

	"context"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/throttler"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	throttlerdatapb "vitess.io/vitess/go/vt/proto/throttlerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

<<<<<<< HEAD
// TxThrottler throttles transactions based on replication lag.
=======
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
>>>>>>> 2b25639f25 (TxThrottler: dont throttle unless lag (#14789))
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
// A TxThrottler object is generally not thread-safe: at any given time at most one goroutine should
// be executing a method. The only exception is the 'Throttle' method where multiple goroutines are
// allowed to execute it concurrently.
type TxThrottler struct {
	// config stores the transaction throttler's configuration.
	// It is populated in NewTxThrottler and is not modified
	// since.
	config *txThrottlerConfig

	// state holds an open transaction throttler state. It is nil
	// if the TransactionThrottler is closed.
	state *txThrottlerState

	target *querypb.Target
}

// NewTxThrottler tries to construct a TxThrottler from the
// relevant fields in the tabletenv.Config object. It returns a disabled TxThrottler if
// any error occurs.
// This function calls tryCreateTxThrottler that does the actual creation work
// and returns an error if one occurred.
func NewTxThrottler(config *tabletenv.TabletConfig, topoServer *topo.Server) *TxThrottler {
	txThrottler, err := tryCreateTxThrottler(config, topoServer)
	if err != nil {
		log.Errorf("Error creating transaction throttler. Transaction throttling will"+
			" be disabled. Error: %v", err)
		txThrottler, err = newTxThrottler(&txThrottlerConfig{enabled: false})
		if err != nil {
			panic("BUG: Can't create a disabled transaction throttler")
		}
	} else {
		log.Infof("Initialized transaction throttler with config: %+v", txThrottler.config)
	}
	return txThrottler
}

// InitDBConfig initializes the target parameters for the throttler.
func (t *TxThrottler) InitDBConfig(target *querypb.Target) {
	t.target = proto.Clone(target).(*querypb.Target)
}

func tryCreateTxThrottler(config *tabletenv.TabletConfig, topoServer *topo.Server) (*TxThrottler, error) {
	if !config.EnableTxThrottler {
		return newTxThrottler(&txThrottlerConfig{enabled: false})
	}

	var throttlerConfig throttlerdatapb.Configuration
	if err := prototext.Unmarshal([]byte(config.TxThrottlerConfig), &throttlerConfig); err != nil {
		return nil, err
	}

	// Clone tsv.TxThrottlerHealthCheckCells so that we don't assume tsv.TxThrottlerHealthCheckCells
	// is immutable.
	healthCheckCells := make([]string, len(config.TxThrottlerHealthCheckCells))
	copy(healthCheckCells, config.TxThrottlerHealthCheckCells)

	return newTxThrottler(&txThrottlerConfig{
		enabled:          true,
		topoServer:       topoServer,
		throttlerConfig:  &throttlerConfig,
		healthCheckCells: healthCheckCells,
	})
}

// txThrottlerConfig holds the parameters that need to be
// passed when constructing a TxThrottler object.
type txThrottlerConfig struct {
	// enabled is true if the transaction throttler is enabled. All methods
	// of a disabled transaction throttler do nothing and Throttle() always
	// returns false.
	enabled bool

	topoServer      *topo.Server
	throttlerConfig *throttlerdatapb.Configuration
	// healthCheckCells stores the cell names in which running vttablets will be monitored for
	// replication lag.
	healthCheckCells []string
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

// txThrottlerState holds the state of an open TxThrottler object.
type txThrottlerState struct {
	// throttleMu serializes calls to throttler.Throttler.Throttle(threadId).
	// That method is required to be called in serial for each threadId.
	throttleMu      sync.Mutex
	throttler       ThrottlerInterface
	stopHealthCheck context.CancelFunc

	healthCheck      discovery.HealthCheck
<<<<<<< HEAD
	topologyWatchers []TopologyWatcherInterface
=======
	healthCheckChan  chan *discovery.TabletHealth
	healthCheckCells []string
	cellsFromTopo    bool

	// tabletTypes stores the tablet types for throttling
	tabletTypes map[topodatapb.TabletType]bool

	maxLag             int64
	done               chan bool
	waitForTermination sync.WaitGroup
>>>>>>> 2b25639f25 (TxThrottler: dont throttle unless lag (#14789))
}

// These vars store the functions used to create the topo server, healthcheck,
// topology watchers and go/vt/throttler. These are provided here so that they can be overridden
// in tests to generate mocks.
type healthCheckFactoryFunc func(topoServer *topo.Server, cell string, cellsToWatch []string) discovery.HealthCheck
type topologyWatcherFactoryFunc func(topoServer *topo.Server, hc discovery.HealthCheck, cell, keyspace, shard string, refreshInterval time.Duration, topoReadConcurrency int) TopologyWatcherInterface
type throttlerFactoryFunc func(name, unit string, threadCount int, maxRate, maxReplicationLag int64) (ThrottlerInterface, error)

var (
	healthCheckFactory     healthCheckFactoryFunc
	topologyWatcherFactory topologyWatcherFactoryFunc
	throttlerFactory       throttlerFactoryFunc
)

func init() {
	resetTxThrottlerFactories()
}

func resetTxThrottlerFactories() {
	healthCheckFactory = func(topoServer *topo.Server, cell string, cellsToWatch []string) discovery.HealthCheck {
		return discovery.NewHealthCheck(context.Background(), discovery.DefaultHealthCheckRetryDelay, discovery.DefaultHealthCheckTimeout, topoServer, cell, strings.Join(cellsToWatch, ","))
	}
	topologyWatcherFactory = func(topoServer *topo.Server, hc discovery.HealthCheck, cell, keyspace, shard string, refreshInterval time.Duration, topoReadConcurrency int) TopologyWatcherInterface {
		return discovery.NewCellTabletsWatcher(context.Background(), topoServer, hc, discovery.NewFilterByKeyspace([]string{keyspace}), cell, refreshInterval, true, topoReadConcurrency)
	}
	throttlerFactory = func(name, unit string, threadCount int, maxRate, maxReplicationLag int64) (ThrottlerInterface, error) {
		return throttler.NewThrottler(name, unit, threadCount, maxRate, maxReplicationLag)
	}
}

// TxThrottlerName is the name the wrapped go/vt/throttler object will be registered with
// go/vt/throttler.GlobalManager.
const TxThrottlerName = "TransactionThrottler"

func newTxThrottler(config *txThrottlerConfig) (*TxThrottler, error) {
	if config.enabled {
		// Verify config.
		err := throttler.MaxReplicationLagModuleConfig{Configuration: config.throttlerConfig}.Verify()
		if err != nil {
			return nil, err
		}
		if len(config.healthCheckCells) == 0 {
			return nil, fmt.Errorf("empty healthCheckCells given. %+v", config)
		}
	}
	return &TxThrottler{
		config: config,
	}, nil
}

// Open opens the transaction throttler. It must be called prior to 'Throttle'.
func (t *TxThrottler) Open() error {
	if !t.config.enabled {
		return nil
	}
	if t.state != nil {
		return nil
	}
	log.Info("TxThrottler: opening")
	var err error
	t.state, err = newTxThrottlerState(t.config, t.target.Keyspace, t.target.Shard, t.target.Cell)
	return err
}

// Close closes the TxThrottler object and releases resources.
// It should be called after the throttler is no longer needed.
// It's ok to call this method on a closed throttler--in which case the method does nothing.
func (t *TxThrottler) Close() {
	if !t.config.enabled {
		return
	}
	if t.state == nil {
		return
	}
	t.state.deallocateResources()
	t.state = nil
	log.Info("TxThrottler: closed")
}

// Throttle should be called before a new transaction is started.
// It returns true if the transaction should not proceed (the caller
// should back off). Throttle requires that Open() was previously called
// successfully.
func (t *TxThrottler) Throttle() (result bool) {
	if !t.config.enabled {
		return false
	}
	if t.state == nil {
		panic("BUG: Throttle() called on a closed TxThrottler")
	}
<<<<<<< HEAD
	return t.state.throttle()
=======

	// Throttle according to both what the throttler state says and the priority. Workloads with lower priority value
	// are less likely to be throttled.
	result = rand.Intn(sqlparser.MaxPriorityValue) < priority && t.state.throttle()

	t.requestsTotal.Add(workload, 1)
	if result {
		t.requestsThrottled.Add(workload, 1)
	}

	return result && !t.config.TxThrottlerDryRun
>>>>>>> 2b25639f25 (TxThrottler: dont throttle unless lag (#14789))
}

func newTxThrottlerState(config *txThrottlerConfig, keyspace, shard, cell string) (*txThrottlerState, error) {
	t, err := throttlerFactory(
		TxThrottlerName,
		"TPS",                           /* unit */
		1,                               /* threadCount */
		throttler.MaxRateModuleDisabled, /* maxRate */
		config.throttlerConfig.MaxReplicationLagSec /* maxReplicationLag */)
	if err != nil {
		return nil, err
	}
	if err := t.UpdateConfiguration(config.throttlerConfig, true /* copyZeroValues */); err != nil {
		t.Close()
		return nil, err
	}
	result := &txThrottlerState{
		throttler: t,
	}
	createTxThrottlerHealthCheck(config, result, cell)

<<<<<<< HEAD
	result.topologyWatchers = make(
		[]TopologyWatcherInterface, 0, len(config.healthCheckCells))
	for _, cell := range config.healthCheckCells {
		result.topologyWatchers = append(
			result.topologyWatchers,
			topologyWatcherFactory(
				config.topoServer,
				result.healthCheck,
				cell,
				keyspace,
				shard,
				discovery.DefaultTopologyWatcherRefreshInterval,
				discovery.DefaultTopoReadConcurrency))
=======
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
>>>>>>> 2b25639f25 (TxThrottler: dont throttle unless lag (#14789))
	}
	return result, nil
}

func createTxThrottlerHealthCheck(config *txThrottlerConfig, result *txThrottlerState, cell string) {
	ctx, cancel := context.WithCancel(context.Background())
<<<<<<< HEAD
	result.stopHealthCheck = cancel
	result.healthCheck = healthCheckFactory(config.topoServer, cell, config.healthCheckCells)
	ch := result.healthCheck.Subscribe()
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case th := <-ch:
				result.StatsUpdate(th)
			}
=======
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
>>>>>>> 2b25639f25 (TxThrottler: dont throttle unless lag (#14789))
		}
	}(ctx)
}

func (ts *txThrottlerState) throttle() bool {
	if ts.throttler == nil {
		panic("BUG: throttle called after deallocateResources was called.")
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

func (ts *txThrottlerState) deallocateResources() {
	// We don't really need to nil out the fields here
	// as deallocateResources is not expected to be called
	// more than once, but it doesn't hurt to do so.
	for _, watcher := range ts.topologyWatchers {
		watcher.Stop()
	}
	ts.topologyWatchers = nil

	ts.healthCheck.Close()
	ts.healthCheck = nil

<<<<<<< HEAD
	// After ts.healthCheck is closed txThrottlerState.StatsUpdate() is guaranteed not
=======
	ts.done <- true
	ts.waitForTermination.Wait()
	// After ts.healthCheck is closed txThrottlerStateImpl.StatsUpdate() is guaranteed not
>>>>>>> 2b25639f25 (TxThrottler: dont throttle unless lag (#14789))
	// to be executing, so we can safely close the throttler.
	ts.throttler.Close()
	ts.throttler = nil
}

// StatsUpdate updates the health of a tablet with the given healthcheck.
func (ts *txThrottlerState) StatsUpdate(tabletStats *discovery.TabletHealth) {
	// Ignore PRIMARY and RDONLY stats.
	// We currently do not monitor RDONLY tablets for replication lag. RDONLY tablets are not
	// candidates for becoming primary during failover, and it's acceptable to serve somewhat
	// stale date from these.
	// TODO(erez): If this becomes necessary, we can add a configuration option that would
	// determine whether we consider RDONLY tablets here, as well.
	if tabletStats.Target.TabletType != topodatapb.TabletType_REPLICA {
		return
	}
	ts.throttler.RecordReplicationLag(time.Now(), tabletStats)
}
