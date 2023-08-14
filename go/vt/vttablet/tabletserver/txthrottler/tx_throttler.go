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
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/flagutil"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/throttler"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/planbuilder"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	throttlerdatapb "vitess.io/vitess/go/vt/proto/throttlerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	ErrThrottledConnPoolUsageHard = vterrors.New(vtrpcpb.Code_RESOURCE_EXHAUSTED, "Query throttled: ConnPoolUsageHard")
	ErrThrottledConnPoolUsageSoft = vterrors.New(vtrpcpb.Code_RESOURCE_EXHAUSTED, "Query throttled: ConnPoolUsageSoft")
	ErrThrottledReplicationLag    = vterrors.New(vtrpcpb.Code_RESOURCE_EXHAUSTED, "Transaction throttled: ReplicationLag")
	ErrThrottledTxPoolUsageHard   = vterrors.New(vtrpcpb.Code_RESOURCE_EXHAUSTED, "Transaction throttled: TxPoolUsageHard")
	ErrThrottledTxPoolUsageSoft   = vterrors.New(vtrpcpb.Code_RESOURCE_EXHAUSTED, "Transaction throttled: TxPoolUsageSoft")
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
	Throttle(plan *planbuilder.Plan, options *querypb.ExecuteOptions) error
}

// TabletserverEngineInterface defines an interface that provides throttling
// signals based on pool usage.
type TabletserverEngineInterface interface {
	GetPoolUsagePercent() float64
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
//		// Assuming topoServer is a topo.Server variable pointing to a Vitess topology server.
//		t := NewTxThrottler(config, topoServer)
//
//		// A transaction throttler must be opened before its first use:
//		if err := t.Open(keyspace, shard); err != nil {
//		  return err
//		}
//
//		// Checking whether to throttle can be done as follows before starting a transaction.
//		if err := t.Throttle(); err != nil {
//		  return fmt.Errorf("Transaction throttled: %w", err)
//		} else {
//	  		// execute transaction.
//		}
//
//		// To release the resources used by the throttler the caller should call Close().
//		t.Close()
//
// A txThrottler object is generally not thread-safe: at any given time at most one goroutine should
// be executing a method. The only exception is the 'Throttle' method where multiple goroutines are
// allowed to execute it concurrently.
type txThrottler struct {
	config *tabletenv.TabletConfig

	// state holds an open transaction throttler state. It is nil
	// if the TransactionThrottler is closed.
	state txThrottlerState

	// engines
	queryEngine TabletserverEngineInterface
	txEngine    TabletserverEngineInterface

	target     *querypb.Target
	topoServer *topo.Server

	// stats
	throttlerRunning          *stats.Gauge
	topoWatchers              *stats.GaugesWithSingleLabel
	healthChecksReadTotal     *stats.CountersWithMultiLabels
	healthChecksRecordedTotal *stats.CountersWithMultiLabels
	requestsTotal             *stats.CountersWithMultiLabels
	requestsThrottled         *stats.CountersWithMultiLabels
}

type txThrottlerState interface {
	deallocateResources()
	StatsUpdate(tabletStats *discovery.TabletHealth)
	throttle(plan *planbuilder.Plan) error
}

// txThrottlerStateImpl holds the state of an open TxThrottler object.
type txThrottlerStateImpl struct {
	config      *tabletenv.TabletConfig
	txThrottler *txThrottler

	// throttleMu serializes calls to throttler.Throttler.Throttle(threadId).
	// That method is required to be called in serial for each threadId.
	throttleMu       sync.Mutex
	throttler        ThrottlerInterface
	stopHealthCheck  context.CancelFunc
	queryEngine      TabletserverEngineInterface
	txEngine         TabletserverEngineInterface
	topologyWatchers map[string]TopologyWatcherInterface

	healthCheck      discovery.HealthCheck
	healthCheckChan  chan *discovery.TabletHealth
	healthCheckCells []string
	cellsFromTopo    bool

	// tabletTypes stores the tablet types for throttling
	tabletTypes map[topodatapb.TabletType]bool
}

// NewTxThrottler tries to construct a txThrottler from the relevant
// fields in the tabletenv.Env and topo.Server objects.
func NewTxThrottler(env tabletenv.Env, topoServer *topo.Server, queryEngine, txEngine TabletserverEngineInterface) TxThrottler {
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
		queryEngine:      queryEngine,
		txEngine:         txEngine,
		topoServer:       topoServer,
		throttlerRunning: env.Exporter().NewGauge(TxThrottlerName+"Running", "transaction throttler running state"),
		topoWatchers:     env.Exporter().NewGaugesWithSingleLabel(TxThrottlerName+"TopoWatchers", "transaction throttler topology watchers", "cell"),
		healthChecksReadTotal: env.Exporter().NewCountersWithMultiLabels(TxThrottlerName+"HealthchecksRead", "transaction throttler healthchecks read",
			[]string{"cell", "DbType"}),
		healthChecksRecordedTotal: env.Exporter().NewCountersWithMultiLabels(TxThrottlerName+"HealthchecksRecorded", "transaction throttler healthchecks recorded",
			[]string{"cell", "DbType"}),
		requestsTotal: env.Exporter().NewCountersWithMultiLabels(TxThrottlerName+"Requests", "transaction throttler requests",
			[]string{"plan", "workload"},
		),
		requestsThrottled: env.Exporter().NewCountersWithMultiLabels(TxThrottlerName+"Throttled", "transaction throttler requests throttled",
			[]string{"plan", "cause", "workload"},
		),
	}
}

// getPriorityFromOptions returns the priority of an operation as an integer between 0 and
// 100. The defaultPriority is returned if none is found in *querypb.ExecuteOptions.
func (t *txThrottler) getPriorityFromOptions(options *querypb.ExecuteOptions) int {
	priority := t.config.TxThrottlerDefaultPriority
	if options == nil {
		return priority
	}
	if options.Priority == "" {
		return priority
	}

	optionsPriority, err := strconv.Atoi(options.Priority)
	// This should never error out, as the value for Priority has been validated in the vtgate already.
	// Still, handle it just to make sure.
	if err != nil {
		log.Errorf(
			"The value of the %s query directive could not be converted to integer, using the "+
				"default value. Error was: %s",
			sqlparser.DirectivePriority, priority, err)

		return priority
	}

	return optionsPriority
}

// InitDBConfig initializes the target parameters for the throttler.
func (t *txThrottler) InitDBConfig(target *querypb.Target) {
	t.target = proto.Clone(target).(*querypb.Target)
}

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
	t.state, err = newTxThrottlerState(t, t.config, t.target, t.queryEngine, t.txEngine)
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

// Throttle should be called before a new transaction is started. It
// returns an error if the transaction should not proceed (the caller
// should back off). Throttle requires that Open() was previously
// called successfully.
func (t *txThrottler) Throttle(plan *planbuilder.Plan, options *querypb.ExecuteOptions) (err error) {
	if !t.config.EnableTxThrottler {
		return nil
	}
	if t.state == nil {
		return nil
	}

	// get priority from execute options, skip if the priority
	// is equal to sqlparser.MinPriorityValue
	planType := plan.PlanID.String()
	workload := options.GetWorkloadName()
	metricLabels := []string{planType, workload}
	t.requestsTotal.Add(metricLabels, 1)
	priority := t.getPriorityFromOptions(options)
	if priority == sqlparser.MinPriorityValue {
		return nil
	}

	// check if any throttling is needed
	throttleErr := t.state.throttle(plan)
	if throttleErr == nil {
		return nil
	}

	// Throttle probabilistically according to both what the throttler state says and the priority.
	// Workloads with lower priority value are less likely to be throttled. "Hard" pool usage
	// errors will throttle regardless of priority.
	metricLabels = []string{planType, throttleErr.Error(), workload}
	switch throttleErr {
	case ErrThrottledConnPoolUsageHard, ErrThrottledTxPoolUsageHard:
		t.requestsThrottled.Add(metricLabels, 1)
		err = throttleErr
	case ErrThrottledConnPoolUsageSoft, ErrThrottledTxPoolUsageSoft, ErrThrottledReplicationLag:
		if rand.Intn(sqlparser.MaxPriorityValue) < priority {
			t.requestsThrottled.Add(metricLabels, 1)
			err = throttleErr
		}
	}

	if t.config.TxThrottlerDryRun {
		err = nil
	}
	return err
}

func newTxThrottlerState(txThrottler *txThrottler, config *tabletenv.TabletConfig, target *querypb.Target, queryEngine, txEngine TabletserverEngineInterface) (txThrottlerState, error) {
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
		queryEngine:      queryEngine,
		txEngine:         txEngine,
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

	return state, nil
}

func (ts *txThrottlerStateImpl) initHealthCheckStream(topoServer *topo.Server, target *querypb.Target) {
	ts.healthCheck = healthCheckFactory(topoServer, target.Cell, ts.healthCheckCells)
	ts.healthCheckChan = ts.healthCheck.Subscribe()

	ts.topologyWatchers = make(
		map[string]TopologyWatcherInterface, len(ts.healthCheckCells))
	for _, cell := range ts.healthCheckCells {
		ts.topologyWatchers[cell] = topologyWatcherFactory(
			topoServer,
			ts.healthCheck,
			cell,
			target.Keyspace,
			target.Shard,
			discovery.DefaultTopologyWatcherRefreshInterval,
			discovery.DefaultTopoReadConcurrency,
		)
		ts.txThrottler.topoWatchers.Add(cell, 1)
	}
}

func (ts *txThrottlerStateImpl) closeHealthCheckStream() {
	if ts.healthCheck == nil {
		return
	}
	for cell, watcher := range ts.topologyWatchers {
		watcher.Stop()
		ts.txThrottler.topoWatchers.Reset(cell)
	}
	ts.topologyWatchers = nil
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

func checkEnginePoolUsage(engine TabletserverEngineInterface, thresholds *flagutil.LowHighIntValues, highErr, lowErr error) error {
	if thresholds.Low <= 0 && thresholds.High >= 100 {
		return nil
	}
	// Calls to .GetPoolUsagePercent() are serialized by the underlying engine.
	switch usagePercent := engine.GetPoolUsagePercent(); {
	case thresholds.High < 100 && int(usagePercent) >= thresholds.High:
		return highErr
	case thresholds.Low > 0 && int(usagePercent) >= thresholds.Low:
		return lowErr
	default:
		return nil
	}
}

func (ts *txThrottlerStateImpl) throttle(plan *planbuilder.Plan) error {
	if ts.throttler == nil {
		log.Error("txThrottler: throttle called after deallocateResources was called")
		return nil
	}
	if plan == nil {
		log.Error("txThrottler: throttle called with no plan")
		return nil
	}
	switch plan.PlanID {
	case planbuilder.PlanSelect, planbuilder.PlanSelectImpossible, planbuilder.PlanShow:
		// check query engine conn pool usage
		return checkEnginePoolUsage(ts.queryEngine, ts.config.TxThrottlerQueryPoolThresholds,
			ErrThrottledConnPoolUsageHard, ErrThrottledConnPoolUsageSoft)
	default:
		// check tx engine pool usage
		if err := checkEnginePoolUsage(ts.txEngine, ts.config.TxThrottlerTxPoolThresholds,
			ErrThrottledTxPoolUsageHard, ErrThrottledTxPoolUsageSoft); err != nil {
			return err
		}

		// check max replication lag.
		// serialize calls to ts.throttle.Throttle().
		ts.throttleMu.Lock()
		defer ts.throttleMu.Unlock()
		if ts.throttler.Throttle(0 /* threadId */) > 0 {
			return ErrThrottledReplicationLag
		}
	}
	return nil
}

func (ts *txThrottlerStateImpl) deallocateResources() {
	// Close healthcheck and topo watchers
	ts.closeHealthCheckStream()
	ts.healthCheck = nil

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
