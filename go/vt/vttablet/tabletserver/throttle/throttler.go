/*
Copyright 2023 The Vitess Authors.

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

// This codebase originates from https://github.com/github/freno, See https://github.com/github/freno/blob/master/LICENSE
/*
	MIT License

	Copyright (c) 2017 GitHub

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
*/

package throttle

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/stats"

	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/heartbeat"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/config"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

const (
	leaderCheckInterval            = 5 * time.Second
	activeCollectInterval          = 250 * time.Millisecond // PRIMARY polls replicas
	dormantCollectInterval         = 5 * time.Second        // PRIMARY polls replicas when dormant (no recent checks)
	inventoryRefreshInterval       = 10 * time.Second       // Refreshing tablet inventory
	metricsAggregateInterval       = 125 * time.Millisecond
	throttledAppsSnapshotInterval  = 5 * time.Second
	recentCheckRateLimiterInterval = 1 * time.Second // Ticker assisting in determining when the throttler was last checked

	aggregatedMetricsExpiration = 5 * time.Second
	recentAppsExpiration        = time.Hour

	dormantPeriod              = time.Minute // How long since last check to be considered dormant
	DefaultAppThrottleDuration = time.Hour
	DefaultThrottleRatio       = 1.0

	defaultReplicationLagQuery = "select unix_timestamp(now(6))-max(ts/1000000000) as replication_lag from %s.heartbeat"
	threadsRunningQuery        = "show global status like 'threads_running'"

	inventoryPrefix       = "inventory/"
	throttlerConfigPrefix = "config/"
)

var (
	throttleTabletTypes = "replica"
)

var (
	statsThrottlerHeartbeatRequests    = stats.NewCounter("ThrottlerHeartbeatRequests", "heartbeat requests")
	statsThrottlerProbeRecentlyChecked = stats.NewCounter("ThrottlerProbeRecentlyChecked", "probe recently checked")
)

func init() {
	servenv.OnParseFor("vtcombo", registerThrottlerFlags)
	servenv.OnParseFor("vttablet", registerThrottlerFlags)
}

func registerThrottlerFlags(fs *pflag.FlagSet) {
	fs.StringVar(&throttleTabletTypes, "throttle_tablet_types", throttleTabletTypes, "Comma separated VTTablet types to be considered by the throttler. default: 'replica'. example: 'replica,rdonly'. 'replica' always implicitly included")
}

var (
	ErrThrottlerNotOpen = errors.New("throttler not open")
)

// throttlerTopoService represents the functionality we expect from a TopoServer, abstracted so that
// it can be mocked in unit tests
type throttlerTopoService interface {
	GetTablet(ctx context.Context, alias *topodatapb.TabletAlias) (*topo.TabletInfo, error)
	FindAllTabletAliasesInShard(ctx context.Context, keyspace, shard string) ([]*topodatapb.TabletAlias, error)
	GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error)
}

// Throttler is the main entity in the throttling mechanism. This service runs, probes, collects data,
// aggregates, reads inventory, provides information, etc.
type Throttler struct {
	keyspace    string
	shard       string
	tabletAlias *topodatapb.TabletAlias

	check     *ThrottlerCheck
	isEnabled atomic.Bool
	isLeader  atomic.Bool
	isOpen    atomic.Bool

	leaderCheckInterval           time.Duration
	activeCollectInterval         time.Duration
	dormantCollectInterval        time.Duration
	inventoryRefreshInterval      time.Duration
	metricsAggregateInterval      time.Duration
	throttledAppsSnapshotInterval time.Duration
	dormantPeriod                 time.Duration

	configSettings   *config.ConfigurationSettings
	env              tabletenv.Env
	pool             *connpool.Pool
	tabletTypeFunc   func() topodatapb.TabletType
	ts               throttlerTopoService
	srvTopoServer    srvtopo.Server
	heartbeatWriter  heartbeat.HeartbeatWriter
	overrideTmClient tmclient.TabletManagerClient

	recentCheckRateLimiter *timer.RateLimiter
	recentCheckDormantDiff int64
	recentCheckDiff        int64

	throttleTabletTypesMap map[topodatapb.TabletType]bool

	throttleMetricChan  chan *base.ThrottleMetric
	clusterProbesChan   chan *base.ClusterProbes
	throttlerConfigChan chan *topodatapb.ThrottlerConfig
	serialFuncChan      chan func() // Used by unit tests to inject non-racy behavior

	inventory *base.Inventory

	customMetricsQuery atomic.Value
	MetricsThreshold   atomic.Uint64
	checkAsCheckSelf   atomic.Bool

	metricThresholds  *cache.Cache
	aggregatedMetrics *cache.Cache
	throttledApps     *cache.Cache
	recentApps        *cache.Cache
	metricsHealth     *cache.Cache
	appCheckedMetrics *cache.Cache

	initMutex           sync.Mutex
	enableMutex         sync.Mutex
	cancelOpenContext   context.CancelFunc
	cancelEnableContext context.CancelFunc
	throttledAppsMutex  sync.Mutex

	readSelfThrottleMetrics func(context.Context) base.ThrottleMetrics // overwritten by unit test
}

// ThrottlerStatus published some status values from the throttler
type ThrottlerStatus struct {
	Keyspace string
	Shard    string

	IsLeader        bool
	IsOpen          bool
	IsEnabled       bool
	IsDormant       bool
	RecentlyChecked bool

	Query                   string
	CustomQuery             string
	Threshold               float64
	MetricNameUsedAsDefault string

	AggregatedMetrics map[string]base.MetricResult
	MetricsThresholds map[string]float64
	MetricsHealth     base.MetricHealthMap
	ThrottledApps     []base.AppThrottle
	AppCheckedMetrics map[string]string
	RecentApps        map[string](*base.RecentApp)
}

// NewThrottler creates a Throttler
func NewThrottler(env tabletenv.Env, srvTopoServer srvtopo.Server, ts *topo.Server, tabletAlias *topodatapb.TabletAlias, heartbeatWriter heartbeat.HeartbeatWriter, tabletTypeFunc func() topodatapb.TabletType) *Throttler {
	throttler := &Throttler{
		tabletAlias:     tabletAlias,
		env:             env,
		tabletTypeFunc:  tabletTypeFunc,
		srvTopoServer:   srvTopoServer,
		ts:              ts,
		heartbeatWriter: heartbeatWriter,
		pool: connpool.NewPool(env, "ThrottlerPool", tabletenv.ConnPoolConfig{
			Size:        2,
			IdleTimeout: env.Config().OltpReadPool.IdleTimeout,
		}),
	}

	throttler.throttleMetricChan = make(chan *base.ThrottleMetric)
	throttler.clusterProbesChan = make(chan *base.ClusterProbes)
	throttler.throttlerConfigChan = make(chan *topodatapb.ThrottlerConfig)
	throttler.serialFuncChan = make(chan func())
	throttler.inventory = base.NewInventory()

	throttler.throttledApps = cache.New(cache.NoExpiration, 0)
	throttler.metricThresholds = cache.New(cache.NoExpiration, 0)
	throttler.aggregatedMetrics = cache.New(aggregatedMetricsExpiration, 0)
	throttler.recentApps = cache.New(recentAppsExpiration, recentAppsExpiration)
	throttler.metricsHealth = cache.New(cache.NoExpiration, 0)
	throttler.appCheckedMetrics = cache.New(cache.NoExpiration, 0)

	throttler.initThrottleTabletTypes()
	throttler.check = NewThrottlerCheck(throttler)

	throttler.leaderCheckInterval = leaderCheckInterval
	throttler.activeCollectInterval = activeCollectInterval
	throttler.dormantCollectInterval = dormantCollectInterval
	throttler.inventoryRefreshInterval = inventoryRefreshInterval
	throttler.metricsAggregateInterval = metricsAggregateInterval
	throttler.throttledAppsSnapshotInterval = throttledAppsSnapshotInterval
	throttler.dormantPeriod = dormantPeriod
	throttler.recentCheckDormantDiff = int64(throttler.dormantPeriod / recentCheckRateLimiterInterval)
	throttler.recentCheckDiff = int64(1 * time.Second / recentCheckRateLimiterInterval)
	if throttler.recentCheckDiff < 1 {
		throttler.recentCheckDiff = 1
	}

	throttler.StoreMetricsThreshold(base.RegisteredSelfMetrics[base.LagMetricName].DefaultThreshold())
	throttler.readSelfThrottleMetrics = func(ctx context.Context) base.ThrottleMetrics {
		return throttler.readSelfThrottleMetricsInternal(ctx)
	}
	return throttler
}

// tabletAliasString returns tablet alias as string
func (throttler *Throttler) tabletAliasString() string {
	if throttler.tabletAlias == nil {
		return ""
	}
	return topoproto.TabletAliasString(throttler.tabletAlias)
}

func (throttler *Throttler) StoreMetricsThreshold(threshold float64) {
	throttler.MetricsThreshold.Store(math.Float64bits(threshold))
}

// initThrottleTabletTypes reads the user supplied throttle_tablet_types and sets these
// for the duration of this tablet's lifetime
func (throttler *Throttler) initThrottleTabletTypes() {
	throttler.throttleTabletTypesMap = make(map[topodatapb.TabletType]bool)

	tokens := textutil.SplitDelimitedList(throttleTabletTypes)
	for _, token := range tokens {
		token = strings.ToUpper(token)
		if value, ok := topodatapb.TabletType_value[token]; ok {
			throttler.throttleTabletTypesMap[topodatapb.TabletType(value)] = true
		}
	}
	// always on:
	throttler.throttleTabletTypesMap[topodatapb.TabletType_REPLICA] = true
}

// InitDBConfig initializes keyspace and shard
func (throttler *Throttler) InitDBConfig(keyspace, shard string) {
	throttler.keyspace = keyspace
	throttler.shard = shard
}

func (throttler *Throttler) GetMetricsQuery() string {
	if customQuery := throttler.GetCustomMetricsQuery(); customQuery != "" {
		return customQuery
	}
	lagSelfMetric, ok := base.RegisteredSelfMetrics[base.LagMetricName].(*base.LagSelfMetric)
	if !ok {
		return ""
	}
	return lagSelfMetric.GetQuery()
}

func (throttler *Throttler) GetCustomMetricsQuery() string {
	val := throttler.customMetricsQuery.Load()
	if val == nil {
		return ""
	}
	return val.(string)
}

func (throttler *Throttler) GetMetricsThreshold() float64 {
	return math.Float64frombits(throttler.MetricsThreshold.Load())
}

// initThrottler initializes config
func (throttler *Throttler) initConfig() {
	log.Infof("Throttler: initializing config")

	throttler.configSettings = &config.ConfigurationSettings{
		MySQLStore: config.MySQLConfigurationSettings{
			IgnoreDialTCPErrors: true,
		},
	}
}

// readThrottlerConfig proactively reads the throttler's config from SrvKeyspace in local topo
func (throttler *Throttler) readThrottlerConfig(ctx context.Context) (*topodatapb.ThrottlerConfig, error) {
	srvks, err := throttler.ts.GetSrvKeyspace(ctx, throttler.tabletAlias.Cell, throttler.keyspace)
	if err != nil {
		return nil, err
	}
	return throttler.normalizeThrottlerConfig(srvks.ThrottlerConfig), nil
}

// normalizeThrottlerConfig normalizes missing throttler config information, as needed.
func (throttler *Throttler) normalizeThrottlerConfig(throttlerConfig *topodatapb.ThrottlerConfig) *topodatapb.ThrottlerConfig {
	if throttlerConfig == nil {
		throttlerConfig = &topodatapb.ThrottlerConfig{}
	}
	if throttlerConfig.ThrottledApps == nil {
		throttlerConfig.ThrottledApps = make(map[string]*topodatapb.ThrottledAppRule)
	}
	if throttlerConfig.AppCheckedMetrics == nil {
		throttlerConfig.AppCheckedMetrics = make(map[string]*topodatapb.ThrottlerConfig_MetricNames)
	}
	if throttlerConfig.MetricThresholds == nil {
		throttlerConfig.MetricThresholds = make(map[string]float64)
	}
	if throttlerConfig.CustomQuery == "" {
		// no custom query; we check replication lag
		if throttlerConfig.Threshold == 0 {
			throttlerConfig.Threshold = base.RegisteredSelfMetrics[base.LagMetricName].DefaultThreshold()
		}
	}
	return throttlerConfig
}

func (throttler *Throttler) WatchSrvKeyspaceCallback(srvks *topodatapb.SrvKeyspace, err error) bool {
	if err != nil {
		if !topo.IsErrType(err, topo.Interrupted) && !errors.Is(err, context.Canceled) {
			log.Errorf("WatchSrvKeyspaceCallback error: %v", err)
		}
		return true
	}
	throttlerConfig := throttler.normalizeThrottlerConfig(srvks.ThrottlerConfig)

	if throttler.IsEnabled() {
		// Throttler is enabled and we should apply the config change
		// through Operate() or else we get into race conditions.
		go func() {
			throttler.throttlerConfigChan <- throttlerConfig
		}()
	} else {
		throttler.initMutex.Lock()
		defer throttler.initMutex.Unlock()
		// Throttler is not enabled, we should apply directly.
		throttler.applyThrottlerConfig(context.Background(), throttlerConfig)
	}

	return true
}

// convergeMetricThresholds looks at metric thresholds as defined by:
//   - inventory (also includes changes to throttler.MetricsThreshold). This always includes all known metrics
//     ie lag, threads_running, etc...
//   - throttler config. This can be a list of zero or more entries. These metrics override the inventory.
func (throttler *Throttler) convergeMetricThresholds() {
	for _, metricName := range base.KnownMetricNames {
		if val, ok := throttler.metricThresholds.Get(throttlerConfigPrefix + metricName.String()); ok {
			// Value supplied by throttler config takes precedence
			throttler.metricThresholds.Set(metricName.String(), val, cache.DefaultExpiration)
			continue
		}
		// metric not indicated in the throttler config, therefore we should use the default threshold for that metric
		if val, ok := throttler.metricThresholds.Get(inventoryPrefix + metricName.String()); ok {
			throttler.metricThresholds.Set(metricName.String(), val, cache.DefaultExpiration)
		}
	}
}

// applyThrottlerConfig receives a Throttlerconfig as read from SrvKeyspace, and applies the configuration.
// This may cause the throttler to be enabled/disabled, and of course it affects the throttling query/threshold.
// Note: you should be holding the initMutex when calling this function.
func (throttler *Throttler) applyThrottlerConfig(ctx context.Context, throttlerConfig *topodatapb.ThrottlerConfig) {
	log.Infof("Throttler: applying topo config: %+v", throttlerConfig)
	throttler.customMetricsQuery.Store(throttlerConfig.CustomQuery)
	if throttlerConfig.Threshold > 0 || throttlerConfig.CustomQuery != "" {
		// We do not allow Threshold=0, unless there is a custom query.
		// Without a custom query, the theshold applies to replication lag,
		// which does not make sense to have as zero.
		// This is already validated in VtctldServer.UpdateThrottlerConfig() in grpcvtctldserver/server.go,
		// but worth validating again.
		// Once transition to multi-metrics is complete (1 or 2 versions after first shipping it),
		// this legacy behavior can be removed. We won't be using `Threadhold` anymore, and we will
		// require per-metric threshold.
		throttler.StoreMetricsThreshold(throttlerConfig.Threshold)
	}
	throttler.checkAsCheckSelf.Store(throttlerConfig.CheckAsCheckSelf)
	{
		// Throttled apps/rules
		for _, appRule := range throttlerConfig.ThrottledApps {
			throttler.ThrottleApp(appRule.Name, protoutil.TimeFromProto(appRule.ExpiresAt).UTC(), appRule.Ratio, appRule.Exempt)
		}
		for app := range throttler.throttledAppsSnapshot() {
			if app == throttlerapp.TestingAlwaysThrottlerName.String() {
				// Never remove this app
				continue
			}
			if _, ok := throttlerConfig.ThrottledApps[app]; !ok {
				// app not indicated in the throttler config, therefore should be removed from the map
				throttler.UnthrottleApp(app)
			}
		}
	}
	{
		// throttler.appCheckedMetrics needs to reflect throttlerConfig.AppCheckedMetrics
		for app, metrics := range throttlerConfig.AppCheckedMetrics {
			metricNames := base.MetricNames{}
			if len(metrics.Names) > 0 {
				for _, name := range metrics.Names {
					metricName := base.MetricName(name)
					metricNames = append(metricNames, metricName)
				}
				throttler.appCheckedMetrics.Set(app, metricNames, cache.DefaultExpiration)
			} else {
				// Empty list of metrics means we should use the default metrics
				throttler.appCheckedMetrics.Delete(app)
			}
		}
		for app := range throttler.appCheckedMetricsSnapshot() {
			if _, ok := throttlerConfig.AppCheckedMetrics[app]; !ok {
				// app not indicated in the throttler config, therefore should be removed from the map
				throttler.appCheckedMetrics.Delete(app)
			}
		}
	}
	{
		// Metric thresholds
		for metricName, threshold := range throttlerConfig.MetricThresholds {
			throttler.metricThresholds.Set(throttlerConfigPrefix+metricName, threshold, cache.DefaultExpiration)
		}
		for _, metricName := range base.KnownMetricNames {
			if _, ok := throttlerConfig.MetricThresholds[metricName.String()]; !ok {
				// metric not indicated in the throttler config, therefore should be removed from the map
				// so that we know to apply the inventory default threshold
				throttler.metricThresholds.Delete(throttlerConfigPrefix + metricName.String())
			}
		}
		throttler.convergeMetricThresholds()
	}
	if throttlerConfig.Enabled {
		go throttler.Enable()
	} else {
		go throttler.Disable()
	}
}

func (throttler *Throttler) IsEnabled() bool {
	return throttler.isEnabled.Load()
}

func (throttler *Throttler) IsOpen() bool {
	return throttler.isOpen.Load()
}

// CheckIsOpen checks if this throttler is ready to serve. If not, it
// returns an error.
func (throttler *Throttler) CheckIsOpen() error {
	if throttler.IsOpen() {
		// all good
		return nil
	}
	return ErrThrottlerNotOpen
}

func (throttler *Throttler) IsRunning() bool {
	return throttler.IsOpen() && throttler.IsEnabled()
}

// Enable activates the throttler probes; when enabled, the throttler responds to check queries based on
// the collected metrics.
// The function returns a WaitGroup that can be used to wait for the throttler to be fully disabled, ie when
// the Operate() goroutine function terminates and caches are invalidated.
func (throttler *Throttler) Enable() *sync.WaitGroup {
	throttler.enableMutex.Lock()
	defer throttler.enableMutex.Unlock()

	if wasEnabled := throttler.isEnabled.Swap(true); wasEnabled {
		log.Infof("Throttler: already enabled")
		return nil
	}
	log.Infof("Throttler: enabling")

	wg := &sync.WaitGroup{}
	var ctx context.Context
	ctx, throttler.cancelEnableContext = context.WithCancel(context.Background())
	throttler.check.SelfChecks(ctx)
	throttler.Operate(ctx, wg)

	// Make a one-time request for a lease of heartbeats
	throttler.requestHeartbeats()

	return wg
}

// Disable deactivates the probes and associated operations. When disabled, the throttler responds to check
// queries with "200 OK" irrespective of lag or any other metrics.
func (throttler *Throttler) Disable() bool {
	throttler.enableMutex.Lock()
	defer throttler.enableMutex.Unlock()

	if wasEnabled := throttler.isEnabled.Swap(false); !wasEnabled {
		log.Infof("Throttler: already disabled")
		return false
	}
	log.Infof("Throttler: disabling")

	throttler.cancelEnableContext()
	return true
}

// retryReadAndApplyThrottlerConfig() is called by Open(), read throttler config from topo, applies it, and starts watching
// for topo changes.
// But also, we're in an Open() function, which blocks state manager's operation, and affects
// opening of all other components. We thus read the throttler config in the background.
// However, we want to handle a situation where the read errors out.
// So we kick a loop that keeps retrying reading the config, for as long as this throttler is open.
func (throttler *Throttler) retryReadAndApplyThrottlerConfig(ctx context.Context) {
	var watchSrvKeyspaceOnce sync.Once
	retryInterval := 10 * time.Second
	retryTicker := time.NewTicker(retryInterval)
	defer retryTicker.Stop()
	for {
		if !throttler.IsOpen() {
			// Throttler is not open so no need to keep retrying.
			log.Warningf("Throttler.retryReadAndApplyThrottlerConfig(): throttler no longer seems to be open, exiting")
			return
		}

		requestCtx, requestCancel := context.WithTimeout(ctx, 5*time.Second)
		defer requestCancel()
		throttlerConfig, err := throttler.readThrottlerConfig(requestCtx)
		if err == nil {
			log.Infof("Throttler.retryReadAndApplyThrottlerConfig(): success reading throttler config: %+v", throttlerConfig)
			// It's possible that during a retry-sleep, the throttler is closed and opened again, leading
			// to two (or more) instances of this goroutine. That's not a big problem; it's fine if all
			// attempt to read the throttler config; but we just want to ensure they don't step on each other
			// while applying the changes.
			throttler.initMutex.Lock()
			defer throttler.initMutex.Unlock()
			throttler.applyThrottlerConfig(ctx, throttlerConfig) // may issue an Enable
			go watchSrvKeyspaceOnce.Do(func() {
				// We start watching SrvKeyspace only after we know it's been created. Now is that time!
				// We watch using the given ctx, which is cancelled when the throttler is Close()d.
				throttler.srvTopoServer.WatchSrvKeyspace(ctx, throttler.tabletAlias.Cell, throttler.keyspace, throttler.WatchSrvKeyspaceCallback)
			})
			return
		}
		log.Errorf("Throttler.retryReadAndApplyThrottlerConfig(): error reading throttler config. Will retry in %v. Err=%+v", retryInterval, err)
		select {
		case <-ctx.Done():
			// Throttler is not open so no need to keep retrying.
			log.Infof("Throttler.retryReadAndApplyThrottlerConfig(): throttler no longer seems to be open, exiting")
			return
		case <-retryTicker.C:
		}
	}
}

// Open opens database pool and initializes the schema
func (throttler *Throttler) Open() error {
	log.Infof("Throttler: started execution of Open. Acquiring initMutex lock")
	throttler.initMutex.Lock()
	defer throttler.initMutex.Unlock()

	isOpen := throttler.isOpen.Swap(true)
	if isOpen {
		// already open
		log.Infof("Throttler: throttler is already open")
		return nil
	}
	log.Infof("Throttler: opening")
	var ctx context.Context
	ctx, throttler.cancelOpenContext = context.WithCancel(context.Background())
	throttler.customMetricsQuery.Store("")
	throttler.initConfig()
	throttler.pool.Open(throttler.env.Config().DB.AppWithDB(), throttler.env.Config().DB.DbaWithDB(), throttler.env.Config().DB.AppDebugWithDB())

	throttler.ThrottleApp(throttlerapp.TestingAlwaysThrottlerName.String(), time.Now().Add(time.Hour*24*365*10), DefaultThrottleRatio, false)

	go throttler.retryReadAndApplyThrottlerConfig(ctx)

	return nil
}

// Close frees resources
func (throttler *Throttler) Close() {
	log.Infof("Throttler: started execution of Close. Acquiring initMutex lock")
	throttler.initMutex.Lock()
	log.Infof("Throttler: acquired initMutex lock")
	defer throttler.initMutex.Unlock()
	isOpen := throttler.isOpen.Swap(false)
	if !isOpen {
		log.Infof("Throttler: throttler is not open")
		return
	}
	throttler.Disable()
	throttler.isLeader.Store(false)

	// The below " != nil " checks are relevant to unit tests, where perhaps not all
	// fields are supplied.
	if throttler.pool != nil {
		log.Infof("Throttler: closing pool")
		throttler.pool.Close()
	}
	if throttler.cancelOpenContext != nil {
		throttler.cancelOpenContext()
	}
	log.Infof("Throttler: finished execution of Close")
}

// requestHeartbeats sends a heartbeat lease request to the heartbeat writer.
// This action is recorded in stats.
func (throttler *Throttler) requestHeartbeats() {
	if !throttler.isLeader.Load() {
		return
	}
	go throttler.heartbeatWriter.RequestHeartbeats()
	statsThrottlerHeartbeatRequests.Add(1)
}

// stimulatePrimaryThrottler sends a check request to the primary tablet in the shard, to stimulate
// it to request for heartbeats.
func (throttler *Throttler) stimulatePrimaryThrottler(ctx context.Context, tmClient tmclient.TabletManagerClient) error {
	// Some reasonable timeout, to ensure we release connections even if they're hanging (otherwise grpc-go keeps polling those connections forever)
	ctx, cancel := context.WithTimeout(ctx, throttler.dormantPeriod)
	defer cancel()

	tabletAliases, err := throttler.ts.FindAllTabletAliasesInShard(ctx, throttler.keyspace, throttler.shard)
	if err != nil {
		return err
	}
	for _, tabletAlias := range tabletAliases {
		tablet, err := throttler.ts.GetTablet(ctx, tabletAlias)
		if err != nil {
			return err
		}
		if tablet.Type != topodatapb.TabletType_PRIMARY {
			continue
		}
		req := &tabletmanagerdatapb.CheckThrottlerRequest{AppName: throttlerapp.ThrottlerStimulatorName.String()}
		_, err = tmClient.CheckThrottler(ctx, tablet.Tablet, req)
		if err != nil {
			log.Errorf("stimulatePrimaryThrottler: %+v", err)
		}
		return err
	}
	return nil
}

// throttledAppsSnapshot returns a snapshot (a copy) of current throttled apps
func (throttler *Throttler) throttledAppsSnapshot() map[string]cache.Item {
	return throttler.throttledApps.Items()
}

// ThrottledAppsSnapshot returns a snapshot (a copy) of current throttled apps
func (throttler *Throttler) ThrottledApps() (result []base.AppThrottle) {
	for _, item := range throttler.throttledAppsSnapshot() {
		appThrottle, _ := item.Object.(*base.AppThrottle)
		result = append(result, *appThrottle)
	}
	return result
}

// isDormant returns true when the last check was more than dormantPeriod ago.
// Instead of measuring actual time, we use the fact recentCheckRateLimiter ticks every second, and take
// a logical diff, counting the number of ticks since the last check. This is a good enough approximation.
func (throttler *Throttler) isDormant() bool {
	if throttler.recentCheckRateLimiter == nil {
		return false
	}
	return throttler.recentCheckRateLimiter.Diff() > throttler.recentCheckDormantDiff
}

// recentlyChecked returns true when this throttler was checked "just now" (whereabouts of once second or two)
func (throttler *Throttler) recentlyChecked() bool {
	if throttler.recentCheckRateLimiter == nil {
		return false
	}
	return throttler.recentCheckRateLimiter.Diff() <= throttler.recentCheckDiff
}

// Operate is the main entry point for the throttler operation and logic. It will
// run the probes, collect metrics, refresh inventory, etc.
func (throttler *Throttler) Operate(ctx context.Context, wg *sync.WaitGroup) {
	tickers := [](*timer.SuspendableTicker){}
	addTicker := func(d time.Duration) *timer.SuspendableTicker {
		t := timer.NewSuspendableTicker(d, false)
		tickers = append(tickers, t)
		return t
	}
	leaderCheckTicker := addTicker(throttler.leaderCheckInterval)
	activeCollectTicker := addTicker(throttler.activeCollectInterval)
	dormantCollectTicker := addTicker(throttler.dormantCollectInterval)
	inventoryRefreshTicker := addTicker(throttler.inventoryRefreshInterval)
	metricsAggregateTicker := addTicker(throttler.metricsAggregateInterval)
	throttledAppsTicker := addTicker(throttler.throttledAppsSnapshotInterval)
	primaryStimulatorRateLimiter := timer.NewRateLimiter(throttler.dormantPeriod)
	throttler.recentCheckRateLimiter = timer.NewRateLimiter(recentCheckRateLimiterInterval)

	wg.Add(1)
	go func() {
		defer wg.Done() // Called last, once all tickers are stopped.

		defer func() {
			throttler.recentCheckRateLimiter.Stop()
			primaryStimulatorRateLimiter.Stop()
			throttler.aggregatedMetrics.Flush()
			throttler.recentApps.Flush()
			clear(throttler.inventory.TabletMetrics)
		}()
		// we do not flush throttler.throttledApps because this is data submitted by the user; the user expects the data to survive a disable+enable

		defer log.Infof("Throttler: Operate terminated, tickers stopped")
		for _, t := range tickers {
			defer t.Stop()
			// since we just started the tickers now, speed up the ticks by forcing an immediate tick
			go t.TickNow()
		}

		tmClient := throttler.overrideTmClient
		if tmClient == nil {
			// This is the normal production behavior.
			// throttler.overrideTmClient != nil only in unit testing
			tmClient = tmclient.NewTabletManagerClient()
			defer tmClient.Close()
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-leaderCheckTicker.C:
				func() {
					throttler.initMutex.Lock()
					defer throttler.initMutex.Unlock()

					// sparse
					shouldBeLeader := false
					if throttler.IsOpen() && throttler.tabletTypeFunc() == topodatapb.TabletType_PRIMARY {
						shouldBeLeader = true
					}

					isLeader := throttler.isLeader.Swap(shouldBeLeader)
					transitionedIntoLeader := false
					if shouldBeLeader && !isLeader {
						log.Infof("Throttler: transition into leadership")
						transitionedIntoLeader = true
					}
					if !shouldBeLeader && isLeader {
						log.Infof("Throttler: transition out of leadership")
					}

					if transitionedIntoLeader {
						// transitioned into leadership, let's speed up the next 'refresh' and 'collect' ticks
						go inventoryRefreshTicker.TickNow()
						throttler.requestHeartbeats()
					}
				}()
			case <-activeCollectTicker.C:
				if throttler.IsOpen() {
					// frequent
					// Always collect self metrics:
					throttler.collectSelfMetrics(ctx)
					if !throttler.isDormant() {
						throttler.collectShardMetrics(ctx, tmClient)
					}
					//
					if throttler.recentlyChecked() {
						if !throttler.isLeader.Load() {
							// This is a replica, and has just recently been checked.
							// We want to proactively "stimulate" the primary throttler to renew the heartbeat lease.
							// The intent is to "wake up" an on-demand heartbeat lease. We don't need to poke the
							// primary for every single time this replica was checked, so we rate limit. The idea is that
							// once heartbeats update, more checks will be successful, this replica will be "recently checked"
							// more than not, and the primary throttler will pick that up, extending the on-demand lease
							// even further.
							// Another outcome is that the primary will go out of "dormant" mode, and start collecting
							// replica metrics more frequently.
							primaryStimulatorRateLimiter.Do(
								func() error {
									return throttler.stimulatePrimaryThrottler(ctx, tmClient)
								})
						}
					}

				}
			case <-dormantCollectTicker.C:
				if throttler.IsOpen() {
					// infrequent
					if throttler.isDormant() {
						throttler.collectShardMetrics(ctx, tmClient)
					}
				}
			case metric := <-throttler.throttleMetricChan:
				// incoming metric, frequent, as result of collectMetrics()
				metricResultsMap, ok := throttler.inventory.TabletMetrics[metric.GetTabletAlias()]
				if !ok {
					metricResultsMap = base.NewMetricResultMap()
					throttler.inventory.TabletMetrics[metric.GetTabletAlias()] = metricResultsMap
				}
				metricResultsMap[metric.Name] = metric
			case <-inventoryRefreshTicker.C:
				// sparse
				if throttler.IsOpen() {
					throttler.refreshInventory(ctx)
				}
			case probes := <-throttler.clusterProbesChan:
				// incoming structural update, sparse, as result of refreshInventory()
				throttler.updateClusterProbes(probes)
			case <-metricsAggregateTicker.C:
				if throttler.IsOpen() {
					throttler.aggregateMetrics()
				}
			case <-throttledAppsTicker.C:
				if throttler.IsOpen() {
					go throttler.expireThrottledApps()
				}
			case throttlerConfig := <-throttler.throttlerConfigChan:
				throttler.applyThrottlerConfig(ctx, throttlerConfig)
			case f := <-throttler.serialFuncChan:
				// Used in unit testing to serialize operations and avoid race conditions
				f()
			}
		}
	}()
}

func (throttler *Throttler) generateTabletProbeFunction(scope base.Scope, tmClient tmclient.TabletManagerClient, probe *base.Probe) (probeFunc func(context.Context) base.ThrottleMetrics) {
	metricsWithError := func(err error) base.ThrottleMetrics {
		metrics := base.ThrottleMetrics{}
		for _, metricName := range base.KnownMetricNames {
			metrics[metricName] = &base.ThrottleMetric{
				Name:  metricName,
				Scope: scope,
				Alias: probe.Alias,
				Err:   err,
			}
		}
		return metrics
	}
	return func(ctx context.Context) base.ThrottleMetrics {
		// Some reasonable timeout, to ensure we release connections even if they're hanging (otherwise grpc-go keeps polling those connections forever)
		ctx, cancel := context.WithTimeout(ctx, 4*activeCollectInterval)
		defer cancel()

		// Hit a tablet's `check-self` via HTTP, and convert its CheckResult JSON output into a ThrottleMetric
		throttleMetric := base.NewThrottleMetric()
		throttleMetric.Name = base.DefaultMetricName
		throttleMetric.Scope = scope
		throttleMetric.Alias = probe.Alias

		if probe.Tablet == nil {
			return metricsWithError(fmt.Errorf("found nil tablet reference for alias '%v'", probe.Alias))
		}
		metrics := make(base.ThrottleMetrics)

		req := &tabletmanagerdatapb.CheckThrottlerRequest{MultiMetricsEnabled: true} // We leave AppName empty; it will default to VitessName anyway, and we can save some proto space
		resp, gRPCErr := tmClient.CheckThrottler(ctx, probe.Tablet, req)
		if gRPCErr != nil {
			return metricsWithError(fmt.Errorf("gRPC error accessing tablet %v. Err=%v", probe.Alias, gRPCErr))
		}
		throttleMetric.Value = resp.Value
		if resp.ResponseCode == tabletmanagerdatapb.CheckThrottlerResponseCode_INTERNAL_ERROR {
			throttleMetric.Err = fmt.Errorf("response code: %d", resp.ResponseCode)
		}
		if resp.StatusCode == http.StatusInternalServerError {
			throttleMetric.Err = fmt.Errorf("status code: %d", resp.StatusCode)
		}
		if resp.RecentlyChecked {
			// We have just probed a tablet, and it reported back that someone just recently "check"ed it.
			// We therefore renew the heartbeats lease.
			throttler.requestHeartbeats()
			statsThrottlerProbeRecentlyChecked.Add(1)
		}
		for name, metric := range resp.Metrics {
			metricName := base.MetricName(name)
			metrics[metricName] = &base.ThrottleMetric{
				Name:  metricName,
				Scope: scope,
				Alias: probe.Alias,
				Value: metric.Value,
			}
			if metric.Error != "" {
				metrics[metricName].Err = errors.New(metric.Error)
			}
		}
		if len(resp.Metrics) == 0 {
			// Backwards compatibility to v20. v20 does not report multi metrics.
			throttleMetric.Name = throttler.metricNameUsedAsDefault()
			metrics[throttleMetric.Name] = throttleMetric
		}

		return metrics
	}
}

// readSelfThrottleMetricsInternal rreads all registsred self metrics on this tablet (or backend MySQL server).
// This is the actual place where metrics are read, to be later aggregated and/or propagated to other tablets.
func (throttler *Throttler) readSelfThrottleMetricsInternal(ctx context.Context) base.ThrottleMetrics {
	result := make(base.ThrottleMetrics, len(base.RegisteredSelfMetrics))
	writeMetric := func(metric *base.ThrottleMetric) {
		select {
		case <-ctx.Done():
			return
		case throttler.throttleMetricChan <- metric:
		}
	}
	readMetric := func(selfMetric base.SelfMetric) *base.ThrottleMetric {
		if !selfMetric.RequiresConn() {
			return selfMetric.Read(ctx, throttler, nil)
		}
		conn, err := throttler.pool.Get(ctx, nil)
		if err != nil {
			return &base.ThrottleMetric{Err: err}
		}
		defer conn.Recycle()
		return selfMetric.Read(ctx, throttler, conn.Conn)
	}
	for metricName, selfMetric := range base.RegisteredSelfMetrics {
		if metricName == base.DefaultMetricName {
			continue
		}
		metric := readMetric(selfMetric)
		metric.Name = metricName
		metric.Alias = throttler.tabletAliasString()

		go writeMetric(metric)
		result[metricName] = metric
	}

	return result
}

func (throttler *Throttler) collectSelfMetrics(ctx context.Context) {
	probe := throttler.inventory.ClustersProbes[throttler.tabletAliasString()]
	if probe == nil {
		// probe not created yet
		return
	}
	go func() {
		// Avoid querying the same server twice at the same time. If previous read is still there,
		// we avoid re-reading it.
		if !atomic.CompareAndSwapInt64(&probe.QueryInProgress, 0, 1) {
			return
		}
		defer atomic.StoreInt64(&probe.QueryInProgress, 0)

		// Throttler is probing its own tablet's metrics:
		_ = base.ReadThrottleMetrics(ctx, probe, throttler.readSelfThrottleMetrics)
	}()
}

func (throttler *Throttler) collectShardMetrics(ctx context.Context, tmClient tmclient.TabletManagerClient) {
	// probes is known not to change. It can be *replaced*, but not changed.
	// so it's safe to iterate it
	for _, probe := range throttler.inventory.ClustersProbes {
		if probe.Alias == throttler.tabletAliasString() {
			// We skip collecting our own metrics
			continue
		}
		go func(probe *base.Probe) {
			// Avoid querying the same server twice at the same time. If previous read is still there,
			// we avoid re-reading it.
			if !atomic.CompareAndSwapInt64(&probe.QueryInProgress, 0, 1) {
				return
			}
			defer atomic.StoreInt64(&probe.QueryInProgress, 0)

			// Throttler probing other tablets:
			throttleMetricFunc := throttler.generateTabletProbeFunction(base.ShardScope, tmClient, probe)

			throttleMetrics := base.ReadThrottleMetrics(ctx, probe, throttleMetricFunc)
			for _, metric := range throttleMetrics {
				select {
				case <-ctx.Done():
					return
				case throttler.throttleMetricChan <- metric:
				}
			}
		}(probe)
	}
}

// refreshInventory will re-structure the inventory based on reading config settings
func (throttler *Throttler) refreshInventory(ctx context.Context) error {
	// distribute the query/threshold from the throttler down to the cluster settings and from there to the probes
	addProbe := func(alias string, tablet *topodatapb.Tablet, scope base.Scope, mysqlSettings *config.MySQLConfigurationSettings, probes base.Probes) bool {
		for _, ignore := range mysqlSettings.IgnoreHosts {
			if strings.Contains(alias, ignore) {
				log.Infof("Throttler: tablet ignored: %+v", alias)
				return false
			}
		}
		if scope != base.SelfScope {
			if alias == "" {
				log.Errorf("Throttler: got empty alias for scope: %+v", scope)
				return false
			}
			if tablet == nil {
				log.Errorf("Throttler: got nil tablet for alias: %v in scope: %+v", alias, scope)
				return false
			}
		}

		probe := &base.Probe{
			Alias:       alias,
			Tablet:      tablet,
			CacheMillis: mysqlSettings.CacheMillis,
		}
		probes[alias] = probe
		return true
	}

	attemptWriteProbes := func(clusterProbes *base.ClusterProbes) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case throttler.clusterProbesChan <- clusterProbes:
			return nil
		}
	}

	metricNameUsedAsDefault := throttler.metricNameUsedAsDefault()
	metricsThreshold := throttler.GetMetricsThreshold()
	for metricName, selfMetric := range base.RegisteredSelfMetrics {
		threshold := selfMetric.DefaultThreshold()
		if (metricName == metricNameUsedAsDefault || metricName == base.DefaultMetricName) && metricsThreshold != 0 {
			// backwards compatibility to v20:
			threshold = metricsThreshold
		}
		throttler.metricThresholds.Set(inventoryPrefix+metricName.String(), threshold, cache.DefaultExpiration)
	}
	throttler.convergeMetricThresholds()

	var clusterSettingsCopy config.MySQLConfigurationSettings = throttler.configSettings.MySQLStore
	// config may dynamically change, but internal structure (config.Settings().MySQLStore.Clusters in our case)
	// is immutable and can only be _replaced_. Hence, it's safe to read in a goroutine:
	collect := func() error {
		clusterProbes := &base.ClusterProbes{
			IgnoreHostsCount: clusterSettingsCopy.IgnoreHostsCount,
			TabletProbes:     base.NewProbes(),
		}
		// self tablet
		addProbe(throttler.tabletAliasString(), nil, base.SelfScope, &clusterSettingsCopy, clusterProbes.TabletProbes)
		if !throttler.isLeader.Load() {
			// This tablet may have used to be the primary, but it isn't now. It may have a recollection
			// of previous clusters it used to probe. It may have recollection of specific probes for such clusters.
			// This now ensures any existing cluster probes are overridden with an empty list of probes.
			// `clusterProbes` was created above as empty, and identifiable via `scope`. This will in turn
			// be used to overwrite throttler.inventory.ClustersProbes[clusterProbes.scope] in
			// updateClusterProbes().
			return attemptWriteProbes(clusterProbes)
			// not the leader (primary tablet)? Then no more work for us.
		}
		// The primary tablet is also in charge of collecting the shard's metrics
		ctx, cancel := context.WithTimeout(ctx, inventoryRefreshInterval)
		defer cancel()

		tabletAliases, err := throttler.ts.FindAllTabletAliasesInShard(ctx, throttler.keyspace, throttler.shard)
		if err != nil {
			return err
		}
		for _, tabletAlias := range tabletAliases {
			tablet, err := throttler.ts.GetTablet(ctx, tabletAlias)
			if err != nil {
				return err
			}
			if throttler.throttleTabletTypesMap[tablet.Type] {
				addProbe(topoproto.TabletAliasString(tabletAlias), tablet.Tablet, base.ShardScope, &clusterSettingsCopy, clusterProbes.TabletProbes)
			}
		}

		return attemptWriteProbes(clusterProbes)
	}
	go func() {
		if err := collect(); err != nil {
			log.Errorf("refreshInventory: %+v", err)
		}
	}()
	return nil
}

// synchronous update of inventory
func (throttler *Throttler) updateClusterProbes(clusterProbes *base.ClusterProbes) error {
	throttler.inventory.ClustersProbes = clusterProbes.TabletProbes
	throttler.inventory.IgnoreHostsCount = clusterProbes.IgnoreHostsCount
	throttler.inventory.IgnoreHostsThreshold = clusterProbes.IgnoreHostsThreshold

	for alias := range throttler.inventory.TabletMetrics {
		if alias == "" {
			// *this* tablet uses the empty alias to identify itself.
			continue
		}
		if _, found := clusterProbes.TabletProbes[alias]; !found {
			// There seems to be a metric stored for some alias, say zone1-0000000102,
			// but there is no alias for this probe in the new clusterProbes. This
			// suggests that the corresponding tablet has been removed, or its type was changed
			// (e.g. from REPLICA to RDONLY). We should therefore remove this cached metric.
			delete(throttler.inventory.TabletMetrics, alias)
		}
	}

	return nil
}

func (throttler *Throttler) metricNameUsedAsDefault() base.MetricName {
	if throttler.GetCustomMetricsQuery() == "" {
		return base.LagMetricName
	}
	return base.CustomMetricName
}

// synchronous aggregation of collected data
func (throttler *Throttler) aggregateMetrics() error {
	metricNameUsedAsDefault := throttler.metricNameUsedAsDefault()
	aggregateTabletsMetrics := func(scope base.Scope, metricName base.MetricName, tabletResultsMap base.TabletResultMap) {
		ignoreHostsCount := throttler.inventory.IgnoreHostsCount
		ignoreHostsThreshold := throttler.inventory.IgnoreHostsThreshold
		ignoreDialTCPErrors := throttler.configSettings.MySQLStore.IgnoreDialTCPErrors

		aggregatedMetric := base.AggregateTabletMetricResults(metricName, tabletResultsMap, ignoreHostsCount, ignoreDialTCPErrors, ignoreHostsThreshold)
		aggregatedMetricName := metricName.AggregatedName(scope)
		throttler.aggregatedMetrics.Set(aggregatedMetricName, aggregatedMetric, cache.DefaultExpiration)
		if metricName == metricNameUsedAsDefault {
			throttler.aggregatedMetrics.Set(base.DefaultMetricName.AggregatedName(scope), aggregatedMetric, cache.DefaultExpiration)
		}
	}
	for _, metricName := range base.KnownMetricNames {
		if metricName == base.DefaultMetricName {
			// "default metric" is for backwards compatibility with v20, which does not support multi-metrics.
			// We do not measure "default metric". Instead, we aggregate _actual_ metrics, and decide which one
			// is to be stored as "default"
			continue
		}
		selfResultsMap, shardResultsMap := throttler.inventory.TabletMetrics.Split(throttler.tabletAliasString())
		aggregateTabletsMetrics(base.SelfScope, metricName, selfResultsMap)
		aggregateTabletsMetrics(base.ShardScope, metricName, shardResultsMap)
	}
	return nil
}

func (throttler *Throttler) getAggregatedMetric(aggregatedName string) base.MetricResult {
	if metricResultVal, found := throttler.aggregatedMetrics.Get(aggregatedName); found {
		return metricResultVal.(base.MetricResult)
	}
	return base.NoSuchMetric
}

func (throttler *Throttler) getScopedMetric(scope base.Scope, metricName base.MetricName) (base.MetricResult, float64) {
	thresholdVal, found := throttler.metricThresholds.Get(metricName.String())
	if !found {
		return base.NoSuchMetric, 0
	}
	threshold, _ := thresholdVal.(float64)
	aggregatedName := metricName.AggregatedName(scope)
	return throttler.getAggregatedMetric(aggregatedName), threshold
}

func (throttler *Throttler) metricThresholdsSnapshot() map[string]float64 {
	snapshot := make(map[string]float64)
	for key, value := range throttler.metricThresholds.Items() {
		threshold, _ := value.Object.(float64)
		snapshot[key] = threshold
	}
	return snapshot
}

func (throttler *Throttler) appCheckedMetricsSnapshot() map[string]string {
	snapshot := make(map[string]string)
	for key, item := range throttler.appCheckedMetrics.Items() {
		var metricNames base.MetricNames
		switch val := item.Object.(type) {
		case base.MetricNames:
			metricNames = val
		case []base.MetricName:
			metricNames = val
		}
		snapshot[key] = metricNames.String()
	}
	return snapshot
}

func (throttler *Throttler) aggregatedMetricsSnapshot() map[string]base.MetricResult {
	snapshot := make(map[string]base.MetricResult)
	for key, value := range throttler.aggregatedMetrics.Items() {
		metricResult, _ := value.Object.(base.MetricResult)
		snapshot[key] = metricResult
	}
	return snapshot
}

func (throttler *Throttler) expireThrottledApps() {
	now := time.Now()
	for appName, item := range throttler.throttledApps.Items() {
		appThrottle := item.Object.(*base.AppThrottle)
		if !appThrottle.ExpireAt.After(now) {
			throttler.UnthrottleApp(appName)
		}
	}
}

// ThrottleApp instructs the throttler to begin throttling an app, to some period and with some ratio.
func (throttler *Throttler) ThrottleApp(appName string, expireAt time.Time, ratio float64, exempt bool) (appThrottle *base.AppThrottle) {
	throttler.throttledAppsMutex.Lock()
	defer throttler.throttledAppsMutex.Unlock()

	now := time.Now()
	if object, found := throttler.throttledApps.Get(appName); found {
		appThrottle = object.(*base.AppThrottle)
		if !expireAt.IsZero() {
			appThrottle.ExpireAt = expireAt
		}
		if ratio >= 0 {
			appThrottle.Ratio = ratio
		}
		appThrottle.Exempt = exempt
	} else {
		if expireAt.IsZero() {
			expireAt = now.Add(DefaultAppThrottleDuration)
		}
		if ratio < 0 {
			ratio = DefaultThrottleRatio
		}
		appThrottle = base.NewAppThrottle(appName, expireAt, ratio, exempt)
	}
	if appThrottle.ExpireAt.After(now) {
		throttler.throttledApps.Set(appName, appThrottle, cache.DefaultExpiration)
	} else {
		throttler.UnthrottleApp(appName)
	}
	return appThrottle
}

// UnthrottleApp cancels any throttling, if any, for a given app
func (throttler *Throttler) UnthrottleApp(appName string) (appThrottle *base.AppThrottle) {
	throttler.throttledApps.Delete(appName)
	// the app is likely to check
	throttler.requestHeartbeats()
	return base.NewAppThrottle(appName, time.Now(), 0, false)
}

// IsAppThrottled tells whether some app should be throttled.
// Assuming an app is throttled to some extend, it will randomize the result based
// on the throttle ratio
func (throttler *Throttler) IsAppThrottled(appName string) (bool, string) {
	appFound := ""
	isSingleAppNameThrottled := func(singleAppName string) bool {
		object, found := throttler.throttledApps.Get(singleAppName)
		if !found {
			return false
		}
		appThrottle := object.(*base.AppThrottle)
		if !appThrottle.ExpireAt.After(time.Now()) {
			// throttling cleanup hasn't purged yet, but it is expired.
			return false
		}
		// From this point on, we consider that this app has some throttling configuration
		// of any sort.
		appFound = singleAppName
		if appThrottle.Exempt {
			return false
		}
		// handle ratio
		if rand.Float64() < appThrottle.Ratio {
			return true
		}
		return false
	}
	if isSingleAppNameThrottled(appName) {
		return true, appName
	}
	for _, singleAppName := range throttlerapp.Name(appName).SplitStrings() {
		if singleAppName == "" {
			continue
		}
		if isSingleAppNameThrottled(singleAppName) {
			return true, singleAppName
		}
	}
	// If app was found then there was some explicit throttle instruction for the app, and the app
	// passed the test.
	if appFound != "" {
		return false, appFound
	}
	// If the app was not found, ie no specific throttle instruction was found for the app, then
	// the app should also consider the case where the "all" app is throttled.
	if isSingleAppNameThrottled(throttlerapp.AllName.String()) {
		// Means the "all" app is throttled. This is a special case, and it means "all apps are throttled"
		return true, throttlerapp.AllName.String()
	}
	return false, appName
}

// IsAppExempt
func (throttler *Throttler) IsAppExempted(appName string) (bool, string) {
	isSingleAppNameExempted := func(singleAppName string) bool {
		if throttlerapp.ExemptFromChecks(appName) { // well known statically exempted apps
			return true
		}
		object, found := throttler.throttledApps.Get(singleAppName)
		if !found {
			return false
		}
		appThrottle := object.(*base.AppThrottle)
		if !appThrottle.ExpireAt.After(time.Now()) {
			// throttling cleanup hasn't purged yet, but it is expired
			return false
		}
		if appThrottle.Exempt {
			return true
		}
		return false
	}
	if isSingleAppNameExempted(appName) {
		return true, appName
	}
	for _, singleAppName := range throttlerapp.Name(appName).SplitStrings() {
		if singleAppName == "" {
			continue
		}
		if isSingleAppNameExempted(singleAppName) {
			return true, singleAppName
		}
	}

	if isSingleAppNameExempted(throttlerapp.AllName.String()) {
		if throttled, _ := throttler.IsAppThrottled(appName); !throttled {
			return true, throttlerapp.AllName.String()
		}
	}

	return false, appName
}

// ThrottledAppsMap returns a (copy) map of currently throttled apps
func (throttler *Throttler) ThrottledAppsMap() (result map[string](*base.AppThrottle)) {
	result = make(map[string](*base.AppThrottle))

	for appName, item := range throttler.throttledApps.Items() {
		appThrottle := item.Object.(*base.AppThrottle)
		result[appName] = appThrottle
	}
	return result
}

// markRecentApp takes note that an app has just asked about throttling, making it "recent"
func (throttler *Throttler) markRecentApp(appName string, statusCode int, responseCode tabletmanagerdatapb.CheckThrottlerResponseCode) {
	recentApp := base.NewRecentApp(appName, statusCode, responseCode)
	throttler.recentApps.Set(appName, recentApp, cache.DefaultExpiration)
}

// recentAppsSnapshot returns a (copy) map of apps which checked for throttling recently
func (throttler *Throttler) recentAppsSnapshot() (result map[string](*base.RecentApp)) {
	result = make(map[string](*base.RecentApp))

	for recentAppKey, item := range throttler.recentApps.Items() {
		recentApp := item.Object.(*base.RecentApp)
		result[recentAppKey] = recentApp
	}
	return result
}

// markMetricHealthy will mark the time "now" as the last time a given metric was checked to be "OK"
func (throttler *Throttler) markMetricHealthy(metricName string) {
	throttler.metricsHealth.Set(metricName, time.Now(), cache.DefaultExpiration)
}

// timeSinceMetricHealthy returns time elapsed since the last time a metric checked "OK"
func (throttler *Throttler) timeSinceMetricHealthy(metricName string) (timeSinceHealthy time.Duration, found bool) {
	if lastOKTime, found := throttler.metricsHealth.Get(metricName); found {
		return time.Since(lastOKTime.(time.Time)), true
	}
	return 0, false
}

func (throttler *Throttler) metricsHealthSnapshot() base.MetricHealthMap {
	snapshot := make(base.MetricHealthMap)
	for key, value := range throttler.metricsHealth.Items() {
		lastHealthyAt, _ := value.Object.(time.Time)
		snapshot[key] = base.NewMetricHealth(lastHealthyAt)
	}
	return snapshot
}

// AppRequestMetricResult gets a metric result in the context of a specific app
func (throttler *Throttler) AppRequestMetricResult(ctx context.Context, appName string, metricResultFunc base.MetricResultFunc, denyApp bool) (metricResult base.MetricResult, threshold float64, matchedApp string) {
	if denyApp {
		return base.AppDeniedMetric, 0, appName
	}
	throttled, matchedApp := throttler.IsAppThrottled(appName)
	if throttled {
		return base.AppDeniedMetric, 0, matchedApp
	}
	metricResult, threshold = metricResultFunc()
	return metricResult, threshold, matchedApp
}

// checkScope checks the aggregated value of given store
func (throttler *Throttler) checkScope(ctx context.Context, appName string, scope base.Scope, metricNames base.MetricNames, flags *CheckFlags) (checkResult *CheckResult) {
	if !throttler.IsRunning() {
		return okMetricCheckResult
	}
	if exempted, matchedApp := throttler.IsAppExempted(appName); exempted {
		// Some apps are exempt from checks. They are always responded with OK. This is because those apps are
		// continuous and do not generate a substantial load.
		result := okMetricCheckResult
		result.AppName = matchedApp
		return result
	}

	matchedApp := appName
	if len(metricNames) == 0 {
		// No explicit metrics requested.
		// Get the metric names mappd to the given app
		for _, appToken := range throttlerapp.Name(appName).SplitStrings() {
			if val, found := throttler.appCheckedMetrics.Get(appToken); found {
				// Due to golang type system, it's possible that we put in a base.MetricNames and get back a []base.MetricName.
				// We allow both forms, which are technically the same type.
				switch val := val.(type) {
				case base.MetricNames:
					metricNames = append(metricNames, val...)
				case []base.MetricName:
					metricNames = append(metricNames, val...)
				}
				matchedApp = appToken
			}
		}
	}
	if len(metricNames) == 0 && !throttlerapp.AllName.Equals(appName) {
		// No specific metrics mapped to this app. Are there specific metrics
		// mapped to the "all" app?
		if val, found := throttler.appCheckedMetrics.Get(throttlerapp.AllName.String()); found {
			switch val := val.(type) {
			case base.MetricNames:
				metricNames = val
			case []base.MetricName:
				metricNames = val
			}
			matchedApp = throttlerapp.AllName.String()
		}
	}
	if throttlerapp.VitessName.Equals(appName) {
		// "vitess" always checks all metrics, irrespective of what is mapped.
		metricNames = base.KnownMetricNames
		matchedApp = appName
	}
	if len(metricNames) == 0 {
		// Nothing mapped? For backwards compatibility and as default, we use the "default" metric.
		metricNames = base.MetricNames{throttler.metricNameUsedAsDefault()}
	}
	checkResult = throttler.check.Check(ctx, appName, scope, metricNames, flags)
	checkResult.AppName = matchedApp

	shouldRequestHeartbeats := !flags.SkipRequestHeartbeats
	if throttlerapp.VitessName.Equals(appName) {
		// Override: "vitess" app never requests heartbeats.
		shouldRequestHeartbeats = false
	}
	if throttlerapp.ThrottlerStimulatorName.Equals(appName) {
		// Ovreride: "throttler-stimulator" app always requests heartbeats.
		shouldRequestHeartbeats = true
	}

	if shouldRequestHeartbeats {
		throttler.requestHeartbeats()
		throttler.recentCheckRateLimiter.DoEmpty()
		// This check was made by someone other than the throttler itself, i.e. this came from online-ddl or vreplication or other.
		// We mark the fact that someone just made a check. If this is a REPLICA or RDONLY tables, this will be reported back
		// to the PRIMARY so that it knows it must renew the heartbeat lease.
		checkResult.RecentlyChecked = true
	}
	if !checkResult.RecentlyChecked {
		checkResult.RecentlyChecked = throttler.recentlyChecked()
	}

	return checkResult
}

// Check runs a check by requested check type
func (throttler *Throttler) Check(ctx context.Context, appName string, metricNames base.MetricNames, flags *CheckFlags) (checkResult *CheckResult) {
	scope := base.UndefinedScope
	if flags != nil {
		scope = flags.Scope
	}
	if scope == base.ShardScope && throttler.checkAsCheckSelf.Load() {
		scope = base.SelfScope
	}
	return throttler.checkScope(ctx, appName, scope, metricNames, flags)
}

func (throttler *Throttler) MetricName(s string) base.MetricName {
	if s == "" {
		return base.DefaultMetricName
	}
	return base.MetricName(s)
}

func (throttler *Throttler) MetricNames(s []string) base.MetricNames {
	result := make(base.MetricNames, len(s))
	for i, metricName := range s {
		result[i] = throttler.MetricName(metricName)
	}
	if len(s) == 0 {
		result = append(result, base.DefaultMetricName)
	}
	return result
}

// Status exports a status breakdown
func (throttler *Throttler) Status() *ThrottlerStatus {
	return &ThrottlerStatus{
		Keyspace: throttler.keyspace,
		Shard:    throttler.shard,

		IsLeader:        throttler.isLeader.Load(),
		IsOpen:          throttler.isOpen.Load(),
		IsEnabled:       throttler.isEnabled.Load(),
		IsDormant:       throttler.isDormant(),
		RecentlyChecked: throttler.recentlyChecked(),

		Query:                   throttler.GetMetricsQuery(),
		CustomQuery:             throttler.GetCustomMetricsQuery(),
		Threshold:               throttler.GetMetricsThreshold(),
		MetricNameUsedAsDefault: throttler.metricNameUsedAsDefault().String(),

		AggregatedMetrics: throttler.aggregatedMetricsSnapshot(),
		MetricsThresholds: throttler.metricThresholdsSnapshot(),
		MetricsHealth:     throttler.metricsHealthSnapshot(),
		ThrottledApps:     throttler.ThrottledApps(),
		AppCheckedMetrics: throttler.appCheckedMetricsSnapshot(),
		RecentApps:        throttler.recentAppsSnapshot(),
	}
}
