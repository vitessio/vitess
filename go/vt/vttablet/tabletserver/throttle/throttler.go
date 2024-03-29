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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/protoutil"
	"vitess.io/vitess/go/stats"

	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/log"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/heartbeat"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/config"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/mysql"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
)

const (
	leaderCheckInterval            = 5 * time.Second
	mysqlCollectInterval           = 250 * time.Millisecond // PRIMARY polls replicas
	mysqlDormantCollectInterval    = 5 * time.Second        // PRIMARY polls replicas when dormant (no recent checks)
	mysqlRefreshInterval           = 10 * time.Second       // Refreshing tablet inventory
	mysqlAggregateInterval         = 125 * time.Millisecond
	throttledAppsSnapshotInterval  = 5 * time.Second
	recentCheckRateLimiterInterval = 1 * time.Second // Ticker assisting in determining when the throttler was last checked

	aggregatedMetricsExpiration = 5 * time.Second
	recentAppsExpiration        = time.Hour * 24

	nonDeprioritizedAppMapExpiration = time.Second

	dormantPeriod              = time.Minute // How long since last check to be considered dormant
	DefaultAppThrottleDuration = time.Hour
	DefaultThrottleRatio       = 1.0

	shardStoreName = "shard"
	selfStoreName  = "self"

	defaultReplicationLagQuery = "select unix_timestamp(now(6))-max(ts/1000000000) as replication_lag from %s.heartbeat"
)

var (
	// flag vars
	defaultThrottleLagThreshold = 5 * time.Second
	throttleTabletTypes         = "replica"
)

var (
	statsThrottlerHeartbeatRequests    = stats.NewCounter("ThrottlerHeartbeatRequests", "heartbeat requests")
	statsThrottlerRecentlyChecked      = stats.NewCounter("ThrottlerRecentlyChecked", "recently checked")
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

// ThrottleCheckType allows a client to indicate what type of check it wants to issue. See available types below.
type ThrottleCheckType int // nolint:revive

const (
	// ThrottleCheckPrimaryWrite indicates a check before making a write on a primary server
	ThrottleCheckPrimaryWrite ThrottleCheckType = iota
	// ThrottleCheckSelf indicates a check on a specific server health
	ThrottleCheckSelf
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
	keyspace string
	shard    string
	cell     string

	check     *ThrottlerCheck
	isEnabled atomic.Bool
	isLeader  atomic.Bool
	isOpen    atomic.Bool

	leaderCheckInterval           time.Duration
	mysqlCollectInterval          time.Duration
	mysqlDormantCollectInterval   time.Duration
	mysqlRefreshInterval          time.Duration
	mysqlAggregateInterval        time.Duration
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

	throttleTabletTypesMap map[topodatapb.TabletType]bool

	mysqlThrottleMetricChan chan *mysql.MySQLThrottleMetric
	mysqlInventoryChan      chan *mysql.Inventory
	mysqlClusterProbesChan  chan *mysql.ClusterProbes
	throttlerConfigChan     chan *topodatapb.ThrottlerConfig

	mysqlInventory *mysql.Inventory

	metricsQuery     atomic.Value
	MetricsThreshold atomic.Uint64
	checkAsCheckSelf atomic.Bool

	mysqlClusterThresholds *cache.Cache
	aggregatedMetrics      *cache.Cache
	throttledApps          *cache.Cache
	recentApps             *cache.Cache
	metricsHealth          *cache.Cache

	initMutex           sync.Mutex
	enableMutex         sync.Mutex
	cancelOpenContext   context.CancelFunc
	cancelEnableContext context.CancelFunc
	throttledAppsMutex  sync.Mutex

	readSelfThrottleMetric func(context.Context, *mysql.Probe) *mysql.MySQLThrottleMetric // overwritten by unit test

	nonLowPriorityAppRequestsThrottled *cache.Cache
	httpClient                         *http.Client
}

// ThrottlerStatus published some status values from the throttler
type ThrottlerStatus struct {
	Keyspace string
	Shard    string

	IsLeader  bool
	IsOpen    bool
	IsEnabled bool
	IsDormant bool

	Query     string
	Threshold float64

	AggregatedMetrics map[string]base.MetricResult
	MetricsHealth     base.MetricHealthMap
}

// NewThrottler creates a Throttler
func NewThrottler(env tabletenv.Env, srvTopoServer srvtopo.Server, ts *topo.Server, cell string, heartbeatWriter heartbeat.HeartbeatWriter, tabletTypeFunc func() topodatapb.TabletType) *Throttler {
	throttler := &Throttler{
		cell:            cell,
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

	throttler.mysqlThrottleMetricChan = make(chan *mysql.MySQLThrottleMetric)
	throttler.mysqlInventoryChan = make(chan *mysql.Inventory, 1)
	throttler.mysqlClusterProbesChan = make(chan *mysql.ClusterProbes)
	throttler.throttlerConfigChan = make(chan *topodatapb.ThrottlerConfig)
	throttler.mysqlInventory = mysql.NewInventory()

	throttler.throttledApps = cache.New(cache.NoExpiration, 0)
	throttler.mysqlClusterThresholds = cache.New(cache.NoExpiration, 0)
	throttler.aggregatedMetrics = cache.New(aggregatedMetricsExpiration, 0)
	throttler.recentApps = cache.New(recentAppsExpiration, 0)
	throttler.metricsHealth = cache.New(cache.NoExpiration, 0)
	throttler.nonLowPriorityAppRequestsThrottled = cache.New(nonDeprioritizedAppMapExpiration, 0)

	throttler.httpClient = base.SetupHTTPClient(2 * mysqlCollectInterval)
	throttler.initThrottleTabletTypes()
	throttler.check = NewThrottlerCheck(throttler)

	throttler.leaderCheckInterval = leaderCheckInterval
	throttler.mysqlCollectInterval = mysqlCollectInterval
	throttler.mysqlDormantCollectInterval = mysqlDormantCollectInterval
	throttler.mysqlRefreshInterval = mysqlRefreshInterval
	throttler.mysqlAggregateInterval = mysqlAggregateInterval
	throttler.throttledAppsSnapshotInterval = throttledAppsSnapshotInterval
	throttler.dormantPeriod = dormantPeriod
	throttler.recentCheckDormantDiff = int64(throttler.dormantPeriod / recentCheckRateLimiterInterval)

	throttler.StoreMetricsThreshold(defaultThrottleLagThreshold.Seconds()) //default
	throttler.readSelfThrottleMetric = func(ctx context.Context, p *mysql.Probe) *mysql.MySQLThrottleMetric {
		return throttler.readSelfMySQLThrottleMetric(ctx, p)
	}

	return throttler
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
	return throttler.metricsQuery.Load().(string)
}

func (throttler *Throttler) GetMetricsThreshold() float64 {
	return math.Float64frombits(throttler.MetricsThreshold.Load())
}

// initThrottler initializes config
func (throttler *Throttler) initConfig() {
	log.Infof("Throttler: initializing config")

	throttler.configSettings = &config.ConfigurationSettings{
		Stores: config.StoresSettings{
			MySQL: config.MySQLConfigurationSettings{
				IgnoreDialTCPErrors: true,
				Clusters:            map[string](*config.MySQLClusterConfigurationSettings){},
			},
		},
	}
	throttler.configSettings.Stores.MySQL.Clusters[selfStoreName] = &config.MySQLClusterConfigurationSettings{
		MetricQuery:       throttler.GetMetricsQuery(),
		ThrottleThreshold: &throttler.MetricsThreshold,
		IgnoreHostsCount:  0,
	}
	throttler.configSettings.Stores.MySQL.Clusters[shardStoreName] = &config.MySQLClusterConfigurationSettings{
		MetricQuery:       throttler.GetMetricsQuery(),
		ThrottleThreshold: &throttler.MetricsThreshold,
		IgnoreHostsCount:  0,
	}
}

// readThrottlerConfig proactively reads the throttler's config from SrvKeyspace in local topo
func (throttler *Throttler) readThrottlerConfig(ctx context.Context) (*topodatapb.ThrottlerConfig, error) {
	srvks, err := throttler.ts.GetSrvKeyspace(ctx, throttler.cell, throttler.keyspace)
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
	if throttlerConfig.CustomQuery == "" {
		// no custom query; we check replication lag
		if throttlerConfig.Threshold == 0 {
			throttlerConfig.Threshold = defaultThrottleLagThreshold.Seconds()
		}
	}
	return throttlerConfig
}

func (throttler *Throttler) WatchSrvKeyspaceCallback(srvks *topodatapb.SrvKeyspace, err error) bool {
	if err != nil {
		if !topo.IsErrType(err, topo.Interrupted) && !errors.Is(err, context.Canceled) {
			log.Errorf("WatchSrvKeyspaceCallback error: %v", err)
		}
		return false
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

// applyThrottlerConfig receives a Throttlerconfig as read from SrvKeyspace, and applies the configuration.
// This may cause the throttler to be enabled/disabled, and of course it affects the throttling query/threshold.
// Note: you should be holding the initMutex when calling this function.
func (throttler *Throttler) applyThrottlerConfig(ctx context.Context, throttlerConfig *topodatapb.ThrottlerConfig) {
	log.Infof("Throttler: applying topo config: %+v", throttlerConfig)
	if throttlerConfig.CustomQuery == "" {
		throttler.metricsQuery.Store(sqlparser.BuildParsedQuery(defaultReplicationLagQuery, sidecar.GetIdentifier()).Query)
	} else {
		throttler.metricsQuery.Store(throttlerConfig.CustomQuery)
	}
	throttler.StoreMetricsThreshold(throttlerConfig.Threshold)
	throttler.checkAsCheckSelf.Store(throttlerConfig.CheckAsCheckSelf)
	for _, appRule := range throttlerConfig.ThrottledApps {
		throttler.ThrottleApp(appRule.Name, protoutil.TimeFromProto(appRule.ExpiresAt).UTC(), appRule.Ratio, appRule.Exempt)
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
	// _ = throttler.updateConfig(ctx, false, throttler.MetricsThreshold.Get()) // TODO(shlomi)

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
				throttler.srvTopoServer.WatchSrvKeyspace(ctx, throttler.cell, throttler.keyspace, throttler.WatchSrvKeyspaceCallback)
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
	// The query needs to be dynamically built because the sidecar database name
	// is not known when the TabletServer is created, which in turn creates the
	// Throttler.
	throttler.metricsQuery.Store(sqlparser.BuildParsedQuery(defaultReplicationLagQuery, sidecar.GetIdentifier()).Query) // default
	throttler.initConfig()
	throttler.pool.Open(throttler.env.Config().DB.AppWithDB(), throttler.env.Config().DB.DbaWithDB(), throttler.env.Config().DB.AppDebugWithDB())

	throttler.ThrottleApp("always-throttled-app", time.Now().Add(time.Hour*24*365*10), DefaultThrottleRatio, false)

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

func (throttler *Throttler) generateSelfMySQLThrottleMetricFunc(ctx context.Context, probe *mysql.Probe) func() *mysql.MySQLThrottleMetric {
	f := func() *mysql.MySQLThrottleMetric {
		return throttler.readSelfThrottleMetric(ctx, probe)
	}
	return f
}

// readSelfMySQLThrottleMetric reads the mysql metric from thi very tablet's backend mysql.
func (throttler *Throttler) readSelfMySQLThrottleMetric(ctx context.Context, probe *mysql.Probe) *mysql.MySQLThrottleMetric {
	metric := &mysql.MySQLThrottleMetric{
		ClusterName: selfStoreName,
		Alias:       "",
		Value:       0,
		Err:         nil,
	}
	conn, err := throttler.pool.Get(ctx, nil)
	if err != nil {
		metric.Err = err
		return metric
	}
	defer conn.Recycle()

	tm, err := conn.Conn.Exec(ctx, probe.MetricQuery, 1, true)
	if err != nil {
		metric.Err = err
		return metric
	}
	row := tm.Named().Row()
	if row == nil {
		metric.Err = fmt.Errorf("no results for readSelfMySQLThrottleMetric")
		return metric
	}

	metricsQueryType := mysql.GetMetricsQueryType(throttler.GetMetricsQuery())
	switch metricsQueryType {
	case mysql.MetricsQueryTypeSelect:
		// We expect a single row, single column result.
		// The "for" iteration below is just a way to get first result without knowing column name
		for k := range row {
			metric.Value, metric.Err = row.ToFloat64(k)
		}
	case mysql.MetricsQueryTypeShowGlobal:
		metric.Value, metric.Err = strconv.ParseFloat(row["Value"].ToString(), 64)
	default:
		metric.Err = fmt.Errorf("Unsupported metrics query type for query: %s", throttler.GetMetricsQuery())
	}

	return metric
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
	return throttler.recentCheckRateLimiter.Diff() > throttler.recentCheckDormantDiff
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
	mysqlCollectTicker := addTicker(throttler.mysqlCollectInterval)
	mysqlDormantCollectTicker := addTicker(throttler.mysqlDormantCollectInterval)
	mysqlRefreshTicker := addTicker(throttler.mysqlRefreshInterval)
	mysqlAggregateTicker := addTicker(throttler.mysqlAggregateInterval)
	throttledAppsTicker := addTicker(throttler.throttledAppsSnapshotInterval)
	primaryStimulatorRateLimiter := timer.NewRateLimiter(throttler.dormantPeriod)
	throttler.recentCheckRateLimiter = timer.NewRateLimiter(recentCheckRateLimiterInterval)

	wg.Add(1)
	go func() {
		defer func() {
			throttler.recentCheckRateLimiter.Stop()
			primaryStimulatorRateLimiter.Stop()
			throttler.aggregatedMetrics.Flush()
			throttler.recentApps.Flush()
			throttler.nonLowPriorityAppRequestsThrottled.Flush()
			wg.Done()
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
						go mysqlRefreshTicker.TickNow()
						throttler.requestHeartbeats()
					}
				}()
			case <-mysqlCollectTicker.C:
				if throttler.IsOpen() {
					// frequent
					// Always collect self metrics:
					throttler.collectMySQLMetrics(ctx, tmClient, func(clusterName string) bool {
						return clusterName == selfStoreName
					})
					if !throttler.isDormant() {
						throttler.collectMySQLMetrics(ctx, tmClient, func(clusterName string) bool {
							return clusterName != selfStoreName
						})
					}
					//
					if throttler.recentCheckRateLimiter.Diff() <= 1 { // recently checked
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
			case <-mysqlDormantCollectTicker.C:
				if throttler.IsOpen() {
					// infrequent
					if throttler.isDormant() {
						throttler.collectMySQLMetrics(ctx, tmClient, func(clusterName string) bool {
							return clusterName != selfStoreName
						})
					}
				}
			case metric := <-throttler.mysqlThrottleMetricChan:
				// incoming MySQL metric, frequent, as result of collectMySQLMetrics()
				throttler.mysqlInventory.TabletMetrics[metric.GetClusterTablet()] = metric
			case <-mysqlRefreshTicker.C:
				// sparse
				if throttler.IsOpen() {
					throttler.refreshMySQLInventory(ctx)
				}
			case probes := <-throttler.mysqlClusterProbesChan:
				// incoming structural update, sparse, as result of refreshMySQLInventory()
				throttler.updateMySQLClusterProbes(ctx, probes)
			case <-mysqlAggregateTicker.C:
				if throttler.IsOpen() {
					throttler.aggregateMySQLMetrics(ctx)
				}
			case <-throttledAppsTicker.C:
				if throttler.IsOpen() {
					go throttler.expireThrottledApps()
				}
			case throttlerConfig := <-throttler.throttlerConfigChan:
				throttler.applyThrottlerConfig(ctx, throttlerConfig)
			}
		}
	}()
}

func (throttler *Throttler) generateTabletProbeFunction(ctx context.Context, clusterName string, tmClient tmclient.TabletManagerClient, probe *mysql.Probe) (probeFunc func() *mysql.MySQLThrottleMetric) {
	return func() *mysql.MySQLThrottleMetric {
		// Some reasonable timeout, to ensure we release connections even if they're hanging (otherwise grpc-go keeps polling those connections forever)
		ctx, cancel := context.WithTimeout(ctx, 4*mysqlCollectInterval)
		defer cancel()

		// Hit a tablet's `check-self` via HTTP, and convert its CheckResult JSON output into a MySQLThrottleMetric
		mySQLThrottleMetric := mysql.NewMySQLThrottleMetric()
		mySQLThrottleMetric.ClusterName = clusterName
		mySQLThrottleMetric.Alias = probe.Alias

		if probe.Tablet == nil {
			mySQLThrottleMetric.Err = fmt.Errorf("found nil tablet reference for alias %v, hostname %v", probe.Alias, probe.Tablet.Hostname)
			return mySQLThrottleMetric
		}
		req := &tabletmanagerdatapb.CheckThrottlerRequest{} // We leave AppName empty; it will default to VitessName anyway, and we can save some proto space
		resp, gRPCErr := tmClient.CheckThrottler(ctx, probe.Tablet, req)
		if gRPCErr != nil {
			mySQLThrottleMetric.Err = fmt.Errorf("gRPC error accessing tablet %v. Err=%v", probe.Alias, gRPCErr)
			return mySQLThrottleMetric
		}
		mySQLThrottleMetric.Value = resp.Value
		if resp.StatusCode == http.StatusInternalServerError {
			mySQLThrottleMetric.Err = fmt.Errorf("Status code: %d", resp.StatusCode)
		}
		if resp.RecentlyChecked {
			// We have just probed a tablet, and it reported back that someone just recently "check"ed it.
			// We therefore renew the heartbeats lease.
			throttler.requestHeartbeats()
			statsThrottlerProbeRecentlyChecked.Add(1)
		}
		return mySQLThrottleMetric
	}
}

func (throttler *Throttler) collectMySQLMetrics(ctx context.Context, tmClient tmclient.TabletManagerClient, includeCluster func(clusterName string) bool) error {
	// synchronously, get lists of probes
	for clusterName, probes := range throttler.mysqlInventory.ClustersProbes {
		if !includeCluster(clusterName) {
			continue
		}
		clusterName := clusterName
		// probes is known not to change. It can be *replaced*, but not changed.
		// so it's safe to iterate it
		for _, probe := range probes {
			go func(probe *mysql.Probe) {
				// Avoid querying the same server twice at the same time. If previous read is still there,
				// we avoid re-reading it.
				if !atomic.CompareAndSwapInt64(&probe.QueryInProgress, 0, 1) {
					return
				}
				defer atomic.StoreInt64(&probe.QueryInProgress, 0)

				var throttleMetricFunc func() *mysql.MySQLThrottleMetric
				if clusterName == selfStoreName {
					// Throttler is probing its own tablet's metrics:
					throttleMetricFunc = throttler.generateSelfMySQLThrottleMetricFunc(ctx, probe)
				} else {
					// Throttler probing other tablets:
					throttleMetricFunc = throttler.generateTabletProbeFunction(ctx, clusterName, tmClient, probe)
				}
				throttleMetrics := mysql.ReadThrottleMetric(probe, clusterName, throttleMetricFunc)
				select {
				case <-ctx.Done():
					return
				case throttler.mysqlThrottleMetricChan <- throttleMetrics:
				}
			}(probe)
		}
	}
	return nil
}

// refreshMySQLInventory will re-structure the inventory based on reading config settings
func (throttler *Throttler) refreshMySQLInventory(ctx context.Context) error {
	// distribute the query/threshold from the throttler down to the cluster settings and from there to the probes
	metricsQuery := throttler.GetMetricsQuery()
	metricsThreshold := throttler.MetricsThreshold.Load()
	addProbe := func(alias string, tablet *topodatapb.Tablet, clusterName string, clusterSettings *config.MySQLClusterConfigurationSettings, probes mysql.Probes) bool {
		for _, ignore := range clusterSettings.IgnoreHosts {
			if strings.Contains(alias, ignore) {
				log.Infof("Throttler: tablet ignored: %+v", alias)
				return false
			}
		}
		if clusterName != selfStoreName {
			if alias == "" {
				log.Errorf("Throttler: got empty alias for cluster: %+v", clusterName)
				return false
			}
			if tablet == nil {
				log.Errorf("Throttler: got nil tablet for alias: %v in cluster: %+v", alias, clusterName)
				return false
			}
		}

		probe := &mysql.Probe{
			Alias:       alias,
			Tablet:      tablet,
			MetricQuery: clusterSettings.MetricQuery,
			CacheMillis: clusterSettings.CacheMillis,
		}
		probes[alias] = probe
		return true
	}

	attemptWriteProbes := func(clusterProbes *mysql.ClusterProbes) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case throttler.mysqlClusterProbesChan <- clusterProbes:
			return nil
		}
	}

	for clusterName, clusterSettings := range throttler.configSettings.Stores.MySQL.Clusters {
		clusterName := clusterName
		clusterSettings.MetricQuery = metricsQuery
		clusterSettings.ThrottleThreshold.Store(metricsThreshold)

		clusterSettingsCopy := *clusterSettings
		// config may dynamically change, but internal structure (config.Settings().Stores.MySQL.Clusters in our case)
		// is immutable and can only be _replaced_. Hence, it's safe to read in a goroutine:
		collect := func() error {
			throttler.mysqlClusterThresholds.Set(clusterName, math.Float64frombits(clusterSettingsCopy.ThrottleThreshold.Load()), cache.DefaultExpiration)
			clusterProbes := &mysql.ClusterProbes{
				ClusterName:      clusterName,
				IgnoreHostsCount: clusterSettingsCopy.IgnoreHostsCount,
				TabletProbes:     mysql.NewProbes(),
			}

			if clusterName == selfStoreName {
				// special case: just looking at this tablet's MySQL server.
				// We will probe this "cluster" (of one server) is a special way.
				addProbe("", nil, clusterName, &clusterSettingsCopy, clusterProbes.TabletProbes)
				return attemptWriteProbes(clusterProbes)
			}
			if !throttler.isLeader.Load() {
				// This tablet may have used to be the primary, but it isn't now. It may have a recollection
				// of previous clusters it used to probe. It may have recollection of specific probes for such clusters.
				// This now ensures any existing cluster probes are overridden with an empty list of probes.
				// `clusterProbes` was created above as empty, and identifiable via `clusterName`. This will in turn
				// be used to overwrite throttler.mysqlInventory.ClustersProbes[clusterProbes.ClusterName] in
				// updateMySQLClusterProbes().
				return attemptWriteProbes(clusterProbes)
				// not the leader (primary tablet)? Then no more work for us.
			}
			// The primary tablet is also in charge of collecting the shard's metrics
			ctx, cancel := context.WithTimeout(ctx, mysqlRefreshInterval)
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
					addProbe(topoproto.TabletAliasString(tabletAlias), tablet.Tablet, clusterName, &clusterSettingsCopy, clusterProbes.TabletProbes)
				}
			}
			return attemptWriteProbes(clusterProbes)
		}
		go func() {
			if err := collect(); err != nil {
				log.Errorf("refreshMySQLInventory: %+v", err)
			}
		}()
	}
	return nil
}

// synchronous update of inventory
func (throttler *Throttler) updateMySQLClusterProbes(ctx context.Context, clusterProbes *mysql.ClusterProbes) error {
	throttler.mysqlInventory.ClustersProbes[clusterProbes.ClusterName] = clusterProbes.TabletProbes
	throttler.mysqlInventory.IgnoreHostsCount[clusterProbes.ClusterName] = clusterProbes.IgnoreHostsCount
	throttler.mysqlInventory.IgnoreHostsThreshold[clusterProbes.ClusterName] = clusterProbes.IgnoreHostsThreshold
	return nil
}

// synchronous aggregation of collected data
func (throttler *Throttler) aggregateMySQLMetrics(ctx context.Context) error {
	for clusterName, probes := range throttler.mysqlInventory.ClustersProbes {
		metricName := fmt.Sprintf("mysql/%s", clusterName)
		ignoreHostsCount := throttler.mysqlInventory.IgnoreHostsCount[clusterName]
		ignoreHostsThreshold := throttler.mysqlInventory.IgnoreHostsThreshold[clusterName]
		aggregatedMetric := aggregateMySQLProbes(ctx, probes, clusterName, throttler.mysqlInventory.TabletMetrics, ignoreHostsCount, throttler.configSettings.Stores.MySQL.IgnoreDialTCPErrors, ignoreHostsThreshold)
		throttler.aggregatedMetrics.Set(metricName, aggregatedMetric, cache.DefaultExpiration)
	}
	return nil
}

func (throttler *Throttler) getNamedMetric(metricName string) base.MetricResult {
	if metricResultVal, found := throttler.aggregatedMetrics.Get(metricName); found {
		return metricResultVal.(base.MetricResult)
	}
	return base.NoSuchMetric
}

func (throttler *Throttler) getMySQLClusterMetrics(ctx context.Context, clusterName string) (base.MetricResult, float64) {
	if thresholdVal, found := throttler.mysqlClusterThresholds.Get(clusterName); found {
		threshold, _ := thresholdVal.(float64)
		metricName := fmt.Sprintf("mysql/%s", clusterName)
		return throttler.getNamedMetric(metricName), threshold
	}

	return base.NoSuchMetric, 0
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
		if appThrottle.ExpireAt.Before(now) {
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
	if now.Before(appThrottle.ExpireAt) {
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
func (throttler *Throttler) IsAppThrottled(appName string) bool {
	isSingleAppNameThrottled := func(singleAppName string) bool {
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
			return false
		}
		// handle ratio
		if rand.Float64() < appThrottle.Ratio {
			return true
		}
		return false
	}
	if isSingleAppNameThrottled(appName) {
		return true
	}
	for _, singleAppName := range strings.Split(appName, ":") {
		if singleAppName == "" {
			continue
		}
		if isSingleAppNameThrottled(singleAppName) {
			return true
		}
	}
	return false
}

// IsAppExempt
func (throttler *Throttler) IsAppExempted(appName string) bool {
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
		return true
	}
	for _, singleAppName := range strings.Split(appName, ":") {
		if singleAppName == "" {
			continue
		}
		if isSingleAppNameExempted(singleAppName) {
			return true
		}
	}
	return false
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
func (throttler *Throttler) markRecentApp(appName string, remoteAddr string) {
	recentAppKey := fmt.Sprintf("%s/%s", appName, remoteAddr)
	throttler.recentApps.Set(recentAppKey, time.Now(), cache.DefaultExpiration)
}

// RecentAppsMap returns a (copy) map of apps which checked for throttling recently
func (throttler *Throttler) RecentAppsMap() (result map[string](*base.RecentApp)) {
	result = make(map[string](*base.RecentApp))

	for recentAppKey, item := range throttler.recentApps.Items() {
		recentApp := base.NewRecentApp(item.Object.(time.Time))
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
func (throttler *Throttler) AppRequestMetricResult(ctx context.Context, appName string, metricResultFunc base.MetricResultFunc, denyApp bool) (metricResult base.MetricResult, threshold float64) {
	if denyApp {
		return base.AppDeniedMetric, 0
	}
	if throttler.IsAppThrottled(appName) {
		return base.AppDeniedMetric, 0
	}
	return metricResultFunc()
}

// checkStore checks the aggregated value of given MySQL store
func (throttler *Throttler) checkStore(ctx context.Context, appName string, storeName string, remoteAddr string, flags *CheckFlags) (checkResult *CheckResult) {
	if !throttler.IsRunning() {
		return okMetricCheckResult
	}
	if throttler.IsAppExempted(appName) {
		// Some apps are exempt from checks. They are always responded with OK. This is because those apps are
		// continuous and do not generate a substantial load.
		return okMetricCheckResult
	}

	checkResult = throttler.check.Check(ctx, appName, "mysql", storeName, remoteAddr, flags)

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
		statsThrottlerRecentlyChecked.Add(1)
	}

	return checkResult
}

// checkShard checks the health of the shard, and runs on the primary tablet only
func (throttler *Throttler) checkShard(ctx context.Context, appName string, remoteAddr string, flags *CheckFlags) (checkResult *CheckResult) {
	return throttler.checkStore(ctx, appName, shardStoreName, remoteAddr, flags)
}

// CheckSelf is checks the mysql/self metric, and is available on each tablet
func (throttler *Throttler) checkSelf(ctx context.Context, appName string, remoteAddr string, flags *CheckFlags) (checkResult *CheckResult) {
	return throttler.checkStore(ctx, appName, selfStoreName, remoteAddr, flags)
}

// CheckByType runs a check by requested check type
func (throttler *Throttler) CheckByType(ctx context.Context, appName string, remoteAddr string, flags *CheckFlags, checkType ThrottleCheckType) (checkResult *CheckResult) {
	switch checkType {
	case ThrottleCheckSelf:
		return throttler.checkSelf(ctx, appName, remoteAddr, flags)
	case ThrottleCheckPrimaryWrite:
		if throttler.checkAsCheckSelf.Load() {
			return throttler.checkSelf(ctx, appName, remoteAddr, flags)
		}
		return throttler.checkShard(ctx, appName, remoteAddr, flags)
	default:
		return invalidCheckTypeCheckResult
	}
}

// Status exports a status breakdown
func (throttler *Throttler) Status() *ThrottlerStatus {
	return &ThrottlerStatus{
		Keyspace: throttler.keyspace,
		Shard:    throttler.shard,

		IsLeader:  throttler.isLeader.Load(),
		IsOpen:    throttler.isOpen.Load(),
		IsEnabled: throttler.isEnabled.Load(),
		IsDormant: throttler.isDormant(),

		Query:     throttler.GetMetricsQuery(),
		Threshold: throttler.GetMetricsThreshold(),

		AggregatedMetrics: throttler.aggregatedMetricsSnapshot(),
		MetricsHealth:     throttler.metricsHealthSnapshot(),
	}
}
