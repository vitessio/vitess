/*
 Copyright 2017 GitHub Inc.

 Licensed under MIT License. See https://github.com/github/freno/blob/master/LICENSE
*/

package throttle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/heartbeat"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/config"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/mysql"
)

const (
	leaderCheckInterval         = 5 * time.Second
	mysqlCollectInterval        = 250 * time.Millisecond
	mysqlDormantCollectInterval = 5 * time.Second
	mysqlRefreshInterval        = 10 * time.Second
	mysqlAggregateInterval      = 125 * time.Millisecond

	aggregatedMetricsExpiration   = 5 * time.Second
	aggregatedMetricsCleanup      = 10 * time.Second
	throttledAppsSnapshotInterval = 5 * time.Second
	recentAppsExpiration          = time.Hour * 24

	nonDeprioritizedAppMapExpiration = time.Second
	nonDeprioritizedAppMapInterval   = 10 * time.Second

	dormantPeriod             = time.Minute
	defaultThrottleTTLMinutes = 60
	defaultThrottleRatio      = 1.0

	shardStoreName = "shard"
	selfStoreName  = "self"
)

var (
	// flag vars
	throttleThreshold         = 1 * time.Second
	throttleTabletTypes       = "replica"
	throttleMetricQuery       string
	throttleMetricThreshold   = math.MaxFloat64
	throttlerCheckAsCheckSelf = false
)

func init() {
	servenv.OnParseFor("vtcombo", registerThrottlerFlags)
	servenv.OnParseFor("vttablet", registerThrottlerFlags)
}

func registerThrottlerFlags(fs *pflag.FlagSet) {
	fs.DurationVar(&throttleThreshold, "throttle_threshold", throttleThreshold, "Replication lag threshold for default lag throttling")
	fs.StringVar(&throttleTabletTypes, "throttle_tablet_types", throttleTabletTypes, "Comma separated VTTablet types to be considered by the throttler. default: 'replica'. example: 'replica,rdonly'. 'replica' aways implicitly included")
	fs.StringVar(&throttleMetricQuery, "throttle_metrics_query", throttleMetricQuery, "Override default heartbeat/lag metric. Use either `SELECT` (must return single row, single value) or `SHOW GLOBAL ... LIKE ...` queries. Set -throttle_metrics_threshold respectively.")
	fs.Float64Var(&throttleMetricThreshold, "throttle_metrics_threshold", throttleMetricThreshold, "Override default throttle threshold, respective to -throttle_metrics_query")
	fs.BoolVar(&throttlerCheckAsCheckSelf, "throttle_check_as_check_self", throttlerCheckAsCheckSelf, "Should throttler/check return a throttler/check-self result (changes throttler behavior for writes)")
}

var (
	replicationLagQuery = `select unix_timestamp(now(6))-max(ts/1000000000) as replication_lag from _vt.heartbeat`

	ErrThrottlerNotReady = errors.New("throttler not enabled/ready")
)

// ThrottleCheckType allows a client to indicate what type of check it wants to issue. See available types below.
type ThrottleCheckType int // nolint:revive

const (
	// ThrottleCheckPrimaryWrite indicates a check before making a write on a primary server
	ThrottleCheckPrimaryWrite ThrottleCheckType = iota
	// ThrottleCheckSelf indicates a check on a specific server health
	ThrottleCheckSelf
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Throttler is the main entity in the throttling mechanism. This service runs, probes, collects data,
// aggregates, reads inventory, provides information, etc.
type Throttler struct {
	keyspace string
	shard    string

	check     *ThrottlerCheck
	isEnabled bool
	isLeader  int64
	isOpen    int64

	env             tabletenv.Env
	pool            *connpool.Pool
	tabletTypeFunc  func() topodatapb.TabletType
	ts              *topo.Server
	heartbeatWriter heartbeat.HeartbeatWriter

	throttleTabletTypesMap map[topodatapb.TabletType]bool

	mysqlThrottleMetricChan chan *mysql.MySQLThrottleMetric
	mysqlInventoryChan      chan *mysql.Inventory
	mysqlClusterProbesChan  chan *mysql.ClusterProbes

	mysqlInventory *mysql.Inventory

	metricsQuery     string
	MetricsThreshold sync2.AtomicFloat64
	metricsQueryType mysql.MetricsQueryType

	mysqlClusterThresholds *cache.Cache
	aggregatedMetrics      *cache.Cache
	throttledApps          *cache.Cache
	recentApps             *cache.Cache
	metricsHealth          *cache.Cache

	lastCheckTimeNano int64

	initMutex          sync.Mutex
	throttledAppsMutex sync.Mutex
	tickers            [](*timer.SuspendableTicker)

	nonLowPriorityAppRequestsThrottled *cache.Cache
	httpClient                         *http.Client
}

// ThrottlerStatus published some status values from the throttler
type ThrottlerStatus struct {
	Keyspace string
	Shard    string

	IsLeader  bool
	IsOpen    bool
	IsDormant bool

	AggregatedMetrics map[string]base.MetricResult
	MetricsHealth     base.MetricHealthMap
}

// NewThrottler creates a Throttler
func NewThrottler(env tabletenv.Env, ts *topo.Server, heartbeatWriter heartbeat.HeartbeatWriter, tabletTypeFunc func() topodatapb.TabletType) *Throttler {
	throttler := &Throttler{
		isLeader: 0,
		isOpen:   0,

		env:             env,
		tabletTypeFunc:  tabletTypeFunc,
		ts:              ts,
		heartbeatWriter: heartbeatWriter,
		pool: connpool.NewPool(env, "ThrottlerPool", tabletenv.ConnPoolConfig{
			Size:               2,
			IdleTimeoutSeconds: env.Config().OltpReadPool.IdleTimeoutSeconds,
		}),
	}

	if env.Config().EnableLagThrottler {
		throttler.isEnabled = true
		throttler.mysqlThrottleMetricChan = make(chan *mysql.MySQLThrottleMetric)

		throttler.mysqlInventoryChan = make(chan *mysql.Inventory, 1)
		throttler.mysqlClusterProbesChan = make(chan *mysql.ClusterProbes)
		throttler.mysqlInventory = mysql.NewInventory()

		throttler.metricsQuery = replicationLagQuery
		throttler.MetricsThreshold = sync2.NewAtomicFloat64(throttleThreshold.Seconds())

		throttler.throttledApps = cache.New(cache.NoExpiration, 10*time.Second)
		throttler.mysqlClusterThresholds = cache.New(cache.NoExpiration, 0)
		throttler.aggregatedMetrics = cache.New(aggregatedMetricsExpiration, aggregatedMetricsCleanup)
		throttler.recentApps = cache.New(recentAppsExpiration, time.Minute)
		throttler.metricsHealth = cache.New(cache.NoExpiration, 0)

		throttler.tickers = [](*timer.SuspendableTicker){}
		throttler.nonLowPriorityAppRequestsThrottled = cache.New(nonDeprioritizedAppMapExpiration, nonDeprioritizedAppMapInterval)

		throttler.httpClient = base.SetupHTTPClient(2 * mysqlCollectInterval)
		throttler.initThrottleTabletTypes()
		throttler.ThrottleApp("always-throttled-app", time.Now().Add(time.Hour*24*365*10), defaultThrottleRatio)
		throttler.check = NewThrottlerCheck(throttler)
		throttler.initConfig()
		throttler.check.SelfChecks(context.Background())
	} else {
		// Create an empty cache, just so that it isn't nil
		throttler.throttledApps = cache.New(cache.NoExpiration, 0)
	}
	return throttler
}

// CheckIsReady checks if this throttler is ready to serve. If not, it returns an error
func (throttler *Throttler) CheckIsReady() error {
	if throttler.isEnabled && throttler.IsOpen() {
		// all good
		return nil
	}
	return ErrThrottlerNotReady
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
	if throttler.env.Config().EnableLagThrottler {
		go throttler.Operate(context.Background())
	}
}

// initThrottler initializes config
func (throttler *Throttler) initConfig() {
	log.Infof("Throttler: initializing config")
	config.Instance = &config.ConfigurationSettings{
		Stores: config.StoresSettings{
			MySQL: config.MySQLConfigurationSettings{
				IgnoreDialTCPErrors: true,
				Clusters:            map[string](*config.MySQLClusterConfigurationSettings){},
			},
		},
	}
	if throttleMetricQuery != "" {
		throttler.metricsQuery = throttleMetricQuery
	}
	if throttleMetricThreshold != math.MaxFloat64 {
		throttler.MetricsThreshold = sync2.NewAtomicFloat64(throttleMetricThreshold)
	}
	throttler.metricsQueryType = mysql.GetMetricsQueryType(throttler.metricsQuery)

	config.Instance.Stores.MySQL.Clusters[selfStoreName] = &config.MySQLClusterConfigurationSettings{
		MetricQuery:       throttler.metricsQuery,
		ThrottleThreshold: &throttler.MetricsThreshold,
		IgnoreHostsCount:  0,
	}
	config.Instance.Stores.MySQL.Clusters[shardStoreName] = &config.MySQLClusterConfigurationSettings{
		MetricQuery:       throttler.metricsQuery,
		ThrottleThreshold: &throttler.MetricsThreshold,
		IgnoreHostsCount:  0,
	}
}

func (throttler *Throttler) IsOpen() bool {
	throttler.initMutex.Lock()
	defer throttler.initMutex.Unlock()
	return atomic.LoadInt64(&throttler.isOpen) > 0
}

// Open opens database pool and initializes the schema
func (throttler *Throttler) Open() error {
	throttler.initMutex.Lock()
	defer throttler.initMutex.Unlock()
	if atomic.LoadInt64(&throttler.isOpen) > 0 {
		// already open
		return nil
	}

	throttler.pool.Open(throttler.env.Config().DB.AppWithDB(), throttler.env.Config().DB.DbaWithDB(), throttler.env.Config().DB.AppDebugWithDB())
	atomic.StoreInt64(&throttler.isOpen, 1)

	for _, t := range throttler.tickers {
		t.Resume()
		// since we just resume now, speed up the tickers by forcng an immediate tick
		go t.TickNow()
	}
	go throttler.heartbeatWriter.RequestHeartbeats()

	return nil
}

// Close frees resources
func (throttler *Throttler) Close() {
	log.Infof("Throttler - started execution of Close. Acquiring initMutex lock")
	throttler.initMutex.Lock()
	log.Infof("Throttler - acquired initMutex lock")
	defer throttler.initMutex.Unlock()
	if atomic.LoadInt64(&throttler.isOpen) == 0 {
		log.Infof("Throttler - no throttler is open")
		// not open
		return
	}
	for _, t := range throttler.tickers {
		t.Suspend()
	}
	log.Infof("Throttler - finished suspending tickers")
	atomic.StoreInt64(&throttler.isLeader, 0)

	log.Infof("Throttler - closing pool")
	throttler.pool.Close()
	atomic.StoreInt64(&throttler.isOpen, 0)
	log.Infof("Throttler - finished execution of Close")
}

// readSelfMySQLThrottleMetric reads the mysql metric from thi very tablet's backend mysql.
func (throttler *Throttler) readSelfMySQLThrottleMetric() *mysql.MySQLThrottleMetric {
	metric := &mysql.MySQLThrottleMetric{
		ClusterName: selfStoreName,
		Key:         *mysql.SelfInstanceKey,
		Value:       0,
		Err:         nil,
	}
	ctx := context.Background()
	conn, err := throttler.pool.Get(ctx)
	if err != nil {
		metric.Err = err
		return metric
	}
	defer conn.Recycle()

	tm, err := conn.Exec(ctx, throttler.metricsQuery, 1, true)
	if err != nil {
		metric.Err = err
		return metric
	}
	row := tm.Named().Row()
	if row == nil {
		metric.Err = fmt.Errorf("no results for readSelfMySQLThrottleMetric")
		return metric
	}

	switch throttler.metricsQueryType {
	case mysql.MetricsQueryTypeSelect:
		// We expect a single row, single column result.
		// The "for" iteration below is just a way to get first result without knowning column name
		for k := range row {
			metric.Value, metric.Err = row.ToFloat64(k)
		}
	case mysql.MetricsQueryTypeShowGlobal:
		metric.Value, metric.Err = strconv.ParseFloat(row["Value"].ToString(), 64)
	default:
		metric.Err = fmt.Errorf("Unsupported metrics query type for query %s", throttler.metricsQuery)
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

// isDormant returns true when the last check was more than dormantPeriod ago
func (throttler *Throttler) isDormant() bool {
	lastCheckTime := time.Unix(0, atomic.LoadInt64(&throttler.lastCheckTimeNano))
	return time.Since(lastCheckTime) > dormantPeriod
}

// Operate is the main entry point for the throttler operation and logic. It will
// run the probes, colelct metrics, refresh inventory, etc.
func (throttler *Throttler) Operate(ctx context.Context) {

	addTicker := func(d time.Duration) *timer.SuspendableTicker {
		throttler.initMutex.Lock()
		defer throttler.initMutex.Unlock()

		t := timer.NewSuspendableTicker(d, true)
		throttler.tickers = append(throttler.tickers, t)
		return t
	}

	leaderCheckTicker := addTicker(leaderCheckInterval)
	mysqlCollectTicker := addTicker(mysqlCollectInterval)
	mysqlDormantCollectTicker := addTicker(mysqlDormantCollectInterval)
	mysqlRefreshTicker := addTicker(mysqlRefreshInterval)
	mysqlAggregateTicker := addTicker(mysqlAggregateInterval)
	throttledAppsTicker := addTicker(throttledAppsSnapshotInterval)

	for {
		select {
		case <-leaderCheckTicker.C:
			{
				func() {
					throttler.initMutex.Lock()
					defer throttler.initMutex.Unlock()

					// sparse
					shouldBeLeader := int64(0)
					if atomic.LoadInt64(&throttler.isOpen) > 0 {
						if throttler.tabletTypeFunc() == topodatapb.TabletType_PRIMARY {
							shouldBeLeader = 1
						}
					}

					transitionedIntoLeader := false
					if shouldBeLeader > throttler.isLeader {
						log.Infof("Throttler: transition into leadership")
						transitionedIntoLeader = true
					}
					if shouldBeLeader < throttler.isLeader {
						log.Infof("Throttler: transition out of leadership")
					}

					atomic.StoreInt64(&throttler.isLeader, shouldBeLeader)

					if transitionedIntoLeader {
						// transitioned into leadership, let's speed up the next 'refresh' and 'collect' ticks
						go mysqlRefreshTicker.TickNow()
						go throttler.heartbeatWriter.RequestHeartbeats()
					}
				}()
			}
		case <-mysqlCollectTicker.C:
			{
				if atomic.LoadInt64(&throttler.isOpen) > 0 {
					// frequent
					if !throttler.isDormant() {
						throttler.collectMySQLMetrics(ctx)
					}
				}
			}
		case <-mysqlDormantCollectTicker.C:
			{
				if atomic.LoadInt64(&throttler.isOpen) > 0 {
					// infrequent
					if throttler.isDormant() {
						throttler.collectMySQLMetrics(ctx)
					}
				}
			}
		case metric := <-throttler.mysqlThrottleMetricChan:
			{
				// incoming MySQL metric, frequent, as result of collectMySQLMetrics()
				throttler.mysqlInventory.InstanceKeyMetrics[metric.GetClusterInstanceKey()] = metric
			}
		case <-mysqlRefreshTicker.C:
			{
				// sparse
				if atomic.LoadInt64(&throttler.isOpen) > 0 {
					go throttler.refreshMySQLInventory(ctx)
				}
			}
		case probes := <-throttler.mysqlClusterProbesChan:
			{
				// incoming structural update, sparse, as result of refreshMySQLInventory()
				throttler.updateMySQLClusterProbes(ctx, probes)
			}
		case <-mysqlAggregateTicker.C:
			{
				if atomic.LoadInt64(&throttler.isOpen) > 0 {
					throttler.aggregateMySQLMetrics(ctx)
				}
			}
		case <-throttledAppsTicker.C:
			{
				if atomic.LoadInt64(&throttler.isOpen) > 0 {
					go throttler.expireThrottledApps()
				}
			}
		}
	}
}

func (throttler *Throttler) generateTabletHTTPProbeFunction(ctx context.Context, clusterName string, probe *mysql.Probe) (probeFunc func() *mysql.MySQLThrottleMetric) {
	return func() *mysql.MySQLThrottleMetric {
		// Hit a tablet's `check-self` via HTTP, and convert its CheckResult JSON output into a MySQLThrottleMetric
		mySQLThrottleMetric := mysql.NewMySQLThrottleMetric()
		mySQLThrottleMetric.ClusterName = clusterName
		mySQLThrottleMetric.Key = probe.Key

		tabletCheckSelfURL := fmt.Sprintf("http://%s:%d/throttler/check-self?app=vitess", probe.TabletHost, probe.TabletPort)
		resp, err := throttler.httpClient.Get(tabletCheckSelfURL)
		if err != nil {
			mySQLThrottleMetric.Err = err
			return mySQLThrottleMetric
		}
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			mySQLThrottleMetric.Err = err
			return mySQLThrottleMetric
		}
		checkResult := &CheckResult{}
		if err := json.Unmarshal(b, checkResult); err != nil {
			mySQLThrottleMetric.Err = err
			return mySQLThrottleMetric
		}
		mySQLThrottleMetric.Value = checkResult.Value

		if checkResult.StatusCode == http.StatusInternalServerError {
			mySQLThrottleMetric.Err = fmt.Errorf("Status code: %d", checkResult.StatusCode)
		}
		return mySQLThrottleMetric
	}
}

func (throttler *Throttler) collectMySQLMetrics(ctx context.Context) error {
	// synchronously, get lists of probes
	for clusterName, probes := range throttler.mysqlInventory.ClustersProbes {
		clusterName := clusterName
		probes := probes
		go func() {
			// probes is known not to change. It can be *replaced*, but not changed.
			// so it's safe to iterate it
			for _, probe := range *probes {
				probe := probe
				go func() {
					// Avoid querying the same server twice at the same time. If previous read is still there,
					// we avoid re-reading it.
					if !atomic.CompareAndSwapInt64(&probe.QueryInProgress, 0, 1) {
						return
					}
					defer atomic.StoreInt64(&probe.QueryInProgress, 0)

					var throttleMetricFunc func() *mysql.MySQLThrottleMetric
					if clusterName == selfStoreName {
						throttleMetricFunc = throttler.readSelfMySQLThrottleMetric
					} else {
						throttleMetricFunc = throttler.generateTabletHTTPProbeFunction(ctx, clusterName, probe)
					}
					throttleMetrics := mysql.ReadThrottleMetric(probe, clusterName, throttleMetricFunc)
					throttler.mysqlThrottleMetricChan <- throttleMetrics
				}()
			}
		}()
	}
	return nil
}

// refreshMySQLInventory will re-structure the inventory based on reading config settings
func (throttler *Throttler) refreshMySQLInventory(ctx context.Context) error {

	addInstanceKey := func(tabletHost string, tabletPort int, key *mysql.InstanceKey, clusterName string, clusterSettings *config.MySQLClusterConfigurationSettings, probes *mysql.Probes) {
		for _, ignore := range clusterSettings.IgnoreHosts {
			if strings.Contains(key.StringCode(), ignore) {
				log.Infof("Throttler: instance key ignored: %+v", key)
				return
			}
		}
		if !key.IsValid() && !key.IsSelf() {
			log.Infof("Throttler: read invalid instance key: [%+v] for cluster %+v", key, clusterName)
			return
		}

		probe := &mysql.Probe{
			Key:         *key,
			TabletHost:  tabletHost,
			TabletPort:  tabletPort,
			MetricQuery: clusterSettings.MetricQuery,
			CacheMillis: clusterSettings.CacheMillis,
		}
		(*probes)[*key] = probe
	}

	for clusterName, clusterSettings := range config.Settings().Stores.MySQL.Clusters {
		clusterName := clusterName
		clusterSettings := clusterSettings
		// config may dynamically change, but internal structure (config.Settings().Stores.MySQL.Clusters in our case)
		// is immutable and can only be _replaced_. Hence, it's safe to read in a goroutine:
		go func() {
			throttler.mysqlClusterThresholds.Set(clusterName, clusterSettings.ThrottleThreshold.Get(), cache.DefaultExpiration)
			clusterProbes := &mysql.ClusterProbes{
				ClusterName:      clusterName,
				IgnoreHostsCount: clusterSettings.IgnoreHostsCount,
				InstanceProbes:   mysql.NewProbes(),
			}

			if clusterName == selfStoreName {
				// special case: just looking at this tablet's MySQL server
				// We will probe this "cluster" (of one server) is a special way.
				addInstanceKey("", 0, mysql.SelfInstanceKey, clusterName, clusterSettings, clusterProbes.InstanceProbes)
				throttler.mysqlClusterProbesChan <- clusterProbes
				return
			}
			if atomic.LoadInt64(&throttler.isLeader) == 0 {
				// not the leader (primary tablet)? Then no more work for us.
				return
			}
			// The primary tablet is also in charge of collecting the shard's metrics
			err := func() error {
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
						key := mysql.InstanceKey{Hostname: tablet.MysqlHostname, Port: int(tablet.MysqlPort)}
						addInstanceKey(tablet.Hostname, int(tablet.PortMap["vt"]), &key, clusterName, clusterSettings, clusterProbes.InstanceProbes)
					}
				}
				throttler.mysqlClusterProbesChan <- clusterProbes
				return nil
			}()
			if err != nil {
				log.Errorf("refreshMySQLInventory: %+v", err)
			}
		}()
	}
	return nil
}

// synchronous update of inventory
func (throttler *Throttler) updateMySQLClusterProbes(ctx context.Context, clusterProbes *mysql.ClusterProbes) error {
	throttler.mysqlInventory.ClustersProbes[clusterProbes.ClusterName] = clusterProbes.InstanceProbes
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
		aggregatedMetric := aggregateMySQLProbes(ctx, probes, clusterName, throttler.mysqlInventory.InstanceKeyMetrics, ignoreHostsCount, config.Settings().Stores.MySQL.IgnoreDialTCPErrors, ignoreHostsThreshold)
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

// ThrottleApp instructs the throttler to begin throttling an app, to som eperiod and with some ratio.
func (throttler *Throttler) ThrottleApp(appName string, expireAt time.Time, ratio float64) (appThrottle *base.AppThrottle) {
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
	} else {
		if expireAt.IsZero() {
			expireAt = now.Add(defaultThrottleTTLMinutes * time.Minute)
		}
		if ratio < 0 {
			ratio = defaultThrottleRatio
		}
		appThrottle = base.NewAppThrottle(appName, expireAt, ratio)
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
	go throttler.heartbeatWriter.RequestHeartbeats()
	return base.NewAppThrottle(appName, time.Now(), 0)
}

// IsAppThrottled tells whether some app should be throttled.
// Assuming an app is throttled to some extend, it will randomize the result based
// on the throttle ratio
func (throttler *Throttler) IsAppThrottled(appName string) bool {
	isSingleAppNameThrottled := func(singleAppName string) bool {
		if object, found := throttler.throttledApps.Get(singleAppName); found {
			appThrottle := object.(*base.AppThrottle)
			if appThrottle.ExpireAt.Before(time.Now()) {
				// throttling cleanup hasn't purged yet, but it is expired
				return false
			}
			// handle ratio
			if rand.Float64() < appThrottle.Ratio {
				return true
			}
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
	if !throttler.env.Config().EnableLagThrottler {
		return okMetricCheckResult
	}
	return throttler.check.Check(ctx, appName, "mysql", storeName, remoteAddr, flags)
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
	go throttler.heartbeatWriter.RequestHeartbeats()
	switch checkType {
	case ThrottleCheckSelf:
		return throttler.checkSelf(ctx, appName, remoteAddr, flags)
	case ThrottleCheckPrimaryWrite:
		if throttlerCheckAsCheckSelf {
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

		IsLeader:  (atomic.LoadInt64(&throttler.isLeader) > 0),
		IsOpen:    (atomic.LoadInt64(&throttler.isOpen) > 0),
		IsDormant: throttler.isDormant(),

		AggregatedMetrics: throttler.aggregatedMetricsSnapshot(),
		MetricsHealth:     throttler.metricsHealthSnapshot(),
	}
}
