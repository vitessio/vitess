/*
 Copyright 2017 GitHub Inc.

 Licensed under MIT License. See https://github.com/github/freno/blob/master/LICENSE
*/

package throttle

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/log"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/base"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/config"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/mysql"

	"github.com/patrickmn/go-cache"
)

const (
	leaderCheckInterval         = 5 * time.Second
	mysqlCollectInterval        = 100 * time.Millisecond
	mysqlDormantCollectInterval = 5 * time.Second
	mysqlRefreshInterval        = 10 * time.Second
	mysqlAggregateInterval      = 100 * time.Millisecond

	aggregatedMetricsExpiration   = 5 * time.Second
	aggregatedMetricsCleanup      = 1 * time.Second
	throttledAppsSnapshotInterval = 5 * time.Second
	recentAppsExpiration          = time.Hour * 24

	nonDeprioritizedAppMapExpiration = time.Second
	nonDeprioritizedAppMapInterval   = 100 * time.Millisecond

	dormantPeriod             = time.Minute
	defaultThrottleTTLMinutes = 60
	defaultThrottleRatio      = 1.0

	maxPasswordLength = 32

	localStoreName = "local"
)

var throttleThreshold = flag.Duration("throttle_threshold", 1*time.Second, "Replication lag threshold for throttling")
var throttleTabletTypes = flag.String("throttle_tablet_types", "replica", "Comma separated VTTablet types to be considered by the throttler. default: 'replica'. example: 'replica,rdonly'. 'replica' aways implicitly included")

var (
	throttlerUser  = "vt_tablet_throttler"
	throttlerGrant = fmt.Sprintf("'%s'@'%s'", throttlerUser, "%")

	sqlCreateThrottlerUser = []string{
		`CREATE USER IF NOT EXISTS %s IDENTIFIED BY '%s'`,
		`ALTER USER %s IDENTIFIED BY '%s'`,
	}
	sqlGrantThrottlerUser = []string{
		`GRANT SELECT ON _vt.heartbeat TO %s`,
	}
	replicationLagQuery = `select unix_timestamp(now(6))-max(ts/1000000000) from _vt.heartbeat`
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Throttler is the main entity in the throttling mechanism. This service runs, probes, collects data,
// aggregates, reads inventory, provides information, etc.
type Throttler struct {
	keyspace string
	shard    string

	check    *ThrottlerCheck
	isLeader int64
	isOpen   int64

	env            tabletenv.Env
	pool           *connpool.Pool
	tabletTypeFunc func() topodatapb.TabletType
	ts             *topo.Server

	throttleTabletTypesMap map[topodatapb.TabletType]bool

	mysqlThrottleMetricChan chan *mysql.MySQLThrottleMetric
	mysqlInventoryChan      chan *mysql.Inventory
	mysqlClusterProbesChan  chan *mysql.ClusterProbes

	mysqlInventory *mysql.Inventory

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
func NewThrottler(env tabletenv.Env, ts *topo.Server, tabletTypeFunc func() topodatapb.TabletType) *Throttler {
	throttler := &Throttler{
		isLeader: 0,
		isOpen:   0,

		env:            env,
		tabletTypeFunc: tabletTypeFunc,
		ts:             ts,
		pool: connpool.NewPool(env, "ThrottlerPool", tabletenv.ConnPoolConfig{
			Size:               2,
			IdleTimeoutSeconds: env.Config().OltpReadPool.IdleTimeoutSeconds,
		}),

		mysqlThrottleMetricChan: make(chan *mysql.MySQLThrottleMetric),

		mysqlInventoryChan:     make(chan *mysql.Inventory, 1),
		mysqlClusterProbesChan: make(chan *mysql.ClusterProbes),
		mysqlInventory:         mysql.NewInventory(),

		throttledApps:          cache.New(cache.NoExpiration, 10*time.Second),
		mysqlClusterThresholds: cache.New(cache.NoExpiration, 0),
		aggregatedMetrics:      cache.New(aggregatedMetricsExpiration, aggregatedMetricsCleanup),
		recentApps:             cache.New(recentAppsExpiration, time.Minute),
		metricsHealth:          cache.New(cache.NoExpiration, 0),

		tickers: [](*timer.SuspendableTicker){},

		nonLowPriorityAppRequestsThrottled: cache.New(nonDeprioritizedAppMapExpiration, nonDeprioritizedAppMapInterval),

		httpClient: base.SetupHTTPClient(0),
	}
	throttler.initThrottleTabletTypes()
	throttler.ThrottleApp("abusing-app", time.Now().Add(time.Hour*24*365*10), defaultThrottleRatio)
	throttler.check = NewThrottlerCheck(throttler)

	return throttler
}

func (throttler *Throttler) initThrottleTabletTypes() {
	throttler.throttleTabletTypesMap = make(map[topodatapb.TabletType]bool)

	tokens := textutil.SplitDelimitedList(*throttleTabletTypes)
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
func (throttler *Throttler) initConfig(password string) {
	log.Infof("Throttler: initializing config")
	config.Instance = &config.ConfigurationSettings{
		Stores: config.StoresSettings{
			MySQL: config.MySQLConfigurationSettings{
				IgnoreDialTCPErrors: true,
				Clusters: map[string](*config.MySQLClusterConfigurationSettings){
					localStoreName: &config.MySQLClusterConfigurationSettings{
						User:              throttlerUser,
						Password:          password,
						ThrottleThreshold: throttleThreshold.Seconds(),
						MetricQuery:       replicationLagQuery,
						IgnoreHostsCount:  0,
					},
				},
			},
		},
	}
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
	}

	return nil
}

// Close frees resources
func (throttler *Throttler) Close() {
	throttler.initMutex.Lock()
	defer throttler.initMutex.Unlock()
	if atomic.LoadInt64(&throttler.isOpen) == 0 {
		// not open
		return
	}
	for _, t := range throttler.tickers {
		t.Suspend()
	}
	atomic.StoreInt64(&throttler.isLeader, 0)

	throttler.pool.Close()
	atomic.StoreInt64(&throttler.isOpen, 0)
}

// createThrottlerUser creates or updates the throttler account and assigns it a random password
func (throttler *Throttler) createThrottlerUser(ctx context.Context) (password string, err error) {
	if atomic.LoadInt64(&throttler.isOpen) == 0 {
		return "", fmt.Errorf("createThrottlerUser: not open")
	}

	conn, err := dbconnpool.NewDBConnection(ctx, throttler.env.Config().DB.DbaWithDB())
	if err != nil {
		return password, err
	}
	defer conn.Close()

	// Double check this server is writable
	tm, err := conn.ExecuteFetch("select @@global.read_only as read_only from dual", 1, true)
	if err != nil {
		return password, err
	}
	row := tm.Named().Row()
	if row == nil {
		return password, fmt.Errorf("unexpected result for MySQL variables: %+v", tm.Rows)
	}
	readOnly, err := row.ToBool("read_only")
	if err != nil {
		return password, err
	}
	if readOnly {
		return password, fmt.Errorf("createThrottlerUser(): server is read_only")
	}

	password = textutil.RandomHash()[0:maxPasswordLength]
	{
		// There seems to be a bug where CREATE USER hangs. If CREATE USER is preceded by
		// any query that writes to the binary log, CREATE USER does not hang.
		// The simplest such query is FLUSH STATUS. Other options are FLUSH PRIVILEGES or similar.
		// The bug was found in MySQL 8.0.21, and not found in 5.7.30
		// at this time, Vitess only supports 5.7 an ddoes not support 8.0,
		// but please keep this code in anticipation of supporting 8.0
		// - shlomi
		simpleBinlogQuery := `FLUSH STATUS`
		if _, err := conn.ExecuteFetch(simpleBinlogQuery, 0, false); err != nil {
			return password, err
		}
	}
	for _, query := range sqlCreateThrottlerUser {
		parsed := sqlparser.BuildParsedQuery(query, throttlerGrant, password)
		if _, err := conn.ExecuteFetch(parsed.Query, 0, false); err != nil {
			return password, err
		}
	}
	for _, query := range sqlGrantThrottlerUser {
		parsed := sqlparser.BuildParsedQuery(query, throttlerGrant)
		if _, err := conn.ExecuteFetch(parsed.Query, 0, false); err != nil {
			return password, err
		}
	}
	log.Infof("Throttler: user created/updated")
	return password, nil
}

// ThrottledAppsSnapshot returns a snapshot (a copy) of current throttled apps
func (throttler *Throttler) ThrottledAppsSnapshot() map[string]cache.Item {
	return throttler.throttledApps.Items()
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

	shouldCreateThrottlerUser := false
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
						if throttler.tabletTypeFunc() == topodatapb.TabletType_MASTER {
							shouldBeLeader = 1
						}
					}

					if shouldBeLeader > throttler.isLeader {
						log.Infof("Throttler: transition into leadership")
						shouldCreateThrottlerUser = true
					}
					if shouldBeLeader < throttler.isLeader {
						log.Infof("Throttler: transition out of leadership")
					}

					atomic.StoreInt64(&throttler.isLeader, shouldBeLeader)

					if shouldCreateThrottlerUser {
						password, err := throttler.createThrottlerUser(ctx)
						if err == nil {
							throttler.initConfig(password)
							shouldCreateThrottlerUser = false
						} else {
							log.Errorf("Error creating throttler account: %+v", err)
						}
					}
				}()
			}
		case <-mysqlCollectTicker.C:
			{
				if atomic.LoadInt64(&throttler.isLeader) > 0 {
					// frequent
					if !throttler.isDormant() {
						throttler.collectMySQLMetrics(ctx)
					}
				}
			}
		case <-mysqlDormantCollectTicker.C:
			{
				if atomic.LoadInt64(&throttler.isLeader) > 0 {
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
				if atomic.LoadInt64(&throttler.isLeader) > 0 {
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
				if atomic.LoadInt64(&throttler.isLeader) > 0 {
					throttler.aggregateMySQLMetrics(ctx)
				}
			}
		case <-throttledAppsTicker.C:
			{
				if atomic.LoadInt64(&throttler.isLeader) > 0 {
					go throttler.expireThrottledApps()
				}
			}
		}
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
					throttleMetrics := mysql.ReadThrottleMetric(probe, clusterName)
					throttler.mysqlThrottleMetricChan <- throttleMetrics
				}()
			}
		}()
	}
	return nil
}

// refreshMySQLInventory will re-structure the inventory based on reading config settings, and potentially
// re-querying dynamic data such as HAProxy list of hosts
func (throttler *Throttler) refreshMySQLInventory(ctx context.Context) error {
	log.Infof("refreshing MySQL inventory")

	addInstanceKey := func(key *mysql.InstanceKey, clusterName string, clusterSettings *config.MySQLClusterConfigurationSettings, probes *mysql.Probes) {
		for _, ignore := range clusterSettings.IgnoreHosts {
			if strings.Contains(key.StringCode(), ignore) {
				log.Infof("Throttler: instance key ignored: %+v", key)
				return
			}
		}
		if !key.IsValid() {
			log.Infof("Throttler: read invalid instance key: [%+v] for cluster %+v", key, clusterName)
			return
		}
		log.Infof("Throttler: read instance key: %+v", key)

		probe := &mysql.Probe{
			Key:         *key,
			User:        clusterSettings.User,
			Password:    clusterSettings.Password,
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
			err := func() error {
				throttler.mysqlClusterThresholds.Set(clusterName, clusterSettings.ThrottleThreshold, cache.DefaultExpiration)

				tabletAliases, err := throttler.ts.FindAllTabletAliasesInShard(ctx, throttler.keyspace, throttler.shard)
				if err != nil {
					return err
				}
				clusterProbes := &mysql.ClusterProbes{
					ClusterName:      clusterName,
					IgnoreHostsCount: clusterSettings.IgnoreHostsCount,
					InstanceProbes:   mysql.NewProbes(),
				}
				for _, tabletAlias := range tabletAliases {
					tablet, err := throttler.ts.GetTablet(ctx, tabletAlias)
					if err != nil {
						return err
					}
					if throttler.throttleTabletTypesMap[tablet.Type] {
						key := mysql.InstanceKey{Hostname: tablet.MysqlHostname, Port: int(tablet.MysqlPort)}
						addInstanceKey(&key, clusterName, clusterSettings, clusterProbes.InstanceProbes)
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
	log.Infof("Throttler: updating MySQLClusterProbes: %s", clusterProbes.ClusterName)
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
func (throttler *Throttler) ThrottleApp(appName string, expireAt time.Time, ratio float64) {
	throttler.throttledAppsMutex.Lock()
	defer throttler.throttledAppsMutex.Unlock()

	var appThrottle *base.AppThrottle
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
		appThrottle = base.NewAppThrottle(expireAt, ratio)
	}
	if now.Before(appThrottle.ExpireAt) {
		throttler.throttledApps.Set(appName, appThrottle, cache.DefaultExpiration)
	} else {
		throttler.UnthrottleApp(appName)
	}
}

// UnthrottleApp cancels any throttling, if any, for a given app
func (throttler *Throttler) UnthrottleApp(appName string) {
	throttler.throttledApps.Delete(appName)
}

// IsAppThrottled tells whether some app should be throttled.
// Assuming an app is throttled to some extend, it will randomize the result based
// on the throttle ratio
func (throttler *Throttler) IsAppThrottled(appName string) bool {
	if object, found := throttler.throttledApps.Get(appName); found {
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

// Check is the main serving function of the throttler, and returns a check result for this cluster's lag
func (throttler *Throttler) Check(ctx context.Context, appName string, remoteAddr string, flags *CheckFlags) (checkResult *CheckResult) {
	if !throttler.env.Config().EnableLagThrottler {
		return okMetricCheckResult
	}
	return throttler.check.Check(ctx, appName, "mysql", localStoreName, remoteAddr, flags)
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
