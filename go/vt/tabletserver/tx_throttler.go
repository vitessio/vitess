package tabletserver

import (
	"fmt"
	"sync"
	"time"

	log "github.com/golang/glog"

	"github.com/golang/protobuf/proto"

	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/tb"
	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/throttler"
	"github.com/youtube/vitess/go/vt/topo"

	throttlerdatapb "github.com/youtube/vitess/go/vt/proto/throttlerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// TxThrottler throttles transactions based on replication lag.
// It's a thin wrapper around the throttler found in vitess/go/vt/throttler.
// It uses a discovery.HealthCheck to send replication-lag updates to the wrapped throttler.
//
// Intended Usage:
//   t := CreateTxThrottlerFromTabletConfig(&tabletserver.Config{...})
//
//   // A transaction throttler must be opened before its first use:
//   err := t.Open(keyspace, shard)
//   if err != nil {
//     panic("Error: %v", err)
//   }
//
//   // Checking whether to throttle can be done as follows before starting a transaction.
//   if t.Throttle() {
//     return fmt.Errorf("Transaction throttled!")
//   } else {
//     // execute transaction.
//   }
//
//   // To release the resources used by the throttler the caller should call Close().
//   t.Close()
//
// A TxThrottler object is generally not thread-safe: at any given time at most one goroutine should
// be executing a method. The only exception is the 'Throttle' method where multiple goroutines are
// allowed to execute it concurrently.
//
// TODO(erez): Move this to its own package. Currently, only exported functions and types are
// intended to be used from outside this file by non-test code.
type TxThrottler struct {
	// config stores the transaction throttler's configuration.
	// It is populated in NewTxThrottler and is not modified
	// since.
	config *txThrottlerConfig

	// uncaughtPanicsCount counts the number of uncaught panics.
	uncaughtPanicsCount *stats.Int

	// state holds an open transaction throttler state. It is nil
	// if the TransactionThrottler is closed.
	state *txThrottlerState
}

// CreateTxThrottlerFromTabletConfig tries to construct a TxThrottler from the
// relevant fields in the given tabletserver.Config object. It returns a disabled TxThrottler if
// any error occurs.
// This function calls tryCreateTxThrottler that does the actual creation work
// and returns an error if one occurred.
// TODO(erez): This function should return an error instead of returning a disabled
// transaction throttler. Fix this after NewTabletServer is changed to return an error as well.
func CreateTxThrottlerFromTabletConfig(tsvConfig *Config) *TxThrottler {
	txThrottler, err := tryCreateTxThrottler(tsvConfig)
	if err != nil {
		log.Errorf("Error creating transaction throttler. Transaction throttling will"+
			" be disabled. Error: %v", err)
		config := &txThrottlerConfig{
			enabled:            false,
			enablePublishStats: tsvConfig.EnablePublishStats,
			statsPrefix:        tsvConfig.StatsPrefix,
		}
		txThrottler, err = newTxThrottler(config)
		if err != nil {
			panic("BUG: Can't create a disabled transaction throttler")
		}
	} else {
		log.Infof("Initialized transaction throttler with config: %+v", txThrottler.config)
	}
	return txThrottler
}

func tryCreateTxThrottler(tsvConfig *Config) (*TxThrottler, error) {
	config := &txThrottlerConfig{
		enabled:            tsvConfig.EnableTxThrottler,
		enablePublishStats: tsvConfig.EnablePublishStats,
		statsPrefix:        tsvConfig.StatsPrefix,
	}
	if !tsvConfig.EnableTxThrottler {
		return newTxThrottler(config)
	}
	var throttlerConfig throttlerdatapb.Configuration

	if err := proto.UnmarshalText(tsvConfig.TxThrottlerConfig, &throttlerConfig); err != nil {
		return nil, err
	}

	// Clone tsv.TxThrottlerHealthCheckCells so that we don't assume tsv.TxThrottlerHealthCheckCells
	// is immutable.
	config.healthCheckCells = make([]string, len(tsvConfig.TxThrottlerHealthCheckCells))
	copy(config.healthCheckCells, tsvConfig.TxThrottlerHealthCheckCells)
	return newTxThrottler(config)
}

// txThrottlerConfig holds the parameters that need to be
// passed when constructing a TxThrottler object.
type txThrottlerConfig struct {
	// enabled is true if the transaction throttler is enabled. All methods
	// of a disabled transaction throttler do nothing and Throttle() always
	// returns false.
	enabled bool

	// enablePublishStats controls whether we want to enable publishing stats.
	enablePublishStats bool

	// statsPrefix contains the prefix to add to exported variables. It's only
	// used if enablePublishStats is true.
	statsPrefix string

	throttlerConfig *throttlerdatapb.Configuration
	// healthCheckCells stores the cell names in which running vttablets will be monitored for
	// replication lag.
	healthCheckCells []string
}

// txThrottlerState holds the state of an open TxThrottler object.
type txThrottlerState struct {
	// throttleMu serializes calls to throttler.Throttler.Throttle(threadId).
	// That method is required to be called in serial for each threadId.
	throttleMu sync.Mutex
	throttler  *throttler.Throttler

	topoServer       topo.Server
	healthCheck      discovery.HealthCheck
	topologyWatchers []*discovery.TopologyWatcher
}

// TxThrottlerName is the name the wrapped go/vt/throttler object will be registered
// with go/vt/throttler.GlobalManager.
const TxThrottlerName = "TransactionThrottler"

func newTxThrottler(config *txThrottlerConfig) (*TxThrottler, error) {
	if config.enabled {
		// Verify config.
		err := throttler.MaxReplicationLagModuleConfig{Configuration: *config.throttlerConfig}.Verify()
		if err != nil {
			return nil, err
		}
		if len(config.healthCheckCells) == 0 {
			return nil, fmt.Errorf("Empty healthCheckCells given. %+v", config)
		}
	}

	uncaughtPanicsStatName := config.statsPrefix + "TxThrottlerUncaughtPanics"
	if !config.enablePublishStats {
		// Clearing the name of the stats will cause it not to be published.
		uncaughtPanicsStatName = ""
	}
	return &TxThrottler{
		config:              config,
		uncaughtPanicsCount: stats.NewInt(uncaughtPanicsStatName),
	}, nil
}

// Open opens the transaction throttler. It must be called prior to 'Throttle'.
func (t *TxThrottler) Open(keyspace, shard string) error {
	if !t.config.enabled {
		return nil
	}
	if t.state != nil {
		return fmt.Errorf("Transaction throttler already opened")
	}
	var err error
	t.state, err = newTxThrottlerState(t.config, keyspace, shard)
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
	log.Infof("Shutting down transaction throttler.")
	t.state.deallocateResources()
	t.state = nil
}

// Throttle should be called before a new transaction is started.
// It returns true if the transaction should not proceed (the caller
// should back off). Throttle requires that Open() was previously called
// successfuly.
func (t *TxThrottler) Throttle() (result bool) {
	if !t.config.enabled {
		return false
	}
	// We catch panics here and allow the request to go through rather
	// than have a panic in the throttler code abort the transaction.
	defer func() {
		if x := recover(); x != nil {
			log.Errorf("Uncaught panic in throttle. Throttle() will return 'false'. %v, Stack trace:%s",
				x,
				tb.Stack(4))
			t.uncaughtPanicsCount.Add(1)
			result = false
		}
	}()
	if t.state == nil {
		panic("BUG: Throttle() called on a closed TxThrottler")
	}
	return t.state.throttle()
}

func newTxThrottlerState(config *txThrottlerConfig, keyspace, shard string,
) (*txThrottlerState, error) {
	t, err := throttler.NewThrottler(
		TxThrottlerName,
		"TPS", /* unit */
		1,     /* threadCount */
		throttler.MaxRateModuleDisabled, /* maxRate */
		config.throttlerConfig.MaxReplicationLagSec /* maxReplicationLag */)
	if err != nil {
		return nil, err
	}
	if t.UpdateConfiguration(config.throttlerConfig, true /* copyZeroValues */); err != nil {
		t.Close()
		return nil, err
	}
	result := &txThrottlerState{
		throttler: t,
	}
	result.topoServer = topo.Open()
	result.healthCheck = discovery.NewDefaultHealthCheck()
	result.healthCheck.SetListener(result, false /* sendDownEvents */)
	result.topologyWatchers = make([]*discovery.TopologyWatcher, 0, len(config.healthCheckCells))
	for _, cell := range config.healthCheckCells {
		result.topologyWatchers = append(
			result.topologyWatchers,
			discovery.NewShardReplicationWatcher(
				result.topoServer,
				result.healthCheck, /* TabletRecorder */
				cell,
				keyspace,
				shard,
				discovery.DefaultTopologyWatcherRefreshInterval,
				discovery.DefaultTopoReadConcurrency))
	}
	return result, nil
}

func (ts *txThrottlerState) throttle() bool {
	if ts.throttler == nil {
		panic("BUG: throttle called after deallocateResources was called.")
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
	for _, watcher := range ts.topologyWatchers {
		watcher.Stop()
	}
	ts.topologyWatchers = nil

	ts.healthCheck.Close()
	ts.healthCheck = nil

	ts.topoServer.Close()

	// After ts.healthCheck is closed txThrottlerState.StatsUpdate() is guaranteed not
	// to be executing, so we can safely close the throttler.
	ts.throttler.Close()
	ts.throttler = nil
}

// StatsUpdate is part of the HealthCheckStatsListener interface.
func (ts *txThrottlerState) StatsUpdate(tabletStats *discovery.TabletStats) {
	// Ignore MASTER and RDONLY stats.
	// We currently do not monitor RDONLY tablets for replication lag. RDONLY tablets are not
	// candidates for becoming master during failover, and it's acceptable to serve somewhat
	// stale date from these.
	// TODO(erez): If this becomes necessary, we can add a configuration option that would
	// determine whether we consider RDONLY tablets here, as well.
	if tabletStats.Target.TabletType != topodatapb.TabletType_REPLICA {
		return
	}
	ts.throttler.RecordReplicationLag(time.Now(), tabletStats)
}
