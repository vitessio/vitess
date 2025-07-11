/*
   Copyright 2014 Outbrain Inc.

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

package logic

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/sjmudd/stopwatch"
	"golang.org/x/sync/errgroup"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtorc/collection"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/discovery"
	"vitess.io/vitess/go/vt/vtorc/inst"
	ometrics "vitess.io/vitess/go/vt/vtorc/metrics"
	"vitess.io/vitess/go/vt/vtorc/process"
	"vitess.io/vitess/go/vt/vtorc/util"
)

const (
	DiscoveryMetricsName = "DISCOVERY_METRICS"
)

// discoveryQueue is a channel of deduplicated instanceKey-s
// that were requested for discovery.  It can be continuously updated
// as discovery process progresses.
var discoveryQueue *discovery.Queue
var snapshotDiscoveryKeys chan string
var snapshotDiscoveryKeysMutex sync.Mutex
var hasReceivedSIGTERM int32

var (
	discoveriesCounter                 = stats.NewCounter("DiscoveriesAttempt", "Number of discoveries attempted")
	failedDiscoveriesCounter           = stats.NewCounter("DiscoveriesFail", "Number of failed discoveries")
	instancePollSecondsExceededCounter = stats.NewCounter("DiscoveriesInstancePollSecondsExceeded", "Number of instances that took longer than InstancePollSeconds to poll")
	discoveryQueueLengthGauge          = stats.NewGauge("DiscoveriesQueueLength", "Length of the discovery queue")
	discoveryRecentCountGauge          = stats.NewGauge("DiscoveriesRecentCount", "Number of recent discoveries")
	discoveryWorkersGauge              = stats.NewGauge("DiscoveryWorkers", "Number of discovery workers")
	discoveryWorkersActiveGauge        = stats.NewGauge("DiscoveryWorkersActive", "Number of discovery workers actively discovering tablets")

	discoverInstanceTimingsActions = []string{"Backend", "Instance", "Other"}
	discoverInstanceTimings        = stats.NewTimings("DiscoverInstanceTimings", "Timings for instance discovery actions", "Action", discoverInstanceTimingsActions...)
)

var discoveryMetrics = collection.CreateOrReturnCollection(DiscoveryMetricsName)

var recentDiscoveryOperationKeys *cache.Cache

func init() {
	snapshotDiscoveryKeys = make(chan string, 10)

	ometrics.OnMetricsTick(func() {
		discoveryQueueLengthGauge.Set(int64(discoveryQueue.QueueLen()))
	})
	ometrics.OnMetricsTick(func() {
		if recentDiscoveryOperationKeys == nil {
			return
		}
		discoveryRecentCountGauge.Set(int64(recentDiscoveryOperationKeys.ItemCount()))
	})
}

// closeVTOrc runs all the operations required to cleanly shutdown VTOrc
func closeVTOrc() {
	log.Infof("Starting VTOrc shutdown")
	atomic.StoreInt32(&hasReceivedSIGTERM, 1)
	discoveryMetrics.StopAutoExpiration()
	// Poke other go routines to stop cleanly here ...
	_ = inst.AuditOperation("shutdown", "", "Triggered via SIGTERM")
	// wait for the locks to be released
	waitForLocksRelease()
	ts.Close()
	log.Infof("VTOrc closed")
}

// waitForLocksRelease is used to wait for release of locks
func waitForLocksRelease() {
	timeout := time.After(shutdownWaitTime)
	for {
		count := atomic.LoadInt64(&shardsLockCounter)
		if count == 0 {
			break
		}
		select {
		case <-timeout:
			log.Infof("wait for lock release timed out. Some locks might not have been released.")
		default:
			time.Sleep(50 * time.Millisecond)
			continue
		}
		break
	}
}

// handleDiscoveryRequests iterates the discoveryQueue channel and calls upon
// instance discovery per entry.
func handleDiscoveryRequests() {
	discoveryQueue = discovery.NewQueue()
	// create a pool of discovery workers
	for i := uint(0); i < config.GetDiscoveryWorkers(); i++ {
		discoveryWorkersGauge.Add(1)
		go func() {
			for {
				// .Consume() blocks until there is a new key to process.
				// We are not "active" until we got a tablet alias.
				tabletAlias := discoveryQueue.Consume()
				func() {
					discoveryWorkersActiveGauge.Add(1)
					defer discoveryWorkersActiveGauge.Add(-1)

					DiscoverInstance(tabletAlias, false /* forceDiscovery */)
					discoveryQueue.Release(tabletAlias)
				}()
			}
		}()
	}
}

// DiscoverInstance will attempt to discover (poll) an instance (unless
// it is already up-to-date) and will also ensure that its primary and
// replicas (if any) are also checked.
func DiscoverInstance(tabletAlias string, forceDiscovery bool) {
	if inst.InstanceIsForgotten(tabletAlias) {
		log.Infof("discoverInstance: skipping discovery of %+v because it is set to be forgotten", tabletAlias)
		return
	}

	// create stopwatch entries
	latency := stopwatch.NewNamedStopwatch()
	_ = latency.AddMany([]string{
		"backend",
		"instance",
		"total"})
	latency.Start("total") // start the total stopwatch (not changed anywhere else)
	var metric *discovery.Metric
	defer func() {
		latency.Stop("total")
		discoveryTime := latency.Elapsed("total")
		if discoveryTime > config.GetInstancePollTime() {
			instancePollSecondsExceededCounter.Add(1)
			log.Warningf("discoverInstance exceeded InstancePollSeconds for %+v, took %.4fs", tabletAlias, discoveryTime.Seconds())
			if metric != nil {
				metric.InstancePollSecondsDurationCount = 1
			}
		}
	}()

	if tabletAlias == "" {
		return
	}

	// Calculate the expiry period each time as InstancePollSeconds
	// _may_ change during the run of the process (via SIGHUP) and
	// it is not possible to change the cache's default expiry..
	if existsInCacheError := recentDiscoveryOperationKeys.Add(tabletAlias, true, config.GetInstancePollTime()); existsInCacheError != nil && !forceDiscovery {
		// Just recently attempted
		return
	}

	latency.Start("backend")
	instance, found, _ := inst.ReadInstance(tabletAlias)
	latency.Stop("backend")
	if !forceDiscovery && found && instance.IsUpToDate && instance.IsLastCheckValid {
		// we've already discovered this one. Skip!
		return
	}

	discoveriesCounter.Add(1)

	// First we've ever heard of this instance. Continue investigation:
	instance, err := inst.ReadTopologyInstanceBufferable(tabletAlias, latency)
	// panic can occur (IO stuff). Therefore it may happen
	// that instance is nil. Check it, but first get the timing metrics.
	totalLatency := latency.Elapsed("total")
	backendLatency := latency.Elapsed("backend")
	instanceLatency := latency.Elapsed("instance")
	otherLatency := totalLatency - (backendLatency + instanceLatency)

	discoverInstanceTimings.Add("Backend", backendLatency)
	discoverInstanceTimings.Add("Instance", instanceLatency)
	discoverInstanceTimings.Add("Other", otherLatency)

	if forceDiscovery {
		log.Infof("Force discovered - %+v, err - %v", instance, err)
	}

	if instance == nil {
		failedDiscoveriesCounter.Add(1)
		metric = &discovery.Metric{
			Timestamp:       time.Now(),
			TabletAlias:     tabletAlias,
			TotalLatency:    totalLatency,
			BackendLatency:  backendLatency,
			InstanceLatency: instanceLatency,
			Err:             err,
		}
		_ = discoveryMetrics.Append(metric)
		if util.ClearToLog("discoverInstance", tabletAlias) {
			log.Warningf(" DiscoverInstance(%+v) instance is nil in %.3fs (Backend: %.3fs, Instance: %.3fs), error=%+v",
				tabletAlias,
				totalLatency.Seconds(),
				backendLatency.Seconds(),
				instanceLatency.Seconds(),
				err)
		}
		return
	}

	metric = &discovery.Metric{
		Timestamp:       time.Now(),
		TabletAlias:     tabletAlias,
		TotalLatency:    totalLatency,
		BackendLatency:  backendLatency,
		InstanceLatency: instanceLatency,
		Err:             nil,
	}
	_ = discoveryMetrics.Append(metric)
}

// onHealthTick handles the actions to take to discover/poll instances
func onHealthTick() {
	tabletAliases, err := inst.ReadOutdatedInstanceKeys()
	if err != nil {
		log.Error(err)
	}

	func() {
		// Normally onHealthTick() shouldn't run concurrently. It is kicked by a ticker.
		// However it _is_ invoked inside a goroutine. I like to be safe here.
		snapshotDiscoveryKeysMutex.Lock()
		defer snapshotDiscoveryKeysMutex.Unlock()

		countSnapshotKeys := len(snapshotDiscoveryKeys)
		for i := 0; i < countSnapshotKeys; i++ {
			tabletAliases = append(tabletAliases, <-snapshotDiscoveryKeys)
		}
	}()
	// avoid any logging unless there's something to be done
	if len(tabletAliases) > 0 {
		for _, tabletAlias := range tabletAliases {
			if tabletAlias != "" {
				discoveryQueue.Push(tabletAlias)
			}
		}
	}
}

// ContinuousDiscovery starts an asynchronous infinite discovery process where instances are
// periodically investigated and their status captured, and long since unseen instances are
// purged and forgotten.
// nolint SA1015: using time.Tick leaks the underlying ticker
func ContinuousDiscovery() {
	log.Infof("continuous discovery: setting up")
	recentDiscoveryOperationKeys = cache.New(config.GetInstancePollTime(), time.Second)

	if !config.GetAllowRecovery() {
		log.Info("--allow-recovery is set to 'false', disabling recovery actions")
		if err := DisableRecovery(); err != nil {
			log.Errorf("failed to disable recoveries: %+v", err)
			return
		}
	}

	go handleDiscoveryRequests()

	healthTick := time.Tick(config.HealthPollSeconds * time.Second)
	caretakingTick := time.Tick(time.Minute)
	recoveryTick := time.Tick(config.GetRecoveryPollDuration())
	tabletTopoTick := OpenTabletDiscovery()
	var recoveryEntrance int64
	var snapshotTopologiesTick <-chan time.Time
	if config.GetSnapshotTopologyInterval() > 0 {
		snapshotTopologiesTick = time.Tick(config.GetSnapshotTopologyInterval())
	}

	go func() {
		_ = ometrics.InitMetrics()
	}()
	// On termination of the server, we should close VTOrc cleanly
	servenv.OnTermSync(closeVTOrc)

	log.Infof("continuous discovery: starting")
	for {
		select {
		case <-healthTick:
			go func() {
				onHealthTick()
			}()
		case <-caretakingTick:
			// Various periodic internal maintenance tasks
			go func() {
				go inst.ForgetLongUnseenInstances()
				go inst.ExpireAudit()
				go inst.ExpireStaleInstanceBinlogCoordinates()
				go ExpireRecoveryDetectionHistory()
				go ExpireTopologyRecoveryHistory()
				go ExpireTopologyRecoveryStepsHistory()
			}()
		case <-recoveryTick:
			go func() {
				go inst.ExpireInstanceAnalysisChangelog()

				go func() {
					// This function is non re-entrant (it can only be running once at any point in time)
					if atomic.CompareAndSwapInt64(&recoveryEntrance, 0, 1) {
						defer atomic.StoreInt64(&recoveryEntrance, 0)
					} else {
						return
					}
					CheckAndRecover()
				}()
			}()
		case <-snapshotTopologiesTick:
			go func() {
				go inst.SnapshotTopologies()
			}()
		case <-tabletTopoTick:
			ctx, cancel := context.WithTimeout(context.Background(), config.GetTopoInformationRefreshDuration())
			if err := refreshAllInformation(ctx); err != nil {
				log.Errorf("failed to refresh topo information: %+v", err)
			}
			cancel()
		}
	}
}

// refreshAllInformation refreshes both shard and tablet information. This is meant to be run on tablet topo ticks.
func refreshAllInformation(ctx context.Context) error {
	// Create an errgroup
	eg, ctx := errgroup.WithContext(ctx)

	// Refresh all keyspace information.
	eg.Go(func() error {
		return RefreshAllKeyspacesAndShards(ctx)
	})

	// Refresh all tablets.
	eg.Go(func() error {
		return refreshAllTablets(ctx)
	})

	// Wait for both the refreshes to complete
	err := eg.Wait()
	if err == nil {
		process.FirstDiscoveryCycleComplete.Store(true)
	}
	return err
}
