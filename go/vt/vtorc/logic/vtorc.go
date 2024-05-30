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
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/sjmudd/stopwatch"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vtorc/collection"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/discovery"
	"vitess.io/vitess/go/vt/vtorc/inst"
	ometrics "vitess.io/vitess/go/vt/vtorc/metrics"
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

// The metrics are registered with deprecated names. The old metric names can be removed in v21.
var discoveriesCounter = stats.NewCounterWithDeprecatedName("DiscoveriesAttempt", "discoveries.attempt", "Number of discoveries attempted")
var failedDiscoveriesCounter = stats.NewCounterWithDeprecatedName("DiscoveriesFail", "discoveries.fail", "Number of failed discoveries")
var instancePollSecondsExceededCounter = stats.NewCounterWithDeprecatedName("DiscoveriesInstancePollSecondsExceeded", "discoveries.instance_poll_seconds_exceeded", "Number of instances that took longer than InstancePollSeconds to poll")
var discoveryQueueLengthGauge = stats.NewGaugeWithDeprecatedName("DiscoveriesQueueLength", "discoveries.queue_length", "Length of the discovery queue")
var discoveryRecentCountGauge = stats.NewGaugeWithDeprecatedName("DiscoveriesRecentCount", "discoveries.recent_count", "Number of recent discoveries")
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

// used in several places
func instancePollSecondsDuration() time.Duration {
	return time.Duration(config.Config.InstancePollSeconds) * time.Second
}

// acceptSighupSignal registers for SIGHUP signal from the OS to reload the configuration files.
func acceptSighupSignal() {
	c := make(chan os.Signal, 1)

	signal.Notify(c, syscall.SIGHUP)
	go func() {
		for range c {
			log.Infof("Received SIGHUP. Reloading configuration")
			_ = inst.AuditOperation("reload-configuration", "", "Triggered via SIGHUP")
			config.Reload()
			discoveryMetrics.SetExpirePeriod(time.Duration(config.DiscoveryCollectionRetentionSeconds) * time.Second)
		}
	}()
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
		count := atomic.LoadInt32(&shardsLockCounter)
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
	discoveryQueue = discovery.CreateOrReturnQueue("DEFAULT")
	// create a pool of discovery workers
	for i := uint(0); i < config.DiscoveryMaxConcurrency; i++ {
		go func() {
			for {
				tabletAlias := discoveryQueue.Consume()
				DiscoverInstance(tabletAlias, false /* forceDiscovery */)
				discoveryQueue.Release(tabletAlias)
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
		if discoveryTime > instancePollSecondsDuration() {
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
	if existsInCacheError := recentDiscoveryOperationKeys.Add(tabletAlias, true, instancePollSecondsDuration()); existsInCacheError != nil && !forceDiscovery {
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
	recentDiscoveryOperationKeys = cache.New(instancePollSecondsDuration(), time.Second)

	go handleDiscoveryRequests()

	healthTick := time.Tick(config.HealthPollSeconds * time.Second)
	caretakingTick := time.Tick(time.Minute)
	recoveryTick := time.Tick(time.Duration(config.Config.RecoveryPollSeconds) * time.Second)
	tabletTopoTick := OpenTabletDiscovery()
	var recoveryEntrance int64
	var snapshotTopologiesTick <-chan time.Time
	if config.Config.SnapshotTopologiesIntervalHours > 0 {
		snapshotTopologiesTick = time.Tick(time.Duration(config.Config.SnapshotTopologiesIntervalHours) * time.Hour)
	}

	go func() {
		_ = ometrics.InitMetrics()
	}()
	go acceptSighupSignal()
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
			refreshAllInformation()
		}
	}
}

// refreshAllInformation refreshes both shard and tablet information. This is meant to be run on tablet topo ticks.
func refreshAllInformation() {
	// Create a wait group
	var wg sync.WaitGroup

	// Refresh all keyspace information.
	wg.Add(1)
	go func() {
		defer wg.Done()
		RefreshAllKeyspacesAndShards()
	}()

	// Refresh all tablets.
	wg.Add(1)
	go func() {
		defer wg.Done()
		refreshAllTablets()
	}()

	// Wait for both the refreshes to complete
	wg.Wait()
}
