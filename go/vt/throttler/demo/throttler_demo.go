// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"math/rand"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/youtube/vitess/go/vt/discovery"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/throttler"
	"github.com/youtube/vitess/go/vt/topo/memorytopo"
	"github.com/youtube/vitess/go/vt/vttablet/grpcqueryservice"
	"github.com/youtube/vitess/go/vt/vttablet/queryservice/fakes"
	"github.com/youtube/vitess/go/vt/vttablet/tmclient"
	"github.com/youtube/vitess/go/vt/wrangler"
	"github.com/youtube/vitess/go/vt/wrangler/testlib"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"

	log "github.com/golang/glog"
)

// This file contains a demo binary that demonstrates how the resharding
// throttler adapts its throttling rate to the replication lag.
//
// The throttler is necessary because replicas apply transactions at a slower
// rate than masters and fall behind at high write throughput.
// (Mostly they fall behind because MySQL replication is single threaded but
//  the write throughput on the master does not have to.)
//
// This demo simulates a client (writer), a master and a replica.
// The client writes to the master which in turn replicas everything to the
// replica.
// The replica measures its replication lag via the timestamp which is part of
// each message.
// While the master has no rate limit, the replica is limited to
// --rate (see below) transactions/second. The client runs the resharding
// throttler which tries to throttle the client based on the observed
// replication lag.

var (
	rate                     = flag.Int64("rate", 1000, "maximum rate of the throttled demo server at the start")
	duration                 = flag.Duration("duration", 600*time.Second, "total duration the demo runs")
	lagUpdateInterval        = flag.Duration("lag_update_interval", 5*time.Second, "interval at which the current replication lag will be broadcasted to the throttler")
	replicaDegrationInterval = flag.Duration("replica_degration_interval", 0*time.Second, "simulate a throughput degration of the replica every X interval (i.e. the replica applies transactions at a slower rate for -reparent_duration and the replication lag might go up)")
	replicaDegrationDuration = flag.Duration("replica_degration_duration", 10*time.Second, "duration a simulated degration should take")
)

// master simulates an *unthrottled* MySQL master which replicates every
// received "execute" call to a known "replica".
type master struct {
	replica *replica
}

// execute is the simulated RPC which is called by the client.
func (m *master) execute(msg time.Time) {
	m.replica.replicate(msg)
}

// replica simulates a *throttled* MySQL replica.
// If it cannot keep up with applying the master writes, it will report a
// replication lag > 0 seconds.
type replica struct {
	fakeTablet *testlib.FakeTablet
	qs         *fakes.StreamHealthQueryService

	// replicationStream is the incoming stream of messages from the master.
	replicationStream chan time.Time

	// throttler is used to enforce the maximum rate at which replica applies
	// transactions. It must not be confused with the client's throttler.
	throttler         *throttler.Throttler
	lastHealthUpdate  time.Time
	lagUpdateInterval time.Duration

	degrationInterval   time.Duration
	degrationDuration   time.Duration
	nextDegration       time.Time
	currentDegrationEnd time.Time

	stopChan chan struct{}
	wg       sync.WaitGroup
}

func newReplica(lagUpdateInterval, degrationInterval, degrationDuration time.Duration) *replica {
	t := &testing.T{}
	ts := memorytopo.NewServer("cell1")
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	fakeTablet := testlib.NewFakeTablet(t, wr, "cell1", 0,
		topodatapb.TabletType_REPLICA, nil, testlib.TabletKeyspaceShard(t, "ks", "-80"))
	fakeTablet.StartActionLoop(t, wr)

	target := querypb.Target{
		Keyspace:   "ks",
		Shard:      "-80",
		TabletType: topodatapb.TabletType_REPLICA,
	}
	qs := fakes.NewStreamHealthQueryService(target)
	grpcqueryservice.Register(fakeTablet.RPCServer, qs)

	throttler, err := throttler.NewThrottler("replica", "TPS", 1, *rate, throttler.ReplicationLagModuleDisabled)
	if err != nil {
		log.Fatal(err)
	}

	var nextDegration time.Time
	if degrationInterval != time.Duration(0) {
		nextDegration = time.Now().Add(degrationInterval)
	}
	r := &replica{
		fakeTablet:        fakeTablet,
		qs:                qs,
		throttler:         throttler,
		replicationStream: make(chan time.Time, 1*1024*1024),
		lagUpdateInterval: lagUpdateInterval,
		degrationInterval: degrationInterval,
		degrationDuration: degrationDuration,
		nextDegration:     nextDegration,
		stopChan:          make(chan struct{}),
	}
	r.wg.Add(1)
	go r.processReplicationStream()
	return r
}

func (r *replica) replicate(msg time.Time) {
	r.replicationStream <- msg
}

func (r *replica) processReplicationStream() {
	defer r.wg.Done()

	// actualRate counts the number of requests per r.lagUpdateInterval.
	actualRate := 0
	for msg := range r.replicationStream {
		select {
		case <-r.stopChan:
			return
		default:
		}

		now := time.Now()
		if now.Sub(r.lastHealthUpdate) > r.lagUpdateInterval {
			// Broadcast current lag every "lagUpdateInterval".
			//
			// Use integer values to calculate the lag. In consequence, the reported
			// lag will constantly vary between the floor and ceil value e.g.
			// an actual lag of 0.5s could be reported as 0s or 1s based on the
			// truncation of the two times.
			lagTruncated := uint32(now.Unix() - msg.Unix())
			// Display lag with a higher precision as well.
			lag := now.Sub(msg).Seconds()
			log.Infof("current lag: %1ds (%1.1fs) replica rate: % 7.1f chan len: % 6d", lagTruncated, lag, float64(actualRate)/r.lagUpdateInterval.Seconds(), len(r.replicationStream))
			r.qs.AddHealthResponseWithSecondsBehindMaster(lagTruncated)
			r.lastHealthUpdate = now
			actualRate = 0
		}
		if !r.nextDegration.IsZero() && time.Now().After(r.nextDegration) && r.currentDegrationEnd.IsZero() {
			degradedRate := rand.Int63n(*rate)
			log.Infof("degrading the replica for %.f seconds from %v TPS to %v", r.degrationDuration.Seconds(), *rate, degradedRate)
			r.throttler.SetMaxRate(degradedRate)
			r.currentDegrationEnd = time.Now().Add(r.degrationDuration)
		}
		if !r.currentDegrationEnd.IsZero() && time.Now().After(r.currentDegrationEnd) {
			log.Infof("degrading the replica stopped. Restoring TPS to: %v", *rate)
			r.throttler.SetMaxRate(*rate)
			r.currentDegrationEnd = time.Time{}
			r.nextDegration = time.Now().Add(r.degrationInterval)
		}

		for {
			backoff := r.throttler.Throttle(0 /* threadID */)
			if backoff == throttler.NotThrottled {
				break
			}
			time.Sleep(backoff)
		}
		actualRate++
	}
}

func (r *replica) stop() {
	close(r.replicationStream)
	close(r.stopChan)
	log.Info("Triggered replica shutdown. Waiting for it to stop.")
	r.wg.Wait()
	r.fakeTablet.StopActionLoop(&testing.T{})
}

// client simulates a client which should throttle itself based on the
// replication lag of all replicas.
type client struct {
	master *master

	healthCheck discovery.HealthCheck
	throttler   *throttler.Throttler

	stopChan chan struct{}
	wg       sync.WaitGroup
}

func newClient(master *master, replica *replica) *client {
	t, err := throttler.NewThrottler("client", "TPS", 1, throttler.MaxRateModuleDisabled, 5 /* seconds */)
	if err != nil {
		log.Fatal(err)
	}

	healthCheck := discovery.NewHealthCheck(1*time.Minute, 5*time.Second, 1*time.Minute)
	c := &client{
		master:      master,
		healthCheck: healthCheck,
		throttler:   t,
		stopChan:    make(chan struct{}),
	}
	c.healthCheck.SetListener(c, false /* sendDownEvents */)
	c.healthCheck.AddTablet(replica.fakeTablet.Tablet, "name")
	return c
}

func (c *client) run() {
	c.wg.Add(1)
	go c.loop()
}

func (c *client) loop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.stopChan:
			return
		default:
		}

		for {
			backoff := c.throttler.Throttle(0 /* threadID */)
			if backoff == throttler.NotThrottled {
				break
			}
			time.Sleep(backoff)
		}

		c.master.execute(time.Now())
	}
}

func (c *client) stop() {
	close(c.stopChan)
	c.wg.Wait()

	c.healthCheck.Close()
	c.throttler.Close()
}

// StatsUpdate implements discovery.HealthCheckStatsListener.
// It gets called by the healthCheck instance every time a tablet broadcasts
// a health update.
func (c *client) StatsUpdate(ts *discovery.TabletStats) {
	// Ignore unless REPLICA or RDONLY.
	if ts.Target.TabletType != topodatapb.TabletType_REPLICA && ts.Target.TabletType != topodatapb.TabletType_RDONLY {
		return
	}

	c.throttler.RecordReplicationLag(time.Now(), ts)
}

func main() {
	flag.Parse()

	go servenv.RunDefault()
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/throttlerz", http.StatusTemporaryRedirect)
	})

	log.Infof("start rate set to: %v", *rate)
	replica := newReplica(*lagUpdateInterval, *replicaDegrationInterval, *replicaDegrationDuration)
	master := &master{replica: replica}
	client := newClient(master, replica)
	client.run()

	time.Sleep(*duration)
	client.stop()
	replica.stop()
}

func init() {
	servenv.RegisterDefaultFlags()
}
