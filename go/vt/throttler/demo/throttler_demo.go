/*
Copyright 2019 The Vitess Authors.

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

package main

import (
	"context"
	"flag"
	"math/rand"
	"net/http"
	"sync"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/throttler"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vttablet/grpcqueryservice"
	"vitess.io/vitess/go/vt/vttablet/queryservice/fakes"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"
	"vitess.io/vitess/go/vt/wrangler/testlib"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// This file contains a demo binary that demonstrates how the resharding
// throttler adapts its throttling rate to the replication lag.
//
// The throttler is necessary because replicas apply transactions at a slower
// rate than primaries and fall behind at high write throughput.
// (Mostly they fall behind because MySQL replication is single threaded but
//  the write throughput on the primary does not have to.)
//
// This demo simulates a client (writer), a primary and a replica.
// The client writes to the primary which in turn replicas everything to the
// replica.
// The replica measures its replication lag via the timestamp which is part of
// each message.
// While the primary has no rate limit, the replica is limited to
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

// primary simulates an *unthrottled* MySQL primary which replicates every
// received "execute" call to a known "replica".
type primary struct {
	replica *replica
}

// execute is the simulated RPC which is called by the client.
func (m *primary) execute(msg time.Time) {
	m.replica.replicate(msg)
}

// replica simulates a *throttled* MySQL replica.
// If it cannot keep up with applying the primary writes, it will report a
// replication lag > 0 seconds.
type replica struct {
	fakeTablet *testlib.FakeTablet
	qs         *fakes.StreamHealthQueryService

	// replicationStream is the incoming stream of messages from the primary.
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

func newReplica(lagUpdateInterval, degrationInterval, degrationDuration time.Duration, ts *topo.Server) *replica {
	t := &testing.T{}
	wr := wrangler.New(logutil.NewConsoleLogger(), ts, tmclient.NewTabletManagerClient())
	fakeTablet := testlib.NewFakeTablet(t, wr, "cell1", 0,
		topodatapb.TabletType_REPLICA, nil, testlib.TabletKeyspaceShard(t, "ks", "-80"))
	fakeTablet.StartActionLoop(t, wr)

	target := &querypb.Target{
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
			r.qs.AddHealthResponseWithReplicationLag(lagTruncated)
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
	primary *primary

	healthCheck discovery.HealthCheck
	throttler   *throttler.Throttler

	stopChan      chan struct{}
	wg            sync.WaitGroup
	healthcheckCh chan *discovery.TabletHealth
}

func newClient(primary *primary, replica *replica, ts *topo.Server) *client {
	t, err := throttler.NewThrottler("client", "TPS", 1, throttler.MaxRateModuleDisabled, 5 /* seconds */)
	if err != nil {
		log.Fatal(err)
	}

	healthCheck := discovery.NewHealthCheck(context.Background(), 5*time.Second, 1*time.Minute, ts, "cell1", "")
	c := &client{
		primary:     primary,
		healthCheck: healthCheck,
		throttler:   t,
		stopChan:    make(chan struct{}),
	}
	healthcheckCh := c.healthCheck.Subscribe()
	c.healthcheckCh = healthcheckCh
	c.healthCheck.AddTablet(replica.fakeTablet.Tablet)
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
		case th := <-c.healthcheckCh:
			c.StatsUpdate(th)
		default:
		}

		for {
			backoff := c.throttler.Throttle(0 /* threadID */)
			if backoff == throttler.NotThrottled {
				break
			}
			time.Sleep(backoff)
		}

		c.primary.execute(time.Now())
	}
}

func (c *client) stop() {
	close(c.stopChan)
	c.wg.Wait()

	c.healthCheck.Close()
	c.throttler.Close()
}

// StatsUpdate gets called by the healthCheck instance every time a tablet broadcasts
// a health update.
func (c *client) StatsUpdate(ts *discovery.TabletHealth) {
	// Ignore unless REPLICA or RDONLY.
	if ts.Target.TabletType != topodatapb.TabletType_REPLICA && ts.Target.TabletType != topodatapb.TabletType_RDONLY {
		return
	}

	c.throttler.RecordReplicationLag(time.Now(), ts)
}

func main() {
	servenv.ParseFlags("throttler_demo")

	go servenv.RunDefault()
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/throttlerz", http.StatusTemporaryRedirect)
	})

	log.Infof("start rate set to: %v", *rate)
	ts := memorytopo.NewServer("cell1")
	replica := newReplica(*lagUpdateInterval, *replicaDegrationInterval, *replicaDegrationDuration, ts)
	primary := &primary{replica: replica}
	client := newClient(primary, replica, ts)
	client.run()

	time.Sleep(*duration)
	client.stop()
	replica.stop()
}

func init() {
	servenv.RegisterDefaultFlags()
	servenv.RegisterGRPCServerAuthFlags()
}
