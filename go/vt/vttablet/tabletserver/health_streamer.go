/*
Copyright 2020 The Vitess Authors.

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

package tabletserver

import (
	"io"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

var (
	// blpFunc is a legaacy feature.
	// TODO(sougou): remove after legacy resharding worflows are removed.
	blpFunc = vreplication.StatusSummary

	errUnintialized = "tabletserver uninitialized"
)

// healthStreamer streams health information to callers.
type healthStreamer struct {
	interval           time.Duration
	degradedThreshold  time.Duration
	unhealthyThreshold time.Duration

	replStatusFunc func() (time.Duration, error)
	stats          *tabletenv.Stats
	ticks          *timer.Timer

	mu      sync.Mutex
	clients map[chan *querypb.StreamHealthResponse]struct{}
	state   *querypb.StreamHealthResponse
	// serving reflects the state of stateManager.
	// We may still broadcast as not serving if there are
	// replication errors, or if lag is above threshold.
	serving bool
}

func newHealthStreamer(env tabletenv.Env, alias topodatapb.TabletAlias, replStatusFunc func() (time.Duration, error)) *healthStreamer {
	hc := env.Config().Healthcheck
	return &healthStreamer{
		interval:           hc.IntervalSeconds.Get(),
		degradedThreshold:  hc.DegradedThresholdSeconds.Get(),
		unhealthyThreshold: hc.UnhealthyThresholdSeconds.Get(),

		replStatusFunc: replStatusFunc,
		stats:          env.Stats(),
		clients:        make(map[chan *querypb.StreamHealthResponse]struct{}),
		ticks:          timer.NewTimer(hc.IntervalSeconds.Get()),

		state: &querypb.StreamHealthResponse{
			Target:      &querypb.Target{},
			TabletAlias: &alias,
			RealtimeStats: &querypb.RealtimeStats{
				HealthError: errUnintialized,
			},
		},
	}
}

func (hs *healthStreamer) InitDBConfig(target querypb.Target) {
	hs.state.Target = &target
}

func (hs *healthStreamer) Stream(ctx context.Context, callback func(*querypb.StreamHealthResponse) error) error {
	ch := hs.register()
	defer hs.unregister(ch)

	for {
		select {
		case <-ctx.Done():
			return nil
		case shr := <-ch:
			log.Infof("sending: %v", shr)
			if err := callback(shr); err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
		}
	}
}

func (hs *healthStreamer) register() chan *querypb.StreamHealthResponse {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	ch := make(chan *querypb.StreamHealthResponse, 1)
	hs.clients[ch] = struct{}{}

	// Start is idempotent.
	hs.ticks.Start(hs.Broadcast)

	// Send the current state immediately.
	ch <- proto.Clone(hs.state).(*querypb.StreamHealthResponse)
	return ch
}

func (hs *healthStreamer) unregister(ch chan *querypb.StreamHealthResponse) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	delete(hs.clients, ch)

	if len(hs.clients) == 0 {
		hs.ticks.Stop()
	}
}

func (hs *healthStreamer) Broadcast() {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	var healthy bool
	lag, err := hs.replStatusFunc()
	if err != nil {
		hs.state.RealtimeStats.HealthError = err.Error()
		hs.state.RealtimeStats.SecondsBehindMaster = 0
		healthy = false
	} else {
		hs.state.RealtimeStats.HealthError = ""
		hs.state.RealtimeStats.SecondsBehindMaster = uint32(lag.Seconds())
		healthy = lag <= hs.unhealthyThreshold
	}
	hs.state.Serving = hs.serving && healthy

	hs.state.RealtimeStats.SecondsBehindMasterFilteredReplication, hs.state.RealtimeStats.BinlogPlayersCount = blpFunc()
	hs.state.RealtimeStats.Qps = hs.stats.QPSRates.TotalRate()

	shr := proto.Clone(hs.state).(*querypb.StreamHealthResponse)

	for ch := range hs.clients {
		select {
		case ch <- shr:
		default:
		}
	}
}

func (hs *healthStreamer) ChangeState(tabletType topodatapb.TabletType, terTimestamp time.Time, serving bool) {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	log.Infof("State changed: %v %v %v", tabletType, terTimestamp, serving)

	hs.state.Target.TabletType = tabletType
	if tabletType == topodatapb.TabletType_MASTER {
		hs.state.TabletExternallyReparentedTimestamp = terTimestamp.Unix()
	} else {
		hs.state.TabletExternallyReparentedTimestamp = 0
	}
	hs.serving = serving
	hs.ticks.Trigger()
}
