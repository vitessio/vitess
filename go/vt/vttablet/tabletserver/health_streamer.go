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
	"vitess.io/vitess/go/history"
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
	stats *tabletenv.Stats

	mu      sync.Mutex
	clients map[chan *querypb.StreamHealthResponse]struct{}
	state   *querypb.StreamHealthResponse

	history *history.History
}

func newHealthStreamer(env tabletenv.Env, alias topodatapb.TabletAlias) *healthStreamer {
	return &healthStreamer{
		stats:   env.Stats(),
		clients: make(map[chan *querypb.StreamHealthResponse]struct{}),

		state: &querypb.StreamHealthResponse{
			Target:      &querypb.Target{},
			TabletAlias: &alias,
			RealtimeStats: &querypb.RealtimeStats{
				HealthError: errUnintialized,
			},
		},

		history: history.New(5),
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

	// Send the current state immediately.
	ch <- proto.Clone(hs.state).(*querypb.StreamHealthResponse)
	return ch
}

func (hs *healthStreamer) unregister(ch chan *querypb.StreamHealthResponse) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	delete(hs.clients, ch)
}

func (hs *healthStreamer) ChangeState(tabletType topodatapb.TabletType, terTimestamp time.Time, lag time.Duration, err error, serving bool) {
	hs.mu.Lock()
	defer hs.mu.Unlock()

	hs.state.Target.TabletType = tabletType
	if tabletType == topodatapb.TabletType_MASTER {
		hs.state.TabletExternallyReparentedTimestamp = terTimestamp.Unix()
	} else {
		hs.state.TabletExternallyReparentedTimestamp = 0
	}
	if err != nil {
		hs.state.RealtimeStats.HealthError = err.Error()
		hs.state.RealtimeStats.SecondsBehindMaster = 0
	} else {
		hs.state.RealtimeStats.HealthError = ""
		hs.state.RealtimeStats.SecondsBehindMaster = uint32(lag.Seconds())
	}
	hs.state.Serving = serving

	hs.state.RealtimeStats.SecondsBehindMasterFilteredReplication, hs.state.RealtimeStats.BinlogPlayersCount = blpFunc()
	hs.state.RealtimeStats.Qps = hs.stats.QPSRates.TotalRate()

	shr := proto.Clone(hs.state).(*querypb.StreamHealthResponse)

	for ch := range hs.clients {
		select {
		case ch <- shr:
		default:
		}
	}
	hs.history.Add(&historyRecord{
		Time:       time.Now(),
		serving:    shr.Serving,
		tabletType: shr.Target.TabletType,
		lag:        lag,
		err:        err,
	})
}

func (hs *healthStreamer) Healthy() string {
	hs.mu.Lock()
	defer hs.mu.Unlock()
	if hs.state.Serving {
		return ""
	}
	if hs.state.RealtimeStats.HealthError == "" {
		return "uhealthy"
	}
	return hs.state.RealtimeStats.HealthError
}
