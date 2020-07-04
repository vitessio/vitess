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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

func TestHealthStreamerBroadcast(t *testing.T) {
	config := tabletenv.NewDefaultConfig()
	config.Healthcheck.IntervalSeconds = tabletenv.Seconds(0.01)
	env := tabletenv.NewEnv(config, "ReplTrackerTest")
	alias := topodatapb.TabletAlias{
		Cell: "cell",
		Uid:  1,
	}
	blpFunc = testBlpFunc
	hs := newHealthStreamer(env, alias, replFunc)
	target := querypb.Target{}
	hs.InitDBConfig(target)

	ch, cancel := testStream(t, hs)
	defer cancel()

	shr := <-ch
	want := &querypb.StreamHealthResponse{
		Target:      &querypb.Target{},
		TabletAlias: &alias,
		RealtimeStats: &querypb.RealtimeStats{
			HealthError: "tabletserver uninitialized",
		},
	}
	assert.Equal(t, want, shr)

	// The next fetch will broadcast newly obtained info.
	shr = <-ch
	want = &querypb.StreamHealthResponse{
		Target:      &querypb.Target{},
		TabletAlias: &alias,
		RealtimeStats: &querypb.RealtimeStats{
			SecondsBehindMaster:                    1,
			SecondsBehindMasterFilteredReplication: 1,
			BinlogPlayersCount:                     2,
		},
	}
	assert.Equal(t, want, shr)

	// Test master and timestamp.
	now := time.Now()
	hs.ChangeState(topodatapb.TabletType_MASTER, now, true)
	shr = <-ch
	want = &querypb.StreamHealthResponse{
		Target: &querypb.Target{
			TabletType: topodatapb.TabletType_MASTER,
		},
		TabletAlias:                         &alias,
		Serving:                             true,
		TabletExternallyReparentedTimestamp: now.Unix(),
		RealtimeStats: &querypb.RealtimeStats{
			SecondsBehindMaster:                    1,
			SecondsBehindMasterFilteredReplication: 1,
			BinlogPlayersCount:                     2,
		},
	}
	assert.Equal(t, want, shr)

	// Test non-serving, and 0 timestamp for non-master.
	hs.ChangeState(topodatapb.TabletType_REPLICA, now, false)
	shr = <-ch
	want = &querypb.StreamHealthResponse{
		Target: &querypb.Target{
			TabletType: topodatapb.TabletType_REPLICA,
		},
		TabletAlias: &alias,
		RealtimeStats: &querypb.RealtimeStats{
			SecondsBehindMaster:                    1,
			SecondsBehindMasterFilteredReplication: 1,
			BinlogPlayersCount:                     2,
		},
	}
	assert.Equal(t, want, shr)

	// Test Health error.
	hs.mu.Lock()
	hs.replStatusFunc = replFuncErr
	hs.mu.Unlock()
	hs.ChangeState(topodatapb.TabletType_REPLICA, now, true)
	shr = <-ch
	want = &querypb.StreamHealthResponse{
		Target: &querypb.Target{
			TabletType: topodatapb.TabletType_REPLICA,
		},
		TabletAlias: &alias,
		RealtimeStats: &querypb.RealtimeStats{
			HealthError:                            "repl err",
			SecondsBehindMasterFilteredReplication: 1,
			BinlogPlayersCount:                     2,
		},
	}
	assert.Equal(t, want, shr)

	// Test Unhealthy threshold
	hs.mu.Lock()
	hs.replStatusFunc = replFuncUnhealthy
	hs.mu.Unlock()
	shr = <-ch
	want = &querypb.StreamHealthResponse{
		Target: &querypb.Target{
			TabletType: topodatapb.TabletType_REPLICA,
		},
		TabletAlias: &alias,
		RealtimeStats: &querypb.RealtimeStats{
			SecondsBehindMaster:                    10800,
			SecondsBehindMasterFilteredReplication: 1,
			BinlogPlayersCount:                     2,
		},
	}
	assert.Equal(t, want, shr)

	// Test everything back to normal.
	hs.mu.Lock()
	hs.replStatusFunc = replFunc
	hs.mu.Unlock()
	shr = <-ch
	want = &querypb.StreamHealthResponse{
		Target: &querypb.Target{
			TabletType: topodatapb.TabletType_REPLICA,
		},
		Serving:     true,
		TabletAlias: &alias,
		RealtimeStats: &querypb.RealtimeStats{
			SecondsBehindMaster:                    1,
			SecondsBehindMasterFilteredReplication: 1,
			BinlogPlayersCount:                     2,
		},
	}
	assert.Equal(t, want, shr)
}

func testStream(t *testing.T, hs *healthStreamer) (<-chan *querypb.StreamHealthResponse, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan *querypb.StreamHealthResponse)
	go func() {
		_ = hs.Stream(ctx, func(shr *querypb.StreamHealthResponse) error {
			ch <- shr
			return nil
		})
	}()
	return ch, cancel
}

func replFunc() (time.Duration, error) {
	return 1 * time.Second, nil
}

func replFuncUnhealthy() (time.Duration, error) {
	return 3 * time.Hour, nil
}

func replFuncErr() (time.Duration, error) {
	return 0, errors.New("repl err")
}

func testBlpFunc() (int64, int32) {
	return 1, 2
}
