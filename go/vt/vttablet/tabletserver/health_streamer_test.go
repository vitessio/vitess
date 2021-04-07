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

func TestHealthStreamerClosed(t *testing.T) {
	config := tabletenv.NewDefaultConfig()
	env := tabletenv.NewEnv(config, "ReplTrackerTest")
	alias := topodatapb.TabletAlias{
		Cell: "cell",
		Uid:  1,
	}
	blpFunc = testBlpFunc
	hs := newHealthStreamer(env, alias)
	err := hs.Stream(context.Background(), func(shr *querypb.StreamHealthResponse) error {
		return nil
	})
	assert.Contains(t, err.Error(), "tabletserver is shutdown")
}

func TestHealthStreamerBroadcast(t *testing.T) {
	config := tabletenv.NewDefaultConfig()
	env := tabletenv.NewEnv(config, "ReplTrackerTest")
	alias := topodatapb.TabletAlias{
		Cell: "cell",
		Uid:  1,
	}
	blpFunc = testBlpFunc
	hs := newHealthStreamer(env, alias)
	hs.Open()
	defer hs.Close()
	target := querypb.Target{}
	hs.InitDBConfig(target)

	ch, cancel := testStream(hs)
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

	hs.ChangeState(topodatapb.TabletType_REPLICA, time.Time{}, 0, nil, false)
	shr = <-ch
	want = &querypb.StreamHealthResponse{
		Target: &querypb.Target{
			TabletType: topodatapb.TabletType_REPLICA,
		},
		TabletAlias: &alias,
		RealtimeStats: &querypb.RealtimeStats{
			SecondsBehindMasterFilteredReplication: 1,
			BinlogPlayersCount:                     2,
		},
	}
	assert.Equal(t, want, shr)

	// Test master and timestamp.
	now := time.Now()
	hs.ChangeState(topodatapb.TabletType_MASTER, now, 0, nil, true)
	shr = <-ch
	want = &querypb.StreamHealthResponse{
		Target: &querypb.Target{
			TabletType: topodatapb.TabletType_MASTER,
		},
		TabletAlias:                         &alias,
		Serving:                             true,
		TabletExternallyReparentedTimestamp: now.Unix(),
		RealtimeStats: &querypb.RealtimeStats{
			SecondsBehindMasterFilteredReplication: 1,
			BinlogPlayersCount:                     2,
		},
	}
	assert.Equal(t, want, shr)

	// Test non-serving, and 0 timestamp for non-master.
	hs.ChangeState(topodatapb.TabletType_REPLICA, now, 1*time.Second, nil, false)
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
	hs.ChangeState(topodatapb.TabletType_REPLICA, now, 0, errors.New("repl err"), false)
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
}

func testStream(hs *healthStreamer) (<-chan *querypb.StreamHealthResponse, context.CancelFunc) {
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

func testBlpFunc() (int64, int32) {
	return 1, 2
}
