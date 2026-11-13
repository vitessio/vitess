/*
Copyright 2026 The Vitess Authors.

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

package consultopo

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
)

// TestWatchPollTimeout verifies that when the Consul server stops answering a watch's long polls, the watch
// reports topo.Timeout once a poll outlives twice the poll duration, although the watch's own ctx is still
// live: the poll's deadline is the heartbeat that detects a dead connection, so its expiry must be reported
// as a timeout rather than as the transport error it also is.
func TestWatchPollTimeout(t *testing.T) {
	originalWatchPollDuration := watchPollDuration
	t.Cleanup(func() { watchPollDuration = originalWatchPollDuration })
	watchPollDuration = 50 * time.Millisecond

	// A Consul that answers the initial read and never answers a long poll.
	consul := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("index") != "" {
			<-r.Context().Done()
			return
		}
		w.Header().Set("X-Consul-Index", "5")
		w.Header().Set("X-Consul-KnownLeader", "true")
		w.Header().Set("X-Consul-LastContact", "0")
		_ = json.NewEncoder(w).Encode([]*api.KVPair{{
			Key:         "root/file",
			Value:       []byte("contents"),
			CreateIndex: 5,
			ModifyIndex: 5,
		}})
	}))
	t.Cleanup(consul.Close)

	cfg := api.DefaultConfig()
	cfg.Address = strings.TrimPrefix(consul.URL, "http://")
	client, err := api.NewClient(cfg)
	require.NoError(t, err)
	s := &Server{
		client: client,
		kv:     client.KV(),
		root:   "root",
		locks:  make(map[string]*lockInstance),
	}
	t.Cleanup(s.Close)

	current, changes, err := s.Watch(t.Context(), "file")
	require.NoError(t, err)
	require.Equal(t, "contents", string(current.Contents))

	var got *topo.WatchData
	require.Eventually(t, func() bool {
		select {
		case got = <-changes:
			return true
		default:
			return false
		}
	}, 30*time.Second, 10*time.Millisecond, "the watch did not report the stalled poll")
	require.NotNil(t, got)
	require.True(t, topo.IsErrType(got.Err, topo.Timeout), "expected a topo.Timeout error, got %v", got.Err)
}
