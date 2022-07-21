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

package consultopo

import (
	"flag"
	"path"
	"time"

	"context"

	"github.com/hashicorp/consul/api"

	"vitess.io/vitess/go/vt/topo"
)

var (
	watchPollDuration = flag.Duration("topo_consul_watch_poll_duration", 30*time.Second, "time of the long poll for watch queries.")
)

// Watch is part of the topo.Conn interface.
func (s *Server) Watch(ctx context.Context, filePath string) (*topo.WatchData, <-chan *topo.WatchData, topo.CancelFunc) {
	// Initial get.
	nodePath := path.Join(s.root, filePath)
	pair, _, err := s.kv.Get(nodePath, nil)
	if err != nil {
		return &topo.WatchData{Err: err}, nil, nil
	}
	if pair == nil {
		// Node doesn't exist.
		return &topo.WatchData{Err: topo.NewError(topo.NoNode, nodePath)}, nil, nil
	}

	// Initial value to return.
	wd := &topo.WatchData{
		Contents: pair.Value,
		Version:  ConsulVersion(pair.ModifyIndex),
	}

	// Create a context, will be used to cancel the watch.
	watchCtx, watchCancel := context.WithCancel(context.Background())

	// Create the notifications channel, send updates to it.
	notifications := make(chan *topo.WatchData, 10)
	go func() {
		defer close(notifications)

		var getCtx context.Context
		// Initialize to no-op function to avoid having to check for nil.
		cancelGetCtx := func() {}

		defer cancelGetCtx()

		for {
			// Wait/poll until we get a new version.
			// Get with a WaitIndex and WaitTime will return
			// the current version at the end of WaitTime
			// if it didn't change. So we just check for that
			// and swallow the notifications when version matches.
			waitIndex := pair.ModifyIndex
			opts := &api.QueryOptions{
				WaitIndex: waitIndex,
				WaitTime:  *watchPollDuration,
			}

			// Make a new Context for just this one Get() call.
			// The server should send us something after WaitTime at the latest.
			// If it takes more than 2x that long, assume we've lost contact.
			// This essentially uses WaitTime as a heartbeat interval to detect
			// a dead connection.
			cancelGetCtx()
			getCtx, cancelGetCtx = context.WithTimeout(watchCtx, 2*opts.WaitTime)

			pair, _, err = s.kv.Get(nodePath, opts.WithContext(getCtx))
			if err != nil {
				// Serious error or context timeout/cancelled.
				notifications <- &topo.WatchData{
					Err: convertError(err, nodePath),
				}
				cancelGetCtx()
				return
			}

			// If the node disappeared, pair is nil.
			if pair == nil {
				notifications <- &topo.WatchData{
					Err: topo.NewError(topo.NoNode, nodePath),
				}
				cancelGetCtx()
				return
			}

			// If we got a new value, send it.
			if pair.ModifyIndex != waitIndex {
				notifications <- &topo.WatchData{
					Contents: pair.Value,
					Version:  ConsulVersion(pair.ModifyIndex),
				}
			}

			// See if the watch was canceled.
			select {
			case <-watchCtx.Done():
				notifications <- &topo.WatchData{
					Err: convertError(watchCtx.Err(), nodePath),
				}
				cancelGetCtx()
				return
			default:
			}
		}
	}()

	return wd, notifications, topo.CancelFunc(watchCancel)
}
