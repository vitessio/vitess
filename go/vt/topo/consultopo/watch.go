/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package consultopo

import (
	"flag"
	"fmt"
	"path"
	"time"

	"golang.org/x/net/context"

	"github.com/hashicorp/consul/api"
	"github.com/youtube/vitess/go/vt/topo"
)

var (
	watchPollDuration = flag.Duration("topo_consul_watch_poll_duration", 30*time.Second, "time of the long poll for watch queries. Interrupting a watch may wait for up to that time.")
)

// Watch is part of the topo.Backend interface.
func (s *Server) Watch(ctx context.Context, cell, filePath string) (*topo.WatchData, <-chan *topo.WatchData, topo.CancelFunc) {
	// Initial get.
	c, err := s.clientForCell(ctx, cell)
	if err != nil {
		return &topo.WatchData{Err: fmt.Errorf("Watch cannot get cell: %v", err)}, nil, nil
	}
	nodePath := path.Join(c.root, filePath)

	pair, _, err := c.kv.Get(nodePath, nil)
	if err != nil {
		return &topo.WatchData{Err: err}, nil, nil
	}
	if pair == nil {
		// Node doesn't exist.
		return &topo.WatchData{Err: topo.ErrNoNode}, nil, nil
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

		for {
			// Wait/poll until we get a new version.
			// Get with a WaitIndex and WaitTime will return
			// the current version at the end of WaitTime
			// if it didn't change. So we just check for that
			// and swallow the notifications when version matches.
			waitIndex := pair.ModifyIndex
			pair, _, err = c.kv.Get(nodePath, &api.QueryOptions{
				WaitIndex: waitIndex,
				WaitTime:  *watchPollDuration,
			})
			if err != nil {
				// Serious error.
				notifications <- &topo.WatchData{
					Err: err,
				}
				return
			}

			// If the node disappeared, pair is nil.
			if pair == nil {
				notifications <- &topo.WatchData{
					Err: topo.ErrNoNode,
				}
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
					Err: convertError(watchCtx.Err()),
				}
				return
			default:
			}
		}
	}()

	return wd, notifications, topo.CancelFunc(watchCancel)
}
