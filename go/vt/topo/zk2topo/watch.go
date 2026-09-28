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

package zk2topo

import (
	"context"
	"fmt"
	"path"

	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/topo"
)

// Watch is part of the topo.Conn interface.
func (zs *Server) Watch(ctx context.Context, filePath string) (*topo.WatchData, <-chan *topo.WatchData, error) {
	zkPath := path.Join(zs.root, filePath)

	// Get the initial value, set the initial watch
	initialCtx, initialCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer initialCancel()

	data, stats, watch, err := zs.conn.GetW(initialCtx, zkPath)
	if err != nil {
		return nil, nil, convertError(err, zkPath)
	}
	if stats == nil {
		// No stats --> node doesn't exist.
		return nil, nil, topo.NewError(topo.NoNode, zkPath)
	}
	wd := &topo.WatchData{
		Contents: data,
		Version:  ZKVersion(stats.Version),
	}

	c := make(chan *topo.WatchData, 10)
	go func() {
		defer close(c)

		for {
			// Act on the watch, or on 'stop' close.
			select {
			case event, ok := <-watch:
				if !ok {
					c <- &topo.WatchData{Err: fmt.Errorf("watch on %v was closed", zkPath)}
					return
				}

				if event.Err != nil {
					c <- &topo.WatchData{Err: vterrors.Wrapf(event.Err, "received a non-OK event for %v", zkPath)}
					return
				}

			case <-ctx.Done():
				// user is not interested any more
				c <- &topo.WatchData{Err: topo.NewError(topo.Interrupted, "watch")}
				return
			}

			// Get the value again, and send it, or error.
			data, stats, watch, err = zs.conn.GetW(ctx, zkPath)
			if err != nil {
				c <- &topo.WatchData{Err: convertError(err, zkPath)}
				return
			}
			if stats == nil {
				// No data --> node doesn't exist
				c <- &topo.WatchData{Err: topo.NewError(topo.NoNode, zkPath)}
				return
			}
			wd := &topo.WatchData{
				Contents: data,
				Version:  ZKVersion(stats.Version),
			}
			c <- wd
			if wd.Err != nil {
				return
			}
		}
	}()

	return wd, c, nil
}

// WatchRecursive is part of the topo.Conn interface.
func (zs *Server) WatchRecursive(_ context.Context, path string) ([]*topo.WatchDataRecursive, <-chan *topo.WatchDataRecursive, error) {
	// This isn't implemented yet, but potentially can be implemented if we want
	// to update the minimum ZooKeeper requirement to 3.6.0 and use recursive watches.
	// Also see https://zookeeper.apache.org/doc/r3.6.3/zookeeperProgrammers.html#sc_WatchPersistentRecursive
	return nil, nil, topo.NewError(topo.NoImplementation, path)
}
