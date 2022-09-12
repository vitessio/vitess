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

package etcd2topo

import (
	"context"
	"path"
	"strings"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

// Watch is part of the topo.Conn interface.
func (s *Server) Watch(ctx context.Context, filePath string) (*topo.WatchData, <-chan *topo.WatchData, error) {
	nodePath := path.Join(s.root, filePath)

	// Get the initial version of the file
	initialCtx, initialCancel := context.WithTimeout(ctx, *topo.RemoteOperationTimeout)
	defer initialCancel()
	initial, err := s.cli.Get(initialCtx, nodePath)
	if err != nil {
		// Generic error.
		return nil, nil, convertError(err, nodePath)
	}

	if len(initial.Kvs) != 1 {
		// Node doesn't exist.
		return nil, nil, topo.NewError(topo.NoNode, nodePath)
	}
	wd := &topo.WatchData{
		Contents: initial.Kvs[0].Value,
		Version:  EtcdVersion(initial.Kvs[0].ModRevision),
	}

	// Create an outer context that will be canceled on return and will cancel all inner watches.
	outerCtx, outerCancel := context.WithCancel(ctx)

	// Create a context, will be used to cancel the watch on retry.
	watchCtx, watchCancel := context.WithCancel(outerCtx)

	// Create the Watcher.  We start watching from the response we
	// got, not from the file original version, as the server may
	// not have that much history.
	watcher := s.cli.Watch(watchCtx, nodePath, clientv3.WithRev(initial.Header.Revision))
	if watcher == nil {
		watchCancel()
		outerCancel()
		return nil, nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "Watch failed")
	}

	// Create the notifications channel, send updates to it.
	notifications := make(chan *topo.WatchData, 10)
	go func() {
		defer close(notifications)
		defer outerCancel()

		var currVersion = initial.Header.Revision
		var watchRetries int
		for {
			select {
			case <-s.running:
				return
			case <-watchCtx.Done():
				// This includes context cancellation errors.
				notifications <- &topo.WatchData{
					Err: convertError(watchCtx.Err(), nodePath),
				}
				return
			case wresp, ok := <-watcher:
				if !ok {
					if watchRetries > 10 {
						t := time.NewTimer(time.Duration(watchRetries) * time.Second)
						select {
						case <-t.C:
							t.Stop()
						case <-s.running:
							t.Stop()
							continue
						case <-watchCtx.Done():
							t.Stop()
							continue
						}
					}
					watchRetries++
					// Cancel inner context on retry and create new one.
					watchCancel()
					watchCtx, watchCancel = context.WithCancel(ctx)
					newWatcher := s.cli.Watch(watchCtx, nodePath, clientv3.WithRev(currVersion))
					if newWatcher == nil {
						log.Warningf("watch %v failed and get a nil channel returned, currVersion: %v", nodePath, currVersion)
					} else {
						watcher = newWatcher
					}
					continue
				}

				watchRetries = 0

				if wresp.Canceled {
					// Final notification.
					notifications <- &topo.WatchData{
						Err: convertError(wresp.Err(), nodePath),
					}
					return
				}

				currVersion = wresp.Header.GetRevision()

				for _, ev := range wresp.Events {
					switch ev.Type {
					case mvccpb.PUT:
						notifications <- &topo.WatchData{
							Contents: ev.Kv.Value,
							Version:  EtcdVersion(ev.Kv.Version),
						}
					case mvccpb.DELETE:
						// Node is gone, send a final notice.
						notifications <- &topo.WatchData{
							Err: topo.NewError(topo.NoNode, nodePath),
						}
						return
					default:
						notifications <- &topo.WatchData{
							Err: vterrors.Errorf(vtrpc.Code_INTERNAL, "unexpected event received: %v", ev),
						}
						return
					}
				}
			}
		}
	}()

	return wd, notifications, nil
}

// WatchRecursive is part of the topo.Conn interface.
func (s *Server) WatchRecursive(ctx context.Context, dirpath string) ([]*topo.WatchDataRecursive, <-chan *topo.WatchDataRecursive, error) {
	nodePath := path.Join(s.root, dirpath)
	if !strings.HasSuffix(nodePath, "/") {
		nodePath = nodePath + "/"
	}

	// Get the initial version of the file
	initial, err := s.cli.Get(ctx, nodePath, clientv3.WithPrefix())
	if err != nil {
		return nil, nil, convertError(err, nodePath)
	}

	var initialwd []*topo.WatchDataRecursive

	for _, kv := range initial.Kvs {
		var wd topo.WatchDataRecursive
		wd.Path = string(kv.Key)
		wd.Contents = kv.Value
		wd.Version = EtcdVersion(initial.Kvs[0].ModRevision)
		initialwd = append(initialwd, &wd)
	}

	// Create an outer context that will be canceled on return and will cancel all inner watches.
	outerCtx, outerCancel := context.WithCancel(ctx)

	// Create a context, will be used to cancel the watch on retry.
	watchCtx, watchCancel := context.WithCancel(outerCtx)

	// Create the Watcher.  We start watching from the response we
	// got, not from the file original version, as the server may
	// not have that much history.
	watcher := s.cli.Watch(watchCtx, nodePath, clientv3.WithRev(initial.Header.Revision), clientv3.WithPrefix())
	if watcher == nil {
		watchCancel()
		outerCancel()
		return nil, nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "Watch failed")
	}

	// Create the notifications channel, send updates to it.
	notifications := make(chan *topo.WatchDataRecursive, 10)
	go func() {
		defer close(notifications)
		defer outerCancel()

		var currVersion = initial.Header.Revision
		var watchRetries int
		for {
			select {
			case <-s.running:
				return
			case <-watchCtx.Done():
				// This includes context cancellation errors.
				notifications <- &topo.WatchDataRecursive{
					WatchData: topo.WatchData{Err: convertError(watchCtx.Err(), nodePath)},
				}
				return
			case wresp, ok := <-watcher:
				if !ok {
					if watchRetries > 10 {
						select {
						case <-time.After(time.Duration(watchRetries) * time.Second):
						case <-s.running:
							continue
						case <-watchCtx.Done():
							continue
						}
					}
					watchRetries++
					// Cancel inner context on retry and create new one.
					watchCancel()
					watchCtx, watchCancel = context.WithCancel(ctx)

					newWatcher := s.cli.Watch(watchCtx, nodePath, clientv3.WithRev(currVersion), clientv3.WithPrefix())
					if newWatcher == nil {
						log.Warningf("watch %v failed and get a nil channel returned, currVersion: %v", nodePath, currVersion)
					} else {
						watcher = newWatcher
					}
					continue
				}

				watchRetries = 0

				if wresp.Canceled {
					// Final notification.
					notifications <- &topo.WatchDataRecursive{
						WatchData: topo.WatchData{Err: convertError(wresp.Err(), nodePath)},
					}
					return
				}

				currVersion = wresp.Header.GetRevision()

				for _, ev := range wresp.Events {
					switch ev.Type {
					case mvccpb.PUT:
						notifications <- &topo.WatchDataRecursive{
							Path: string(ev.Kv.Key),
							WatchData: topo.WatchData{
								Contents: ev.Kv.Value,
								Version:  EtcdVersion(ev.Kv.Version),
							},
						}
					case mvccpb.DELETE:
						notifications <- &topo.WatchDataRecursive{
							Path: string(ev.Kv.Key),
							WatchData: topo.WatchData{
								Err: topo.NewError(topo.NoNode, nodePath),
							},
						}
					}
				}
			}
		}
	}()

	return initialwd, notifications, nil
}
