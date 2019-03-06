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

package etcd2topo

import (
	"path"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"golang.org/x/net/context"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

// Watch is part of the topo.Conn interface.
func (s *Server) Watch(ctx context.Context, filePath string) (*topo.WatchData, <-chan *topo.WatchData, topo.CancelFunc) {
	nodePath := path.Join(s.root, filePath)

	// Get the initial version of the file
	initial, err := s.cli.Get(ctx, nodePath)
	if err != nil {
		// Generic error.
		return &topo.WatchData{Err: convertError(err, nodePath)}, nil, nil
	}
	if len(initial.Kvs) != 1 {
		// Node doesn't exist.
		return &topo.WatchData{Err: topo.NewError(topo.NoNode, nodePath)}, nil, nil
	}
	wd := &topo.WatchData{
		Contents: initial.Kvs[0].Value,
		Version:  EtcdVersion(initial.Kvs[0].ModRevision),
	}

	// Create a context, will be used to cancel the watch.
	watchCtx, watchCancel := context.WithCancel(context.Background())

	// Create the Watcher.  We start watching from the response we
	// got, not from the file original version, as the server may
	// not have that much history.
	watcher := s.cli.Watch(watchCtx, nodePath, clientv3.WithRev(initial.Header.Revision))
	if watcher == nil {
		return &topo.WatchData{Err: vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "Watch failed")}, nil, nil
	}

	// Create the notifications channel, send updates to it.
	notifications := make(chan *topo.WatchData, 10)
	go func() {
		defer close(notifications)

		var currVersion = initial.Header.Revision
		var watchRetries int
		for {
			select {

			case <-watchCtx.Done():
				// This includes context cancelation errors.
				notifications <- &topo.WatchData{
					Err: convertError(watchCtx.Err(), nodePath),
				}
				return
			case wresp, ok := <-watcher:
				if !ok {
					if watchRetries > 10 {
						time.Sleep(time.Duration(watchRetries) * time.Second)
					}
					watchRetries++
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

	return wd, notifications, topo.CancelFunc(watchCancel)
}
