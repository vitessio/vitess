/*
Copyright 2020 The Vitess Authors.

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

package k8stopo

import (
	"context"
	"path"
	"strings"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	vtv1beta1 "vitess.io/vitess/go/vt/topo/k8stopo/apis/topo/v1beta1"
)

// Watch is part of the topo.Conn interface.
func (s *Server) Watch(ctx context.Context, filePath string) (*topo.WatchData, <-chan *topo.WatchData, error) {
	log.Info("Starting Kubernetes topo Watch on ", filePath)

	nodePath := path.Join(s.root, filePath)

	// get current
	initialCtx, initialCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer initialCancel()

	contents, ver, err := s.Get(initialCtx, filePath)
	if err != nil {
		return nil, nil, err
	}
	current := &topo.WatchData{
		Contents: contents,
		Version:  ver,
	}

	// Create the changes channel
	changes := make(chan *topo.WatchData, 10)

	// create a context that will be canceled when the watched node is deleted
	deleteCtx, deleted := context.WithCancel(context.Background())

	log.Infof("Starting watch on %q", nodePath)
	removeWatcher := s.watchGroup.addWatcher(&exactWatcher{
		nodePath: nodePath,
		addUpdateFunc: func(vtn *vtv1beta1.VitessTopoNode) {
			contents, err := unpackValue([]byte(vtn.Data.Value))
			if err != nil {
				log.Errorf("Error unpacking value for %v: %v", vtn.Name, err)
			}
			changes <- &topo.WatchData{
				Contents: contents,
				Version:  KubernetesVersion(vtn.GetResourceVersion()),
			}
		},
		deleteFunc: func(vtn *vtv1beta1.VitessTopoNode) {
			deleted()
		},
	})

	// Handle interrupts and deletes
	go closeOnDone(ctx, deleteCtx.Done(), filePath, changes, removeWatcher)

	return current, changes, nil
}

func closeOnDone(ctx context.Context, deleteChan <-chan struct{}, filePath string, changes chan *topo.WatchData, cleanup func()) {
	select {
	case <-ctx.Done():
		if err := ctx.Err(); err != nil && err == context.Canceled {
			changes <- &topo.WatchData{Err: topo.NewError(topo.Interrupted, filePath)}
		}
	case <-deleteChan:
		changes <- &topo.WatchData{
			Err: topo.NewError(topo.NoNode, filePath),
		}
	}

	cleanup()
	close(changes)
}

// WatchRecursive is part of the topo.Conn interface.
func (s *Server) WatchRecursive(ctx context.Context, dirpath string) ([]*topo.WatchDataRecursive, <-chan *topo.WatchDataRecursive, error) {
	log.Info("Starting recursive Kubernetes topo Watch on ", dirpath)

	nodePathPrefix := path.Join(s.root, dirpath)
	if !strings.HasSuffix(nodePathPrefix, "/") {
		nodePathPrefix = nodePathPrefix + "/"
	}

	// get current
	initialCtx, initialCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer initialCancel()

	Kvs, err := s.List(initialCtx, dirpath)
	if err != nil {
		return nil, nil, err
	}

	var currentWD []*topo.WatchDataRecursive
	for _, kv := range Kvs {
		currentWD = append(currentWD, &topo.WatchDataRecursive{
			Path: string(kv.Key),
			WatchData: topo.WatchData{
				Contents: kv.Value,
				Version:  kv.Version,
			},
		})
	}

	// Create the changes channel
	changes := make(chan *topo.WatchDataRecursive, 10)

	// add a watcher for any changes that match the prefix
	removeWatcher := s.watchGroup.addWatcher(&prefixWatcher{
		nodePathPrefix: nodePathPrefix,
		addUpdateFunc: func(vtn *vtv1beta1.VitessTopoNode) {
			contents, err := unpackValue([]byte(vtn.Data.Value))
			if err != nil {
				log.Errorf("Error unpacking value for %v: %v", vtn.Name, err)
			}
			changes <- &topo.WatchDataRecursive{
				Path: vtn.Name,
				WatchData: topo.WatchData{
					Contents: contents,
					Version:  KubernetesVersion(vtn.GetResourceVersion()),
				},
			}
		},
		deleteFunc: func(vtn *vtv1beta1.VitessTopoNode) {
			changes <- &topo.WatchDataRecursive{
				Path: vtn.Name,
				WatchData: topo.WatchData{
					Err: topo.NewError(topo.NoNode, vtn.Name),
				},
			}
		},
	})

	// Handle interrupts
	go closeRecursiveOnDone(ctx, nodePathPrefix, changes, removeWatcher)

	return currentWD, changes, nil
}

func closeRecursiveOnDone(ctx context.Context, filePath string, changes chan *topo.WatchDataRecursive, cleanup func()) {
	<-ctx.Done()
	if err := ctx.Err(); err != nil && err == context.Canceled {
		changes <- &topo.WatchDataRecursive{
			WatchData: topo.WatchData{
				Err: topo.NewError(topo.Interrupted, filePath),
			},
		}
	}

	close(changes)
	cleanup()
}
