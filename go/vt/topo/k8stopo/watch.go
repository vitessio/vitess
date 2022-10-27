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

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/tools/cache"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	vtv1beta1 "vitess.io/vitess/go/vt/topo/k8stopo/apis/topo/v1beta1"
)

// Watch is part of the topo.Conn interface.
func (s *Server) Watch(ctx context.Context, filePath string) (*topo.WatchData, <-chan *topo.WatchData, error) {
	log.Info("Starting Kubernetes topo Watch on ", filePath)

	current := &topo.WatchData{}

	// get current
	initialCtx, initialCancel := context.WithTimeout(ctx, topo.RemoteOperationTimeout)
	defer initialCancel()

	contents, ver, err := s.Get(initialCtx, filePath)
	if err != nil {
		return nil, nil, err
	}
	current.Contents = contents
	current.Version = ver

	// Create the changes channel
	changes := make(chan *topo.WatchData, 10)

	// Create a signal channel for non-interrupt shutdowns
	gracefulShutdown := make(chan struct{})

	resource, err := s.buildFileResource(filePath, []byte{})
	if err != nil {
		return nil, nil, err
	}

	// Create the informer / indexer to watch the single resource
	restClient := s.vtKubeClient.TopoV1beta1().RESTClient()
	listwatch := cache.NewListWatchFromClient(restClient, "vitesstoponodes", s.namespace, fields.OneTermEqualSelector("metadata.name", resource.Name))

	// set up index funcs
	indexers := cache.Indexers{}
	indexers["by_parent"] = indexByParent

	_, memberInformer := cache.NewIndexerInformer(listwatch, &vtv1beta1.VitessTopoNode{}, 0,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj any) {
				vtn := obj.(*vtv1beta1.VitessTopoNode)
				out, err := unpackValue([]byte(vtn.Data.Value))
				if err != nil {
					changes <- &topo.WatchData{Err: err}
					close(gracefulShutdown)
				} else {
					changes <- &topo.WatchData{
						Contents: out,
						Version:  KubernetesVersion(vtn.GetResourceVersion()),
					}
				}
			},
			UpdateFunc: func(oldObj, newObj any) {
				vtn := newObj.(*vtv1beta1.VitessTopoNode)
				out, err := unpackValue([]byte(vtn.Data.Value))
				if err != nil {
					changes <- &topo.WatchData{Err: err}
					close(gracefulShutdown)
				} else {
					changes <- &topo.WatchData{
						Contents: out,
						Version:  KubernetesVersion(vtn.GetResourceVersion()),
					}
				}
			},
			DeleteFunc: func(obj any) {
				vtn := obj.(*vtv1beta1.VitessTopoNode)
				changes <- &topo.WatchData{Err: topo.NewError(topo.NoNode, vtn.Name)}
				close(gracefulShutdown)
			},
		}, indexers)

	// create control chan for informer and start it
	informerChan := make(chan struct{})
	go memberInformer.Run(informerChan)

	// Handle interrupts
	go closeOnDone(ctx, filePath, informerChan, gracefulShutdown, changes)

	return current, changes, nil
}

func closeOnDone(ctx context.Context, filePath string, informerChan chan struct{}, gracefulShutdown chan struct{}, changes chan *topo.WatchData) {
	select {
	case <-ctx.Done():
		if err := ctx.Err(); err != nil && err == context.Canceled {
			changes <- &topo.WatchData{Err: topo.NewError(topo.Interrupted, filePath)}
		}
	case <-gracefulShutdown:
	}
	close(informerChan)
	close(changes)
}

// WatchRecursive is part of the topo.Conn interface.
func (s *Server) WatchRecursive(_ context.Context, path string) ([]*topo.WatchDataRecursive, <-chan *topo.WatchDataRecursive, error) {
	// Kubernetes doesn't seem to provide a primitive that watches a prefix
	// or directory, so this likely can never be implemented.
	return nil, nil, topo.NewError(topo.NoImplementation, path)
}
