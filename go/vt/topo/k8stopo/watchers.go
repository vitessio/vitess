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

/*
Package k8stopo implements topo.Server with the Kubernetes API as the backend.

We expect the following behavior from the kubernetes client library:

  - TODO

We follow these conventions within this package:

  - TODO
*/
package k8stopo

import (
	"strings"
	"sync"

	"github.com/google/uuid"

	vtv1beta1 "vitess.io/vitess/go/vt/topo/k8stopo/apis/topo/v1beta1"
)

// watcher is an interface that can be implemented to watch for changes to topology
type watcher interface {
	// onAddUpdate is called when a node is updated or added
	// we do not differentiate between the two at this time
	onAddUpdate(vtn *vtv1beta1.VitessTopoNode)

	// onDelete is called when a node is deleted
	onDelete(vtn *vtv1beta1.VitessTopoNode)
}

type watchGroup struct {
	watchers *sync.Map
}

var _ watcher = (*watchGroup)(nil)

// addWatcher adds the given watcher to the topo server watch map
// it returns a function that can be called to remove the watcher
func (wg *watchGroup) addWatcher(w watcher) func() {
	// add to the watchers
	id := uuid.NewString()

	wg.watchers.Store(id, w)

	return func() {
		wg.watchers.Delete(id)
	}
}

// onAddUpdate is called when a node is updated or added
//
// It calls the onUpdate function on all the watchers in the watchGroup
// by iterating over the map in a thread safe way
func (wg *watchGroup) onAddUpdate(vtn *vtv1beta1.VitessTopoNode) {
	wg.watchers.Range(func(_, value interface{}) bool {
		value.(watcher).onAddUpdate(vtn)
		return true
	})
}

// onDelete is called when a node is deleted, it calls the onDelete function
// on all the watchers in the watchGroup
func (wg *watchGroup) onDelete(vtn *vtv1beta1.VitessTopoNode) {
	wg.watchers.Range(func(_, value interface{}) bool {
		value.(watcher).onDelete(vtn)
		return true
	})
}

// a prefixWatcher watches all changes but only calls the stored funcs
// if the node path starts with the given prefix
type prefixWatcher struct {
	nodePathPrefix string
	addUpdateFunc  func(vtn *vtv1beta1.VitessTopoNode)
	deleteFunc     func(vtn *vtv1beta1.VitessTopoNode)
}

var _ watcher = (*prefixWatcher)(nil)

func (w *prefixWatcher) onAddUpdate(vtn *vtv1beta1.VitessTopoNode) {
	if w.addUpdateFunc == nil {
		return
	}

	if strings.HasPrefix(vtn.Data.Key, w.nodePathPrefix) {
		w.addUpdateFunc(vtn)
	}
}

func (w *prefixWatcher) onDelete(vtn *vtv1beta1.VitessTopoNode) {
	if w.deleteFunc == nil {
		return
	}

	if strings.HasPrefix(vtn.Data.Key, w.nodePathPrefix) {
		w.deleteFunc(vtn)
	}
}

// an exactWatcher watches all changes but only calls the stored funcs
// if the node path is exactly the given path
type exactWatcher struct {
	nodePath      string
	addUpdateFunc func(vtn *vtv1beta1.VitessTopoNode)
	deleteFunc    func(vtn *vtv1beta1.VitessTopoNode)
}

var _ watcher = (*exactWatcher)(nil)

func (w *exactWatcher) onAddUpdate(vtn *vtv1beta1.VitessTopoNode) {
	if w.addUpdateFunc == nil {
		return
	}

	if vtn.Data.Key == w.nodePath {
		w.addUpdateFunc(vtn)
	}
}

func (w *exactWatcher) onDelete(vtn *vtv1beta1.VitessTopoNode) {
	if w.deleteFunc == nil {
		return
	}

	if vtn.Data.Key == w.nodePath {
		w.deleteFunc(vtn)
	}
}
