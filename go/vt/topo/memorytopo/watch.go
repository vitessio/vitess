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

package memorytopo

import (
	"fmt"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
)

// Watch is part of the topo.Backend interface.
func (mt *MemoryTopo) Watch(ctx context.Context, cell string, filePath string) (*topo.WatchData, <-chan *topo.WatchData, topo.CancelFunc) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	n := mt.nodeByPath(cell, filePath)
	if n == nil {
		return &topo.WatchData{Err: topo.ErrNoNode}, nil, nil
	}
	if n.contents == nil {
		// it's a directory
		return &topo.WatchData{Err: fmt.Errorf("cannot watch directory %v in cell %v", filePath, cell)}, nil, nil
	}
	current := &topo.WatchData{
		Contents: n.contents,
		Version:  NodeVersion(n.version),
	}

	notifications := make(chan *topo.WatchData, 100)
	watchIndex := nextWatchIndex
	nextWatchIndex++
	n.watches[watchIndex] = notifications

	cancel := func() {
		// This function can be called at any point, so we first need
		// to make sure the watch is still valid.
		mt.mu.Lock()
		defer mt.mu.Unlock()

		n := mt.nodeByPath(cell, filePath)
		if n == nil {
			return
		}

		if w, ok := n.watches[watchIndex]; ok {
			delete(n.watches, watchIndex)
			w <- &topo.WatchData{Err: topo.ErrInterrupted}
			close(w)
		}
	}
	return current, notifications, cancel
}
