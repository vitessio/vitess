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

package memorytopo

import (
	"fmt"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/vt/topo"
)

// Watch is part of the topo.Conn interface.
func (c *Conn) Watch(ctx context.Context, filePath string) (*topo.WatchData, <-chan *topo.WatchData, topo.CancelFunc) {
	c.factory.mu.Lock()
	defer c.factory.mu.Unlock()

	if c.factory.err != nil {
		return &topo.WatchData{Err: c.factory.err}, nil, nil
	}

	n := c.factory.nodeByPath(c.cell, filePath)
	if n == nil {
		return &topo.WatchData{Err: topo.NewError(topo.NoNode, filePath)}, nil, nil
	}
	if n.contents == nil {
		// it's a directory
		return &topo.WatchData{Err: fmt.Errorf("cannot watch directory %v in cell %v", filePath, c.cell)}, nil, nil
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
		c.factory.mu.Lock()
		defer c.factory.mu.Unlock()

		n := c.factory.nodeByPath(c.cell, filePath)
		if n == nil {
			return
		}

		if w, ok := n.watches[watchIndex]; ok {
			delete(n.watches, watchIndex)
			w <- &topo.WatchData{Err: topo.NewError(topo.Interrupted, "watch")}
			close(w)
		}
	}
	return current, notifications, cancel
}
