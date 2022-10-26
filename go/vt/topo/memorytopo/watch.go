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
	"context"
	"fmt"

	"vitess.io/vitess/go/vt/topo"
)

// Watch is part of the topo.Conn interface.
func (c *Conn) Watch(ctx context.Context, filePath string) (*topo.WatchData, <-chan *topo.WatchData, error) {
	if c.closed {
		return nil, nil, ErrConnectionClosed
	}

	c.factory.Lock()
	defer c.factory.Unlock()

	if c.factory.err != nil {
		return nil, nil, c.factory.err
	}

	n := c.factory.nodeByPath(c.cell, filePath)
	if n == nil {
		return nil, nil, topo.NewError(topo.NoNode, filePath)
	}
	if n.contents == nil {
		// it's a directory
		return nil, nil, fmt.Errorf("cannot watch directory %v in cell %v", filePath, c.cell)
	}
	current := &topo.WatchData{
		Contents: n.contents,
		Version:  NodeVersion(n.version),
	}

	notifications := make(chan *topo.WatchData, 100)
	watchIndex := nextWatchIndex
	nextWatchIndex++
	n.watches[watchIndex] = watch{contents: notifications}

	go func() {
		<-ctx.Done()
		// This function can be called at any point, so we first need
		// to make sure the watch is still valid.
		c.factory.Lock()
		defer c.factory.Unlock()

		n := c.factory.nodeByPath(c.cell, filePath)
		if n == nil {
			return
		}

		if w, ok := n.watches[watchIndex]; ok {
			delete(n.watches, watchIndex)
			w.contents <- &topo.WatchData{Err: topo.NewError(topo.Interrupted, "watch")}
			close(w.contents)
		}
	}()
	return current, notifications, nil
}

// WatchRecursive is part of the topo.Conn interface.
func (c *Conn) WatchRecursive(ctx context.Context, dirpath string) ([]*topo.WatchDataRecursive, <-chan *topo.WatchDataRecursive, error) {
	if c.closed {
		return nil, nil, ErrConnectionClosed
	}

	c.factory.Lock()
	defer c.factory.Unlock()

	if c.factory.err != nil {
		return nil, nil, c.factory.err
	}

	n := c.factory.getOrCreatePath(c.cell, dirpath)
	if n == nil {
		return nil, nil, topo.NewError(topo.NoNode, dirpath)
	}

	var initialwd []*topo.WatchDataRecursive
	n.recurseContents(func(n *node) {
		initialwd = append(initialwd, &topo.WatchDataRecursive{
			Path: n.name,
			WatchData: topo.WatchData{
				Contents: n.contents,
				Version:  NodeVersion(n.version),
			},
		})
	})

	notifications := make(chan *topo.WatchDataRecursive, 100)
	watchIndex := nextWatchIndex
	nextWatchIndex++
	n.watches[watchIndex] = watch{recursive: notifications}

	go func() {
		defer close(notifications)

		<-ctx.Done()

		c.factory.Lock()
		f := c.factory
		defer f.Unlock()

		n := f.nodeByPath(c.cell, dirpath)
		if n != nil {
			delete(n.watches, watchIndex)
		}

		notifications <- &topo.WatchDataRecursive{WatchData: topo.WatchData{Err: topo.NewError(topo.Interrupted, "watch")}}
	}()

	return initialwd, notifications, nil
}
