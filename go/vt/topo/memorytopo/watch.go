package memorytopo

import (
	"fmt"

	"context"

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
