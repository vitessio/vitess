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
