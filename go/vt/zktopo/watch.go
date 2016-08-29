package zktopo

import (
	"fmt"
	"sync"

	zookeeper "github.com/samuel/go-zookeeper/zk"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
)

func newWatchData(valueType dataType, data []byte, stats *zookeeper.Stat) *topo.WatchData {
	bytes, err := rawDataFromNodeValue(valueType, data)
	if err != nil {
		return &topo.WatchData{Err: err}
	}

	return &topo.WatchData{
		Contents: bytes,
		Version:  ZKVersion(stats.Version),
	}
}

// Watch is part of the topo.Backend interface
func (zkts *Server) Watch(ctx context.Context, cell, filePath string) (*topo.WatchData, <-chan *topo.WatchData, topo.CancelFunc) {
	// Special paths where we need to be backward compatible.
	var valueType dataType
	valueType, filePath = oldTypeAndFilePath(cell, filePath)

	// Get the initial value, set the initial watch
	data, stats, watch, err := zkts.zconn.GetW(filePath)
	if err != nil {
		return &topo.WatchData{Err: convertError(err)}, nil, nil
	}
	if stats == nil {
		// No stats --> node doesn't exist.
		return &topo.WatchData{Err: topo.ErrNoNode}, nil, nil
	}
	wd := newWatchData(valueType, data, stats)
	if wd.Err != nil {
		return wd, nil, nil
	}

	// mu protects the stop channel. We need to make sure the 'cancel'
	// func can be called multiple times, and that we don't close 'stop'
	// too many times.
	mu := sync.Mutex{}
	stop := make(chan struct{})
	cancel := func() {
		mu.Lock()
		defer mu.Unlock()
		if stop != nil {
			close(stop)
			stop = nil
		}
	}

	c := make(chan *topo.WatchData, 10)
	go func(stop chan struct{}) {
		defer close(c)

		for {
			// Act on the watch, or on 'stop' close.
			select {
			case event, ok := <-watch:
				if !ok {
					c <- &topo.WatchData{Err: fmt.Errorf("watch on %v was closed", filePath)}
					return
				}

				if event.Err != nil {
					c <- &topo.WatchData{Err: fmt.Errorf("received a non-OK event for %v: %v", filePath, event.Err)}
					return
				}

			case <-stop:
				// user is not interested any more
				c <- &topo.WatchData{Err: topo.ErrInterrupted}
				return
			}

			// Get the value again, and send it, or error.
			data, stats, watch, err = zkts.zconn.GetW(filePath)
			if err != nil {
				c <- &topo.WatchData{Err: convertError(err)}
				return
			}
			if stats == nil {
				// No data --> node doesn't exist
				c <- &topo.WatchData{Err: topo.ErrNoNode}
				return
			}
			wd := newWatchData(valueType, data, stats)
			c <- wd
			if wd.Err != nil {
				return
			}
		}
	}(stop)

	return wd, c, cancel
}
