package zktopo

import (
	"fmt"

	zookeeper "github.com/samuel/go-zookeeper/zk"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
)

func newWatchData(valueType dataType, data string, stats *zookeeper.Stat) *topo.WatchData {
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
func (zkts *Server) Watch(ctx context.Context, cell string, filePath string) (*topo.WatchData, <-chan *topo.WatchData) {
	// Special paths where we need to be backward compatible.
	var valueType dataType
	valueType, filePath = oldTypeAndFilePath(cell, filePath)

	// Get the initial value, set the initial watch
	data, stats, watch, err := zkts.zconn.GetW(filePath)
	if err != nil {
		return &topo.WatchData{Err: convertError(err)}, nil
	}
	if stats == nil {
		// No stats --> node doesn't exist.
		return &topo.WatchData{Err: topo.ErrNoNode}, nil
	}
	wd := newWatchData(valueType, data, stats)
	if wd.Err != nil {
		return wd, nil
	}

	c := make(chan *topo.WatchData, 10)
	go func() {
		for {
			// Act on the watch, or on context close.
			select {
			case event, ok := <-watch:
				if !ok {
					c <- &topo.WatchData{Err: fmt.Errorf("watch on %v was closed", filePath)}
					close(c)
					return
				}

				if event.Err != nil {
					c <- &topo.WatchData{Err: fmt.Errorf("received a non-OK event for %v: %v", filePath, event.Err)}
					close(c)
					return
				}

			case <-ctx.Done():
				// user is not interested any more
				c <- &topo.WatchData{Err: ctx.Err()}
				close(c)
				return
			}

			// Get the value again, and send it, or error.
			data, stats, watch, err = zkts.zconn.GetW(filePath)
			if err != nil {
				c <- &topo.WatchData{Err: convertError(err)}
				close(c)
				return
			}
			if stats == nil {
				// No data --> node doesn't exist
				c <- &topo.WatchData{Err: topo.ErrNoNode}
				close(c)
				return
			}
			wd := newWatchData(valueType, data, stats)
			c <- wd
			if wd.Err != nil {
				close(c)
				return
			}
		}
	}()
	return wd, c
}
