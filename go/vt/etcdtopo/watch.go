package etcdtopo

import (
	"fmt"

	"github.com/coreos/go-etcd/etcd"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
)

func newWatchData(valueType dataType, node *etcd.Node) *topo.WatchData {
	bytes, err := rawDataFromNodeValue(valueType, node.Value)
	if err != nil {
		return &topo.WatchData{Err: err}
	}

	return &topo.WatchData{
		Contents: bytes,
		Version:  EtcdVersion(node.ModifiedIndex),
	}
}

// Watch is part of the topo.Backend interface
func (s *Server) Watch(ctx context.Context, cellName string, filePath string) (current *topo.WatchData, changes <-chan *topo.WatchData) {
	cell, err := s.getCell(cellName)
	if err != nil {
		return &topo.WatchData{Err: fmt.Errorf("Watch cannot get cell: %v", err)}, nil
	}

	// Special paths where we need to be backward compatible.
	var valueType dataType
	valueType, filePath = oldTypeAndFilePath(filePath)

	// Get the initial version of the file
	initial, err := cell.Get(filePath, false /* sort */, false /* recursive */)
	if err != nil {
		// generic error
		return &topo.WatchData{Err: convertError(err)}, nil
	}
	if initial.Node == nil {
		// node doesn't exist
		return &topo.WatchData{Err: topo.ErrNoNode}, nil
	}
	wd := newWatchData(valueType, initial.Node)
	if wd.Err != nil {
		return wd, nil
	}

	notifications := make(chan *topo.WatchData, 10)

	// This watch go routine will stop if the 'stop' channel is closed.
	// Otherwise it will try to watch everything in a loop, and send events
	// to the 'watch' channel.
	// In any case, the Watch call will close the 'watch' channel.
	watchChannel := make(chan *etcd.Response)
	stop := make(chan bool)
	watchError := make(chan error)
	go func() {
		versionToWatch := initial.Node.ModifiedIndex + 1
		if _, err := cell.Client.Watch(filePath, versionToWatch, false /* recursive */, watchChannel, stop); err != etcd.ErrWatchStoppedByUser {
			// We didn't stop this watch, it errored out.
			// In this case, watch was closed already, we just
			// have to save the error.
			// Note err can never be nil, as we only return when
			// the watch is interrupted or broken.
			watchError <- err
			close(watchError)
		}
	}()

	// This go routine is the main event handling routine:
	// - it will stop if ctx.Done() is closed.
	// - if it receives a notification from the watch, it will forward it
	// to the notifications channel.
	go func() {
		for {
			select {
			case resp, ok := <-watchChannel:
				if !ok {
					// Watch terminated, because of an error
					err := <-watchError
					notifications <- &topo.WatchData{Err: err}
					close(notifications)
					return
				}
				if resp.Node == nil {
					// Node doesn't exist any more, we can
					// stop watching.
					close(stop)
					notifications <- &topo.WatchData{Err: topo.ErrNoNode}
					close(notifications)
					return
				}

				wd := newWatchData(valueType, resp.Node)
				notifications <- wd
				if wd.Err != nil {
					// Error packing / unpacking data,
					// stop the watch.
					close(stop)
					close(notifications)
					return
				}

			case <-ctx.Done():
				close(stop)
				notifications <- &topo.WatchData{Err: ctx.Err()}
				close(notifications)
				return
			}
		}
	}()

	return wd, notifications
}
