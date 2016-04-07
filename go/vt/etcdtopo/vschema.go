package etcdtopo

import (
	"time"

	"github.com/coreos/go-etcd/etcd"
	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
)

/*
This file contains the vschema management code for etcdtopo.Server
*/

// SaveVSchema saves the JSON vschema into the topo.
func (s *Server) SaveVSchema(ctx context.Context, keyspace, vschema string) error {
	_, err := s.getGlobal().Set(vschemaFilePath(keyspace), vschema, 0 /* ttl */)
	if err != nil {
		return convertError(err)
	}
	return nil
}

// GetVSchema fetches the JSON vschema from the topo.
func (s *Server) GetVSchema(ctx context.Context, keyspace string) (string, error) {
	resp, err := s.getGlobal().Get(vschemaFilePath(keyspace), false /* sort */, false /* recursive */)
	if err != nil {
		err = convertError(err)
		if err == topo.ErrNoNode {
			return "{}", nil
		}
		return "", err
	}
	if resp.Node == nil {
		return "", ErrBadResponse
	}
	return resp.Node.Value, nil
}

// WatchVSchema is part of the topo.Server interface
func (s *Server) WatchVSchema(ctx context.Context, keyspace string) (<-chan string, error) {
	filePath := vschemaFilePath(keyspace)

	notifications := make(chan string, 10)

	// The watch go routine will stop if the 'stop' channel is closed.
	// Otherwise it will try to watch everything in a loop, and send events
	// to the 'watch' channel.
	watch := make(chan *etcd.Response)
	stop := make(chan bool)
	go func() {
		vschema := "{}"
		var modifiedVersion int64

		resp, err := s.getGlobal().Get(filePath, false /* sort */, false /* recursive */)
		if err != nil || resp.Node == nil {
			// node doesn't exist
		} else {
			if resp.Node.Value != "" {
				vschema = resp.Node.Value
				modifiedVersion = int64(resp.Node.ModifiedIndex)
			}
		}

		// re-check for stop here to be safe, in case the
		// Get took a long time
		select {
		case <-stop:
			return
		case notifications <- vschema:
		}

		for {
			if _, err := s.getGlobal().Watch(filePath, uint64(modifiedVersion+1), false /* recursive */, watch, stop); err != nil {
				log.Errorf("Watch on %v failed, waiting for %v to retry: %v", filePath, WatchSleepDuration, err)
				timer := time.After(WatchSleepDuration)
				select {
				case <-stop:
					return
				case <-timer:
				}
			}
		}
	}()

	// This go routine is the main event handling routine:
	// - it will stop if ctx.Done() is closed.
	// - if it receives a notification from the watch, it will forward it
	// to the notifications channel.
	go func() {
		for {
			select {
			case resp := <-watch:
				vschema := "{}"
				if resp.Node != nil && resp.Node.Value != "" {
					vschema = resp.Node.Value
				}
				notifications <- vschema
			case <-ctx.Done():
				close(stop)
				close(notifications)
				return
			}
		}
	}()

	return notifications, nil
}
