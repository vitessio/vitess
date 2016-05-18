// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcdtopo

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/coreos/go-etcd/etcd"
	log "github.com/golang/glog"
	"golang.org/x/net/context"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// WatchSleepDuration is how many seconds interval to poll for in case
// we get an error from the Watch method. It is exported so individual
// test and main programs can change it.
var WatchSleepDuration = 30 * time.Second

// UpdateSrvShard implements topo.Server.
func (s *Server) UpdateSrvShard(ctx context.Context, cellName, keyspace, shard string, srvShard *topodatapb.SrvShard) error {
	cell, err := s.getCell(cellName)
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(srvShard, "", "  ")
	if err != nil {
		return err
	}

	_, err = cell.Set(srvShardFilePath(keyspace, shard), string(data), 0 /* ttl */)
	return convertError(err)
}

// GetSrvShard implements topo.Server.
func (s *Server) GetSrvShard(ctx context.Context, cellName, keyspace, shard string) (*topodatapb.SrvShard, error) {
	cell, err := s.getCell(cellName)
	if err != nil {
		return nil, err
	}

	resp, err := cell.Get(srvShardFilePath(keyspace, shard), false /* sort */, false /* recursive */)
	if err != nil {
		return nil, convertError(err)
	}
	if resp.Node == nil {
		return nil, ErrBadResponse
	}

	value := &topodatapb.SrvShard{}
	if err := json.Unmarshal([]byte(resp.Node.Value), value); err != nil {
		return nil, fmt.Errorf("bad serving shard data (%v): %q", err, resp.Node.Value)
	}
	return value, nil
}

// DeleteSrvShard implements topo.Server.
func (s *Server) DeleteSrvShard(ctx context.Context, cellName, keyspace, shard string) error {
	cell, err := s.getCell(cellName)
	if err != nil {
		return err
	}

	_, err = cell.Delete(srvShardDirPath(keyspace, shard), true /* recursive */)
	return convertError(err)
}

// UpdateSrvKeyspace implements topo.Server.
func (s *Server) UpdateSrvKeyspace(ctx context.Context, cellName, keyspace string, srvKeyspace *topodatapb.SrvKeyspace) error {
	cell, err := s.getCell(cellName)
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(srvKeyspace, "", "  ")
	if err != nil {
		return err
	}

	_, err = cell.Set(srvKeyspaceFilePath(keyspace), string(data), 0 /* ttl */)
	return convertError(err)
}

// DeleteSrvKeyspace implements topo.Server.
func (s *Server) DeleteSrvKeyspace(ctx context.Context, cellName, keyspace string) error {
	cell, err := s.getCell(cellName)
	if err != nil {
		return err
	}

	_, err = cell.Delete(srvKeyspaceDirPath(keyspace), true /* recursive */)
	return convertError(err)
}

// GetSrvKeyspace implements topo.Server.
func (s *Server) GetSrvKeyspace(ctx context.Context, cellName, keyspace string) (*topodatapb.SrvKeyspace, error) {
	cell, err := s.getCell(cellName)
	if err != nil {
		return nil, err
	}

	resp, err := cell.Get(srvKeyspaceFilePath(keyspace), false /* sort */, false /* recursive */)
	if err != nil {
		return nil, convertError(err)
	}
	if resp.Node == nil {
		return nil, ErrBadResponse
	}

	value := &topodatapb.SrvKeyspace{}
	if err := json.Unmarshal([]byte(resp.Node.Value), value); err != nil {
		return nil, fmt.Errorf("bad serving keyspace data (%v): %q", err, resp.Node.Value)
	}
	return value, nil
}

// GetSrvKeyspaceNames implements topo.Server.
func (s *Server) GetSrvKeyspaceNames(ctx context.Context, cellName string) ([]string, error) {
	cell, err := s.getCell(cellName)
	if err != nil {
		return nil, err
	}

	resp, err := cell.Get(servingDirPath, true /* sort */, false /* recursive */)
	if err != nil {
		return nil, convertError(err)
	}
	return getNodeNames(resp)
}

// WatchSrvKeyspace is part of the topo.Server interface
func (s *Server) WatchSrvKeyspace(ctx context.Context, cellName, keyspace string) (<-chan *topodatapb.SrvKeyspace, error) {
	cell, err := s.getCell(cellName)
	if err != nil {
		return nil, fmt.Errorf("WatchSrvKeyspace cannot get cell: %v", err)
	}
	filePath := srvKeyspaceFilePath(keyspace)

	notifications := make(chan *topodatapb.SrvKeyspace, 10)

	// The watch go routine will stop if the 'stop' channel is closed.
	// Otherwise it will try to watch everything in a loop, and send events
	// to the 'watch' channel.
	watch := make(chan *etcd.Response)
	stop := make(chan bool)
	go func() {
		var srvKeyspace *topodatapb.SrvKeyspace
		var modifiedVersion int64

		resp, err := cell.Get(filePath, false /* sort */, false /* recursive */)
		if err != nil || resp.Node == nil {
			// node doesn't exist
		} else {
			if resp.Node.Value != "" {
				srvKeyspace = &topodatapb.SrvKeyspace{}
				if err := json.Unmarshal([]byte(resp.Node.Value), srvKeyspace); err != nil {
					log.Warningf("bad SrvKeyspace data (%v): %q", err, resp.Node.Value)
				} else {
					modifiedVersion = int64(resp.Node.ModifiedIndex)
				}
			}
		}

		// re-check for stop here to be safe, in case the
		// Get took a long time
		select {
		case <-stop:
			return
		case notifications <- srvKeyspace:
		}

		for {
			if _, err := cell.Client.Watch(filePath, uint64(modifiedVersion+1), false /* recursive */, watch, stop); err != nil {
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
				var srvKeyspace *topodatapb.SrvKeyspace
				if resp.Node != nil && resp.Node.Value != "" {
					srvKeyspace = &topodatapb.SrvKeyspace{}
					if err := json.Unmarshal([]byte(resp.Node.Value), srvKeyspace); err != nil {
						log.Errorf("failed to Unmarshal SrvKeyspace for %v: %v", filePath, err)
						continue
					}
				}
				notifications <- srvKeyspace
			case <-ctx.Done():
				close(stop)
				close(notifications)
				return
			}
		}
	}()

	return notifications, nil
}
