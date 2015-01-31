// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package etcdtopo

import (
	"encoding/json"
	"fmt"
	"path"
	"time"

	"github.com/coreos/go-etcd/etcd"
	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/jscfg"
	"github.com/youtube/vitess/go/vt/topo"
)

// WatchSleepDuration is how many seconds interval to poll for in case
// we get an error from the Watch method. It is exported so individual
// test and main programs can change it.
var WatchSleepDuration = 30 * time.Second

// GetSrvTabletTypesPerShard implements topo.Server.
func (s *Server) GetSrvTabletTypesPerShard(cellName, keyspace, shard string) ([]topo.TabletType, error) {
	cell, err := s.getCell(cellName)
	if err != nil {
		return nil, err
	}

	resp, err := cell.Get(srvShardDirPath(keyspace, shard), false /* sort */, false /* recursive */)
	if err != nil {
		return nil, convertError(err)
	}
	if resp.Node == nil {
		return nil, ErrBadResponse
	}

	tabletTypes := make([]topo.TabletType, 0, len(resp.Node.Nodes))
	for _, n := range resp.Node.Nodes {
		tabletTypes = append(tabletTypes, topo.TabletType(path.Base(n.Key)))
	}
	return tabletTypes, nil
}

// UpdateEndPoints implements topo.Server.
func (s *Server) UpdateEndPoints(cellName, keyspace, shard string, tabletType topo.TabletType, addrs *topo.EndPoints) error {
	cell, err := s.getCell(cellName)
	if err != nil {
		return err
	}

	data := jscfg.ToJson(addrs)

	_, err = cell.Set(endPointsFilePath(keyspace, shard, string(tabletType)), data, 0 /* ttl */)
	return convertError(err)
}

// updateEndPoints updates the EndPoints file only if the version matches.
func (s *Server) updateEndPoints(cellName, keyspace, shard string, tabletType topo.TabletType, addrs *topo.EndPoints, version int64) error {
	cell, err := s.getCell(cellName)
	if err != nil {
		return err
	}

	data := jscfg.ToJson(addrs)

	_, err = cell.CompareAndSwap(endPointsFilePath(keyspace, shard, string(tabletType)), data, 0, /* ttl */
		"" /* prevValue */, uint64(version))
	return convertError(err)
}

// GetEndPoints implements topo.Server.
func (s *Server) GetEndPoints(cell, keyspace, shard string, tabletType topo.TabletType) (*topo.EndPoints, error) {
	value, _, err := s.getEndPoints(cell, keyspace, shard, tabletType)
	return value, err
}

func (s *Server) getEndPoints(cellName, keyspace, shard string, tabletType topo.TabletType) (*topo.EndPoints, int64, error) {
	cell, err := s.getCell(cellName)
	if err != nil {
		return nil, -1, err
	}

	resp, err := cell.Get(endPointsFilePath(keyspace, shard, string(tabletType)), false /* sort */, false /* recursive */)
	if err != nil {
		return nil, -1, convertError(err)
	}
	if resp.Node == nil {
		return nil, -1, ErrBadResponse
	}

	value := &topo.EndPoints{}
	if resp.Node.Value != "" {
		if err := json.Unmarshal([]byte(resp.Node.Value), value); err != nil {
			return nil, -1, fmt.Errorf("bad end points data (%v): %q", err, resp.Node.Value)
		}
	}
	return value, int64(resp.Node.ModifiedIndex), nil
}

// DeleteEndPoints implements topo.Server.
func (s *Server) DeleteEndPoints(cellName, keyspace, shard string, tabletType topo.TabletType) error {
	cell, err := s.getCell(cellName)
	if err != nil {
		return err
	}

	_, err = cell.Delete(endPointsDirPath(keyspace, shard, string(tabletType)), true /* recursive */)
	return convertError(err)
}

// UpdateSrvShard implements topo.Server.
func (s *Server) UpdateSrvShard(cellName, keyspace, shard string, srvShard *topo.SrvShard) error {
	cell, err := s.getCell(cellName)
	if err != nil {
		return err
	}

	data := jscfg.ToJson(srvShard)

	_, err = cell.Set(srvShardFilePath(keyspace, shard), data, 0 /* ttl */)
	return convertError(err)
}

// GetSrvShard implements topo.Server.
func (s *Server) GetSrvShard(cellName, keyspace, shard string) (*topo.SrvShard, error) {
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

	value := topo.NewSrvShard(int64(resp.Node.ModifiedIndex))
	if err := json.Unmarshal([]byte(resp.Node.Value), value); err != nil {
		return nil, fmt.Errorf("bad serving shard data (%v): %q", err, resp.Node.Value)
	}
	return value, nil
}

// DeleteSrvShard implements topo.Server.
func (s *Server) DeleteSrvShard(cellName, keyspace, shard string) error {
	cell, err := s.getCell(cellName)
	if err != nil {
		return err
	}

	_, err = cell.Delete(srvShardDirPath(keyspace, shard), true /* recursive */)
	return convertError(err)
}

// UpdateSrvKeyspace implements topo.Server.
func (s *Server) UpdateSrvKeyspace(cellName, keyspace string, srvKeyspace *topo.SrvKeyspace) error {
	cell, err := s.getCell(cellName)
	if err != nil {
		return err
	}

	data := jscfg.ToJson(srvKeyspace)

	_, err = cell.Set(srvKeyspaceFilePath(keyspace), data, 0 /* ttl */)
	return convertError(err)
}

// GetSrvKeyspace implements topo.Server.
func (s *Server) GetSrvKeyspace(cellName, keyspace string) (*topo.SrvKeyspace, error) {
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

	value := topo.NewSrvKeyspace(int64(resp.Node.ModifiedIndex))
	if err := json.Unmarshal([]byte(resp.Node.Value), value); err != nil {
		return nil, fmt.Errorf("bad serving keyspace data (%v): %q", err, resp.Node.Value)
	}
	return value, nil
}

// GetSrvKeyspaceNames implements topo.Server.
func (s *Server) GetSrvKeyspaceNames(cellName string) ([]string, error) {
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

// UpdateTabletEndpoint implements topo.Server.
func (s *Server) UpdateTabletEndpoint(cell, keyspace, shard string, tabletType topo.TabletType, addr *topo.EndPoint) error {
	for {
		addrs, version, err := s.getEndPoints(cell, keyspace, shard, tabletType)
		if err == topo.ErrNoNode {
			// It's ok if the EndPoints file doesn't exist yet. See topo.Server.
			return nil
		}
		if err != nil {
			return err
		}

		// Update or add the record for the specified tablet.
		found := false
		for i, ep := range addrs.Entries {
			if ep.Uid == addr.Uid {
				found = true
				addrs.Entries[i] = *addr
				break
			}
		}
		if !found {
			addrs.Entries = append(addrs.Entries, *addr)
		}

		// Update the record
		err = s.updateEndPoints(cell, keyspace, shard, tabletType, addrs, version)
		if err != topo.ErrBadVersion {
			return err
		}
	}
}

// WatchEndPoints is part of the topo.Server interface
func (s *Server) WatchEndPoints(cellName, keyspace, shard string, tabletType topo.TabletType) (<-chan *topo.EndPoints, chan<- struct{}, error) {
	cell, err := s.getCell(cellName)
	if err != nil {
		return nil, nil, fmt.Errorf("WatchEndPoints cannot get cell: %v", err)
	}
	filePath := endPointsFilePath(keyspace, shard, string(tabletType))

	notifications := make(chan *topo.EndPoints, 10)
	stopWatching := make(chan struct{})

	// The watch go routine will stop if the 'stop' channel is closed.
	// Otherwise it will try to watch everything in a loop, and send events
	// to the 'watch' channel.
	watch := make(chan *etcd.Response)
	stop := make(chan bool)
	go func() {
		// get the current version of the file
		ep, modifiedVersion, err := s.getEndPoints(cellName, keyspace, shard, tabletType)
		if err != nil {
			// node doesn't exist
			modifiedVersion = 0
			ep = nil
		}

		// re-check for stop here to be safe, in case the
		// getEndPoints took a long time
		select {
		case <-stop:
			return
		case notifications <- ep:
		}

		for {
			if _, err := cell.Client.Watch(filePath, uint64(modifiedVersion), false /* recursive */, watch, stop); err != nil {
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
	// - it will stop if stopWatching is closed.
	// - if it receives a notification from the watch, it will forward it
	// to the notifications channel.
	go func() {
		for {
			select {
			case resp := <-watch:
				var ep *topo.EndPoints
				if resp.Node != nil && resp.Node.Value != "" {
					ep = &topo.EndPoints{}
					if err := json.Unmarshal([]byte(resp.Node.Value), ep); err != nil {
						log.Errorf("failed to Unmarshal EndPoints for %v: %v", filePath, err)
						continue
					}
				}
				notifications <- ep
			case <-stopWatching:
				close(stop)
				close(notifications)
				return
			}
		}
	}()

	return notifications, stopWatching, nil
}
