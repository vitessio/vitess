// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"time"

	log "github.com/golang/glog"
	zookeeper "github.com/samuel/go-zookeeper/zk"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/zk"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
)

// WatchSleepDuration is how many seconds interval to poll for in case
// the directory that contains a file to watch doesn't exist, or a watch
// is broken. It is exported so individual test and main programs
// can change it.
var WatchSleepDuration = 30 * time.Second

/*
This file contains the serving graph management code of zktopo.Server
*/
func zkPathForCell(cell string) string {
	return fmt.Sprintf("/zk/%v/vt", cell)
}

func zkPathForSrvKeyspaces(cell string) string {
	return path.Join(zkPathForCell(cell), "ns")
}

func zkPathForSrvKeyspace(cell, keyspace string) string {
	return path.Join(zkPathForSrvKeyspaces(cell), keyspace)
}

func zkPathForSrvVSchema(cell string) string {
	return path.Join(zkPathForCell(cell), "vschema")
}

// GetSrvKeyspaceNames is part of the topo.Server interface
func (zkts *Server) GetSrvKeyspaceNames(ctx context.Context, cell string) ([]string, error) {
	children, _, err := zkts.zconn.Children(zkPathForSrvKeyspaces(cell))
	if err != nil {
		err = convertError(err)
		if err == topo.ErrNoNode {
			return nil, nil
		}
		return nil, err
	}

	sort.Strings(children)
	return children, nil
}

// WatchSrvKeyspace is part of the topo.Server interface
func (zkts *Server) WatchSrvKeyspace(ctx context.Context, cell, keyspace string) (<-chan *topodatapb.SrvKeyspace, error) {
	filePath := zkPathForSrvKeyspace(cell, keyspace)

	notifications := make(chan *topodatapb.SrvKeyspace, 10)

	// waitOrInterrupted will return true if context.Done() is triggered
	waitOrInterrupted := func() bool {
		timer := time.After(WatchSleepDuration)
		select {
		case <-ctx.Done():
			close(notifications)
			return true
		case <-timer:
		}
		return false
	}

	go func() {
		for {
			// set the watch
			data, _, watch, err := zkts.zconn.GetW(filePath)
			if err != nil {
				if err == zookeeper.ErrNoNode {
					// the parent directory doesn't exist
					notifications <- nil
				}

				log.Errorf("Cannot set watch on %v, waiting for %v to retry: %v", filePath, WatchSleepDuration, err)
				if waitOrInterrupted() {
					return
				}
				continue
			}

			// get the initial value, send it, or send nil if no
			// data
			var srvKeyspace *topodatapb.SrvKeyspace
			sendIt := true
			if len(data) > 0 {
				srvKeyspace = &topodatapb.SrvKeyspace{}
				if err := json.Unmarshal([]byte(data), srvKeyspace); err != nil {
					log.Errorf("SrvKeyspace unmarshal failed: %v %v", data, err)
					sendIt = false
				}
			}
			if sendIt {
				notifications <- srvKeyspace
			}

			// now act on the watch
			select {
			case event, ok := <-watch:
				if !ok {
					log.Warningf("watch on %v was closed, waiting for %v to retry", filePath, WatchSleepDuration)
					if waitOrInterrupted() {
						return
					}
					continue
				}

				if event.Err != nil {
					log.Warningf("received a non-OK event for %v, waiting for %v to retry: %v", filePath, WatchSleepDuration, event.Err)
					if waitOrInterrupted() {
						return
					}
				}
			case <-ctx.Done():
				// user is not interested any more
				close(notifications)
				return
			}
		}
	}()

	return notifications, nil
}

// UpdateSrvKeyspace is part of the topo.Server interface
func (zkts *Server) UpdateSrvKeyspace(ctx context.Context, cell, keyspace string, srvKeyspace *topodatapb.SrvKeyspace) error {
	path := zkPathForSrvKeyspace(cell, keyspace)
	data, err := json.MarshalIndent(srvKeyspace, "", "  ")
	if err != nil {
		return err
	}
	_, err = zkts.zconn.Set(path, string(data), -1)
	if err == zookeeper.ErrNoNode {
		_, err = zk.CreateRecursive(zkts.zconn, path, string(data), 0, zookeeper.WorldACL(zookeeper.PermAll))
	}
	return convertError(err)
}

// DeleteSrvKeyspace is part of the topo.Server interface
func (zkts *Server) DeleteSrvKeyspace(ctx context.Context, cell, keyspace string) error {
	path := zkPathForSrvKeyspace(cell, keyspace)
	err := zkts.zconn.Delete(path, -1)
	if err != nil {
		return convertError(err)
	}
	return nil
}

// GetSrvKeyspace is part of the topo.Server interface
func (zkts *Server) GetSrvKeyspace(ctx context.Context, cell, keyspace string) (*topodatapb.SrvKeyspace, error) {
	path := zkPathForSrvKeyspace(cell, keyspace)
	data, _, err := zkts.zconn.Get(path)
	if err != nil {
		return nil, convertError(err)
	}
	if len(data) == 0 {
		return nil, topo.ErrNoNode
	}
	srvKeyspace := &topodatapb.SrvKeyspace{}
	if err := json.Unmarshal([]byte(data), srvKeyspace); err != nil {
		return nil, fmt.Errorf("SrvKeyspace unmarshal failed: %v %v", data, err)
	}
	return srvKeyspace, nil
}

// WatchSrvVSchema is part of the topo.Server interface
func (zkts *Server) WatchSrvVSchema(ctx context.Context, cell string) (<-chan *vschemapb.SrvVSchema, error) {
	filePath := zkPathForSrvVSchema(cell)

	notifications := make(chan *vschemapb.SrvVSchema, 10)

	// waitOrInterrupted will return true if context.Done() is triggered
	waitOrInterrupted := func() bool {
		timer := time.After(WatchSleepDuration)
		select {
		case <-ctx.Done():
			close(notifications)
			return true
		case <-timer:
		}
		return false
	}

	go func() {
		for {
			// set the watch
			data, _, watch, err := zkts.zconn.GetW(filePath)
			if err != nil {
				if err == zookeeper.ErrNoNode {
					// the parent directory doesn't exist
					notifications <- nil
				}

				log.Errorf("Cannot set watch on %v, waiting for %v to retry: %v", filePath, WatchSleepDuration, err)
				if waitOrInterrupted() {
					return
				}
				continue
			}

			// get the initial value, send it, or send nil if no
			// data
			var srvVSchema *vschemapb.SrvVSchema
			sendIt := true
			if len(data) > 0 {
				srvVSchema = &vschemapb.SrvVSchema{}
				if err := json.Unmarshal([]byte(data), srvVSchema); err != nil {
					log.Errorf("SrvVSchema unmarshal failed: %v %v", data, err)
					sendIt = false
				}
			}
			if sendIt {
				notifications <- srvVSchema
			}

			// now act on the watch
			select {
			case event, ok := <-watch:
				if !ok {
					log.Warningf("watch on %v was closed, waiting for %v to retry", filePath, WatchSleepDuration)
					if waitOrInterrupted() {
						return
					}
					continue
				}

				if event.Err != nil {
					log.Warningf("received a non-OK event for %v, waiting for %v to retry: %v", filePath, WatchSleepDuration, event.Err)
					if waitOrInterrupted() {
						return
					}
				}
			case <-ctx.Done():
				// user is not interested any more
				close(notifications)
				return
			}
		}
	}()

	return notifications, nil
}

// UpdateSrvVSchema is part of the topo.Server interface
func (zkts *Server) UpdateSrvVSchema(ctx context.Context, cell string, srvVSchema *vschemapb.SrvVSchema) error {
	path := zkPathForSrvVSchema(cell)
	data, err := json.MarshalIndent(srvVSchema, "", "  ")
	if err != nil {
		return err
	}
	_, err = zkts.zconn.Set(path, string(data), -1)
	if err == zookeeper.ErrNoNode {
		_, err = zk.CreateRecursive(zkts.zconn, path, string(data), 0, zookeeper.WorldACL(zookeeper.PermAll))
	}
	return convertError(err)
}

// GetSrvVSchema is part of the topo.Server interface
func (zkts *Server) GetSrvVSchema(ctx context.Context, cell string) (*vschemapb.SrvVSchema, error) {
	path := zkPathForSrvVSchema(cell)
	data, _, err := zkts.zconn.Get(path)
	if err != nil {
		return nil, convertError(err)
	}
	if len(data) == 0 {
		return nil, topo.ErrNoNode
	}
	srvVSchema := &vschemapb.SrvVSchema{}
	if err := json.Unmarshal([]byte(data), srvVSchema); err != nil {
		return nil, fmt.Errorf("SrvVSchema unmarshal failed: %v %v", data, err)
	}
	return srvVSchema, nil
}
