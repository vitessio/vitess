// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"encoding/json"
	"fmt"
	"path"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"
	"launchpad.net/gozk/zookeeper"

	vschemapb "github.com/youtube/vitess/go/vt/proto/vschema"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/zk"
)

/*
This file contains the vschema management code for zktopo.Server
*/

const (
	vschemaPath = "vschema"
)

// SaveVSchema saves the vschema into the topo.
func (zkts *Server) SaveVSchema(ctx context.Context, keyspace string, vschema *vschemapb.Keyspace) error {
	data, err := json.MarshalIndent(vschema, "", "  ")
	if err != nil {
		return err
	}
	vschemaPath := path.Join(GlobalKeyspacesPath, keyspace, vschemaPath)
	_, err = zk.CreateOrUpdate(zkts.zconn, vschemaPath, string(data), 0, zookeeper.WorldACL(zookeeper.PERM_ALL), true)
	return err
}

// GetVSchema fetches the JSON vschema from the topo.
func (zkts *Server) GetVSchema(ctx context.Context, keyspace string) (*vschemapb.Keyspace, error) {
	vschemaPath := path.Join(GlobalKeyspacesPath, keyspace, vschemaPath)
	data, _, err := zkts.zconn.Get(vschemaPath)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			return nil, topo.ErrNoNode
		}
		return nil, err
	}
	var vs vschemapb.Keyspace
	err = json.Unmarshal([]byte(data), &vs)
	if err != nil {
		return nil, fmt.Errorf("bad vschema data (%v): %q", err, data)
	}
	return &vs, nil
}

// WatchVSchema is part of the topo.Server interface
func (zkts *Server) WatchVSchema(ctx context.Context, keyspace string) (<-chan *vschemapb.Keyspace, error) {
	vschemaPath := path.Join(GlobalKeyspacesPath, keyspace, vschemaPath)

	notifications := make(chan *vschemapb.Keyspace, 10)

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
			data, _, watch, err := zkts.zconn.GetW(vschemaPath)
			if err != nil {
				if zookeeper.IsError(err, zookeeper.ZNONODE) {
					// the parent directory doesn't exist
					notifications <- nil
				}

				log.Errorf("Cannot set watch on %v, waiting for %v to retry: %v", vschemaPath, WatchSleepDuration, err)
				if waitOrInterrupted() {
					return
				}
				continue
			}

			// get the initial value, send it, or send {} if no
			// data
			var vs vschemapb.Keyspace
			if len(data) > 0 {
				if err := json.Unmarshal([]byte(data), &vs); err != nil {
					log.Warningf("error unmarhsalling vschema for %v: %v", vschemaPath, err)
					notifications <- nil
				} else {
					notifications <- &vs
				}
			} else {
				notifications <- nil
			}

			// now act on the watch
			select {
			case event, ok := <-watch:
				if !ok {
					log.Warningf("watch on %v was closed, waiting for %v to retry", vschemaPath, WatchSleepDuration)
					if waitOrInterrupted() {
						return
					}
					continue
				}

				if !event.Ok() {
					log.Warningf("received a non-OK event for %v, waiting for %v to retry", vschemaPath, WatchSleepDuration)
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
