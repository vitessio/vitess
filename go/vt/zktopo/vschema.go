// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"path"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"
	"launchpad.net/gozk/zookeeper"

	"github.com/youtube/vitess/go/zk"
)

/*
This file contains the vschema management code for zktopo.Server
*/

const (
	vschemaPath = "vschema"
)

// SaveVSchema saves the JSON vschema into the topo.
func (zkts *Server) SaveVSchema(ctx context.Context, keyspace, vschema string) error {
	vschemaPath := path.Join(GlobalKeyspacesPath, keyspace, vschemaPath)
	_, err := zk.CreateOrUpdate(zkts.zconn, vschemaPath, vschema, 0, zookeeper.WorldACL(zookeeper.PERM_ALL), true)
	return err
}

// GetVSchema fetches the JSON vschema from the topo.
func (zkts *Server) GetVSchema(ctx context.Context, keyspace string) (string, error) {
	vschemaPath := path.Join(GlobalKeyspacesPath, keyspace, vschemaPath)
	data, _, err := zkts.zconn.Get(vschemaPath)
	if err != nil {
		if zookeeper.IsError(err, zookeeper.ZNONODE) {
			return "{}", nil
		}
		return "", err
	}
	return data, nil
}

// WatchVSchema is part of the topo.Server interface
func (zkts *Server) WatchVSchema(ctx context.Context, keyspace string) (<-chan string, error) {
	vschemaPath := path.Join(GlobalKeyspacesPath, keyspace, vschemaPath)

	notifications := make(chan string, 10)

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
					notifications <- "{}"
				}

				log.Errorf("Cannot set watch on %v, waiting for %v to retry: %v", vschemaPath, WatchSleepDuration, err)
				if waitOrInterrupted() {
					return
				}
				continue
			}

			// get the initial value, send it, or send {} if no
			// data
			value := "{}"
			if len(data) > 0 {
				value = data
			}
			notifications <- value

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
