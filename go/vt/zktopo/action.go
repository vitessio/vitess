// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zktopo

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/topo"
	"launchpad.net/gozk/zookeeper"
)

/*
This file contains the remote tablet action code of zktopo.Server
*/

func (zkts *Server) WriteTabletAction(tabletAlias topo.TabletAlias, contents string) (string, error) {
	// Action paths end in a trailing slash to that when we create
	// sequential nodes, they are created as children, not siblings.
	actionPath := TabletActionPathForAlias(tabletAlias) + "/"
	return zkts.zconn.Create(actionPath, contents, zookeeper.SEQUENCE, zookeeper.WorldACL(zookeeper.PERM_ALL))
}

func (zkts *Server) WaitForTabletAction(actionPath string, waitTime time.Duration, interrupted chan struct{}) (string, error) {
	timer := time.NewTimer(waitTime)
	defer timer.Stop()

	// see if the file exists or sets a watch
	// the loop is to resist zk disconnects while we're waiting
	actionLogPath := strings.Replace(actionPath, "/action/", "/actionlog/", 1)
wait:
	for {
		var retryDelay <-chan time.Time
		stat, watch, err := zkts.zconn.ExistsW(actionLogPath)
		if err != nil {
			delay := 5*time.Second + time.Duration(rand.Int63n(55e9))
			log.Warningf("unexpected zk error, delay retry %v: %v", delay, err)
			// No one likes a thundering herd.
			retryDelay = time.After(delay)
		} else if stat != nil {
			// file exists, go on
			break wait
		}

		// if the file doesn't exist yet, wait for creation event.
		// On any other event we'll retry the ExistsW
		select {
		case actionEvent := <-watch:
			if actionEvent.Type == zookeeper.EVENT_CREATED {
				break wait
			} else {
				// Log unexpected events. Reconnects are
				// handled by zk.Conn, so calling ExistsW again
				// will handle a disconnect.
				log.Warningf("unexpected zk event: %v", actionEvent)
			}
		case <-retryDelay:
			continue wait
		case <-timer.C:
			return "", topo.ErrTimeout
		case <-interrupted:
			return "", topo.ErrInterrupted
		}
	}

	// the node exists, read it
	data, _, err := zkts.zconn.Get(actionLogPath)
	if err != nil {
		return "", fmt.Errorf("action err: %v %v", actionLogPath, err)
	}

	return data, nil
}

func (zkts *Server) PurgeTabletActions(tabletAlias topo.TabletAlias, canBePurged func(data string) bool) error {
	actionPath := TabletActionPathForAlias(tabletAlias)
	return zkts.PurgeActions(actionPath, canBePurged)
}
