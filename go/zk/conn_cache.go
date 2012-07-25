// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zk

import (
	"fmt"
	"log"
	"sync"
	"time"

	"launchpad.net/gozk/zookeeper"
)

/* When you need to talk to multiple zk cells, you need a simple
abstraction so you aren't caching clients all over the place.

ConnCache guarantees that you have at most one zookeeper connection per cell.
*/

type cachedConn struct {
	mutex  sync.Mutex // used to notify if multiple goroutine simultaneously want a connection
	zconn *zookeeper.Conn
}

type ConnCache struct {
	mutex           sync.Mutex
	zconnCellMap   map[string]*cachedConn // map cell name to connection
	connectTimeout time.Duration
}

func (cc *ConnCache) ConnForPath(zkPath string) (*zookeeper.Conn, error) {
	zcell := ZkCellFromZkPath(zkPath)

	cc.mutex.Lock()
	conn, ok := cc.zconnCellMap[zcell]
	if !ok {
		conn = &cachedConn{}
		cc.zconnCellMap[zcell] = conn
	}
	cc.mutex.Unlock()

	// We only want one goroutine at a time trying to connect here, so keep the
	// lock during the zk dial process.
	conn.mutex.Lock()
	defer conn.mutex.Unlock()

	if conn.zconn != nil {
		return conn.zconn, nil
	}

	zconn, session, err := zookeeper.Dial(ZkPathToZkAddr(zkPath), cc.connectTimeout)
	if err == nil {
		// Wait for connection.
		// FIXME(msolomon) the deadlines seems to be a bit fuzzy, need to double check
		// and potentially do a high-level select here.
		event := <-session
		if event.State != zookeeper.STATE_CONNECTED {
			err = fmt.Errorf("zk connect failed: %v", event.State)
		}
		if err == nil {
			conn.zconn = zconn
			go cc.handleSessionEvents(zcell, zconn, session)
		} else {
			zconn.Close()
		}
	}
	return conn.zconn, err
}

func (cc *ConnCache) handleSessionEvents(cell string, conn *zookeeper.Conn, session <-chan zookeeper.Event) {
	for event := range session {
		if !event.Ok() {
			conn.Close()
			cc.mutex.Lock()
			delete(cc.zconnCellMap, cell)
			cc.mutex.Unlock()
			log.Printf("zk conn cache: session for cell %v ended: %v", cell, event)
		}
	}
}

func (cc *ConnCache) Close() error {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	for _, conn := range cc.zconnCellMap {
		conn.mutex.Lock()
		if conn.zconn != nil {
			conn.zconn.Close()
			conn.zconn = nil
		}
		conn.mutex.Unlock()
	}
	cc.zconnCellMap = nil
	return nil
}

func NewConnCache(connectTimeout time.Duration) *ConnCache {
	return &ConnCache{
		zconnCellMap: make(map[string]*cachedConn),
		connectTimeout: connectTimeout}
}
