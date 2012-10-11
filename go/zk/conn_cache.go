// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zk

import (
	"log"
	"math/rand"
	"sync"
	"time"

	"launchpad.net/gozk/zookeeper"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

/* When you need to talk to multiple zk cells, you need a simple
abstraction so you aren't caching clients all over the place.

ConnCache guarantees that you have at most one zookeeper connection per cell.
*/

type cachedConn struct {
	mutex sync.Mutex // used to notify if multiple goroutine simultaneously want a connection
	zconn Conn
}

type ConnCache struct {
	mutex          sync.Mutex
	zconnCellMap   map[string]*cachedConn // map cell name to connection
	connectTimeout time.Duration
	useZkocc       bool
}

func (cc *ConnCache) ConnForPath(zkPath string) (cn Conn, err error) {
	zcell := ZkCellFromZkPath(zkPath)

	cc.mutex.Lock()
	if cc.zconnCellMap == nil {
		cc.mutex.Unlock()
		return nil, &zookeeper.Error{Op: "dial", Code: zookeeper.ZCLOSING}
	}

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

	if cc.useZkocc {
		conn.zconn, err = DialZkocc(ZkPathToZkAddr(zkPath, true))
	} else {
		conn.zconn, err = cc.newZookeeperConn(zkPath, zcell)
	}
	return conn.zconn, err
}

func (cc *ConnCache) newZookeeperConn(zkPath, zcell string) (Conn, error) {
	conn, session, err := DialZk(ZkPathToZkAddr(zkPath, false), cc.connectTimeout)
	if err != nil {
		return nil, err
	}
	go cc.handleSessionEvents(zcell, conn, session)
	return conn, nil
}

func (cc *ConnCache) handleSessionEvents(cell string, conn Conn, session <-chan zookeeper.Event) {
	for event := range session {
		switch event.State {
		case zookeeper.STATE_EXPIRED_SESSION:
			conn.Close()
			fallthrough
		case zookeeper.STATE_CLOSED:
			cc.mutex.Lock()
			if cc.zconnCellMap != nil {
				delete(cc.zconnCellMap, cell)
			}
			cc.mutex.Unlock()
			log.Printf("zk conn cache: session for cell %v ended: %v", cell, event)
			return
		default:
			log.Printf("zk conn cache: session for cell %v event: %v", cell, event)
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

func NewConnCache(connectTimeout time.Duration, useZkocc bool) *ConnCache {
	return &ConnCache{
		zconnCellMap:   make(map[string]*cachedConn),
		connectTimeout: connectTimeout,
		useZkocc:       useZkocc}
}
