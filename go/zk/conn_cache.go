// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zk

import (
	"math/rand"
	"sync"
	"time"

	log "github.com/golang/glog"
	zookeeper "github.com/samuel/go-zookeeper/zk"

	"github.com/youtube/vitess/go/stats"
)

var (
	cachedConnStates      = stats.NewStringMap("ZkCachedConn")
	cachedConnStatesMutex sync.Mutex
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// state is the state of the Zookeeper connection. It's not tracked in the
// connection struct itself, only in the stat variable cachedConnStates.
type state string

const (
	disconnected state = "Disconnected"
	connecting         = "Connecting"
	connected          = "Connected"
)

type cachedConn struct {
	mutex sync.Mutex
	// guarded by mutex
	zconn Conn
}

// ConnCache is a cache for Zookeeper connections which guarantees that you have
// at most one zookeeper connection per cell.
type ConnCache struct {
	mutex        sync.Mutex
	zconnCellMap map[string]*cachedConn // map cell name to connection
}

func (cc *ConnCache) setState(zcell string, state state) {
	cachedConnStatesMutex.Lock()
	defer cachedConnStatesMutex.Unlock()
	cachedConnStates.Set(zcell, string(state))
}

// ConnForPath returns a connection for a given Zookeeper path. If no connection
// is cached, it creates a new one.
func (cc *ConnCache) ConnForPath(zkPath string) (cn Conn, err error) {
	zcell, err := ZkCellFromZkPath(zkPath)
	if err != nil {
		return nil, zookeeper.ErrInvalidPath
	}

	cc.mutex.Lock()
	if cc.zconnCellMap == nil {
		cc.mutex.Unlock()
		return nil, zookeeper.ErrClosing
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

	zkAddr, err := ZkPathToZkAddr(zkPath)
	if err != nil {
		return nil, zookeeper.ErrNoNode
	}

	cc.setState(zcell, connecting)
	conn.zconn, err = cc.newZookeeperConn(zkAddr, zcell)
	if conn.zconn != nil {
		cc.setState(zcell, connected)
	} else {
		cc.setState(zcell, disconnected)
	}
	return conn.zconn, err
}

func (cc *ConnCache) newZookeeperConn(zkAddr, zcell string) (Conn, error) {
	conn, session, err := DialZkTimeout(zkAddr, *baseTimeout, *connectTimeout)
	if err != nil {
		return nil, err
	}
	go cc.handleSessionEvents(zcell, conn, session)
	return conn, nil
}

func (cc *ConnCache) handleSessionEvents(cell string, conn Conn, session <-chan zookeeper.Event) {
	closeRequired := false
	for event := range session {
		switch event.State {
		case zookeeper.StateExpired, zookeeper.StateConnecting:
			closeRequired = true
			fallthrough
		case zookeeper.StateDisconnected:
			var cached *cachedConn
			cc.mutex.Lock()
			if cc.zconnCellMap != nil {
				cached = cc.zconnCellMap[cell]
			}
			cc.mutex.Unlock()

			// keek the entry in the map, but nil the Conn
			// (that will trigger a re-dial next time
			// we ask for a variable)
			if cached != nil {
				cached.mutex.Lock()
				if closeRequired {
					cached.zconn.Close()
				}
				cached.zconn = nil
				cc.setState(cell, disconnected)
				cached.mutex.Unlock()
			}

			log.Infof("zk conn cache: session for cell %v ended: %v", cell, event)
			return
		default:
			log.Infof("zk conn cache: session for cell %v event: %v", cell, event)
		}
	}
}

// Close closes all cached connections.
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

// NewConnCache creates a new Zookeeper connection cache.
func NewConnCache() *ConnCache {
	return &ConnCache{
		zconnCellMap: make(map[string]*cachedConn),
	}
}
