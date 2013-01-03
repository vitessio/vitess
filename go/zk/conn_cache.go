// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zk

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"code.google.com/p/vitess/go/stats"
	"launchpad.net/gozk/zookeeper"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

/* When you need to talk to multiple zk cells, you need a simple
abstraction so you aren't caching clients all over the place.

ConnCache guarantees that you have at most one zookeeper connection per cell.
*/

const (
	DISCONNECTED = 0
	CONNECTING   = 1
	CONNECTED    = 2
)

type cachedConn struct {
	mutex  sync.Mutex // used to notify if multiple goroutine simultaneously want a connection
	zconn  Conn
	states *stats.States
}

type ConnCache struct {
	mutex        sync.Mutex
	zconnCellMap map[string]*cachedConn // map cell name to connection
	useZkocc     bool
}

func (cc *ConnCache) ConnForPath(zkPath string) (cn Conn, err error) {
	zcell, err := ZkCellFromZkPath(zkPath)
	if err != nil {
		return nil, &zookeeper.Error{Op: "dial", Code: zookeeper.ZBADARGUMENTS}
	}

	cc.mutex.Lock()
	if cc.zconnCellMap == nil {
		cc.mutex.Unlock()
		return nil, &zookeeper.Error{Op: "dial", Code: zookeeper.ZCLOSING}
	}

	conn, ok := cc.zconnCellMap[zcell]
	if !ok {
		conn = &cachedConn{}
		conn.states = stats.NewStates("", []string{"Disconnected", "Connecting", "Connected"}, time.Now(), DISCONNECTED)
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

	zkAddr, err := ZkPathToZkAddr(zkPath, cc.useZkocc)
	if err != nil {
		return nil, &zookeeper.Error{Op: "dial", Code: zookeeper.ZBADARGUMENTS}
	}

	conn.states.SetState(CONNECTING)
	if cc.useZkocc {
		conn.zconn, err = DialZkocc(zkAddr, *baseTimeout)
	} else {
		conn.zconn, err = cc.newZookeeperConn(zkAddr, zcell)
	}
	if conn.zconn != nil {
		conn.states.SetState(CONNECTED)
	} else {
		conn.states.SetState(DISCONNECTED)
	}
	return conn.zconn, err
}

func (cc *ConnCache) newZookeeperConn(zkAddr, zcell string) (Conn, error) {
	conn, session, err := DialZk(zkAddr, *baseTimeout)
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
				cached.zconn = nil
				cached.states.SetState(DISCONNECTED)
			}

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

// Implements expvar.Var()
func (cc *ConnCache) String() string {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()

	b := bytes.NewBuffer(make([]byte, 0, 4096))
	fmt.Fprintf(b, "{")

	firstCell := true
	for cell, conn := range cc.zconnCellMap {
		if firstCell {
			firstCell = false
		} else {
			fmt.Fprintf(b, ", ")
		}
		fmt.Fprintf(b, "\"%v\": %v", cell, conn.states.String())
	}

	fmt.Fprintf(b, "}")
	return b.String()
}

func NewConnCache(useZkocc bool) *ConnCache {
	return &ConnCache{
		zconnCellMap: make(map[string]*cachedConn),
		useZkocc:     useZkocc}
}
