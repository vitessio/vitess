// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zkocc

import (
	"fmt"
	"log"
	"sync"
	"time"

	"launchpad.net/gozk/zookeeper"
)

// a zkCell object represents a zookeeper cell, with a cache and a connection
// to the real cell.

// Our state. We need this to be independent as we want to decorelate the
// connection from what clients are asking for.
// For instance, if a cell is not used often, and gets disconnected,
// we want to reconnect in the background, independently of the clients.
// Also we want to support a BACKOFF mode for fast client failure
// reporting while protecting the server from high rates of connections.
const (
	// DISCONNECTED: initial state of the cell.
	// connect will only work in that state, and will go to CONNECTING
	CELL_DISCONNECTED = iota

	// CONNECTING: a 'connect' function started the connection process.
	// It will then go to CONNECTED or BACKOFF. Only one connect
	// function will run at a time.
	// requests will be blocked until the state changes (if it goes to
	// CONNECTED, request will then try to get the value, if it goes to
	// CELL_BACKOFF, they will fail)
	CELL_CONNECTING

	// steady state, when all is good and dandy.
	CELL_CONNECTED

	// BACKOFF: we're waiting for a bit before trying to reconnect.
	// a go routine will go to DISCONNECTED and start login soon.
	// we're failing all requests in this state.
	CELL_BACKOFF
)

type zkCell struct {
	// set at creation
	cellName       string
	zkAddr         string
	connectTimeout time.Duration
	zcache         *ZkCache

	// connection related variables
	mutex   sync.Mutex // For connection & state only
	zconn   *zookeeper.Conn
	state   int
	ready   *sync.Cond // will be signaled at connection time
	lastErr error      // last connection error
}

func newZkCell(name, zkaddr string, ctimeout time.Duration) *zkCell {
	result := &zkCell{cellName: name, zkAddr: zkaddr, connectTimeout: ctimeout, zcache: newZkCache()}
	result.ready = sync.NewCond(&result.mutex)
	go result.backgroundRefresher()
	return result
}

// background routine to initiate a connection sequence
// only connect if state == CELL_DISCONNECTED
// will change state to CELL_CONNECTING during the connection process
// will then change to CELL_CONNECTED (and braodcast the cond)
// or to CELL_BACKOFF (and schedule a new reconnection soon)
func (zcell *zkCell) connect() {
	// change our state, we're working on connecting
	zcell.mutex.Lock()
	if zcell.state != CELL_DISCONNECTED {
		// someone else is already connecting
		zcell.mutex.Unlock()
		return
	}
	zcell.state = CELL_CONNECTING
	zcell.mutex.Unlock()

	// now connect
	zconn, session, err := zookeeper.Dial(zcell.zkAddr, zcell.connectTimeout)
	if err == nil {
		// Wait for connection.
		// FIXME(msolomon) the deadlines seems to be a bit fuzzy, need to double check
		// and potentially do a high-level select here.
		event := <-session
		if event.State != zookeeper.STATE_CONNECTED {
			err = fmt.Errorf("zk connect failed: %v", event.State)
		}
		if err == nil {
			zcell.zconn = zconn
			go zcell.handleSessionEvents(zconn, session)
		} else {
			zconn.Close()
		}
	}

	// and change our state
	zcell.mutex.Lock()
	if zcell.state != CELL_CONNECTING {
		panic(fmt.Errorf("Unexpected state: %v", zcell.state))
	}
	if err == nil {
		zcell.state = CELL_CONNECTED
		zcell.lastErr = nil

		// FIXME(alainjobart): trigger a full reload of the
		// map, and sets the triggers.
	} else {
		zcell.state = CELL_BACKOFF
		zcell.lastErr = err

		go func() {
			// we're going to try to reconnect at some point
			// FIXME(alainjobart) backoff algorithm?
			<-time.NewTimer(3 * time.Second).C

			// switch back to DISCONNECTED, and trigger a connect
			zcell.mutex.Lock()
			zcell.state = CELL_DISCONNECTED
			zcell.mutex.Unlock()
			zcell.connect()
		}()
	}

	// we broadcast on the condition to get everybody unstuck,
	// whether we succeeded to connect or not
	zcell.ready.Broadcast()
	zcell.mutex.Unlock()
}

func (zcell *zkCell) handleSessionEvents(conn *zookeeper.Conn, session <-chan zookeeper.Event) {
	for event := range session {
		switch event.State {
		case zookeeper.STATE_EXPIRED_SESSION:
			conn.Close()
			fallthrough
		case zookeeper.STATE_CLOSED:
			zcell.mutex.Lock()
			zcell.state = CELL_DISCONNECTED
			zcell.zconn = nil
			zcell.zcache.markForRefresh()
			// for a closed connection, no backoff at first retry
			// if connect fails again, then we'll backoff
			go zcell.connect()
			zcell.mutex.Unlock()
			log.Printf("zk cell conn: session for cell %v ended: %v", zcell.cellName, event)
			return
		default:
			log.Printf("zk conn cache: session for cell %v event: %v", zcell.cellName, event)
		}
	}
}

func (zcell *zkCell) getConnection() (*zookeeper.Conn, error) {
	zcell.mutex.Lock()
	defer zcell.mutex.Unlock()

	switch zcell.state {
	case CELL_CONNECTED:
		// we are already connected, just return the connection
		return zcell.zconn, nil
	case CELL_DISCONNECTED:
		// trigger the connection sequence and wait for connection
		go zcell.connect()
		fallthrough
	case CELL_CONNECTING:
		for zcell.state != CELL_CONNECTED && zcell.state != CELL_BACKOFF {
			zcell.ready.Wait()
		}
		if zcell.state == CELL_CONNECTED {
			return zcell.zconn, nil
		}
	}

	// we are in BACKOFF or failed to connect
	return nil, zcell.lastErr
}

// runs in the background and refreshes the cache if we're in connected state
// FIXME(alainjobart) configure the refresh interval
func (zcell *zkCell) backgroundRefresher() {
	ticker := time.NewTicker(1 * time.Second)
	for _ = range ticker.C {
		// grab a valid connection
		zcell.mutex.Lock()
		// not connected, what can we do?
		if zcell.state != CELL_CONNECTED {
			zcell.mutex.Unlock()
			continue
		}
		zconn := zcell.zconn
		zcell.mutex.Unlock()

		// get a few values to refresh, and ask for them
		zcell.zcache.refreshSomeValues(zconn)
	}
}
