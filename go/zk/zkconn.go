// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zk

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/henryanand/vitess/go/netutil"
	"github.com/henryanand/vitess/go/sync2"
	"launchpad.net/gozk/zookeeper"
)

// Every blocking call into CGO causes another thread which blows up
// the virtual memory.  It seems better to solve this here at the
// root of the problem rather than forcing all other apps to take into
// account limiting the number of concurrent operations on a zk
// connection.  Since this applies to any zookeeper connection, this
// is global.
var sem *sync2.Semaphore

// ErrConnectionClosed is returned if we try to access a closed connection.
var ErrConnectionClosed = errors.New("ZkConn: connection is closed")

func init() {
	// The zookeeper C module logs quite a bit of useful information,
	// but much of it does not come back in the error API. To aid
	// debugging, enable the log to stderr for warnings.
	//zookeeper.SetLogLevel(zookeeper.LOG_WARN)

	maxConcurrency := 64
	x := os.Getenv("ZK_CLIENT_MAX_CONCURRENCY")
	if x != "" {
		var err error
		maxConcurrency, err = strconv.Atoi(x)
		if err != nil {
			log.Infof("invalid ZK_CLIENT_MAX_CONCURRENCY: %v", err)
		}
	}

	sem = sync2.NewSemaphore(maxConcurrency, 0)
}

// ZkConn is a client class that implements zk.Conn using a zookeeper.Conn.
// The conn member variable is protected by the mutex.
type ZkConn struct {
	mu   sync.Mutex
	conn *zookeeper.Conn
}

// getConn returns the connection in a thread safe way
func (conn *ZkConn) getConn() *zookeeper.Conn {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	return conn.conn
}

// Dial a ZK server and waits for connection event. Returns a ZkConn
// encapsulating the zookeeper.Conn, and the zookeeper session event
// channel to monitor the connection
//
// The value for baseTimeout is used as a session timeout as well, and
// will be used to negotiate a 'good' value with the server. From
// reading the zookeeper source code, it has to be between 6 and 60
// seconds (2x and 20x the tickTime by default, with default tick time
// being 3 seconds).  min session time, max session time and ticktime
// can all be overwritten on the zookeeper server side, so these
// numbers may vary.
//
// Then this baseTimeout is used to compute other related timeouts:
// - connect timeout is 1/3 of baseTimeout
// - recv timeout is 2/3 of baseTimeout minus a ping time
// - send timeout is 1/3 of baseTimeout
// - we try to send a ping a least every baseTimeout / 3
//
// Note the baseTimeout has *nothing* to do with the time between we
// call Dial and the maximum time before we receive the event on the
// session. The library will actually try to re-connect in the background
// (after each timeout), and may *never* send an event if the TCP connections
// always fail. Use DialZkTimeout to enforce a timeout for the initial connect.
func DialZk(zkAddr string, baseTimeout time.Duration) (*ZkConn, <-chan zookeeper.Event, error) {
	resolvedZkAddr, err := resolveZkAddr(zkAddr)
	if err != nil {
		return nil, nil, err
	}

	sem.Acquire()
	defer sem.Release()
	zconn, session, err := zookeeper.Dial(resolvedZkAddr, baseTimeout)
	if err == nil {
		// Wait for connection, possibly forever
		event := <-session
		if event.State != zookeeper.STATE_CONNECTED {
			err = fmt.Errorf("zk connect failed: %v", event.State)
		}
		if err == nil {
			return &ZkConn{conn: zconn}, session, nil
		} else {
			zconn.Close()
		}
	}
	return nil, nil, err
}

func DialZkTimeout(zkAddr string, baseTimeout time.Duration, connectTimeout time.Duration) (*ZkConn, <-chan zookeeper.Event, error) {
	resolvedZkAddr, err := resolveZkAddr(zkAddr)
	if err != nil {
		return nil, nil, err
	}

	sem.Acquire()
	defer sem.Release()
	zconn, session, err := zookeeper.Dial(resolvedZkAddr, baseTimeout)
	if err == nil {
		// Wait for connection, with a timeout
		timer := time.NewTimer(connectTimeout)
		select {
		case <-timer.C:
			err = fmt.Errorf("zk connect timed out")
		case event := <-session:
			if event.State != zookeeper.STATE_CONNECTED {
				err = fmt.Errorf("zk connect failed: %v", event.State)
			}
		}

		if err == nil {
			return &ZkConn{conn: zconn}, session, nil
		} else {
			zconn.Close()
		}
	}
	return nil, nil, err
}

// resolveZkAddr takes a comma-separated list of host:post addresses,
// and resolves the host to replace it with the IP address.
// If a resolution fails, the host is skipped.
// If no host can be resolved, an error is returned.
// This is different fromt he zookeeper C library, that insists on resolving
// *all* hosts before it starts.
func resolveZkAddr(zkAddr string) (string, error) {
	parts := strings.Split(zkAddr, ",")
	resolved := make([]string, 0, len(parts))
	for _, part := range parts {
		// The zookeeper client cannot handle IPv6 addresses before version 3.4.x.
		if r, err := netutil.ResolveIPv4Addr(part); err != nil {
			log.Infof("cannot resolve %v, will not use it: %v", part, err)
		} else {
			resolved = append(resolved, r)
		}
	}
	if len(resolved) == 0 {
		return "", fmt.Errorf("no valid address found in %v", zkAddr)
	}
	return strings.Join(resolved, ","), nil
}

func (conn *ZkConn) Get(path string) (data string, stat Stat, err error) {
	c := conn.getConn()
	if c == nil {
		return "", nil, ErrConnectionClosed
	}

	sem.Acquire()
	defer sem.Release()
	data, s, err := c.Get(path)
	if s == nil {
		// Handle nil-nil interface conversion.
		stat = nil
	} else {
		stat = s
	}
	return
}

func (conn *ZkConn) GetW(path string) (data string, stat Stat, watch <-chan zookeeper.Event, err error) {
	c := conn.getConn()
	if c == nil {
		return "", nil, nil, ErrConnectionClosed
	}

	sem.Acquire()
	defer sem.Release()
	data, s, watch, err := c.GetW(path)
	if s == nil {
		// Handle nil-nil interface conversion.
		stat = nil
	} else {
		stat = s
	}
	return
}

func (conn *ZkConn) Children(path string) (children []string, stat Stat, err error) {
	c := conn.getConn()
	if c == nil {
		return nil, nil, ErrConnectionClosed
	}

	sem.Acquire()
	defer sem.Release()
	children, s, err := c.Children(path)
	if s == nil {
		// Handle nil-nil interface conversion.
		stat = nil
	} else {
		stat = s
	}
	return
}

func (conn *ZkConn) ChildrenW(path string) (children []string, stat Stat, watch <-chan zookeeper.Event, err error) {
	c := conn.getConn()
	if c == nil {
		return nil, nil, nil, ErrConnectionClosed
	}

	sem.Acquire()
	defer sem.Release()
	children, s, watch, err := c.ChildrenW(path)
	if s == nil {
		// Handle nil-nil interface conversion.
		stat = nil
	} else {
		stat = s
	}
	return
}

func (conn *ZkConn) Exists(path string) (stat Stat, err error) {
	c := conn.getConn()
	if c == nil {
		return nil, ErrConnectionClosed
	}

	sem.Acquire()
	defer sem.Release()
	s, err := c.Exists(path)
	if s == nil {
		// Handle nil-nil interface conversion.
		return nil, err
	}
	return s, err
}

func (conn *ZkConn) ExistsW(path string) (stat Stat, watch <-chan zookeeper.Event, err error) {
	c := conn.getConn()
	if c == nil {
		return nil, nil, ErrConnectionClosed
	}

	sem.Acquire()
	defer sem.Release()
	s, w, err := c.ExistsW(path)
	if s == nil {
		// Handle nil-nil interface conversion.
		return nil, w, err
	}
	return s, w, err
}

func (conn *ZkConn) Create(path, value string, flags int, aclv []zookeeper.ACL) (pathCreated string, err error) {
	c := conn.getConn()
	if c == nil {
		return "", ErrConnectionClosed
	}

	sem.Acquire()
	defer sem.Release()
	return c.Create(path, value, flags, aclv)
}

func (conn *ZkConn) Set(path, value string, version int) (stat Stat, err error) {
	c := conn.getConn()
	if c == nil {
		return nil, ErrConnectionClosed
	}

	sem.Acquire()
	defer sem.Release()
	s, err := c.Set(path, value, version)
	if s == nil {
		// Handle nil-nil interface conversion.
		return nil, err
	}
	return s, err
}

func (conn *ZkConn) Delete(path string, version int) (err error) {
	c := conn.getConn()
	if c == nil {
		return ErrConnectionClosed
	}

	sem.Acquire()
	defer sem.Release()
	return c.Delete(path, version)
}

// Close will close the connection asynchronously.  It will never
// fail, even though closing the connection might fail in the
// background.  Accessing this ZkConn after Close has been called will
// return ErrConnectionClosed.
func (conn *ZkConn) Close() error {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.conn == nil {
		return nil
	}
	c := conn.conn
	conn.conn = nil
	go c.Close()
	return nil
}

func (conn *ZkConn) RetryChange(path string, flags int, acl []zookeeper.ACL, changeFunc ChangeFunc) error {
	c := conn.getConn()
	if c == nil {
		return ErrConnectionClosed
	}

	sem.Acquire()
	defer sem.Release()
	return c.RetryChange(path, flags, acl, func(oldValue string, oldStat *zookeeper.Stat) (newValue string, err error) {
		return changeFunc(oldValue, oldStat)
	})
}

func (conn *ZkConn) ACL(path string) (acls []zookeeper.ACL, stat Stat, err error) {
	c := conn.getConn()
	if c == nil {
		return nil, nil, ErrConnectionClosed
	}

	sem.Acquire()
	defer sem.Release()
	acls, s, err := c.ACL(path)
	if s == nil {
		// Handle nil-nil interface conversion.
		stat = nil
	} else {
		stat = s
	}
	return
}

func (conn *ZkConn) SetACL(path string, aclv []zookeeper.ACL, version int) error {
	c := conn.getConn()
	if c == nil {
		return ErrConnectionClosed
	}

	sem.Acquire()
	defer sem.Release()
	return c.SetACL(path, aclv, version)
}
