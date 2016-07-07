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

	"golang.org/x/net/context"

	log "github.com/golang/glog"
	zookeeper "github.com/samuel/go-zookeeper/zk"

	"github.com/youtube/vitess/go/netutil"
	"github.com/youtube/vitess/go/sync2"
)

// Time returns a time.Time from a ZK int64 milliseconds since Epoch time.
func Time(i int64) time.Time {
	return time.Unix(i/1000, i%1000*1000000)
}

// ZkTime returns a ZK time (int64) from a time.Time
func ZkTime(t time.Time) int64 {
	return t.Unix()*1000 + int64(t.Nanosecond()/1000000)
}

// FIXME(alainjobart) just use the original structure, not an interface
type GoZkStat struct {
	s *zookeeper.Stat
}

// GoZkStat methods to match zk.Stat interface
func (zkStat GoZkStat) Czxid() int64 {
	return zkStat.s.Czxid
}

func (zkStat GoZkStat) Mzxid() int64 {
	return zkStat.s.Mzxid
}

func (zkStat GoZkStat) Ctime() int64 {
	return zkStat.s.Ctime
}

func (zkStat GoZkStat) Mtime() int64 {
	return zkStat.s.Mtime
}

func (zkStat GoZkStat) Version() int32 {
	return zkStat.s.Version
}

func (zkStat GoZkStat) Cversion() int32 {
	return zkStat.s.Cversion
}

func (zkStat GoZkStat) Aversion() int32 {
	return zkStat.s.Aversion
}

func (zkStat GoZkStat) EphemeralOwner() int64 {
	return zkStat.s.EphemeralOwner
}

func (zkStat GoZkStat) DataLength() int32 {
	return zkStat.s.DataLength
}

func (zkStat GoZkStat) NumChildren() int32 {
	return zkStat.s.NumChildren
}

func (zkStat GoZkStat) Pzxid() int64 {
	return zkStat.s.Pzxid
}

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
	servers, err := resolveZkAddr(zkAddr)
	if err != nil {
		return nil, nil, err
	}

	sem.Acquire()
	defer sem.Release()
	zconn, session, err := zookeeper.Connect(servers, baseTimeout)
	if err == nil {
		// Wait for connection, possibly forever
		event := <-session
		if event.State != zookeeper.StateConnecting {
			err = fmt.Errorf("zk connect failed waiting for connecting state: %v", event.State)
		} else {
			event = <-session
			if event.State != zookeeper.StateConnected {
				err = fmt.Errorf("zk connect failed waiting for connected: %v", event.State)
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

func DialZkTimeout(zkAddr string, baseTimeout time.Duration, connectTimeout time.Duration) (*ZkConn, <-chan zookeeper.Event, error) {
	servers, err := resolveZkAddr(zkAddr)
	if err != nil {
		return nil, nil, err
	}

	sem.Acquire()
	defer sem.Release()
	zconn, session, err := zookeeper.Connect(servers, baseTimeout)
	if err != nil {
		return nil, nil, err
	}

	// Wait for connection, with a timeout
	timer := time.NewTimer(connectTimeout)
	for {
		select {
		case <-timer.C:
			zconn.Close()
			return nil, nil, context.DeadlineExceeded
		case event := <-session:
			if event.State == zookeeper.StateConnected {
				return &ZkConn{conn: zconn}, session, nil
			}
		}
	}
}

// resolveZkAddr takes a comma-separated list of host:post addresses,
// and resolves the host to replace it with the IP address.
// If a resolution fails, the host is skipped.
// If no host can be resolved, an error is returned.
// This is different fromt he zookeeper C library, that insists on resolving
// *all* hosts before it starts.
func resolveZkAddr(zkAddr string) ([]string, error) {
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
		return nil, fmt.Errorf("no valid address found in %v", zkAddr)
	}
	return resolved, nil
}

func (conn *ZkConn) Get(path string) (string, Stat, error) {
	c := conn.getConn()
	if c == nil {
		return "", nil, ErrConnectionClosed
	}

	sem.Acquire()
	defer sem.Release()
	data, stat, err := c.Get(path)
	if err != nil {
		return "", nil, err
	}
	return string(data), GoZkStat{stat}, nil
}

func (conn *ZkConn) GetW(path string) (string, Stat, <-chan zookeeper.Event, error) {
	c := conn.getConn()
	if c == nil {
		return "", nil, nil, ErrConnectionClosed
	}

	sem.Acquire()
	defer sem.Release()
	data, stat, watch, err := c.GetW(path)
	if err != nil {
		return "", nil, nil, err
	}
	return string(data), GoZkStat{stat}, watch, nil
}

func (conn *ZkConn) Children(path string) ([]string, Stat, error) {
	c := conn.getConn()
	if c == nil {
		return nil, nil, ErrConnectionClosed
	}

	sem.Acquire()
	defer sem.Release()
	children, stat, err := c.Children(path)
	if err != nil {
		return nil, nil, err
	}
	return children, GoZkStat{stat}, nil
}

func (conn *ZkConn) ChildrenW(path string) ([]string, Stat, <-chan zookeeper.Event, error) {
	c := conn.getConn()
	if c == nil {
		return nil, nil, nil, ErrConnectionClosed
	}

	sem.Acquire()
	defer sem.Release()
	children, stat, watch, err := c.ChildrenW(path)
	if err != nil {
		return nil, nil, nil, err
	}
	return children, GoZkStat{stat}, watch, nil
}

func (conn *ZkConn) Exists(path string) (Stat, error) {
	c := conn.getConn()
	if c == nil {
		return nil, ErrConnectionClosed
	}

	sem.Acquire()
	defer sem.Release()
	exists, stat, err := c.Exists(path)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, zookeeper.ErrNoNode
	}
	return GoZkStat{stat}, nil
}

func (conn *ZkConn) ExistsW(path string) (Stat, <-chan zookeeper.Event, error) {
	c := conn.getConn()
	if c == nil {
		return nil, nil, ErrConnectionClosed
	}

	sem.Acquire()
	defer sem.Release()
	exists, stat, watch, err := c.ExistsW(path)
	if err != nil {
		return nil, nil, err
	}
	if !exists {
		return nil, watch, nil
	}
	return GoZkStat{stat}, watch, nil
}

func (conn *ZkConn) Create(path, value string, flags int, aclv []zookeeper.ACL) (pathCreated string, err error) {
	c := conn.getConn()
	if c == nil {
		return "", ErrConnectionClosed
	}

	sem.Acquire()
	defer sem.Release()
	return c.Create(path, []byte(value), int32(flags), aclv)
}

func (conn *ZkConn) Set(path, value string, version int32) (Stat, error) {
	c := conn.getConn()
	if c == nil {
		return nil, ErrConnectionClosed
	}

	sem.Acquire()
	defer sem.Release()
	stat, err := c.Set(path, []byte(value), version)
	if err != nil {
		return nil, err
	}
	return GoZkStat{stat}, nil
}

func (conn *ZkConn) Delete(path string, version int32) (err error) {
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

func (conn *ZkConn) ACL(path string) ([]zookeeper.ACL, Stat, error) {
	c := conn.getConn()
	if c == nil {
		return nil, nil, ErrConnectionClosed
	}

	sem.Acquire()
	defer sem.Release()
	acls, stat, err := c.GetACL(path)
	if err != nil {
		return nil, nil, err
	}
	return acls, GoZkStat{stat}, nil
}

func (conn *ZkConn) SetACL(path string, aclv []zookeeper.ACL, version int32) error {
	c := conn.getConn()
	if c == nil {
		return ErrConnectionClosed
	}

	sem.Acquire()
	defer sem.Release()
	_, err := c.SetACL(path, aclv, version)
	return err
}
