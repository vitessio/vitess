// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/* Emulate a "global" namespace across n zk quorums. */
package zk

import (
	"fmt"
	"sort"
	"time"

	"launchpad.net/gozk/zookeeper"
)

const (
	DEFAULT_MAX_RETRIES = 3
)

type GlobalZookeeperError string

func (e GlobalZookeeperError) Error() string {
	return string(e)
}

type GlobalConn struct {
	serverAddrs []string
	zconns      []*zookeeper.Conn
	maxRetries  int
}

func Dial(serverAddrs []string, recvTimeout time.Duration) (*GlobalConn, <-chan zookeeper.Event, error) {
	zconns := make([]*zookeeper.Conn, len(serverAddrs))
	zchans := make([]<-chan zookeeper.Event, len(serverAddrs))
	for i, addr := range serverAddrs {
		conn, eventChan, err := zookeeper.Dial(addr, recvTimeout)
		if err != nil {
			// teardown
			for j := 0; j < i; j++ {
				zconns[j].Close()
			}
			return nil, nil, err
		}
		zconns[i] = conn
		zchans[i] = eventChan
	}

	eventChan := make(chan zookeeper.Event, 1)
	go func() {
		var e zookeeper.Event
		for _, c := range zchans {
			e = <-c
		}
		eventChan <- e
		close(eventChan)
	}()
	return &GlobalConn{serverAddrs, zconns, DEFAULT_MAX_RETRIES}, eventChan, nil
}

func (gzc *GlobalConn) Close() (err error) {
	for _, zc := range gzc.zconns {
		if zcErr := zc.Close(); zcErr != nil {
			err = zcErr
		}
	}
	return
}

func (gzc *GlobalConn) Create(path, value string, flags int, aclv []zookeeper.ACL) (pathCreated string, err error) {
	createdPaths := make([]string, len(gzc.zconns))
	errs := make([]error, len(gzc.zconns))
	for i, zconn := range gzc.zconns {
		createdPaths[i], errs[i] = zconn.Create(path, value, flags, aclv)
		if errs[i] != nil {
			return "", fmt.Errorf("global create error: %v %v", createdPaths, errs[i])
		}
		if createdPaths[0] != createdPaths[i] {
			return "", fmt.Errorf("inconsistent global create: %v", createdPaths)
		}
	}
	return createdPaths[0], errs[0]
}

func (gzc *GlobalConn) Get(path string) (data string, stat Stat, err error) {
	datas := make([]string, len(gzc.zconns))
	for i, zconn := range gzc.zconns {
		data, stat, err = zconn.Get(path)
		if err != nil {
			return "", nil, fmt.Errorf("global get error: %v", err)
		}
		datas[i] = data
		if datas[0] != data {
			return "", nil, fmt.Errorf("inconsistent global get: %v", err)
		}
	}
	return
}

func (gzc *GlobalConn) Children(path string) (children []string, stat Stat, err error) {
	childrens := make([][]string, len(gzc.zconns))
	for i, zconn := range gzc.zconns {
		children, stat, err = zconn.Children(path)
		sort.Strings(children)
		if err != nil {
			return nil, nil, fmt.Errorf("global children error: %v", err)
		}
		childrens[i] = children
		if !eqSlice(childrens[0], children) {
			return nil, nil, fmt.Errorf("inconsistent global children: %v", err)
		}
	}
	return
}

func eqSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, x := range a {
		if b[i] != x {
			return false
		}
	}
	return true
}

func (gzc *GlobalConn) Set(path, value string, version int) (stat Stat, err error) {
	for _, zconn := range gzc.zconns {
		stat, err = zconn.Set(path, value, version)
		if err != nil {
			return nil, fmt.Errorf("inconsistent global set: %v", err)
		}
	}
	return stat, nil
}

func (gzc *GlobalConn) Delete(path string, version int) (err error) {
	for _, zconn := range gzc.zconns {
		err = zconn.Delete(path, version)
		if err != nil {
			return fmt.Errorf("inconsistent global delete: %v", err)
		}
	}
	return
}
