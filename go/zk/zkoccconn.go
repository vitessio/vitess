// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zk

import (
	"code.google.com/p/vitess/go/rpcplus"
	"code.google.com/p/vitess/go/rpcwrap/bsonrpc"
	"fmt"
	"launchpad.net/gozk/zookeeper"
	"log"
	"math/rand"
	"strings"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type ZkoccUnimplementedError string

func (e ZkoccUnimplementedError) Error() string {
	return string("ZkoccConn doesn't implement " + e)
}

// ZkoccConn is a client class that implements zk.Conn
// but uses a RPC client to talk to a zkocc process
type ZkoccConn struct {
	rpcClient *rpcplus.Client
}

// From the addr (of the form server1:port1,server2:port2,server3:port3:...)
// splits it on commas, randomizes the list, and tries to connect
// to the servers, stopping at the first successful connection
func DialZkocc(addr string, connectTimeout time.Duration) (zkocc *ZkoccConn, err error) {
	servers := strings.Split(addr, ",")
	perm := rand.Perm(len(servers))
	for _, index := range perm {
		server := servers[index]

		rpcClient, err := bsonrpc.DialHTTP("tcp", server, connectTimeout)
		if err == nil {
			return &ZkoccConn{rpcClient: rpcClient}, nil
		}
		log.Printf("zk conn cache: zkocc connection to %v failed: %v", server, err)
	}
	return nil, fmt.Errorf("zkocc connect failed: %v", addr)
}

func (conn *ZkoccConn) Get(path string) (data string, stat Stat, err error) {
	zkPath := &ZkPath{path}
	zkNode := &ZkNode{}
	if err := conn.rpcClient.Call("ZkReader.Get", zkPath, zkNode); err != nil {
		return "", nil, err
	}
	return zkNode.Data, &zkNode.Stat, nil
}

func (conn *ZkoccConn) GetW(path string) (data string, stat Stat, watch <-chan zookeeper.Event, err error) {
	panic(ZkoccUnimplementedError("GetW"))
}

func (conn *ZkoccConn) Children(path string) (children []string, stat Stat, err error) {
	zkPath := &ZkPath{path}
	zkNode := &ZkNode{}
	if err := conn.rpcClient.Call("ZkReader.Children", zkPath, zkNode); err != nil {
		return nil, nil, err
	}
	return zkNode.Children, &zkNode.Stat, nil
}

func (conn *ZkoccConn) ChildrenW(path string) (children []string, stat Stat, watch <-chan zookeeper.Event, err error) {
	panic(ZkoccUnimplementedError("ChildrenW"))
}

// implement Exists using Get
// FIXME(alainjobart) Maybe we should add Exists in rpc API?
func (conn *ZkoccConn) Exists(path string) (stat Stat, err error) {
	zkPath := &ZkPath{path}
	zkNode := &ZkNode{}
	if err := conn.rpcClient.Call("ZkReader.Get", zkPath, zkNode); err != nil {
		return nil, err
	}
	return &zkNode.Stat, nil
}

func (conn *ZkoccConn) ExistsW(path string) (stat Stat, watch <-chan zookeeper.Event, err error) {
	panic(ZkoccUnimplementedError("ExistsW"))
}

func (conn *ZkoccConn) Create(path, value string, flags int, aclv []zookeeper.ACL) (pathCreated string, err error) {
	panic(ZkoccUnimplementedError("Create"))
}

func (conn *ZkoccConn) Set(path, value string, version int) (stat Stat, err error) {
	panic(ZkoccUnimplementedError("Set"))
}

func (conn *ZkoccConn) Delete(path string, version int) (err error) {
	panic(ZkoccUnimplementedError("Delete"))
}

func (conn *ZkoccConn) Close() error {
	return conn.rpcClient.Close()
}

func (conn *ZkoccConn) RetryChange(path string, flags int, acl []zookeeper.ACL, changeFunc ChangeFunc) error {
	panic(ZkoccUnimplementedError("RetryChange"))
}

// might want to add ACL in RPC code
func (conn *ZkoccConn) ACL(path string) ([]zookeeper.ACL, Stat, error) {
	panic(ZkoccUnimplementedError("ACL"))
}

func (conn *ZkoccConn) SetACL(path string, aclv []zookeeper.ACL, version int) error {
	panic(ZkoccUnimplementedError("SetACL"))
}
