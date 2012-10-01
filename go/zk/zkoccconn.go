// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zk

import (
	"code.google.com/p/vitess/go/rpcplus"
	"code.google.com/p/vitess/go/rpcwrap/bsonrpc"
	"code.google.com/p/vitess/go/zk/zkocc/proto"
	"launchpad.net/gozk/zookeeper"
)

type ZkoccUnimplementedError string

func (e ZkoccUnimplementedError) Error() string {
	return string("ZkoccConn doesn't implement " + e)
}

// ZkoccConn is a client class that implements zk.Conn
// but uses a RPC client to talk to a zkocc process
type ZkoccConn struct {
	rpcClient *rpcplus.Client
}

func (conn *ZkoccConn) Dial(addr string) (err error) {
	conn.rpcClient, err = bsonrpc.DialHTTP("tcp", addr)
	return err
}

func (conn *ZkoccConn) Get(path string) (data string, stat Stat, err error) {
	zkPath := &proto.ZkPath{path}
	zkNode := &proto.ZkNode{}
	if err := conn.rpcClient.Call("ZkReader.Get", zkPath, zkNode); err != nil {
		return "", nil, err
	}
	return zkNode.Data, &zkNode.Stat, nil
}

func (conn *ZkoccConn) GetW(path string) (data string, stat Stat, watch <-chan zookeeper.Event, err error) {
	panic(ZkoccUnimplementedError("GetW"))
}

func (conn *ZkoccConn) Children(path string) (children []string, stat Stat, err error) {
	zkPath := &proto.ZkPath{path}
	zkNode := &proto.ZkNode{}
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
func (conn *ZkoccConn) Exists(path string) (exists bool, stat Stat, err error) {
	zkPath := &proto.ZkPath{path}
	zkNode := &proto.ZkNode{}
	if err := conn.rpcClient.Call("ZkReader.Get", zkPath, zkNode); err != nil {
		return false, nil, err
	}
	return true, &zkNode.Stat, nil
}

func (conn *ZkoccConn) ExistsW(path string) (exists bool, stat Stat, watch <-chan zookeeper.Event, err error) {
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

func (conn *ZkoccConn) RetryChange(path string, flags int, acl []zookeeper.ACL, changeFunc zookeeper.ChangeFunc) error {
	panic(ZkoccUnimplementedError("RetryChange"))
}

// might want to add ACL in RPC code
func (conn *ZkoccConn) ACL(path string) ([]zookeeper.ACL, Stat, error) {
	panic(ZkoccUnimplementedError("ACL"))
}

func (conn *ZkoccConn) SetACL(path string, aclv []zookeeper.ACL, version int) error {
	panic(ZkoccUnimplementedError("SetACL"))
}
