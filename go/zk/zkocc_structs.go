// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zk

// contains the structures used for RPC calls to zkocc.

import (
	"time"
)

type ZkStat struct {
	czxid          int64
	mzxid          int64
	cTime          time.Time
	mTime          time.Time
	version        int
	cVersion       int
	aVersion       int
	ephemeralOwner int64
	dataLength     int
	numChildren    int
	pzxid          int64
}

type ZkPath struct {
	Path string
}

type ZkPathV struct {
	Paths []string
}

type ZkNode struct {
	Path     string
	Data     string
	Stat     ZkStat
	Children []string
	Cached   bool // the response comes from the zkocc cache
	Stale    bool // the response is stale because we're not connected
}

type ZkNodeV struct {
	Nodes []*ZkNode
}

// ZkStat methods to match zk.Stat interface
func (zkStat *ZkStat) Czxid() int64 {
	return zkStat.czxid
}

func (zkStat *ZkStat) Mzxid() int64 {
	return zkStat.mzxid
}

func (zkStat *ZkStat) CTime() time.Time {
	return zkStat.cTime
}

func (zkStat *ZkStat) MTime() time.Time {
	return zkStat.mTime
}

func (zkStat *ZkStat) Version() int {
	return zkStat.version
}

func (zkStat *ZkStat) CVersion() int {
	return zkStat.cVersion
}

func (zkStat *ZkStat) AVersion() int {
	return zkStat.aVersion
}

func (zkStat *ZkStat) EphemeralOwner() int64 {
	return zkStat.ephemeralOwner
}

func (zkStat *ZkStat) DataLength() int {
	return zkStat.dataLength
}

func (zkStat *ZkStat) NumChildren() int {
	return zkStat.numChildren
}

func (zkStat *ZkStat) Pzxid() int64 {
	return zkStat.pzxid
}

// helper method
func (zkStat *ZkStat) FromZookeeperStat(zStat Stat) {
	zkStat.czxid = zStat.Czxid()
	zkStat.mzxid = zStat.Mzxid()
	zkStat.cTime = zStat.CTime()
	zkStat.mTime = zStat.MTime()
	zkStat.version = zStat.Version()
	zkStat.cVersion = zStat.CVersion()
	zkStat.aVersion = zStat.AVersion()
	zkStat.ephemeralOwner = zStat.EphemeralOwner()
	zkStat.dataLength = zStat.DataLength()
	zkStat.numChildren = zStat.NumChildren()
	zkStat.pzxid = zStat.Pzxid()
}
