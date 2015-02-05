// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpcproto

import (
	"time"

	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/logutil"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletmanager/actionnode"
)

/*

This file contains the structures used to pack the RPC args and reply
for the gorpc transport for tablet manager.

Arguments are composed of the name of the call + Args.
Replies are composed of the name of the call + Reply.
If a struct is used as both Arguments and Replies, use name of the call + Data.

NOTE(alainjobart) It is OK to rename a structure, as the type is not
passed in through the RPC. I tested that by creating another set of
structures here, adding a '2' to the end of the types, and changing
gorpc_server.go to only use the '2' structures. Everything worked the same.

*/

type GetSchemaArgs struct {
	Tables        []string
	ExcludeTables []string
	IncludeViews  bool
}

type WaitSlavePositionArgs struct {
	Position    myproto.ReplicationPosition
	WaitTimeout time.Duration // pass in zero to wait indefinitely
}

type StopSlaveMinimumArgs struct {
	Position myproto.ReplicationPosition
	WaitTime time.Duration
}

type GetSlavesReply struct {
	Addrs []string
}

type WaitBlpPositionArgs struct {
	BlpPosition blproto.BlpPosition
	WaitTimeout time.Duration
}

type RunBlpUntilArgs struct {
	BlpPositionList *blproto.BlpPositionList
	WaitTimeout     time.Duration
}

type ExecuteFetchArgs struct {
	Query          string
	MaxRows        int
	WantFields     bool
	DisableBinlogs bool
	DBConfigName   string
}

// gorpc doesn't support returning a streaming type during streaming
// and a final return value, so using structures with either one set.

type SnapshotStreamingReply struct {
	Log    *logutil.LoggerEvent
	Result *actionnode.SnapshotReply
}

type TabletExternallyReparentedArgs struct {
	ExternalID string
}
