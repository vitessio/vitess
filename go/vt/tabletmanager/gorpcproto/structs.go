// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gorpcproto

import (
	"time"

	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

type GetSchemaArgs struct {
	Tables       []string
	IncludeViews bool
}

type SetBlacklistedTablesArgs struct {
	Tables []string
}

type SlavePositionReq struct {
	ReplicationPosition myproto.ReplicationPosition
	WaitTimeout         time.Duration // pass in zero to wait indefinitely
}

type StopSlaveMinimumArgs struct {
	GroupdId int64
	WaitTime time.Duration
}

type WaitBlpPositionArgs struct {
	BlpPosition myproto.BlpPosition
	WaitTimeout time.Duration
}

type RunBlpUntilArgs struct {
	BlpPositionList *myproto.BlpPositionList
	WaitTimeout     time.Duration
}
