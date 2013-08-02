// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

type BlPosition struct {
	Position  proto.ReplicationCoordinates
	Timestamp int64
	Xid       uint64
}

type BinlogResponse struct {
	Error string
	BlPosition
	BinlogData
}

type BinlogServerRequest struct {
	StartPosition proto.ReplicationCoordinates
	KeyspaceStart string
	KeyspaceEnd   string
}

type BinlogData struct {
	SqlType    string
	Sql        []string
	KeyspaceId string
	IndexType  string
	IndexId    interface{}
	UserId     uint64
}
