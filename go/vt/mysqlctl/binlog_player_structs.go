// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

type BlPosition struct {
	Position  ReplicationCoordinates
	Timestamp int64
	Xid       uint64
	GroupId   uint64
}

type BinlogResponse struct {
	Error string
	BlPosition
	BinlogData
}

type BinlogServerRequest struct {
	StartPosition ReplicationCoordinates
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
