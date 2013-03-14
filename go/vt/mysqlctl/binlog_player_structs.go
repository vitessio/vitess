// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

type BinlogResponse struct {
	Error string
	BinlogPosition
	BinlogData
}

type BinlogServerRequest struct {
	StartPosition string
	KeyspaceStart string
	KeyspaceEnd   string
}

type BinlogData struct {
	SqlType    string
	Sql        []string
	KeyspaceId string
	IndexType  string
	IndexId    interface{}
	SeqName    string
	SeqId      uint64
	UserId     uint64
}
