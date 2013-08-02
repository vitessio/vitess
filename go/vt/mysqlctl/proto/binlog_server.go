// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	rpc "github.com/youtube/vitess/go/rpcplus"
)

// BinlogServerRequest represents a request to the BinlogServer service.
type BinlogServerRequest struct {
	StartPosition ReplicationCoordinates
	KeyspaceStart string
	KeyspaceEnd   string
}

// BinlogResponse is the response from the BinlogServer service.
type BinlogResponse struct {
	Error    string
	Position BinlogPosition
	Data     BinlogData
}

// BinlogData is the payload for BinlogResponse
type BinlogData struct {
	SqlType    string
	Sql        []string
	KeyspaceId string
	IndexType  string
	IndexId    interface{}
	UserId     uint64
}

// SendBinlogResponse makes it easier to define this interface
type SendBinlogResponse func(response interface{}) error

// BinlogServer interface is used to validate the RPC syntax
type BinlogServer interface {
	// ServeBinlog is the streaming API entry point.
	ServeBinlog(req *BinlogServerRequest, sendReply SendBinlogResponse) error
}

// RegisterBinlogServer makes sure the server implements the right API
func RegisterBinlogServer(server BinlogServer) {
	rpc.Register(server)
}
