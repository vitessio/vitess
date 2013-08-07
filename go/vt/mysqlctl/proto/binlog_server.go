// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	rpc "github.com/youtube/vitess/go/rpcplus"
)

// Possible values for SqlType (all lower case)
const (
	// contains all the statements of the given types
	DDL = "ddl"
	DML = "dml"

	// transation commands
	BEGIN  = "begin"
	COMMIT = "commit"

	// database selection
	USE = "use"
)

var sqlKwMap = map[string]string{
	"alter":    DDL,
	"create":   DDL,
	"drop":     DDL,
	"rename":   DDL,
	"truncate": DDL,

	"insert": DML,
	"update": DML,
	"delete": DML,

	"begin":  BEGIN,
	"commit": COMMIT,

	"use": USE,
}

// GetSqlType returns one of the possible values for SqlType, or
// "" if it cannot be determined.
// firstKeyword has to be normalized to lower case first.
func GetSqlType(firstKeyword string) string {
	return sqlKwMap[firstKeyword]
}

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
	// SqlType is one of the possible constants defined earlier
	SqlType string

	// Sql is the list of statements executed
	Sql []string

	// KeyspaceId is used for routing of events
	KeyspaceId string
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
