// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"github.com/youtube/vitess/go/rpcwrap"
	"github.com/youtube/vitess/go/vt/key"
)

// UpdateStreamRequest is used to make a request for ServeUpdateStream.
type UpdateStreamRequest struct {
	GroupId int64
}

// KeyRangeRequest is used to make a request for StreamKeyRange.
type KeyRangeRequest struct {
	GroupId  int64
	KeyRange key.KeyRange
}

// TablesRequest is used to make a request for StreamTables.
type TablesRequest struct {
	GroupId int64
	Tables  []string
}

// UpdateStream defines the rpc API for the update stream service.
type UpdateStream interface {
	ServeUpdateStream(req *UpdateStreamRequest, sendReply func(reply interface{}) error) (err error)
	StreamKeyRange(req *KeyRangeRequest, sendReply func(reply interface{}) error) (err error)
	StreamTables(req *TablesRequest, sendReply func(reply interface{}) error) (err error)
}

// RegisterAuthenticated registers a variable that satisfies the UpdateStream interface
// as an rpc service that requires authentication.
func RegisterAuthenticated(service UpdateStream) {
	rpcwrap.RegisterAuthenticated(service)
}
