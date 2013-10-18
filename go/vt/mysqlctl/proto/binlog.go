// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package proto

import (
	"bytes"
	"fmt"

	"github.com/youtube/vitess/go/bson"
	"github.com/youtube/vitess/go/bytes2"
)

// ReplicationCoordinates keeps track of a server position.
type ReplicationCoordinates struct {
	MasterFilename string
	MasterPosition uint64
	GroupId        string
}

func NewReplicationCoordinates(masterFile string, masterPos uint64, groupId string) *ReplicationCoordinates {
	return &ReplicationCoordinates{
		MasterFilename: masterFile,
		MasterPosition: masterPos,
		GroupId:        groupId,
	}
}

func (repl *ReplicationCoordinates) String() string {
	return fmt.Sprintf("Master %v:%v GroupId %v", repl.MasterFilename, repl.MasterPosition, repl.GroupId)
}

func (repl *ReplicationCoordinates) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodeString(buf, "MasterFilename", repl.MasterFilename)
	bson.EncodeUint64(buf, "MasterPosition", repl.MasterPosition)
	bson.EncodeString(buf, "GroupId", repl.GroupId)

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (repl *ReplicationCoordinates) UnmarshalBson(buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "MasterFilename":
			repl.MasterFilename = bson.DecodeString(buf, kind)
		case "MasterPosition":
			repl.MasterPosition = bson.DecodeUint64(buf, kind)
		case "GroupId":
			repl.GroupId = bson.DecodeString(buf, kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}

// BinlogPosition keeps track of a server binlog position
type BinlogPosition struct {
	Position  ReplicationCoordinates
	Timestamp int64
	Xid       uint64
}

func (pos *BinlogPosition) String() string {
	return fmt.Sprintf("%v:%v", pos.Position.MasterFilename, pos.Position.MasterPosition)
}

func (pos *BinlogPosition) Valid() bool {
	if pos.Position.MasterFilename == "" || pos.Position.MasterPosition == 0 {
		return false
	}
	return true
}

func (pos *BinlogPosition) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodePrefix(buf, bson.Object, "Position")
	pos.Position.MarshalBson(buf)

	bson.EncodeInt64(buf, "Timestamp", pos.Timestamp)
	bson.EncodeUint64(buf, "Xid", pos.Xid)

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (pos *BinlogPosition) UnmarshalBson(buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "Position":
			pos.Position = ReplicationCoordinates{}
			pos.Position.UnmarshalBson(buf)
		case "Timestamp":
			pos.Timestamp = bson.DecodeInt64(buf, kind)
		case "Xid":
			pos.Xid = bson.DecodeUint64(buf, kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}
