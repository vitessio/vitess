// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zk

// Contains the bson custom marshaler / unmarshaler.
// Note this is not required, but makes the code faster.

import (
	"bytes"

	"github.com/youtube/vitess/go/bson"
	"github.com/youtube/vitess/go/bytes2"
)

func (zkPath *ZkPath) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	bson.EncodeOptionalPrefix(buf, bson.Object, key)
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodeString(buf, "Path", zkPath.Path)

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (zkPath *ZkPath) UnmarshalBson(buf *bytes.Buffer, kind byte) {
	bson.VerifyObject(kind)
	bson.Next(buf, 4)

	kind = bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "Path":
			zkPath.Path = bson.DecodeString(buf, kind)
		default:
			bson.Skip(buf, kind)
		}
		kind = bson.NextByte(buf)
	}
}

func (zkPathV *ZkPathV) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	bson.EncodeOptionalPrefix(buf, bson.Object, key)
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodeStringArray(buf, "Paths", zkPathV.Paths)

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (zkPathV *ZkPathV) UnmarshalBson(buf *bytes.Buffer, kind byte) {
	bson.VerifyObject(kind)
	bson.Next(buf, 4)

	kind = bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "Paths":
			zkPathV.Paths = bson.DecodeStringArray(buf, kind)
		default:
			bson.Skip(buf, kind)
		}
		kind = bson.NextByte(buf)
	}
}

func (zkStat *ZkStat) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	bson.EncodeOptionalPrefix(buf, bson.Object, key)
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodeInt64(buf, "Czxid", zkStat.czxid)
	bson.EncodeInt64(buf, "Mzxid", zkStat.mzxid)
	bson.EncodeTime(buf, "CTime", zkStat.cTime)
	bson.EncodeTime(buf, "MTime", zkStat.mTime)
	bson.EncodeInt32(buf, "Version", int32(zkStat.version))
	bson.EncodeInt32(buf, "CVersion", int32(zkStat.cVersion))
	bson.EncodeInt32(buf, "AVersion", int32(zkStat.aVersion))
	bson.EncodeInt64(buf, "EphemeralOwner", zkStat.ephemeralOwner)
	bson.EncodeInt32(buf, "DataLength", int32(zkStat.dataLength))
	bson.EncodeInt32(buf, "NumChildren", int32(zkStat.numChildren))
	bson.EncodeInt64(buf, "Pzxid", zkStat.pzxid)

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (zkStat *ZkStat) UnmarshalBson(buf *bytes.Buffer, kind byte) {
	bson.VerifyObject(kind)
	bson.Next(buf, 4)

	kind = bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "Czxid":
			zkStat.czxid = bson.DecodeInt64(buf, kind)
		case "Mzxid":
			zkStat.mzxid = bson.DecodeInt64(buf, kind)
		case "CTime":
			zkStat.cTime = bson.DecodeTime(buf, kind)
		case "MTime":
			zkStat.mTime = bson.DecodeTime(buf, kind)
		case "Version":
			zkStat.version = bson.DecodeInt(buf, kind)
		case "CVersion":
			zkStat.cVersion = bson.DecodeInt(buf, kind)
		case "AVersion":
			zkStat.aVersion = bson.DecodeInt(buf, kind)
		case "EphemeralOwner":
			zkStat.ephemeralOwner = bson.DecodeInt64(buf, kind)
		case "DataLength":
			zkStat.dataLength = bson.DecodeInt(buf, kind)
		case "NumChildren":
			zkStat.numChildren = bson.DecodeInt(buf, kind)
		case "Pzxid":
			zkStat.pzxid = bson.DecodeInt64(buf, kind)
		default:
			bson.Skip(buf, kind)
		}
		kind = bson.NextByte(buf)
	}
}

func (zkNode *ZkNode) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	bson.EncodeOptionalPrefix(buf, bson.Object, key)
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodeString(buf, "Path", zkNode.Path)
	bson.EncodeString(buf, "Data", zkNode.Data)
	zkNode.Stat.MarshalBson(buf, "Stat")
	bson.EncodeStringArray(buf, "Children", zkNode.Children)
	bson.EncodeBool(buf, "Cached", zkNode.Cached)
	bson.EncodeBool(buf, "Stale", zkNode.Stale)

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (zkNode *ZkNode) UnmarshalBson(buf *bytes.Buffer, kind byte) {
	bson.VerifyObject(kind)
	bson.Next(buf, 4)

	kind = bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "Path":
			zkNode.Path = bson.DecodeString(buf, kind)
		case "Data":
			zkNode.Data = bson.DecodeString(buf, kind)
		case "Stat":
			if kind != bson.Object {
				panic(bson.NewBsonError("Unexpected data type %v for Stat", kind))
			}
			zkNode.Stat.UnmarshalBson(buf, kind)
		case "Children":
			zkNode.Children = bson.DecodeStringArray(buf, kind)
		case "Cached":
			zkNode.Cached = bson.DecodeBool(buf, kind)
		case "Stale":
			zkNode.Stale = bson.DecodeBool(buf, kind)
		default:
			bson.Skip(buf, kind)
		}
		kind = bson.NextByte(buf)
	}
}

func marshalZkNodeArray(buf *bytes2.ChunkedWriter, name string, values []*ZkNode) {
	if values == nil {
		bson.EncodePrefix(buf, bson.Null, name)
	} else {
		bson.EncodePrefix(buf, bson.Array, name)
		lenWriter := bson.NewLenWriter(buf)
		for i, val := range values {
			val.MarshalBson(buf, bson.Itoa(i))
		}
		buf.WriteByte(0)
		lenWriter.RecordLen()
	}
}

func unmarshalZkNodeArray(buf *bytes.Buffer, name string, kind byte) []*ZkNode {
	switch kind {
	case bson.Array:
		// valid
	case bson.Null:
		return nil
	default:
		panic(bson.NewBsonError("Unexpected data type %v for %v", kind, name))
	}

	bson.Next(buf, 4)
	values := make([]*ZkNode, 0, 8)
	kind = bson.NextByte(buf)
	for kind != bson.EOO {
		if kind != bson.Object {
			panic(bson.NewBsonError("Unexpected data type %v for %v", kind, name))
		}
		bson.SkipIndex(buf)
		zkNode := &ZkNode{}
		zkNode.UnmarshalBson(buf, kind)
		values = append(values, zkNode)
		kind = bson.NextByte(buf)
	}
	return values
}

func (zkNodeV *ZkNodeV) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	bson.EncodeOptionalPrefix(buf, bson.Object, key)
	lenWriter := bson.NewLenWriter(buf)

	marshalZkNodeArray(buf, "Nodes", zkNodeV.Nodes)

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (zkNodeV *ZkNodeV) UnmarshalBson(buf *bytes.Buffer, kind byte) {
	bson.VerifyObject(kind)
	bson.Next(buf, 4)

	kind = bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "Nodes":
			zkNodeV.Nodes = unmarshalZkNodeArray(buf, "ZkNodeV.Nodes", kind)
		default:
			bson.Skip(buf, kind)
		}
		kind = bson.NextByte(buf)
	}
}
