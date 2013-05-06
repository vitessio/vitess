// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package zk

// Contains the bson custom marshaler / unmarshaler.
// Note this is not required, but makes the code faster.

import (
	"bytes"
	"code.google.com/p/vitess/go/bson"
	"code.google.com/p/vitess/go/bytes2"
)

func (zkPath *ZkPath) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodePrefix(buf, bson.Binary, "Path")
	bson.EncodeString(buf, zkPath.Path)

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (zkPath *ZkPath) UnmarshalBson(buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "Path":
			zkPath.Path = bson.DecodeString(buf, kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s", key))
		}
		kind = bson.NextByte(buf)
	}
}

func (zkPathV *ZkPathV) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodeStringArray(buf, "Paths", zkPathV.Paths)

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (zkPathV *ZkPathV) UnmarshalBson(buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "Paths":
			zkPathV.Paths = bson.DecodeStringArray(buf, kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s for ZkPathV.Paths", key))
		}
		kind = bson.NextByte(buf)
	}
}

func (zkStat *ZkStat) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodePrefix(buf, bson.Long, "Czxid")
	bson.EncodeUint64(buf, uint64(zkStat.czxid))

	bson.EncodePrefix(buf, bson.Long, "Mzxid")
	bson.EncodeUint64(buf, uint64(zkStat.mzxid))

	bson.EncodePrefix(buf, bson.Datetime, "CTime")
	bson.EncodeTime(buf, zkStat.cTime)

	bson.EncodePrefix(buf, bson.Datetime, "MTime")
	bson.EncodeTime(buf, zkStat.mTime)

	bson.EncodePrefix(buf, bson.Int, "Version")
	bson.EncodeUint32(buf, uint32(zkStat.version))

	bson.EncodePrefix(buf, bson.Int, "CVersion")
	bson.EncodeUint32(buf, uint32(zkStat.cVersion))

	bson.EncodePrefix(buf, bson.Int, "AVersion")
	bson.EncodeUint32(buf, uint32(zkStat.aVersion))

	bson.EncodePrefix(buf, bson.Long, "EphemeralOwner")
	bson.EncodeUint64(buf, uint64(zkStat.ephemeralOwner))

	bson.EncodePrefix(buf, bson.Int, "DataLength")
	bson.EncodeUint32(buf, uint32(zkStat.dataLength))

	bson.EncodePrefix(buf, bson.Int, "NumChildren")
	bson.EncodeUint32(buf, uint32(zkStat.numChildren))

	bson.EncodePrefix(buf, bson.Long, "Pzxid")
	bson.EncodeUint64(buf, uint64(zkStat.pzxid))

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (zkStat *ZkStat) UnmarshalBson(buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
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
			panic(bson.NewBsonError("Unrecognized tag %s for ZkStat", key))
		}
		kind = bson.NextByte(buf)
	}
}

func (zkNode *ZkNode) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodePrefix(buf, bson.Binary, "Path")
	bson.EncodeString(buf, zkNode.Path)

	bson.EncodePrefix(buf, bson.Binary, "Data")
	bson.EncodeString(buf, zkNode.Data)

	bson.EncodePrefix(buf, bson.Object, "Stat")
	zkNode.Stat.MarshalBson(buf)

	bson.EncodeStringArray(buf, "Children", zkNode.Children)

	bson.EncodePrefix(buf, bson.Boolean, "Cached")
	bson.EncodeBool(buf, zkNode.Cached)

	bson.EncodePrefix(buf, bson.Boolean, "Stale")
	bson.EncodeBool(buf, zkNode.Stale)

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (zkNode *ZkNode) UnmarshalBson(buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
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
			zkNode.Stat.UnmarshalBson(buf)
		case "Children":
			zkNode.Children = bson.DecodeStringArray(buf, kind)
		case "Cached":
			zkNode.Cached = bson.DecodeBool(buf, kind)
		case "Stale":
			zkNode.Stale = bson.DecodeBool(buf, kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s for ZkNode", key))
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
			bson.EncodePrefix(buf, bson.Object, bson.Itoa(i))
			val.MarshalBson(buf)
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
	for i := 0; kind != bson.EOO; i++ {
		if kind != bson.Object {
			panic(bson.NewBsonError("Unexpected data type %v for %v", kind, name))
		}
		bson.ExpectIndex(buf, i)
		zkNode := &ZkNode{}
		zkNode.UnmarshalBson(buf)
		values = append(values, zkNode)
		kind = bson.NextByte(buf)
	}
	return values
}

func (zkNodeV *ZkNodeV) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	marshalZkNodeArray(buf, "Nodes", zkNodeV.Nodes)

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (zkNodeV *ZkNodeV) UnmarshalBson(buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
		case "Nodes":
			zkNodeV.Nodes = unmarshalZkNodeArray(buf, "ZkNodeV.Nodes", kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s for ZkStat", key))
		}
		kind = bson.NextByte(buf)
	}
}
