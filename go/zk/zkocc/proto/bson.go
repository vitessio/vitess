package proto

import (
	"bytes"
	"code.google.com/p/vitess/go/bson"
	"code.google.com/p/vitess/go/bytes2"
	"time"
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

// FIXME(alainjobart) move these two helpers to the bson library
func marshalStringArray(buf *bytes2.ChunkedWriter, name string, values []string) {
	if values == nil {
		bson.EncodePrefix(buf, bson.Null, name)
	} else {
		bson.EncodePrefix(buf, bson.Array, name)
		lenWriter := bson.NewLenWriter(buf)
		for i, val := range values {
			bson.EncodePrefix(buf, bson.Binary, bson.Itoa(i))
			bson.EncodeString(buf, val)
		}
		buf.WriteByte(0)
		lenWriter.RecordLen()
	}
}

func unmarshalStringArray(buf *bytes.Buffer, name string, kind byte) []string {
	switch kind {
	case bson.Array:
		// valid
	case bson.Null:
		return nil
	default:
		panic(bson.NewBsonError("Unexpected data type %v for %v", kind, name))
	}

	bson.Next(buf, 4)
	values := make([]string, 0, 8)
	kind = bson.NextByte(buf)
	for i := 0; kind != bson.EOO; i++ {
		if kind != bson.Binary {
			panic(bson.NewBsonError("Unexpected data type %v for %v", kind, name))
		}
		bson.ExpectIndex(buf, i)
		values = append(values, bson.DecodeString(buf, kind))
		kind = bson.NextByte(buf)
	}
	return values
}

func (zkPathV *ZkPathV) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	marshalStringArray(buf, "Paths", zkPathV.Paths)

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
			zkPathV.Paths = unmarshalStringArray(buf, "ZkPathV.Paths", kind)
		default:
			panic(bson.NewBsonError("Unrecognized tag %s for ZkPathV.Paths", key))
		}
		kind = bson.NextByte(buf)
	}
}

func (zkStat *ZkStat) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	bson.EncodePrefix(buf, bson.Long, "Czxid")
	bson.EncodeUint64(buf, uint64(zkStat.Czxid))

	bson.EncodePrefix(buf, bson.Long, "Mzxid")
	bson.EncodeUint64(buf, uint64(zkStat.Mzxid))

	bson.EncodePrefix(buf, bson.Datetime, "CTime")
	bson.EncodeTime(buf, zkStat.CTime)

	bson.EncodePrefix(buf, bson.Datetime, "MTime")
	bson.EncodeTime(buf, zkStat.MTime)

	bson.EncodePrefix(buf, bson.Int, "Version")
	bson.EncodeUint32(buf, uint32(zkStat.Version))

	bson.EncodePrefix(buf, bson.Int, "CVersion")
	bson.EncodeUint32(buf, uint32(zkStat.CVersion))

	bson.EncodePrefix(buf, bson.Int, "AVersion")
	bson.EncodeUint32(buf, uint32(zkStat.AVersion))

	bson.EncodePrefix(buf, bson.Long, "EphemeralOwner")
	bson.EncodeUint64(buf, uint64(zkStat.EphemeralOwner))

	bson.EncodePrefix(buf, bson.Int, "DataLength")
	bson.EncodeUint32(buf, uint32(zkStat.DataLength))

	bson.EncodePrefix(buf, bson.Int, "NumChildren")
	bson.EncodeUint32(buf, uint32(zkStat.NumChildren))

	bson.EncodePrefix(buf, bson.Long, "Pzxid")
	bson.EncodeUint64(buf, uint64(zkStat.Pzxid))

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
			zkStat.Czxid = bson.DecodeInt64(buf, kind)
		case "Mzxid":
			zkStat.Mzxid = bson.DecodeInt64(buf, kind)
		case "CTime":
			ui64 := bson.Pack.Uint64(buf.Next(8))
			zkStat.CTime = time.Unix(0, int64(ui64)*1e6).UTC()
		case "MTime":
			ui64 := bson.Pack.Uint64(buf.Next(8))
			zkStat.MTime = time.Unix(0, int64(ui64)*1e6).UTC()
		case "Version":
			zkStat.Version = bson.DecodeInt(buf, kind)
		case "CVersion":
			zkStat.CVersion = bson.DecodeInt(buf, kind)
		case "AVersion":
			zkStat.AVersion = bson.DecodeInt(buf, kind)
		case "EphemeralOwner":
			zkStat.EphemeralOwner = bson.DecodeInt64(buf, kind)
		case "DataLength":
			zkStat.DataLength = bson.DecodeInt(buf, kind)
		case "NumChildren":
			zkStat.NumChildren = bson.DecodeInt(buf, kind)
		case "Pzxid":
			zkStat.Pzxid = bson.DecodeInt64(buf, kind)
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

	bson.EncodePrefix(buf, bson.Long, "Stat")
	zkNode.Stat.MarshalBson(buf)

	marshalStringArray(buf, "Children", zkNode.Children)

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
			zkNode.Stat.UnmarshalBson(buf)
		case "Children":
			zkNode.Children = unmarshalStringArray(buf, "ZkPathV.Children", kind)
		case "Cached":
			b, _ := buf.ReadByte()
			if b == 1 {
				zkNode.Cached = true
			} else {
				zkNode.Cached = false
			}
		case "Stale":
			b, _ := buf.ReadByte()
			if b == 1 {
				zkNode.Stale = true
			} else {
				zkNode.Stale = false
			}
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
