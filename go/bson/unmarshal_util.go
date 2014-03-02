// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Utility functions for custom decoders

package bson

import (
	"bytes"
	"math"
	"time"

	"github.com/youtube/vitess/go/hack"
)

var (
	emptybytes = []byte{}
)

func VerifyObject(kind byte) {
	if kind != EOO && kind != Object {
		panic(NewBsonError("unexpected kind: %v", kind))
	}
}

func DecodeFloat64(buf *bytes.Buffer, kind byte) float64 {
	switch kind {
	case Number:
		return float64(math.Float64frombits(Pack.Uint64(Next(buf, 8))))
	case Null:
		return 0
	}
	panic(NewBsonError("Unexpected data type %v for int", kind))
}

func DecodeString(buf *bytes.Buffer, kind byte) string {
	switch kind {
	case String:
		l := int(Pack.Uint32(Next(buf, 4)))
		s := Next(buf, l-1)
		NextByte(buf)
		return string(s)
	case Binary:
		l := int(Pack.Uint32(Next(buf, 4)))
		NextByte(buf)
		return string(Next(buf, l))
	case Null:
		return ""
	}
	panic(NewBsonError("Unexpected data type %v for string", kind))
}

func DecodeBool(buf *bytes.Buffer, kind byte) bool {
	switch kind {
	case Boolean:
		b, _ := buf.ReadByte()
		return (b != 0)
	case Int:
		return (Pack.Uint32(Next(buf, 4)) != 0)
	case Long, Ulong:
		return (Pack.Uint64(Next(buf, 8)) != 0)
	case Null:
		return false
	default:
		panic(NewBsonError("Unexpected data type %v for boolean", kind))
	}
}

func DecodeInt64(buf *bytes.Buffer, kind byte) int64 {
	switch kind {
	case Int:
		return int64(int32(Pack.Uint32(Next(buf, 4))))
	case Long, Ulong:
		return int64(Pack.Uint64(Next(buf, 8)))
	case Null:
		return 0
	}
	panic(NewBsonError("Unexpected data type %v for int", kind))
}

func DecodeInt32(buf *bytes.Buffer, kind byte) int32 {
	switch kind {
	case Int:
		return int32(Pack.Uint32(Next(buf, 4)))
	case Null:
		return 0
	}
	panic(NewBsonError("Unexpected data type %v for int", kind))
}

func DecodeInt(buf *bytes.Buffer, kind byte) int {
	switch kind {
	case Int:
		return int(Pack.Uint32(Next(buf, 4)))
	case Long, Ulong:
		return int(Pack.Uint64(Next(buf, 8)))
	case Null:
		return 0
	}
	panic(NewBsonError("Unexpected data type %v for int", kind))
}

func DecodeUint64(buf *bytes.Buffer, kind byte) uint64 {
	switch kind {
	case Int:
		return uint64(Pack.Uint32(Next(buf, 4)))
	case Long, Ulong:
		return Pack.Uint64(Next(buf, 8))
	case Null:
		return 0
	}
	panic(NewBsonError("Unexpected data type %v for int", kind))
}

func DecodeUint32(buf *bytes.Buffer, kind byte) uint32 {
	switch kind {
	case Int:
		return Pack.Uint32(Next(buf, 4))
	case Ulong:
		return uint32(Pack.Uint64(Next(buf, 8)))
	case Null:
		return 0
	}
	panic(NewBsonError("Unexpected data type %v for int", kind))
}

func DecodeUint(buf *bytes.Buffer, kind byte) uint {
	switch kind {
	case Int:
		return uint(Pack.Uint32(Next(buf, 4)))
	case Long, Ulong:
		return uint(Pack.Uint64(Next(buf, 8)))
	case Null:
		return 0
	}
	panic(NewBsonError("Unexpected data type %v for int", kind))
}

func DecodeBinary(buf *bytes.Buffer, kind byte) []byte {
	switch kind {
	case String:
		l := int(Pack.Uint32(Next(buf, 4)))
		b := Next(buf, l-1)
		NextByte(buf)
		return b
	case Binary:
		l := int(Pack.Uint32(Next(buf, 4)))
		NextByte(buf)
		return Next(buf, l)
	case Null:
		return emptybytes
	}
	panic(NewBsonError("Unexpected data type %v for string", kind))
}

func DecodeTime(buf *bytes.Buffer, kind byte) time.Time {
	switch kind {
	case Datetime:
		ui64 := Pack.Uint64(Next(buf, 8))
		return time.Unix(0, int64(ui64)*1e6).UTC()
	case Null:
		return time.Time{}
	}
	panic(NewBsonError("Unexpected data type %v for time", kind))
}

func DecodeInterface(buf *bytes.Buffer, kind byte) interface{} {
	switch kind {
	case Number:
		return DecodeFloat64(buf, kind)
	case String:
		return DecodeString(buf, kind)
	case Object:
		return DecodeMap(buf, kind)
	case Array:
		return DecodeArray(buf, kind)
	case Binary:
		return DecodeBinary(buf, kind)
	case Boolean:
		return DecodeBool(buf, kind)
	case Datetime:
		return DecodeTime(buf, kind)
	case Null:
		return nil
	case Int:
		return DecodeInt32(buf, kind)
	case Long:
		return DecodeInt64(buf, kind)
	case Ulong:
		return DecodeUint64(buf, kind)
	}
	panic(NewBsonError("Unexpected bson kind %v for interface", kind))
}

func DecodeMap(buf *bytes.Buffer, kind byte) map[string]interface{} {
	result := make(map[string]interface{})
	Next(buf, 4)
	for kind := NextByte(buf); kind != EOO; kind = NextByte(buf) {
		key := ReadCString(buf)
		if kind == Null {
			result[key] = nil
			continue
		}
		result[key] = DecodeInterface(buf, kind)
	}
	return result
}

func DecodeArray(buf *bytes.Buffer, kind byte) []interface{} {
	result := make([]interface{}, 0, 8)
	Next(buf, 4)
	for kind := NextByte(buf); kind != EOO; kind = NextByte(buf) {
		ReadCString(buf)
		if kind == Null {
			result = append(result, nil)
			continue
		}
		result = append(result, DecodeInterface(buf, kind))
	}
	return result
}

func DecodeStringArray(buf *bytes.Buffer, kind byte) []string {
	switch kind {
	case Array:
		// valid
	case Null:
		return nil
	default:
		panic(NewBsonError("Unexpected data type %v for string array", kind))
	}

	Next(buf, 4)
	values := make([]string, 0, 8)
	kind = NextByte(buf)
	for kind != EOO {
		if kind != Binary {
			panic(NewBsonError("Unexpected data type %v for string array", kind))
		}
		SkipIndex(buf)
		values = append(values, DecodeString(buf, kind))
		kind = NextByte(buf)
	}
	return values
}

func SkipIndex(buf *bytes.Buffer) {
	ReadCString(buf)
}

func ReadCString(buf *bytes.Buffer) string {
	index := bytes.IndexByte(buf.Bytes(), 0)
	if index < 0 {
		panic(NewBsonError("Unexpected EOF"))
	}
	// Read including null termination, but
	// return the string without the null.
	return hack.String(Next(buf, index+1)[:index])
}

func Next(buf *bytes.Buffer, n int) []byte {
	b := buf.Next(n)
	if len(b) != n {
		panic(NewBsonError("Unexpected EOF"))
	}
	return b[:n:n]
}

func NextByte(buf *bytes.Buffer) byte {
	return Next(buf, 1)[0]
}
