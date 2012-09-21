// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Utility functions for custom decoders

package bson

import (
	"bytes"
	"strconv"
	"time"
)

func DecodeString(buf *bytes.Buffer, kind byte) string {
	switch kind {
	case String:
		l := int(Pack.Uint32(buf.Next(4)))
		s := buf.Next(l - 1)
		NextByte(buf)
		return string(s)
	case Binary:
		l := int(Pack.Uint32(buf.Next(4)))
		NextByte(buf)
		return string(buf.Next(l))
	case Null:
		return ""
	}
	panic(NewBsonError("Unexpected data type %v for string", kind))
}

func DecodeInt(buf *bytes.Buffer, kind byte) int {
	switch kind {
	case Int:
		return int(Pack.Uint32(buf.Next(4)))
	case Long, Ulong:
		return int(Pack.Uint64(buf.Next(8)))
	case Null:
		return 0
	}
	panic(NewBsonError("Unexpected data type %v for int", kind))
}

func DecodeInt64(buf *bytes.Buffer, kind byte) int64 {
	switch kind {
	case Int:
		return int64(int32(Pack.Uint32(buf.Next(4))))
	case Long, Ulong:
		return int64(Pack.Uint64(buf.Next(8)))
	case Null:
		return 0
	}
	panic(NewBsonError("Unexpected data type %v for int", kind))
}

func DecodeUint64(buf *bytes.Buffer, kind byte) uint64 {
	switch kind {
	case Int:
		return uint64(Pack.Uint32(buf.Next(4)))
	case Long, Ulong:
		return Pack.Uint64(buf.Next(8))
	case Null:
		return 0
	}
	panic(NewBsonError("Unexpected data type %v for int", kind))
}

func DecodeTime(buf *bytes.Buffer, kind byte) time.Time {
	switch kind {
	case Datetime:
		ui64 := Pack.Uint64(buf.Next(8))
		return time.Unix(0, int64(ui64)*1e6).UTC()
	case Null:
		return time.Time{}
	}
	panic(NewBsonError("Unexpected data type %v for time", kind))
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
	for i := 0; kind != EOO; i++ {
		if kind != Binary {
			panic(NewBsonError("Unexpected data type %v for string array", kind))
		}
		ExpectIndex(buf, i)
		values = append(values, DecodeString(buf, kind))
		kind = NextByte(buf)
	}
	return values
}

func DecodeBool(buf *bytes.Buffer, kind byte) bool {
	switch kind {
	case Boolean:
		// valid
	case Null:
		return false
	default:
		panic(NewBsonError("Unexpected data type %v for boolean", kind))
	}

	b, _ := buf.ReadByte()
	if b == 1 {
		return true
	}
	return false
}

func ExpectIndex(buf *bytes.Buffer, index int) {
	key := ReadCString(buf)
	received, err := strconv.Atoi(key)
	if err != nil {
		panic(NewBsonError("%s", err))
	}
	if received != index {
		panic(NewBsonError("non-sequential index in array. Expected: %d, Received: %d", index, received))
	}
}

func ReadCString(buf *bytes.Buffer) string {
	b, err := buf.ReadBytes(0)
	if err != nil {
		panic(NewBsonError("%s", err))
	}
	return string(b[:len(b)-1])
}

func Next(buf *bytes.Buffer, n int) []byte {
	b := buf.Next(n)
	if len(b) != n {
		panic(NewBsonError("EOF"))
	}
	return b
}

func NextByte(buf *bytes.Buffer) byte {
	return Next(buf, 1)[0]
}
