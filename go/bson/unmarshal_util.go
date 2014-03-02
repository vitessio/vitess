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

// VerifyObject verifies kind to make sure it's
// either a top level document (EOO) or an Object.
func VerifyObject(kind byte) {
	if kind != EOO && kind != Object {
		panic(NewBsonError("unexpected kind: %v", kind))
	}
}

// DecodeFloat64 decodes a float64 from buf.
// Allowed types: Number, Null.
func DecodeFloat64(buf *bytes.Buffer, kind byte) float64 {
	switch kind {
	case Number:
		return float64(math.Float64frombits(Pack.Uint64(Next(buf, 8))))
	case Null:
		return 0
	}
	panic(NewBsonError("Unexpected data type %v for int", kind))
}

// DecodeString decodes a string from buf.
// Allowed types: String, Binary, Null,
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

// DecodeBool decodes a bool from buf.
// Allowed types: Boolean, Int, Long, Ulong, Null.
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

// DecodeInt64 decodes a int64 from buf.
// Allowed types: Int, Long, Ulong, Null.
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

// DecodeInt32 decodes a int32 from buf.
// Allowed types: Int, Null.
func DecodeInt32(buf *bytes.Buffer, kind byte) int32 {
	switch kind {
	case Int:
		return int32(Pack.Uint32(Next(buf, 4)))
	case Null:
		return 0
	}
	panic(NewBsonError("Unexpected data type %v for int", kind))
}

// DecodeInt decodes a int64 from buf.
// Allowed types: Int, Long, Ulong, Null.
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

// DecodeUint64 decodes a uint64 from buf.
// Allowed types: Int, Long, Ulong, Null.
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

// DecodeUint32 decodes a uint32 from buf.
// Allowed types: Int, Long, Null.
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

// DecodeUint decodes a uint64 from buf.
// Allowed types: Int, Long, Ulong, Null.
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

// DecodeBinary decodes a []byte from buf.
// Allowed types: String, Binary, Null.
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
		return nil
	}
	panic(NewBsonError("Unexpected data type %v for string", kind))
}

// DecodeBinary decodes a time.Time from buf.
// Allowed types: Datetime, Null.
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

// DecodeInterface decodes the next object into an interface.
// Object is decoded as map[string]interface{}.
// Array is decoded as []interface{}
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

// DecodeMap decodes a map[string]interface{} from buf.
// Allowed types: Object, Null.
func DecodeMap(buf *bytes.Buffer, kind byte) map[string]interface{} {
	switch kind {
	case Object:
		// valid
	case Null:
		return nil
	default:
		panic(NewBsonError("Unexpected data type %v for string array", kind))
	}

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

// DecodeMap decodes a []interface{} from buf.
// Allowed types: Array, Null.
func DecodeArray(buf *bytes.Buffer, kind byte) []interface{} {
	switch kind {
	case Array:
		// valid
	case Null:
		return nil
	default:
		panic(NewBsonError("Unexpected data type %v for string array", kind))
	}

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

// DecodeMap decodes a []string from buf.
// Allowed types: Array, Null.
func DecodeStringArray(buf *bytes.Buffer, kind byte) []string {
	switch kind {
	case Array:
		// valid
	case Null:
		return nil
	default:
		panic(NewBsonError("Unexpected data type %v for string array", kind))
	}

	result := make([]string, 0, 8)
	Next(buf, 4)
	for kind := NextByte(buf); kind != EOO; kind = NextByte(buf) {
		if kind != Binary {
			panic(NewBsonError("Unexpected data type %v for string array", kind))
		}
		SkipIndex(buf)
		result = append(result, DecodeString(buf, kind))
	}
	return result
}

// Skip will skip the next field we don't want to read.
func Skip(buf *bytes.Buffer, kind byte) {
	switch kind {
	case Number, Datetime, Long, Ulong:
		Next(buf, 8)
	case String:
		// length of a string includes the 0 at the end, but not the size
		l := int(Pack.Uint32(Next(buf, 4)))
		Next(buf, l)
	case Object, Array:
		// the encoded length includes the 4 bytes for the size
		l := int(Pack.Uint32(Next(buf, 4)))
		if l < 4 {
			panic(NewBsonError("Object or Array should at least be 4 bytes long"))
		}
		Next(buf, l-4)
	case Binary:
		// length of a binary doesn't include the subtype
		l := int(Pack.Uint32(Next(buf, 4)))
		Next(buf, l+1)
	case Boolean:
		buf.ReadByte()
	case Int:
		Next(buf, 4)
	case Null:
		// no op
	default:
		panic(NewBsonError("don't know how to skip kind %v yet", kind))
	}
}

// SkipIndex must be used to skip indexes in arrays.
func SkipIndex(buf *bytes.Buffer) {
	ReadCString(buf)
}

// ReadCString reads the the bson document tag.
func ReadCString(buf *bytes.Buffer) string {
	index := bytes.IndexByte(buf.Bytes(), 0)
	if index < 0 {
		panic(NewBsonError("Unexpected EOF"))
	}
	// Read including null termination, but
	// return the string without the null.
	return hack.String(Next(buf, index+1)[:index])
}

// Next returns the next n bytes from buf.
func Next(buf *bytes.Buffer, n int) []byte {
	b := buf.Next(n)
	if len(b) != n {
		panic(NewBsonError("Unexpected EOF"))
	}
	return b[:n:n]
}

// NextByte returns the next byte from buf.
func NextByte(buf *bytes.Buffer) byte {
	return Next(buf, 1)[0]
}
