// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Utility functions for custom encoders

package bson

import (
	"time"

	"github.com/youtube/vitess/go/bytes2"
)

// EncodeInterface bson encodes an interface{}. Elements
// can be basic bson encodable types, or []interface{},
// or map[string]interface{}, whose elements have to in
// turn be bson encodable.
func EncodeInterface(buf *bytes2.ChunkedWriter, key string, val interface{}) {
	if val == nil {
		EncodePrefix(buf, Null, key)
		return
	}
	switch val := val.(type) {
	case string:
		EncodeString(buf, key, val)
	case []byte:
		EncodeBinary(buf, key, val)
	case int64:
		EncodeInt64(buf, key, val)
	case int32:
		EncodeInt32(buf, key, val)
	case int:
		EncodeInt(buf, key, val)
	case uint64:
		EncodeUint64(buf, key, val)
	case uint32:
		EncodeUint32(buf, key, val)
	case uint:
		EncodeUint(buf, key, val)
	case float64:
		EncodeFloat64(buf, key, val)
	case bool:
		EncodeBool(buf, key, val)
	case map[string]interface{}:
		if val == nil {
			EncodePrefix(buf, Null, key)
			return
		}
		EncodePrefix(buf, Object, key)
		lenWriter := NewLenWriter(buf)
		for k, v := range val {
			EncodeInterface(buf, k, v)
		}
		buf.WriteByte(0)
		lenWriter.RecordLen()
	case []interface{}:
		if val == nil {
			EncodePrefix(buf, Null, key)
			return
		}
		EncodePrefix(buf, Array, key)
		lenWriter := NewLenWriter(buf)
		for i, v := range val {
			EncodeInterface(buf, Itoa(i), v)
		}
		buf.WriteByte(0)
		lenWriter.RecordLen()
	case time.Time:
		EncodeTime(buf, key, val)
	default:
		panic(NewBsonError("don't know how to marshal %T", val))
	}
}

// EncodeStringArray bson encodes a []string
func EncodeStringArray(buf *bytes2.ChunkedWriter, name string, values []string) {
	if values == nil {
		EncodePrefix(buf, Null, name)
		return
	}
	EncodePrefix(buf, Array, name)
	lenWriter := NewLenWriter(buf)
	for i, val := range values {
		EncodeString(buf, Itoa(i), val)
	}
	buf.WriteByte(0)
	lenWriter.RecordLen()
}
