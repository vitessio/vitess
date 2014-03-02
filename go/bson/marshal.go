// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bson

import (
	"io"
	"math"
	"reflect"
	"strconv"
	"time"

	"github.com/youtube/vitess/go/bytes2"
)

// LenWriter records the current write position on the buffer
// and can later be used to record the number of bytes written
// in conformance to BSON spec
type LenWriter struct {
	buf *bytes2.ChunkedWriter
	off int
	b   []byte
}

func NewLenWriter(buf *bytes2.ChunkedWriter) LenWriter {
	off := buf.Len()
	b := buf.Reserve(WORD32)
	return LenWriter{buf, off, b}
}

func (lw LenWriter) RecordLen() {
	Pack.PutUint32(lw.b, uint32(lw.buf.Len()-lw.off))
}

type Marshaler interface {
	MarshalBson(buf *bytes2.ChunkedWriter, key string)
}

func canMarshal(val reflect.Value) Marshaler {
	// Check the Marshaler interface on T.
	if marshaler, ok := val.Interface().(Marshaler); ok {
		// Don't call custom marshaler for nil values.
		if val.IsNil() {
			return nil
		}
		return marshaler
	}
	// Check the Marshaler interface on *T.
	if val.CanAddr() {
		if marshaler, ok := val.Addr().Interface().(Marshaler); ok {
			return marshaler
		}
	}
	return nil
}

type SimpleContainer struct {
	_Val_ interface{}
}

func (sc *SimpleContainer) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	EncodeOptionalPrefix(buf, Object, key)
	lenWriter := NewLenWriter(buf)
	EncodeField(buf, MAGICTAG, sc._Val_)
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

const DefaultBufferSize = 1024 * 16

func MarshalToStream(writer io.Writer, val interface{}) (err error) {
	buf := bytes2.NewChunkedWriter(DefaultBufferSize)
	if err = MarshalToBuffer(buf, val); err != nil {
		return err
	}
	_, err = buf.WriteTo(writer)
	return err
}

func Marshal(val interface{}) (encoded []byte, err error) {
	buf := bytes2.NewChunkedWriter(DefaultBufferSize)
	err = MarshalToBuffer(buf, val)
	return buf.Bytes(), err
}

func MarshalToBuffer(buf *bytes2.ChunkedWriter, val interface{}) (err error) {
	defer handleError(&err)

	if val == nil {
		return NewBsonError("Cannot marshal empty object")
	}

	// Dereference pointer types
	v := reflect.Indirect(reflect.ValueOf(val))
	if marshaler := canMarshal(v); marshaler != nil {
		marshaler.MarshalBson(buf, "")
		return
	}

	switch v.Kind() {
	case reflect.Float64, reflect.String, reflect.Bool,
		reflect.Int, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint32, reflect.Uint64:
		EncodeSimple(buf, v.Interface())
	case reflect.Struct:
		if v.Type() == timeType {
			EncodeSimple(buf, v.Interface())
		} else {
			encodeStructContent(buf, v)
		}
	case reflect.Map:
		encodeMapContent(buf, v)
	case reflect.Slice, reflect.Array:
		if v.Type() == bytesType {
			EncodeSimple(buf, v.Interface())
		} else {
			encodeSliceContent(buf, v)
		}
	default:
		return NewBsonError("Unexpected type %v\n", v.Type())
	}
	return nil
}

func EncodeSimple(buf *bytes2.ChunkedWriter, val interface{}) {
	v := SimpleContainer{_Val_: val}
	v.MarshalBson(buf, "")
}

func EncodeField(buf *bytes2.ChunkedWriter, key string, val interface{}) {
	encodeField(buf, key, reflect.ValueOf(val))
}

func encodeField(buf *bytes2.ChunkedWriter, key string, val reflect.Value) {
	if marshaler := canMarshal(val); marshaler != nil {
		marshaler.MarshalBson(buf, key)
		return
	}

	switch val.Kind() {
	case reflect.Float64:
		EncodeFloat64(buf, key, val.Float())
	case reflect.String:
		EncodeString(buf, key, val.String())
	case reflect.Bool:
		EncodeBool(buf, key, val.Bool())
	case reflect.Int64:
		EncodeInt64(buf, key, val.Int())
	case reflect.Int32:
		EncodeInt32(buf, key, int32(val.Int()))
	case reflect.Int:
		EncodeInt(buf, key, int(val.Int()))
	case reflect.Uint64:
		EncodeUint64(buf, key, uint64(val.Uint()))
	case reflect.Uint32:
		EncodeUint32(buf, key, uint32(val.Uint()))
	case reflect.Uint:
		EncodeUint(buf, key, uint(val.Uint()))
	case reflect.Struct:
		if val.Type() == timeType {
			EncodeTime(buf, key, val.Interface().(time.Time))
		} else {
			encodeStruct(buf, key, val)
		}
	case reflect.Map:
		if val.IsNil() {
			EncodePrefix(buf, Null, key)
		} else {
			encodeMap(buf, key, val)
		}
	case reflect.Slice:
		if val.IsNil() {
			EncodePrefix(buf, Null, key)
		} else if val.Type() == bytesType {
			EncodeBinary(buf, key, val.Interface().([]byte))
		} else {
			encodeSlice(buf, key, val)
		}
	case reflect.Ptr, reflect.Interface:
		if val.IsNil() {
			EncodePrefix(buf, Null, key)
		} else {
			encodeField(buf, key, val.Elem())
		}
	case reflect.Invalid: // nil interfaces are represented like this
		EncodePrefix(buf, Null, key)
	default:
		panic(NewBsonError("don't know how to marshal %v\n", val.Type()))
	}
}

func EncodeOptionalPrefix(buf *bytes2.ChunkedWriter, etype byte, key string) {
	if key == "" {
		return
	}
	EncodePrefix(buf, etype, key)
}

func EncodePrefix(buf *bytes2.ChunkedWriter, etype byte, key string) {
	b := buf.Reserve(len(key) + 2)
	b[0] = etype
	copy(b[1:], key)
	b[len(b)-1] = 0
}

func EncodeFloat64(buf *bytes2.ChunkedWriter, key string, val float64) {
	EncodePrefix(buf, Number, key)
	bits := math.Float64bits(val)
	putUint64(buf, bits)
}

func EncodeString(buf *bytes2.ChunkedWriter, key string, val string) {
	// Encode strings as binary; go strings are not necessarily unicode
	EncodePrefix(buf, Binary, key)
	putUint32(buf, uint32(len(val)))
	buf.WriteByte(0)
	buf.WriteString(val)
}

func EncodeBool(buf *bytes2.ChunkedWriter, key string, val bool) {
	EncodePrefix(buf, Boolean, key)
	if val {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
}

func EncodeInt64(buf *bytes2.ChunkedWriter, key string, val int64) {
	EncodePrefix(buf, Long, key)
	putUint64(buf, uint64(val))
}

func EncodeInt32(buf *bytes2.ChunkedWriter, key string, val int32) {
	EncodePrefix(buf, Int, key)
	putUint32(buf, uint32(val))
}

func EncodeInt(buf *bytes2.ChunkedWriter, key string, val int) {
	EncodeInt64(buf, key, int64(val))
}

func EncodeUint64(buf *bytes2.ChunkedWriter, key string, val uint64) {
	EncodePrefix(buf, Ulong, key)
	putUint64(buf, val)
}

func EncodeUint32(buf *bytes2.ChunkedWriter, key string, val uint32) {
	EncodeUint64(buf, key, uint64(val))
}

func EncodeUint(buf *bytes2.ChunkedWriter, key string, val uint) {
	EncodeUint64(buf, key, uint64(val))
}

func EncodeTime(buf *bytes2.ChunkedWriter, key string, val time.Time) {
	EncodePrefix(buf, Datetime, key)
	mtime := val.UnixNano() / 1e6
	putUint64(buf, uint64(mtime))
}

func EncodeBinary(buf *bytes2.ChunkedWriter, key string, val []byte) {
	EncodePrefix(buf, Binary, key)
	putUint32(buf, uint32(len(val)))
	buf.WriteByte(0)
	buf.Write(val)
}

func encodeStruct(buf *bytes2.ChunkedWriter, key string, val reflect.Value) {
	EncodePrefix(buf, Object, key)
	encodeStructContent(buf, val)
}

func encodeStructContent(buf *bytes2.ChunkedWriter, val reflect.Value) {
	lenWriter := NewLenWriter(buf)
	t := val.Type()
	for i := 0; i < t.NumField(); i++ {
		key := t.Field(i).Name

		// NOTE(szopa): Ignore private fields (copied from
		// encoding/json). Yes, it feels like a hack.
		if t.Field(i).PkgPath != "" {
			continue
		}
		encodeField(buf, key, val.Field(i))
	}
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func encodeMap(buf *bytes2.ChunkedWriter, key string, val reflect.Value) {
	EncodePrefix(buf, Object, key)
	encodeMapContent(buf, val)
}

// a map seems to lose the 'CanAddr' property. So if we want
// to use a custom marshaler with a struct pointer receiver, like:
//   func (ps *PrivateStruct) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
// the map has to be using pointers, i.e:
//   map[string]*PrivateStruct
// and not:
//   map[string]PrivateStruct
// (see unit test)
func encodeMapContent(buf *bytes2.ChunkedWriter, val reflect.Value) {
	lenWriter := NewLenWriter(buf)
	mt := val.Type()
	if mt.Key().Kind() != reflect.String {
		panic(NewBsonError("can't marshall maps with non-string key types"))
	}
	keys := val.MapKeys()
	for _, k := range keys {
		key := k.String()
		encodeField(buf, key, val.MapIndex(k))
	}
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func encodeSlice(buf *bytes2.ChunkedWriter, key string, val reflect.Value) {
	EncodePrefix(buf, Array, key)
	encodeSliceContent(buf, val)
}

func encodeSliceContent(buf *bytes2.ChunkedWriter, val reflect.Value) {
	lenWriter := NewLenWriter(buf)
	for i := 0; i < val.Len(); i++ {
		encodeField(buf, Itoa(i), val.Index(i))
	}
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func putUint32(buf *bytes2.ChunkedWriter, val uint32) {
	Pack.PutUint32(buf.Reserve(WORD32), val)
}

func putUint64(buf *bytes2.ChunkedWriter, val uint64) {
	Pack.PutUint64(buf.Reserve(WORD64), val)
}

var intStrMap [intAliasSize + 1]string

const (
	intAliasSize = 1024
)

func init() {
	for i := 0; i <= intAliasSize; i++ {
		intStrMap[i] = strconv.Itoa(i)
	}
}

func Itoa(i int) string {
	if i <= intAliasSize {
		return intStrMap[i]
	}
	return strconv.Itoa(i)
}
