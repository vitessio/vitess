/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,           
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY           
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package bson

import (
	"bytes"
	"io"
	"math"
	"reflect"
	"strconv"
	"time"
)

// LenWriter records the current write postion on the buffer
// and can later be used to record the number of bytes written
// in conformance to BSON spec
type LenWriter struct {
	Buf       *bytes.Buffer
	LenOffset int
}

var emptyWORD32 = make([]byte, _WORD32)
var emptyWORD64 = make([]byte, _WORD64)

func NewLenWriter(buf *bytes.Buffer) LenWriter {
	LenOffset := buf.Len()
	buf.Write(emptyWORD32)
	return LenWriter{buf, LenOffset}
}

func (self LenWriter) RecordLen() {
	buf := self.Buf.Bytes()
	final_len := len(buf)
	w32 := buf[self.LenOffset : self.LenOffset+_WORD32]
	Pack.PutUint32(w32, uint32(final_len-self.LenOffset))
}

type Marshaler interface {
	MarshalBson(buf *bytes.Buffer)
}

type SimpleContainer struct {
	_Val_ interface{}
}

func (self *SimpleContainer) MarshalBson(buf *bytes.Buffer) {
	lenWriter := NewLenWriter(buf)
	EncodeField(buf, "_Val_", self._Val_)
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

const DefaultBufferSize = 1024 * 16

func MarshalToStream(writer io.Writer, val interface{}) (err error) {
	buf := bytes.NewBuffer(make([]byte, 0, DefaultBufferSize))
	if err = MarshalToBuffer(buf, val); err != nil {
		return err
	}
	_, err = buf.WriteTo(writer)
	return err
}

func Marshal(val interface{}) (encoded []byte, err error) {
	buf := bytes.NewBuffer(make([]byte, 0, DefaultBufferSize))
	err = MarshalToBuffer(buf, val)
	return buf.Bytes(), err
}

func MarshalToBuffer(buf *bytes.Buffer, val interface{}) (err error) {
	defer handleError(&err)

	if val == nil {
		return NewBsonError("Cannot marshal empty object")
	}

	if marshaler, ok := val.(Marshaler); ok {
		marshaler.MarshalBson(buf)
		return
	}

	// Dereference pointer types
	if v := reflect.ValueOf(val); v.Kind() == reflect.Ptr {
		val = v.Elem().Interface()
	}

	switch fv := reflect.ValueOf(val); fv.Kind() {
	case reflect.Float64, reflect.String, reflect.Bool,
		reflect.Int, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint32, reflect.Uint64,
		reflect.Slice, reflect.Array:
		// Wrap simple types in a container
		val := SimpleContainer{fv.Interface()}
		val.MarshalBson(buf)
	case reflect.Struct:
		EncodeStruct(buf, fv)
	case reflect.Map:
		EncodeMap(buf, fv)
	default:
		return NewBsonError("Unexpected type %v\n", fv.Type())
	}
	return nil
}

func EncodeField(buf *bytes.Buffer, key string, val interface{}) {
	switch v := val.(type) {
	case []byte:
		EncodePrefix(buf, Binary, key)
		EncodeBinary(buf, v)
	case time.Time:
		EncodePrefix(buf, Datetime, key)
		EncodeTime(buf, v)
	default:
		goto CompositeType
	}
	return

CompositeType:
	switch fv := reflect.ValueOf(val); fv.Kind() {
	case reflect.Float64:
		EncodePrefix(buf, Number, key)
		EncodeFloat64(buf, fv.Float())
	case reflect.String:
		// Encode strings as binary; go strings are not necessarily unicode
		EncodePrefix(buf, Binary, key)
		EncodeString(buf, fv.String())
	case reflect.Bool:
		EncodePrefix(buf, Boolean, key)
		EncodeBool(buf, fv.Bool())
	case reflect.Int32:
		EncodePrefix(buf, Int, key)
		EncodeUint32(buf, uint32(fv.Int()))
	case reflect.Int, reflect.Int64:
		EncodePrefix(buf, Long, key)
		EncodeUint64(buf, uint64(fv.Int()))
	case reflect.Uint, reflect.Uint32, reflect.Uint64:
		EncodePrefix(buf, Ulong, key)
		EncodeUint64(buf, fv.Uint())
	case reflect.Struct:
		EncodePrefix(buf, Object, key)
		EncodeStruct(buf, fv)
	case reflect.Map:
		EncodePrefix(buf, Object, key)
		EncodeMap(buf, fv)
	case reflect.Slice:
		EncodePrefix(buf, Array, key)
		EncodeSlice(buf, fv)
	case reflect.Ptr:
		EncodeField(buf, key, fv.Elem().Interface())
	case reflect.Invalid: // nil interface shows up as Invalid
		EncodePrefix(buf, Null, key)
	default:
		panic(NewBsonError("don't know how to marshal %v\n", reflect.ValueOf(val).Type()))
	}
}

func EncodePrefix(buf *bytes.Buffer, etype byte, key string) {
	buf.WriteByte(etype)
	buf.WriteString(key)
	buf.WriteByte(0)
}

func EncodeFloat64(buf *bytes.Buffer, val float64) {
	bits := math.Float64bits(val)
	putUint64(buf, bits)
}

func EncodeString(buf *bytes.Buffer, val string) {
	putUint32(buf, uint32(len(val)))
	buf.WriteByte(0)
	buf.WriteString(val)
}

func EncodeBool(buf *bytes.Buffer, val bool) {
	if val {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
}

func EncodeUint32(buf *bytes.Buffer, val uint32) {
	putUint32(buf, val)
}

func EncodeUint64(buf *bytes.Buffer, val uint64) {
	putUint64(buf, val)
}

func EncodeTime(buf *bytes.Buffer, val time.Time) {
	mtime := val.UnixNano() / 1e6
	putUint64(buf, uint64(mtime))
}

func EncodeBinary(buf *bytes.Buffer, val []byte) {
	putUint32(buf, uint32(len(val)))
	buf.WriteByte(0)
	buf.Write(val)
}

func EncodeStruct(buf *bytes.Buffer, val reflect.Value) {
	lenWriter := NewLenWriter(buf)
	t := val.Type()
	for i := 0; i < t.NumField(); i++ {
		key := t.Field(i).Name
		EncodeField(buf, key, val.Field(i).Interface())
	}
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func EncodeMap(buf *bytes.Buffer, val reflect.Value) {
	lenWriter := NewLenWriter(buf)
	mt := val.Type()
	if mt.Key() != reflect.TypeOf("") {
		panic(NewBsonError("can't marshall maps with non-string key types"))
	}
	keys := val.MapKeys()
	for _, k := range keys {
		key := k.String()
		EncodeField(buf, key, val.MapIndex(k).Interface())
	}
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func EncodeSlice(buf *bytes.Buffer, val reflect.Value) {
	lenWriter := NewLenWriter(buf)
	for i := 0; i < val.Len(); i++ {
		EncodeField(buf, itoaIntened(i), val.Index(i).Interface())
	}
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func putUint32(buf *bytes.Buffer, val uint32) {
	l := buf.Len()
	buf.Write(emptyWORD32)
	w32 := buf.Bytes()[l : l+_WORD32]
	Pack.PutUint32(w32, val)
}

func putUint64(buf *bytes.Buffer, val uint64) {
	l := buf.Len()
	buf.Write(emptyWORD64)
	w64 := buf.Bytes()[l : l+_WORD64]
	Pack.PutUint64(w64, val)
}

var intStrMap map[int]string

const (
	intAliasSize = 1024
)

func init() {
	intStrMap = make(map[int]string, intAliasSize)
	for i := 0; i < intAliasSize; i++ {
		intStrMap[i] = strconv.Itoa(i)
	}
}

func itoaIntened(i int) string {
	if str, ok := intStrMap[i]; ok {
		return str
	}
	return strconv.Itoa(i)
}

var Itoa = itoaIntened

/*
func strInterned(buf []byte) string {
	hasher := fnv.New64()
	hasher.Write(buf)
	hashcode := hasher.Sum64()
	if str, ok := strIntern[hashcode]; ok {
		return str
	}
	str := string(buf)
	strIntern[hashcode] = str
	return str
}
*/
