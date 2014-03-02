// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bson

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"time"
)

// BSON documents are lttle endian
var Pack = binary.LittleEndian

// Words size in bytes.
const (
	_WORD32 = 4
	_WORD64 = 8
)

const (
	EOO = iota
	Number
	String
	Object
	Array
	Binary
	Undefined // deprecated
	OID       // unsupported
	Boolean
	Datetime
	Null
	Regex         // unsupported
	Ref           // deprecated
	Code          // unsupported
	Symbol        // unsupported
	CodeWithScope // unsupported
	Int
	Timestamp // unsupported
	Long
	Ulong  = 0x3F // nonstandard extension
	MinKey = 0xFF // unsupported
	MaxKey = 0x7F // unsupported
)

type BsonError struct {
	Message string
}

func NewBsonError(format string, args ...interface{}) BsonError {
	return BsonError{fmt.Sprintf(format, args...)}
}

func (err BsonError) Error() string {
	return err.Message
}

func handleError(err *error) {
	if x := recover(); x != nil {
		*err = x.(BsonError)
	}
}

// Maps & interface values will not give you a reference to their underlying object.
// You can only update them through their Set methods.
type valueBuilder struct {
	val reflect.Value

	// if map_.IsValid(), write val to map_ using key.
	map_ reflect.Value
	key  reflect.Value

	// index tracks current index if val is an array.
	index int
}

// topLevelBuilder returns a valid unmarshalable valueBuilder or an error
func topLevelBuilder(val interface{}) (sb *valueBuilder, err error) {
	ival := reflect.ValueOf(val)
	if ival.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("expecting pointer value, received %v", ival.Type())
	}
	return newValueBuilder(ival.Elem()), nil
}

// newValuebuilder returns a valueBuilder for val. It perorms all
// necessary memory allocations.
func newValueBuilder(val reflect.Value) *valueBuilder {
	for val.Kind() == reflect.Ptr {
		if val.IsNil() {
			val.Set(reflect.New(val.Type().Elem()))
		}
		val = val.Elem()
	}
	switch val.Kind() {
	case reflect.Map:
		if val.IsNil() {
			val.Set(reflect.MakeMap(val.Type()))
		}
	case reflect.Slice:
		if val.IsNil() {
			val.Set(reflect.MakeSlice(val.Type(), 0, 8))
		}
	}
	return &valueBuilder{val: val}
}

// mapValueBuilder returns a valueBuilder that represents a map value.
// You need to call save after building the value to make sure it gets
// saved to the map.
func mapValueBuilder(typ reflect.Type, map_ reflect.Value, key reflect.Value) *valueBuilder {
	if typ.Kind() == reflect.Ptr {
		addr := reflect.New(typ.Elem())
		map_.SetMapIndex(key, addr)
		return newValueBuilder(addr.Elem())
	}
	builder := newValueBuilder(reflect.New(typ).Elem())
	builder.map_ = map_
	builder.key = key
	return builder
}

// save saves the built value into the map.
func (builder *valueBuilder) save() {
	if builder.map_.IsValid() {
		builder.map_.SetMapIndex(builder.key, builder.val)
	}
}

// setNull sets field k of builder to its zero value.
func (builder *valueBuilder) setNull(k string) {
	if k == "_Val_" {
		setZero(builder.val)
		return
	}

	switch builder.val.Kind() {
	case reflect.Struct:
		t := builder.val.Type()
		for i := 0; i < t.NumField(); i++ {
			if t.Field(i).Name == k {
				setZero(builder.val.Field(i))
				return
			}
		}
	case reflect.Map:
		t := builder.val.Type()
		if t.Key() != reflect.TypeOf(k) {
			break
		}
		key := reflect.ValueOf(k)
		zero := reflect.Zero(t.Elem())
		builder.val.SetMapIndex(key, zero)
		return
	case reflect.Array:
		if builder.index >= builder.val.Len() {
			panic(NewBsonError("array index %v out of bounds", builder.index))
		}
		ind := builder.index
		builder.index++
		setZero(builder.val.Index(ind))
		return
	case reflect.Slice:
		zero := reflect.Zero(builder.val.Type().Elem())
		builder.val.Set(reflect.Append(builder.val, zero))
		setZero(builder.val.Index(builder.val.Len() - 1))
		return
	}
}

func setZero(v reflect.Value) {
	v.Set(reflect.Zero(v.Type()))
}

// getField returns a valueBuilder based on the requested key.
// If the key is a the magic tag _Val_, it returns itself.
// If builder is a struct, it looks for a field of that name.
// If builder is a map, it creates an entry for that key.
// If buider is an array, it ignores the key and returns the next
// element of the array.
// If builder is a slice, it returns a newly appended element.
// If the key cannot be resolved, it returns null.
// key allocates memory as needed. So, bson Null values must
// be handled before calling key.
func (builder *valueBuilder) getField(k string) *valueBuilder {
	if k == "_Val_" {
		return builder
	}
	switch builder.val.Kind() {
	case reflect.Struct:
		t := builder.val.Type()
		for i := 0; i < t.NumField(); i++ {
			if t.Field(i).Name == k {
				return newValueBuilder(builder.val.Field(i))
			}
		}
		return nil
	case reflect.Map:
		t := builder.val.Type()
		if t.Key() != reflect.TypeOf(k) {
			break
		}
		key := reflect.ValueOf(k)
		return mapValueBuilder(t.Elem(), builder.val, key)
	case reflect.Array:
		if builder.index >= builder.val.Len() {
			panic(NewBsonError("array index %v out of bounds", builder.index))
		}
		ind := builder.index
		builder.index++
		return newValueBuilder(builder.val.Index(ind))
	case reflect.Slice:
		zero := reflect.Zero(builder.val.Type().Elem())
		builder.val.Set(reflect.Append(builder.val, zero))
		return newValueBuilder(builder.val.Index(builder.val.Len() - 1))
	}
	panic(NewBsonError("%s not supported as a BSON document", builder.val.Type()))
}

func (builder *valueBuilder) SetInt(i int64) {
	builder.val.SetInt(i)
}

func (builder *valueBuilder) SetUint(u uint64) {
	builder.val.SetUint(u)
}

func (builder *valueBuilder) SetFloat(f float64) {
	builder.val.SetFloat(f)
}

func (builder *valueBuilder) SetString(s string) {
	builder.val.SetString(s)
}

func (builder *valueBuilder) SetBool(tf bool) {
	builder.val.SetBool(tf)
}

func (builder *valueBuilder) SetTime(t time.Time) {
	builder.val.Set(reflect.ValueOf(t))
}

func (builder *valueBuilder) SetBytes(b []byte) {
	builder.val.Set(reflect.ValueOf(b))
}

func (builder *valueBuilder) SetInterface(i interface{}) {
	builder.val.Set(reflect.ValueOf(i))
}

func (builder *valueBuilder) CanUnmarshal() Unmarshaler {
	if builder.val.CanAddr() {
		if unmarshaler, ok := builder.val.Addr().Interface().(Unmarshaler); ok {
			return unmarshaler
		}
	}
	return nil
}

type Unmarshaler interface {
	UnmarshalBson(buf *bytes.Buffer, kind byte)
}

func Unmarshal(b []byte, val interface{}) (err error) {
	return UnmarshalFromBuffer(bytes.NewBuffer(b), val)
}

func UnmarshalFromStream(reader io.Reader, val interface{}) (err error) {
	lenbuf := make([]byte, 4)
	var n int
	n, err = io.ReadFull(reader, lenbuf)
	if err != nil {
		return err
	}
	if n != 4 {
		return io.ErrUnexpectedEOF
	}
	length := Pack.Uint32(lenbuf)
	b := make([]byte, length)
	Pack.PutUint32(b, length)
	n, err = io.ReadFull(reader, b[4:])
	if err != nil {
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	if n != int(length-4) {
		return io.ErrUnexpectedEOF
	}
	if val == nil {
		return nil
	}
	return UnmarshalFromBuffer(bytes.NewBuffer(b), val)
}

func UnmarshalFromBuffer(buf *bytes.Buffer, val interface{}) (err error) {
	defer handleError(&err)

	if unmarshaler, ok := val.(Unmarshaler); ok {
		unmarshaler.UnmarshalBson(buf, EOO)
		return
	}

	sb, terr := topLevelBuilder(val)
	if terr != nil {
		return terr
	}
	Parse(buf, sb, EOO)
	sb.save()
	return
}

func Parse(buf *bytes.Buffer, builder *valueBuilder, kind byte) {
	if kind != EOO && kind != Object && kind != Array {
		panic(NewBsonError("unexpected kind: %v", kind))
	}
	Next(buf, 4)
	for kind := NextByte(buf); kind != EOO; kind = NextByte(buf) {
		key := ReadCString(buf)
		if kind == Null {
			builder.setNull(key)
			continue
		}
		b2 := builder.getField(key)
		if b2 == nil {
			Skip(buf, kind)
			continue
		}
		if unmarshaler := b2.CanUnmarshal(); unmarshaler != nil {
			unmarshaler.UnmarshalBson(buf, kind)
			continue
		}
		switch b2.val.Kind() {
		case reflect.Float64:
			b2.SetFloat(DecodeFloat64(buf, kind))
		case reflect.String:
			b2.SetString(DecodeString(buf, kind))
		case reflect.Bool:
			b2.SetBool(DecodeBool(buf, kind))
		case reflect.Int64:
			b2.SetInt(DecodeInt64(buf, kind))
		case reflect.Int32:
			b2.SetInt(int64(DecodeInt32(buf, kind)))
		case reflect.Int:
			b2.SetInt(int64(DecodeInt(buf, kind)))
		case reflect.Uint64:
			b2.SetUint(DecodeUint64(buf, kind))
		case reflect.Uint32:
			b2.SetUint(uint64(DecodeUint32(buf, kind)))
		case reflect.Uint:
			b2.SetUint(uint64(DecodeUint(buf, kind)))
		case reflect.Slice:
			if b2.val.Type() == BytesType {
				b2.SetBytes(DecodeBinary(buf, kind))
			} else {
				Parse(buf, b2, kind)
			}
		case reflect.Struct:
			if b2.val.Type() == TimeType {
				b2.SetTime(DecodeTime(buf, kind))
			} else {
				Parse(buf, b2, kind)
			}
		case reflect.Map:
			Parse(buf, b2, kind)
		case reflect.Interface:
			b2.SetInterface(DecodeInterface(buf, kind))
		}
		b2.save()
	}
}

// Skip will skip a field we don't want to read
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
