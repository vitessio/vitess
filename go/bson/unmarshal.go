// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bson

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"time"
)

// Unmarshaler is the interface that needs to be satisfied
// by types that want to perform custom unmarshaling.
// If kind is EOO, then the type is being unmarshalled
// as a top level object. Otherwise, it's an embedded
// object, and kind will need to be type-checked
// before unmarshaling.
type Unmarshaler interface {
	UnmarshalBson(buf *bytes.Buffer, kind byte)
}

func (builder *valueBuilder) canUnMarshal() Unmarshaler {
	// Don't use custom unmarshalers for map values.
	// It loses symmetry.
	if builder.map_.IsValid() {
		return nil
	}
	if builder.val.CanAddr() {
		if unmarshaler, ok := builder.val.Addr().Interface().(Unmarshaler); ok {
			return unmarshaler
		}
	}
	return nil
}

// Unmarshal unmarshals b into val.
func Unmarshal(b []byte, val interface{}) (err error) {
	return UnmarshalFromBuffer(bytes.NewBuffer(b), val)
}

// UnmarshalFromStream unmarshals from reader into val.
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
	return UnmarshalFromBuffer(bytes.NewBuffer(b), val)
}

// UnmarshalFromBuffer unmarshals from buf into val.
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
	decodeDocument(buf, sb, EOO)
	sb.save()
	return
}

func decodeDocument(buf *bytes.Buffer, builder *valueBuilder, kind byte) {
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
		if unmarshaler := b2.canUnMarshal(); unmarshaler != nil {
			unmarshaler.UnmarshalBson(buf, kind)
			continue
		}
		switch b2.val.Kind() {
		case reflect.String:
			b2.setString(DecodeString(buf, kind))
		case reflect.Int64:
			b2.setInt(DecodeInt64(buf, kind))
		case reflect.Int32:
			b2.setInt(int64(DecodeInt32(buf, kind)))
		case reflect.Int:
			b2.setInt(int64(DecodeInt(buf, kind)))
		case reflect.Uint64:
			b2.setUint(DecodeUint64(buf, kind))
		case reflect.Uint32:
			b2.setUint(uint64(DecodeUint32(buf, kind)))
		case reflect.Uint:
			b2.setUint(uint64(DecodeUint(buf, kind)))
		case reflect.Float64:
			b2.setFloat(DecodeFloat64(buf, kind))
		case reflect.Bool:
			b2.setBool(DecodeBool(buf, kind))
		case reflect.Struct:
			if b2.val.Type() == timeType {
				b2.setTime(DecodeTime(buf, kind))
			} else {
				decodeDocument(buf, b2, kind)
			}
		case reflect.Map, reflect.Array:
			decodeDocument(buf, b2, kind)
		case reflect.Slice:
			if b2.val.Type() == bytesType {
				b2.setBytes(DecodeBinary(buf, kind))
			} else {
				decodeDocument(buf, b2, kind)
			}
		case reflect.Interface:
			b2.setInterface(DecodeInterface(buf, kind))
		}
		b2.save()
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
	if val == nil {
		return nil, fmt.Errorf("expecting non-nil value to unmarshal into")
	}
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
	if k == MAGICTAG {
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
	if k == MAGICTAG {
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
		if t.Key().Kind() != reflect.String {
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

func (builder *valueBuilder) setInt(i int64) {
	builder.val.SetInt(i)
}

func (builder *valueBuilder) setUint(u uint64) {
	builder.val.SetUint(u)
}

func (builder *valueBuilder) setFloat(f float64) {
	builder.val.SetFloat(f)
}

func (builder *valueBuilder) setString(s string) {
	builder.val.SetString(s)
}

func (builder *valueBuilder) setBool(tf bool) {
	builder.val.SetBool(tf)
}

func (builder *valueBuilder) setTime(t time.Time) {
	builder.val.Set(reflect.ValueOf(t))
}

func (builder *valueBuilder) setBytes(b []byte) {
	builder.val.Set(reflect.ValueOf(b))
}

func (builder *valueBuilder) setInterface(i interface{}) {
	builder.val.Set(reflect.ValueOf(i))
}
