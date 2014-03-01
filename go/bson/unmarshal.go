// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bson

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"math"
	"reflect"
	"strconv"
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

	// isSimple == bool: It's the top level valueBuilder object and it has a simple value
	isSimple bool

	// if map_ != nil, write val to map_[key] when val is finalized, performed by Flush()
	map_ reflect.Value
	key  reflect.Value

	// if interface_ != nil, write val to interface_ when val is finalized, performed by Flush()
	interface_ reflect.Value
}

func ValueBuilder(val reflect.Value) *valueBuilder {
	// Dereference pointers here so we don't have to handle this case everywhere else
	for val.Kind() == reflect.Ptr {
		if val.IsNil() {
			val.Set(reflect.New(val.Type().Elem()))
		}
		val = val.Elem()
	}
	return &valueBuilder{val: val}
}

func MapBuilder(typ reflect.Type, map_ reflect.Value, key reflect.Value) *valueBuilder {
	if typ.Kind() == reflect.Ptr {
		addr := reflect.New(typ.Elem())
		map_.SetMapIndex(key, addr)
		return &valueBuilder{val: addr.Elem()}
	}
	return &valueBuilder{val: reflect.New(typ).Elem(), map_: map_, key: key}
}

// Returns a valid unmarshalable valueBuilder or an error
func TopLevelBuilder(val interface{}) (sb *valueBuilder, err error) {
	ival := reflect.ValueOf(val)
	// We'll allow one level of indirection
	if ival.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("expecting pointer value, received %v", ival.Type())
	}
	switch actual := ival.Elem(); actual.Kind() {
	case reflect.Float64, reflect.String, reflect.Bool,
		reflect.Int, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint32, reflect.Uint64,
		reflect.Slice, reflect.Array:
		sb := ValueBuilder(actual)
		sb.isSimple = true // Prepare to receive a simple value
		return sb, nil
	case reflect.Map, reflect.Struct, reflect.Interface:
		sb := ValueBuilder(actual)
		sb.Object() // Allocate memory if necessary
		return sb, nil
	}
	return nil, fmt.Errorf("unrecognized type %v", ival.Type())
}

// Flush handles the final update for map & interface objects.
func (builder *valueBuilder) Flush() {
	if builder.interface_.IsValid() {
		builder.interface_.Set(builder.val)
		builder.val = builder.interface_
	}
	if builder.map_.IsValid() {
		builder.map_.SetMapIndex(builder.key, builder.val)
	}
}

func (builder *valueBuilder) Int64(i int64) {
	switch builder.val.Kind() {
	case reflect.Int, reflect.Int32, reflect.Int64:
		builder.val.SetInt(i)
	case reflect.Uint, reflect.Uint32, reflect.Uint64:
		builder.val.SetUint(uint64(i))
	case reflect.Float64:
		builder.val.SetFloat(float64(i))
	case reflect.Interface:
		builder.val.Set(reflect.ValueOf(i))
	default:
		panic(NewBsonError("unable to convert int64 %v to %s", i, builder.val.Type()))
	}
}

func (builder *valueBuilder) Uint64(u uint64) {
	switch builder.val.Kind() {
	case reflect.Int, reflect.Int32, reflect.Int64:
		builder.val.SetInt(int64(u))
	case reflect.Uint, reflect.Uint32, reflect.Uint64:
		builder.val.SetUint(u)
	case reflect.Float64:
		builder.val.SetFloat(float64(u))
	case reflect.Interface:
		builder.val.Set(reflect.ValueOf(u))
	default:
		panic(NewBsonError("unable to convert int64 %v to %s", u, builder.val.Type()))
	}
}

func (builder *valueBuilder) Int32(i int32) {
	switch builder.val.Kind() {
	case reflect.Int, reflect.Int32, reflect.Int64:
		builder.val.SetInt(int64(i))
	case reflect.Uint, reflect.Uint32, reflect.Uint64:
		builder.val.SetUint(uint64(i))
	case reflect.Float64:
		builder.val.SetFloat(float64(i))
	case reflect.Interface:
		builder.val.Set(reflect.ValueOf(i))
	default:
		panic(NewBsonError("unable to convert int32 %v to %s", i, builder.val.Type()))
	}
}

func (builder *valueBuilder) Float64(f float64) {
	switch builder.val.Kind() {
	case reflect.Float64:
		builder.val.SetFloat(f)
	case reflect.Interface:
		builder.val.Set(reflect.ValueOf(f))
	default:
		panic(NewBsonError("unable to convert float64 %v to %s", f, builder.val.Type()))
	}
}

func (builder *valueBuilder) Null() {}

func (builder *valueBuilder) String(b []byte) {
	switch builder.val.Kind() {
	case reflect.String:
		builder.val.SetString(string(b))
	case reflect.Interface:
		builder.val.Set(reflect.ValueOf(string(b)))
	default:
		builder.Binary(b)
	}
}

func (builder *valueBuilder) Bool(tf bool) {
	switch builder.val.Kind() {
	case reflect.Bool:
		builder.val.SetBool(tf)
	case reflect.Interface:
		builder.val.Set(reflect.ValueOf(tf))
	default:
		panic(NewBsonError("unable to convert bool %v to %s", tf, builder.val.Type()))
	}
}

func (builder *valueBuilder) Datetime(t time.Time) {
	switch builder.val.Kind() {
	case reflect.Struct, reflect.Interface:
		builder.val.Set(reflect.ValueOf(t))
	default:
		panic(NewBsonError("unable to convert time %v to %s", t, builder.val.Type()))
	}
}

func (builder *valueBuilder) Array() {
	switch builder.val.Kind() {
	case reflect.Array:
		// no op
	case reflect.Slice:
		if builder.val.IsNil() {
			builder.val.Set(reflect.MakeSlice(builder.val.Type(), 0, 8))
		}
	case reflect.Interface:
		builder.interface_ = builder.val
		// Work around reflect's lvalue-rvalue dealio
		place_holder := make([]interface{}, 0, 8)
		builder.val = reflect.ValueOf(&place_holder).Elem()
	default:
		panic(NewBsonError("unable to convert array to %s", builder.val.Type()))
	}
}

func (builder *valueBuilder) Binary(bindata []byte) {
	switch builder.val.Kind() {
	case reflect.Array:
		if builder.val.Cap() < len(bindata) {
			panic(NewBsonError("insufficient space in array. Have: %v, Need: %v", builder.val.Cap(), len(bindata)))
		}
		for i := 0; i < len(bindata); i++ {
			builder.val.Index(i).SetUint(uint64(bindata[i]))
		}
	case reflect.Slice:
		// Just point it to the bindata object
		builder.val.Set(reflect.ValueOf(bindata))
	case reflect.String:
		builder.val.SetString(string(bindata))
	case reflect.Interface:
		builder.val.Set(reflect.ValueOf(bindata))
	default:
		panic(NewBsonError("unable to convert byte array %v to %s", bindata, builder.val.Type()))
	}
}

func (builder *valueBuilder) Object() {
	switch builder.val.Kind() {
	case reflect.Map:
		if builder.val.IsNil() {
			builder.val.Set(reflect.MakeMap(builder.val.Type()))
		}
	case reflect.Struct:
		// no op
	case reflect.Interface:
		map_ := reflect.ValueOf(make(map[string]interface{}))
		builder.val.Set(map_)
		builder.val = map_
	default:
		panic(NewBsonError("unexpected type %s, expecting composite type", builder.val.Type()))
	}
}

func (builder *valueBuilder) Key(k string) *valueBuilder {
	switch builder.val.Kind() {
	case reflect.Struct:
		t := builder.val.Type()
		for i := 0; i < t.NumField(); i++ {
			if t.Field(i).Name == k {
				return ValueBuilder(builder.val.Field(i))
			}
		}
		return nil
	case reflect.Map:
		t := builder.val.Type()
		if t.Key() != reflect.TypeOf(k) {
			break
		}
		key := reflect.ValueOf(k)
		return MapBuilder(t.Elem(), builder.val, key)
	case reflect.Array:
		if builder.isSimple {
			builder.isSimple = false
			return builder
		}
		index, err := strconv.Atoi(k)
		if err != nil {
			panic(BsonError{err.Error()})
		}
		if index < 0 || index >= builder.val.Len() {
			panic(NewBsonError("array index %v out of bounds", index))
		}
		return ValueBuilder(builder.val.Index(index))
	case reflect.Slice:
		if builder.isSimple {
			builder.isSimple = false
			return builder
		}
		zero := reflect.Zero(builder.val.Type().Elem())
		builder.val.Set(reflect.Append(builder.val, zero))
		return ValueBuilder(builder.val.Index(builder.val.Len() - 1))
	case reflect.Float64, reflect.String, reflect.Bool,
		reflect.Int, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint32, reflect.Uint64:
		// Special case. We're unmarshaling into a simple type.
		if builder.isSimple {
			builder.isSimple = false
			return builder
		}
	}
	panic(NewBsonError("%s not supported as a BSON document", builder.val.Type()))
}

func (builder *valueBuilder) CanUnmarshal() Unmarshaler {
	// No support for map values
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

	sb, terr := TopLevelBuilder(val)
	if terr != nil {
		return terr
	}
	Next(buf, 4)
	Parse(buf, sb)
	sb.Flush()
	return
}

func Parse(buf *bytes.Buffer, builder *valueBuilder) {
	kind, _ := buf.ReadByte()

	for kind != EOO {
		key := ReadCString(buf)
		b2 := builder.Key(key)
		if b2 == nil {
			Skip(buf, kind)
		} else if unmarshaler := b2.CanUnmarshal(); unmarshaler != nil {
			unmarshaler.UnmarshalBson(buf, kind)
		} else {
			switch kind {
			case Number:
				ui64 := Pack.Uint64(Next(buf, 8))
				fl64 := math.Float64frombits(ui64)
				b2.Float64(fl64)
			case String:
				l := int(Pack.Uint32(Next(buf, 4)))
				s := Next(buf, l-1)
				buf.ReadByte()
				b2.String(s)
			case Object:
				b2.Object()
				Next(buf, 4)
				Parse(buf, b2)
			case Array:
				b2.Array()
				Next(buf, 4)
				Parse(buf, b2)
			case Binary:
				l := int(Pack.Uint32(Next(buf, 4)))
				Next(buf, 1) // Skip the subtype, we don't care
				b2.Binary(Next(buf, l))
			case Boolean:
				b, _ := buf.ReadByte()
				if b == 1 {
					b2.Bool(true)
				} else {
					b2.Bool(false)
				}
			case Datetime:
				ui64 := Pack.Uint64(Next(buf, 8))
				b2.Datetime(time.Unix(0, int64(ui64)*1e6).UTC())
			case Int:
				ui32 := Pack.Uint32(Next(buf, 4))
				b2.Int32(int32(ui32))
			case Long:
				ui64 := Pack.Uint64(Next(buf, 8))
				b2.Int64(int64(ui64))
			case Ulong:
				ui64 := Pack.Uint64(Next(buf, 8))
				b2.Uint64(ui64)
			case Null:
				// no op
			default:
				panic(NewBsonError("don't know how to handle kind %v yet", kind))
			}
			b2.Flush()
		}

		kind, _ = buf.ReadByte()
	}
}

// Skip will skip a field we don't want to read
func Skip(buf *bytes.Buffer, kind byte) {
	switch kind {
	case Number:
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
	case Datetime:
		Next(buf, 8)
	case Int:
		Next(buf, 4)
	case Long:
		Next(buf, 8)
	case Ulong:
		Next(buf, 8)
	case Null:
		// no op
	default:
		panic(NewBsonError("don't know how to skip kind %v yet", kind))
	}
}
