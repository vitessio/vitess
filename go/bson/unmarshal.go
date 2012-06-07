// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bson

import (
	"bytes"
	"encoding/binary"
	"errors"
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

func (self BsonError) Error() string {
	return self.Message
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
		return nil, errors.New(fmt.Sprintf("expecting pointer value, received %v", ival.Type()))
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
	return nil, errors.New(fmt.Sprintf("unrecognized type %v", ival.Type()))
}

// Flush handles the final update for map & interface objects.
func (self *valueBuilder) Flush() {
	if self.interface_.IsValid() {
		self.interface_.Set(self.val)
		self.val = self.interface_
	}
	if self.map_.IsValid() {
		self.map_.SetMapIndex(self.key, self.val)
	}
}

func (self *valueBuilder) Int64(i int64) {
	switch self.val.Kind() {
	case reflect.Int, reflect.Int32, reflect.Int64:
		self.val.SetInt(i)
	case reflect.Uint, reflect.Uint32, reflect.Uint64:
		self.val.SetUint(uint64(i))
	case reflect.Float64:
		self.val.SetFloat(float64(i))
	case reflect.Interface:
		self.val.Set(reflect.ValueOf(i))
	default:
		panic(NewBsonError("unable to convert int64 %v to %s", i, self.val.Type()))
	}
}

func (self *valueBuilder) Uint64(u uint64) {
	switch self.val.Kind() {
	case reflect.Int, reflect.Int32, reflect.Int64:
		self.val.SetInt(int64(u))
	case reflect.Uint, reflect.Uint32, reflect.Uint64:
		self.val.SetUint(u)
	case reflect.Float64:
		self.val.SetFloat(float64(u))
	case reflect.Interface:
		self.val.Set(reflect.ValueOf(u))
	default:
		panic(NewBsonError("unable to convert int64 %v to %s", u, self.val.Type()))
	}
}

func (self *valueBuilder) Int32(i int32) {
	switch self.val.Kind() {
	case reflect.Int, reflect.Int32, reflect.Int64:
		self.val.SetInt(int64(i))
	case reflect.Uint, reflect.Uint32, reflect.Uint64:
		self.val.SetUint(uint64(i))
	case reflect.Float64:
		self.val.SetFloat(float64(i))
	case reflect.Interface:
		self.val.Set(reflect.ValueOf(i))
	default:
		panic(NewBsonError("unable to convert int32 %v to %s", i, self.val.Type()))
	}
}

func (self *valueBuilder) Float64(f float64) {
	switch self.val.Kind() {
	case reflect.Float64:
		self.val.SetFloat(f)
	case reflect.Interface:
		self.val.Set(reflect.ValueOf(f))
	default:
		panic(NewBsonError("unable to convert float64 %v to %s", f, self.val.Type()))
	}
}

func (self *valueBuilder) Null() {}

func (self *valueBuilder) String(b []byte) {
	switch self.val.Kind() {
	case reflect.String:
		self.val.SetString(string(b))
	case reflect.Interface:
		self.val.Set(reflect.ValueOf(string(b)))
	default:
		self.Binary(b)
	}
}

func (self *valueBuilder) Bool(tf bool) {
	switch self.val.Kind() {
	case reflect.Bool:
		self.val.SetBool(tf)
	case reflect.Interface:
		self.val.Set(reflect.ValueOf(tf))
	default:
		panic(NewBsonError("unable to convert bool %v to %s", tf, self.val.Type()))
	}
}

func (self *valueBuilder) Datetime(t time.Time) {
	switch self.val.Kind() {
	case reflect.Struct, reflect.Interface:
		self.val.Set(reflect.ValueOf(t))
	default:
		panic(NewBsonError("unable to convert time %v to %s", t, self.val.Type()))
	}
}

func (self *valueBuilder) Array() {
	switch self.val.Kind() {
	case reflect.Array:
		// no op
	case reflect.Slice:
		if self.val.IsNil() {
			self.val.Set(reflect.MakeSlice(self.val.Type(), 0, 8))
		}
	case reflect.Interface:
		self.interface_ = self.val
		// Work around reflect's lvalue-rvalue dealio
		place_holder := make([]interface{}, 0, 8)
		self.val = reflect.ValueOf(&place_holder).Elem()
	default:
		panic(NewBsonError("unable to convert array to %s", self.val.Type()))
	}
}

func (self *valueBuilder) Binary(bindata []byte) {
	switch self.val.Kind() {
	case reflect.Array:
		if self.val.Cap() < len(bindata) {
			panic(NewBsonError("insufficient space in array. Have: %v, Need: %v", self.val.Cap(), len(bindata)))
		}
		for i := 0; i < len(bindata); i++ {
			self.val.Index(i).SetUint(uint64(bindata[i]))
		}
	case reflect.Slice:
		// Just point it to the bindata object
		self.val.Set(reflect.ValueOf(bindata))
	case reflect.String:
		self.val.SetString(string(bindata))
	case reflect.Interface:
		self.val.Set(reflect.ValueOf(bindata))
	default:
		panic(NewBsonError("unable to convert byte array %v to %s", bindata, self.val.Type()))
	}
}

func (self *valueBuilder) Elem(i int) *valueBuilder {
	if i < 0 {
		panic(NewBsonError("negative index %v for array element", i))
	}
	switch self.val.Kind() {
	case reflect.Array:
		if i < self.val.Len() {
			return ValueBuilder(self.val.Index(i))
		} else {
			panic(NewBsonError("array index %v out of bounds", i))
		}
	case reflect.Slice:
		if i >= self.val.Cap() {
			n := self.val.Cap()
			if n < 8 {
				n = 8
			}
			for n <= i {
				n *= 2
			}
			nv := reflect.MakeSlice(self.val.Type(), self.val.Len(), n)
			reflect.Copy(nv, self.val)
			self.val.Set(nv)
		}
		if self.val.Len() <= i && i < self.val.Cap() {
			self.val.SetLen(i + 1)
		}
		if i < self.val.Len() {
			return ValueBuilder(self.val.Index(i))
		} else {
			panic(NewBsonError("internal error, realloc failed?"))
		}
	}
	panic(NewBsonError("unexpected type %s, expecting slice or array", self.val.Type()))
}

func (self *valueBuilder) Object() {
	switch self.val.Kind() {
	case reflect.Map:
		if self.val.IsNil() {
			self.val.Set(reflect.MakeMap(self.val.Type()))
		}
	case reflect.Struct:
		// no op
	case reflect.Interface:
		map_ := reflect.ValueOf(make(map[string]interface{}))
		self.val.Set(map_)
		self.val = map_
	default:
		panic(NewBsonError("unexpected type %s, expecting composite type", self.val.Type()))
	}
}

func (self *valueBuilder) Key(k string) *valueBuilder {
	switch self.val.Kind() {
	case reflect.Struct:
		t := self.val.Type()
		for i := 0; i < t.NumField(); i++ {
			if t.Field(i).Name == k {
				return ValueBuilder(self.val.Field(i))
			}
		}
		panic(NewBsonError("Could not find field '%s' in struct object", k))
	case reflect.Map:
		t := self.val.Type()
		if t.Key() != reflect.TypeOf(k) {
			break
		}
		key := reflect.ValueOf(k)
		return MapBuilder(t.Elem(), self.val, key)
	case reflect.Slice, reflect.Array:
		if self.isSimple {
			self.isSimple = false
			return self
		}
		index, err := strconv.Atoi(k)
		if err != nil {
			panic(BsonError{err.Error()})
		}
		return self.Elem(index)
	case reflect.Float64, reflect.String, reflect.Bool,
		reflect.Int, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint32, reflect.Uint64:
		// Special case. We're unmarshaling into a simple type.
		if self.isSimple {
			self.isSimple = false
			return self
		}
	}
	panic(NewBsonError("%s not supported as a BSON document", self.val.Type()))
}

type Unmarshaler interface {
	UnmarshalBson(buf *bytes.Buffer)
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
		unmarshaler.UnmarshalBson(buf)
		return
	}

	sb, terr := TopLevelBuilder(val)
	if terr != nil {
		return terr
	}
	buf.Next(4)
	Parse(buf, sb)
	sb.Flush()
	return
}

func Parse(buf *bytes.Buffer, builder *valueBuilder) {
	kind, _ := buf.ReadByte()

	for kind != EOO {
		b2 := builder.Key(ReadCString(buf))

		switch kind {
		case Number:
			ui64 := Pack.Uint64(buf.Next(8))
			fl64 := math.Float64frombits(ui64)
			b2.Float64(fl64)
		case String:
			l := int(Pack.Uint32(buf.Next(4)))
			s := buf.Next(l - 1)
			buf.ReadByte()
			b2.String(s)
		case Object:
			b2.Object()
			buf.Next(4)
			Parse(buf, b2)
		case Array:
			b2.Array()
			buf.Next(4)
			Parse(buf, b2)
		case Binary:
			l := int(Pack.Uint32(buf.Next(4)))
			buf.Next(1) // Skip the subtype, we don't care
			b2.Binary(buf.Next(l))
		case Boolean:
			b, _ := buf.ReadByte()
			if b == 1 {
				b2.Bool(true)
			} else {
				b2.Bool(false)
			}
		case Datetime:
			ui64 := Pack.Uint64(buf.Next(8))
			b2.Datetime(time.Unix(0, int64(ui64)*1e6).UTC())
		case Int:
			ui32 := Pack.Uint32(buf.Next(4))
			b2.Int32(int32(ui32))
		case Long:
			ui64 := Pack.Uint64(buf.Next(8))
			b2.Int64(int64(ui64))
		case Ulong:
			ui64 := Pack.Uint64(buf.Next(8))
			b2.Uint64(ui64)
		case Null:
			// no op
		default:
			panic(NewBsonError("don't know how to handle kind %v yet", kind))
		}
		b2.Flush()

		kind, _ = buf.ReadByte()
	}
}
