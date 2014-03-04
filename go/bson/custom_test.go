// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bson

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
	"time"
)

var customUnmarshalCases = []struct {
	desc    string
	in      string
	kind    byte
	decoder func(buf *bytes.Buffer, kind byte) interface{}
	out     interface{}
}{{
	"String->string",
	"\x05\x00\x00\x00test\x00",
	String,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeString(buf, kind) },
	"test",
}, {
	"Binary->string",
	"\x04\x00\x00\x00\x00test",
	Binary,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeString(buf, kind) },
	"test",
}, {
	"Null->string",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeString(buf, kind) },
	"",
}, {
	"String->bytes",
	"\x05\x00\x00\x00test\x00",
	String,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeBinary(buf, kind) },
	[]byte("test"),
}, {
	"Binary->bytes",
	"\x04\x00\x00\x00\x00test",
	Binary,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeBinary(buf, kind) },
	[]byte("test"),
}, {
	"Null->bytes",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeBinary(buf, kind) },
	[]byte(nil),
}, {
	"Int->int64",
	"\x01\x00\x00\x00",
	Int,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInt64(buf, kind) },
	int64(1),
}, {
	"Long->int64",
	"\x01\x00\x00\x00\x00\x00\x00\x00",
	Long,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInt64(buf, kind) },
	int64(1),
}, {
	"Ulong->int64",
	"\x01\x00\x00\x00\x00\x00\x00\x00",
	Ulong,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInt64(buf, kind) },
	int64(1),
}, {
	"Null->int64",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInt64(buf, kind) },
	int64(0),
}, {
	"Int->int32",
	"\x01\x00\x00\x00",
	Int,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInt32(buf, kind) },
	int32(1),
}, {
	"Null->int32",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInt32(buf, kind) },
	int32(0),
}, {
	"Int->int",
	"\x01\x00\x00\x00",
	Int,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInt(buf, kind) },
	int(1),
}, {
	"Long->int",
	"\x01\x00\x00\x00\x00\x00\x00\x00",
	Long,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInt(buf, kind) },
	int(1),
}, {
	"Ulong->int",
	"\x01\x00\x00\x00\x00\x00\x00\x00",
	Ulong,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInt(buf, kind) },
	int(1),
}, {
	"Null->int",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInt(buf, kind) },
	int(0),
}, {
	"Int->uint64",
	"\x01\x00\x00\x00",
	Int,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeUint64(buf, kind) },
	uint64(1),
}, {
	"Long->uint64",
	"\x01\x00\x00\x00\x00\x00\x00\x00",
	Long,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeUint64(buf, kind) },
	uint64(1),
}, {
	"Ulong->uint64",
	"\x01\x00\x00\x00\x00\x00\x00\x00",
	Ulong,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeUint64(buf, kind) },
	uint64(1),
}, {
	"Null->uint64",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeUint64(buf, kind) },
	uint64(0),
}, {
	"Int->uint32",
	"\x01\x00\x00\x00",
	Int,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeUint32(buf, kind) },
	uint32(1),
}, {
	"Ulong->uint32",
	"\x01\x00\x00\x00\x00\x00\x00\x00",
	Ulong,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeUint32(buf, kind) },
	uint32(1),
}, {
	"Null->uint32",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeUint32(buf, kind) },
	uint32(0),
}, {
	"Int->uint",
	"\x01\x00\x00\x00",
	Int,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeUint(buf, kind) },
	uint(1),
}, {
	"Long->uint",
	"\x01\x00\x00\x00\x00\x00\x00\x00",
	Long,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeUint(buf, kind) },
	uint(1),
}, {
	"Ulong->uint",
	"\x01\x00\x00\x00\x00\x00\x00\x00",
	Ulong,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeUint(buf, kind) },
	uint(1),
}, {
	"Null->uint",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeUint(buf, kind) },
	uint(0),
}, {
	"Number->float64",
	"\x00\x00\x00\x00\x00\x00\xf0?",
	Number,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeFloat64(buf, kind) },
	float64(1.0),
}, {
	"Null->float64",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeFloat64(buf, kind) },
	float64(0.0),
}, {
	"Boolean->bool",
	"\x01",
	Boolean,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeBool(buf, kind) },
	true,
}, {
	"Null->bool",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeBool(buf, kind) },
	false,
}, {
	"Datetime->time.Time",
	"\x88\xf2\\\x8d\b\x01\x00\x00",
	Datetime,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeTime(buf, kind) },
	time.Unix(1136243045, 0).UTC(),
}, {
	"Null->time.Time",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeTime(buf, kind) },
	time.Time{},
}, {
	"Number->interface{}",
	"\x00\x00\x00\x00\x00\x00\xf0?",
	Number,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInterface(buf, kind) },
	float64(1.0),
}, {
	"String->interface{}",
	"\x05\x00\x00\x00test\x00",
	String,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInterface(buf, kind) },
	"test",
}, {
	"Object->interface{}",
	"\x14\x00\x00\x00\x05Val2\x00\x04\x00\x00\x00\x00test\x00",
	Object,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInterface(buf, kind) },
	map[string]interface{}{"Val2": []byte("test")},
}, {
	"Object->interface{} with null element",
	"\v\x00\x00\x00\nVal2\x00\x00",
	Object,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInterface(buf, kind) },
	map[string]interface{}{"Val2": nil},
}, {
	"Array->interface{}",
	"\x11\x00\x00\x00\x050\x00\x04\x00\x00\x00\x00test\x00",
	Array,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInterface(buf, kind) },
	[]interface{}{[]byte("test")},
}, {
	"Array->interface{} with null element",
	"\x13\x00\x00\x00\n0\x00\x121\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00",
	Array,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInterface(buf, kind) },
	[]interface{}{nil, int64(1)},
}, {
	"Binary->interface{}",
	"\x04\x00\x00\x00\x00test",
	Binary,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInterface(buf, kind) },
	[]byte("test"),
}, {
	"Boolean->interface{}",
	"\x01",
	Boolean,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInterface(buf, kind) },
	true,
}, {
	"Datetime->interface{}",
	"\x88\xf2\\\x8d\b\x01\x00\x00",
	Datetime,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInterface(buf, kind) },
	time.Unix(1136243045, 0).UTC(),
}, {
	"Int->interface{}",
	"\x01\x00\x00\x00",
	Int,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInterface(buf, kind) },
	int32(1),
}, {
	"Long->interface{}",
	"\x01\x00\x00\x00\x00\x00\x00\x00",
	Long,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInterface(buf, kind) },
	int64(1),
}, {
	"Ulong->interface{}",
	"\x01\x00\x00\x00\x00\x00\x00\x00",
	Ulong,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInterface(buf, kind) },
	uint64(1),
}, {
	"Null->interface{}",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInterface(buf, kind) },
	nil,
}, {
	"Null->map[string]interface{}",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeMap(buf, kind) },
	map[string]interface{}(nil),
}, {
	"Null->[]interface{}",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeArray(buf, kind) },
	[]interface{}(nil),
}, {
	"Number->Skip",
	"\x00\x00\x00\x00\x00\x00\xf0?",
	Number,
	func(buf *bytes.Buffer, kind byte) interface{} { Skip(buf, kind); return nil },
	nil,
}, {
	"String->Skip",
	"\x05\x00\x00\x00test\x00",
	String,
	func(buf *bytes.Buffer, kind byte) interface{} { Skip(buf, kind); return nil },
	nil,
}, {
	"Object->Skip",
	"\x14\x00\x00\x00\x05Val2\x00\x04\x00\x00\x00\x00test\x00",
	Object,
	func(buf *bytes.Buffer, kind byte) interface{} { Skip(buf, kind); return nil },
	nil,
}, {
	"Object->Skip with null element",
	"\v\x00\x00\x00\nVal2\x00\x00",
	Object,
	func(buf *bytes.Buffer, kind byte) interface{} { Skip(buf, kind); return nil },
	nil,
}, {
	"Array->Skip",
	"\x11\x00\x00\x00\x050\x00\x04\x00\x00\x00\x00test\x00",
	Array,
	func(buf *bytes.Buffer, kind byte) interface{} { Skip(buf, kind); return nil },
	nil,
}, {
	"Array->Skip with null element",
	"\x13\x00\x00\x00\n0\x00\x121\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00",
	Array,
	func(buf *bytes.Buffer, kind byte) interface{} { Skip(buf, kind); return nil },
	nil,
}, {
	"Binary->Skip",
	"\x04\x00\x00\x00\x00test",
	Binary,
	func(buf *bytes.Buffer, kind byte) interface{} { Skip(buf, kind); return nil },
	nil,
}, {
	"Boolean->Skip",
	"\x01",
	Boolean,
	func(buf *bytes.Buffer, kind byte) interface{} { Skip(buf, kind); return nil },
	nil,
}, {
	"Datetime->Skip",
	"\x88\xf2\\\x8d\b\x01\x00\x00",
	Datetime,
	func(buf *bytes.Buffer, kind byte) interface{} { Skip(buf, kind); return nil },
	nil,
}, {
	"Int->Skip",
	"\x01\x00\x00\x00",
	Int,
	func(buf *bytes.Buffer, kind byte) interface{} { Skip(buf, kind); return nil },
	nil,
}, {
	"Long->Skip",
	"\x01\x00\x00\x00\x00\x00\x00\x00",
	Long,
	func(buf *bytes.Buffer, kind byte) interface{} { Skip(buf, kind); return nil },
	nil,
}, {
	"Ulong->Skip",
	"\x01\x00\x00\x00\x00\x00\x00\x00",
	Ulong,
	func(buf *bytes.Buffer, kind byte) interface{} { Skip(buf, kind); return nil },
	nil,
}, {
	"Null->Skip",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { Skip(buf, kind); return nil },
	nil,
}, {
	"Null->map[string]interface{}",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeMap(buf, kind) },
	map[string]interface{}(nil),
}, {
	"Null->[]interface{}",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeArray(buf, kind) },
	[]interface{}(nil),
}, {
	"Array->[]string",
	"\x1f\x00\x00\x00\x050\x00\x05\x00\x00\x00\x00test1\x051\x00\x05\x00\x00\x00\x00test2\x00",
	Array,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeStringArray(buf, kind) },
	[]string{"test1", "test2"},
}, {
	"Null->[]string",
	"",
	Null,
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeStringArray(buf, kind) },
	[]string(nil),
}}

func TestCustomUnmarshal(t *testing.T) {
	for _, tcase := range customUnmarshalCases {
		buf := bytes.NewBuffer([]byte(tcase.in))
		got := tcase.decoder(buf, tcase.kind)
		if !reflect.DeepEqual(got, tcase.out) {
			t.Errorf("%s: received: %v, want %v", tcase.desc, got, tcase.out)
		}
		if buf.Len() != 0 {
			t.Errorf("%s: %d unread bytes from %q, want 0", tcase.desc, buf.Len(), tcase.in)
		}
	}
}

var customUnmarshalFailureCases = []struct {
	typ     string
	decoder func(buf *bytes.Buffer, kind byte) interface{}
	valid   []byte
}{{
	"string",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeString(buf, kind) },
	[]byte{String, Binary, Null},
}, {
	"[]byte",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeBinary(buf, kind) },
	[]byte{String, Binary, Null},
}, {
	"int64",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInt64(buf, kind) },
	[]byte{Int, Long, Ulong, Null},
}, {
	"int32",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInt32(buf, kind) },
	[]byte{Int, Null},
}, {
	"int",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInt(buf, kind) },
	[]byte{Int, Long, Ulong, Null},
}, {
	"uint64",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeUint64(buf, kind) },
	[]byte{Int, Long, Ulong, Null},
}, {
	"uint32",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeUint32(buf, kind) },
	[]byte{Int, Ulong, Null},
}, {
	"uint",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeUint(buf, kind) },
	[]byte{Int, Long, Ulong, Null},
}, {
	"float64",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeFloat64(buf, kind) },
	[]byte{Number, Null},
}, {
	"bool",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeBool(buf, kind) },
	[]byte{Boolean, Int, Long, Ulong, Null},
}, {
	"time.Time",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeTime(buf, kind) },
	[]byte{Datetime, Null},
}, {
	"interface{}",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeInterface(buf, kind) },
	[]byte{Number, String, Object, Array, Binary, Boolean, Datetime, Null, Int, Long, Ulong},
}, {
	"map",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeMap(buf, kind) },
	[]byte{Object, Null},
}, {
	"slice",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeArray(buf, kind) },
	[]byte{Array, Null},
}, {
	"[]string",
	func(buf *bytes.Buffer, kind byte) interface{} { return DecodeStringArray(buf, kind) },
	[]byte{Array, Null},
}, {
	"skip",
	func(buf *bytes.Buffer, kind byte) interface{} { Skip(buf, kind); return nil },
	[]byte{Number, String, Object, Array, Binary, Boolean, Datetime, Null, Int, Long, Ulong},
}}

func TestCustomUnmarshalFailures(t *testing.T) {
	allKinds := []byte{EOO, Number, String, Object, Array, Binary, Boolean, Datetime, Null, Int, Long, Ulong}
	for _, tcase := range customUnmarshalFailureCases {
		for _, kind := range allKinds {
			want := fmt.Sprintf("unexpected kind %v for %s", kind, tcase.typ)
			func() {
				defer func() {
					if x := recover(); x != nil {
						got := x.(BsonError).Error()
						if got != want {
							t.Errorf("got %s, want %s", got, want)
						}
						return
					}
				}()
				for _, valid := range tcase.valid {
					if kind == valid {
						return
					}
				}
				tcase.decoder(nil, kind)
				t.Errorf("got no error, want %s", want)
			}()
		}
	}
}
