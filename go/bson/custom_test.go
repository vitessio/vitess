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

const (
	bsonString      = "\x05\x00\x00\x00test\x00"
	bsonBinary      = "\x04\x00\x00\x00\x00test"
	bsonInt         = "\x01\x00\x00\x00"
	bsonLong        = "\x01\x00\x00\x00\x00\x00\x00\x00"
	bsonNumber      = "\x00\x00\x00\x00\x00\x00\xf0?"
	bsonDatetime    = "\x88\xf2\\\x8d\b\x01\x00\x00"
	bsonBoolean     = "\x01"
	bsonObject      = "\x14\x00\x00\x00\x05Val2\x00\x04\x00\x00\x00\x00test\x00"
	bsonObjectNull  = "\v\x00\x00\x00\nVal2\x00\x00"
	bsonArray       = "\x11\x00\x00\x00\x050\x00\x04\x00\x00\x00\x00test\x00"
	bsonArrayNull   = "\x13\x00\x00\x00\n0\x00\x121\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00"
	bsonStringArray = "\x1f\x00\x00\x00\x050\x00\x05\x00\x00\x00\x00test1\x051\x00\x05\x00\x00\x00\x00test2\x00"
)

func stringDecoder(buf *bytes.Buffer, kind byte) interface{}      { return DecodeString(buf, kind) }
func binaryDecoder(buf *bytes.Buffer, kind byte) interface{}      { return DecodeBinary(buf, kind) }
func int64Decoder(buf *bytes.Buffer, kind byte) interface{}       { return DecodeInt64(buf, kind) }
func int32Decoder(buf *bytes.Buffer, kind byte) interface{}       { return DecodeInt32(buf, kind) }
func intDecoder(buf *bytes.Buffer, kind byte) interface{}         { return DecodeInt(buf, kind) }
func uint64Decoder(buf *bytes.Buffer, kind byte) interface{}      { return DecodeUint64(buf, kind) }
func uint32Decoder(buf *bytes.Buffer, kind byte) interface{}      { return DecodeUint32(buf, kind) }
func uintDecoder(buf *bytes.Buffer, kind byte) interface{}        { return DecodeUint(buf, kind) }
func float64Decoder(buf *bytes.Buffer, kind byte) interface{}     { return DecodeFloat64(buf, kind) }
func boolDecoder(buf *bytes.Buffer, kind byte) interface{}        { return DecodeBool(buf, kind) }
func timeDecoder(buf *bytes.Buffer, kind byte) interface{}        { return DecodeTime(buf, kind) }
func interfaceDecoder(buf *bytes.Buffer, kind byte) interface{}   { return DecodeInterface(buf, kind) }
func mapDecoder(buf *bytes.Buffer, kind byte) interface{}         { return DecodeMap(buf, kind) }
func arrayDecoder(buf *bytes.Buffer, kind byte) interface{}       { return DecodeArray(buf, kind) }
func skipDecoder(buf *bytes.Buffer, kind byte) interface{}        { Skip(buf, kind); return nil }
func stringArrayDecoder(buf *bytes.Buffer, kind byte) interface{} { return DecodeStringArray(buf, kind) }

var customUnmarshalCases = []struct {
	desc    string
	in      string
	kind    byte
	decoder func(buf *bytes.Buffer, kind byte) interface{}
	out     interface{}
}{
	{"String->string", bsonString, String, stringDecoder, "test"},
	{"Binary->string", bsonBinary, Binary, stringDecoder, "test"},
	{"Null->string", "", Null, stringDecoder, ""},
	{"String->bytes", bsonString, String, binaryDecoder, []byte("test")},
	{"Binary->bytes", bsonBinary, Binary, binaryDecoder, []byte("test")},
	{"Null->bytes", "", Null, binaryDecoder, []byte(nil)},
	{"Int->int64", bsonInt, Int, int64Decoder, int64(1)},
	{"Long->int64", bsonLong, Long, int64Decoder, int64(1)},
	{"Ulong->int64", bsonLong, Ulong, int64Decoder, int64(1)},
	{"Null->int64", "", Null, int64Decoder, int64(0)},
	{"Int->int32", bsonInt, Int, int32Decoder, int32(1)},
	{"Null->int32", "", Null, int32Decoder, int32(0)},
	{"Int->int", bsonInt, Int, intDecoder, int(1)},
	{"Long->int", bsonLong, Long, intDecoder, int(1)},
	{"Ulong->int", bsonLong, Ulong, intDecoder, int(1)},
	{"Null->int", "", Null, intDecoder, int(0)},
	{"Int->uint64", bsonInt, Int, uint64Decoder, uint64(1)},
	{"Long->uint64", bsonLong, Long, uint64Decoder, uint64(1)},
	{"Ulong->uint64", bsonLong, Ulong, uint64Decoder, uint64(1)},
	{"Null->uint64", "", Null, uint64Decoder, uint64(0)},
	{"Int->uint32", bsonInt, Int, uint32Decoder, uint32(1)},
	{"Ulong->uint32", bsonLong, Ulong, uint32Decoder, uint32(1)},
	{"Null->uint32", "", Null, uint32Decoder, uint32(0)},
	{"Int->uint", bsonInt, Int, uintDecoder, uint(1)},
	{"Long->uint", bsonLong, Long, uintDecoder, uint(1)},
	{"Ulong->uint", bsonLong, Ulong, uintDecoder, uint(1)},
	{"Null->uint", "", Null, uintDecoder, uint(0)},
	{"Number->float64", bsonNumber, Number, float64Decoder, float64(1.0)},
	{"Null->float64", "", Null, float64Decoder, float64(0.0)},
	{"Boolean->bool", bsonBoolean, Boolean, boolDecoder, true},
	{"Null->bool", "", Null, boolDecoder, false},
	{"Datetime->time.Time", bsonDatetime, Datetime, timeDecoder, time.Unix(1136243045, 0).UTC()},
	{"Null->time.Time", "", Null, timeDecoder, time.Time{}},
	{"Number->interface{}", bsonNumber, Number, interfaceDecoder, float64(1.0)},
	{"String->interface{}", bsonString, String, interfaceDecoder, "test"},
	{"Object->interface{}", bsonObject, Object, interfaceDecoder, map[string]interface{}{"Val2": []byte("test")}},
	{"Object->interface{} with null element", bsonObjectNull, Object, interfaceDecoder, map[string]interface{}{"Val2": nil}},
	{"Array->interface{}", bsonArray, Array, interfaceDecoder, []interface{}{[]byte("test")}},
	{"Array->interface{} with null element", bsonArrayNull, Array, interfaceDecoder, []interface{}{nil, int64(1)}},
	{"Binary->interface{}", bsonBinary, Binary, interfaceDecoder, []byte("test")},
	{"Boolean->interface{}", bsonBoolean, Boolean, interfaceDecoder, true},
	{"Datetime->interface{}", bsonDatetime, Datetime, interfaceDecoder, time.Unix(1136243045, 0).UTC()},
	{"Int->interface{}", bsonInt, Int, interfaceDecoder, int32(1)},
	{"Long->interface{}", bsonLong, Long, interfaceDecoder, int64(1)},
	{"Ulong->interface{}", bsonLong, Ulong, interfaceDecoder, uint64(1)},
	{"Null->interface{}", "", Null, interfaceDecoder, nil},
	{"Null->map[string]interface{}", "", Null, mapDecoder, map[string]interface{}(nil)},
	{"Null->[]interface{}", "", Null, arrayDecoder, []interface{}(nil)},
	{"Number->Skip", bsonNumber, Number, skipDecoder, nil},
	{"String->Skip", bsonString, String, skipDecoder, nil},
	{"Object->Skip", bsonObject, Object, skipDecoder, nil},
	{"Object->Skip with null element", bsonObjectNull, Object, skipDecoder, nil},
	{"Array->Skip", bsonArray, Array, skipDecoder, nil},
	{"Array->Skip with null element", bsonArrayNull, Array, skipDecoder, nil},
	{"Binary->Skip", bsonBinary, Binary, skipDecoder, nil},
	{"Boolean->Skip", bsonBoolean, Boolean, skipDecoder, nil},
	{"Datetime->Skip", bsonDatetime, Datetime, skipDecoder, nil},
	{"Int->Skip", bsonInt, Int, skipDecoder, nil},
	{"Long->Skip", bsonLong, Long, skipDecoder, nil},
	{"Ulong->Skip", bsonLong, Ulong, skipDecoder, nil},
	{"Null->Skip", "", Null, skipDecoder, nil},
	{"Null->map[string]interface{}", "", Null, mapDecoder, map[string]interface{}(nil)},
	{"Null->[]interface{}", "", Null, arrayDecoder, []interface{}(nil)},
	{"Array->[]string", bsonStringArray, Array, stringArrayDecoder, []string{"test1", "test2"}},
	{"Null->[]string", "", Null, stringArrayDecoder, []string(nil)},
}

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
}{
	{"string", stringDecoder, []byte{String, Binary, Null}},
	{"[]byte", binaryDecoder, []byte{String, Binary, Null}},
	{"int64", int64Decoder, []byte{Int, Long, Ulong, Null}},
	{"int32", int32Decoder, []byte{Int, Null}},
	{"int", intDecoder, []byte{Int, Long, Ulong, Null}},
	{"uint64", uint64Decoder, []byte{Int, Long, Ulong, Null}},
	{"uint32", uint32Decoder, []byte{Int, Ulong, Null}},
	{"uint", uintDecoder, []byte{Int, Long, Ulong, Null}},
	{"float64", float64Decoder, []byte{Number, Null}},
	{"bool", boolDecoder, []byte{Boolean, Int, Long, Ulong, Null}},
	{"time.Time", timeDecoder, []byte{Datetime, Null}},
	{"interface{}", interfaceDecoder, []byte{Number, String, Object, Array, Binary, Boolean, Datetime, Null, Int, Long, Ulong}},
	{"map", mapDecoder, []byte{Object, Null}},
	{"slice", arrayDecoder, []byte{Array, Null}},
	{"[]string", stringArrayDecoder, []byte{Array, Null}},
	{"skip", skipDecoder, []byte{Number, String, Object, Array, Binary, Boolean, Datetime, Null, Int, Long, Ulong}},
}

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
