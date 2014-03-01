// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bson

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"github.com/youtube/vitess/go/bytes2"
)

func TestVariety(t *testing.T) {
	in := map[string]string{"Val": "test"}
	got := VerifyMarshal(t, in)
	want := "\x13\x00\x00\x00\x05Val\x00\x04\x00\x00\x00\x00test\x00"
	if string(got) != want {
		t.Errorf("got: %q, want: %q", string(got), want)
	}

	out := make(map[string]interface{})
	err := Unmarshal(got, &out)
	got2 := string(out["Val"].([]byte))
	if got2 != in["Val"] {
		t.Errorf("got: %q, want: %q", got2, in["Val"])
	}

	var got3 string
	err = Unmarshal(got, &got3)
	if got3 != "test" {
		t.Errorf("got: %q, want: %q, err: %v", got3, "test", err)
	}

	var got4 interface{}
	err = Unmarshal(got, &got4)
	sgot4 := string(got4.(map[string]interface{})["Val"].([]byte))
	if sgot4 != "test" {
		t.Errorf("got: %q, want: %q", sgot4, "test")
	}

	type mystruct struct {
		Val string
	}
	var got5 mystruct
	err = Unmarshal(got, &got5)
	if got5.Val != "test" {
		t.Errorf("got: %q, want: %q", got5.Val, "test")
	}
}

type alltypes struct {
	Bytes   []byte
	Float64 float64
	String  string
	Bool    bool
	Time    time.Time
	Int64   int64
	Int32   int32
	Int     int
	Uint64  uint64
	Uint32  uint32
	Uint    uint
	Strings []string
	Nil     interface{}
}

func (a *alltypes) UnmarshalBson(buf *bytes.Buffer, kind byte) {
	if kind != EOO && kind != Object {
		panic(NewBsonError("unexpected kind: %v", kind))
	}
	Next(buf, 4)

	kind = NextByte(buf)
	for kind != EOO {
		key := ReadCString(buf)
		switch key {
		case "Bytes":
			verifyKind("Bytes", Binary, kind)
			a.Bytes = DecodeBinary(buf, kind)
		case "Float64":
			verifyKind("Float64", Number, kind)
			a.Float64 = DecodeFloat64(buf, kind)
		case "String":
			verifyKind("String", Binary, kind)
			// Put an easter egg here to verify the function is called
			a.String = DecodeString(buf, kind) + "1"
		case "Bool":
			verifyKind("Bool", Boolean, kind)
			a.Bool = DecodeBool(buf, kind)
		case "Time":
			verifyKind("Time", Datetime, kind)
			a.Time = DecodeTime(buf, kind)
		case "Int32":
			verifyKind("Int32", Int, kind)
			a.Int32 = DecodeInt32(buf, kind)
		case "Int":
			verifyKind("Int", Long, kind)
			a.Int = DecodeInt(buf, kind)
		case "Int64":
			verifyKind("Int64", Long, kind)
			a.Int64 = DecodeInt64(buf, kind)
		case "Uint64":
			verifyKind("Uint64", Ulong, kind)
			a.Uint64 = DecodeUint64(buf, kind)
		case "Uint32":
			verifyKind("Uint32", Ulong, kind)
			a.Uint32 = DecodeUint32(buf, kind)
		case "Uint":
			verifyKind("Uint", Ulong, kind)
			a.Uint = DecodeUint(buf, kind)
		case "Strings":
			verifyKind("Strings", Array, kind)
			a.Strings = DecodeStringArray(buf, kind)
		case "Nil":
			verifyKind("Nil", Null, kind)
		default:
			Skip(buf, kind)
		}
		kind = NextByte(buf)
	}
}

func verifyKind(tag string, want, got byte) {
	if want != got {
		panic(NewBsonError("Decode %s: got %v, want %v", tag, got, want))
	}
}

func TestUnmarshalUtil(t *testing.T) {
	a := alltypes{
		Bytes:   []byte("bytes"),
		Float64: float64(64),
		String:  "string",
		Bool:    true,
		Time:    time.Unix(1136243045, 0),
		Int64:   int64(-0x8000000000000000),
		Int32:   int32(-0x80000000),
		Int:     int(-0x80000000),
		Uint64:  uint64(0xFFFFFFFFFFFFFFFF),
		Uint32:  uint32(0xFFFFFFFF),
		Uint:    uint(0xFFFFFFFF),
		Strings: []string{"a", "b"},
		Nil:     nil,
	}
	got := VerifyMarshal(t, a)
	var out alltypes
	err := Unmarshal(got, &out)
	if err != nil {
		t.Fatalf("unmarshal fail: %v\n", err)
	}
	// Time doesn't compare well with DeepEqual
	if out.Time.Unix() != 1136243045 {
		t.Errorf("time fail: %v", out.Time)
	}
	out.Time = a.Time
	// Verify easter egg
	if out.String != "string1" {
		t.Errorf("got: %s, want %s", out.String, "string1")
	}
	out.String = "string"
	if !reflect.DeepEqual(a, out) {
		t.Errorf("got\n%+v, want\n%+v", out, a)
	}

	b := alltypes{Bytes: []byte(""), Strings: []string{"a"}}
	got = VerifyMarshal(t, b)
	var outb alltypes
	err = Unmarshal(got, &outb)
	if err != nil {
		t.Fatalf("unmarshal fail: %v\n", err)
	}
	if outb.Bytes == nil || len(outb.Bytes) != 0 {
		t.Errorf("got: %q, want nil", string(outb.Bytes))
	}
}

func TestTypes(t *testing.T) {
	in := make(map[string]interface{})
	in["bytes"] = []byte("bytes")
	in["float64"] = float64(64)
	in["string"] = "string"
	in["bool"] = true
	in["time"] = time.Unix(1136243045, 0)
	in["int64"] = int64(-0x8000000000000000)
	in["int32"] = int32(-0x80000000)
	in["int"] = int(-0x80000000)
	in["uint64"] = uint64(0xFFFFFFFFFFFFFFFF)
	in["uint32"] = uint32(0xFFFFFFFF)
	in["uint"] = uint(0xFFFFFFFF)
	in["slice"] = []interface{}{1, nil}
	in["nil"] = nil
	got := VerifyMarshal(t, in)

	out := make(map[string]interface{})
	err := Unmarshal(got, &out)
	if err != nil {
		t.Fatalf("unmarshal fail: %v\n", err)
	}

	if string(in["bytes"].([]byte)) != "bytes" {
		t.Errorf("got: %v, want %v", string(in["bytes"].([]byte)), "bytes")
	}
	if out["float64"].(float64) != float64(64) {
		t.Errorf("got: %v, want %v", out["float64"].(float64), float64(64))
	}
	if string(out["string"].([]byte)) != "string" {
		t.Errorf("got: %v, want %v", string(out["string"].([]byte)), "string")
	}
	if out["bool"].(bool) == false {
		t.Errorf("got: %v, want %v", out["bool"].(bool), false)
	}
	tm, ok := out["time"].(time.Time)
	if !ok {
		t.Errorf("got: %T, want time.Time", out["time"].(time.Time))
	}
	if tm.Unix() != 1136243045 {
		t.Errorf("got: %v, want %v", tm.Unix(), 1136243045)
	}
	if v := out["int64"].(int64); v != int64(-0x8000000000000000) {
		t.Errorf("got: %v, want %v", v, int64(-0x8000000000000000))
	}
	if v := out["int32"].(int32); v != int32(-0x80000000) {
		t.Errorf("got: %v, want %v", v, int32(-0x80000000))
	}
	if v := out["int"].(int64); v != int64(-0x80000000) {
		t.Errorf("got: %v, want %v", v, int64(-0x80000000))
	}
	if v := out["uint64"].(uint64); v != uint64(0xFFFFFFFFFFFFFFFF) {
		t.Errorf("got: %v, want %v", v, uint64(0xFFFFFFFFFFFFFFFF))
	}
	if v := out["uint32"].(uint64); v != uint64(0xFFFFFFFF) {
		t.Errorf("got: %v, want %v", v, uint64(0xFFFFFFFF))
	}
	if v := out["uint"].(uint64); v != uint64(0xFFFFFFFF) {
		t.Errorf("got: %v, want %v", v, uint64(0xFFFFFFFF))
	}
	if v := out["slice"].([]interface{})[0].(int64); v != 1 {
		t.Errorf("got: %v, want %v", v, 1)
	}
	if v := out["slice"].([]interface{})[1]; v != nil {
		t.Errorf("got: %v, want %v", v, nil)
	}
	if nilval, ok := out["nil"]; !ok || nilval != nil {
		t.Errorf("got: %v, want %v", out["nil"], nil)
	}
}

func TestBinary(t *testing.T) {
	in := map[string][]byte{"Val": []byte("test")}
	got := VerifyMarshal(t, in)
	want := "\x13\x00\x00\x00\x05Val\x00\x04\x00\x00\x00\x00test\x00"
	if string(got) != want {
		t.Errorf("got: %v, want %v", string(got), want)
	}

	out := make(map[string]interface{})
	err := Unmarshal(got, &out)
	if string(out["Val"].([]byte)) != "test" {
		t.Errorf("got: %v, want %v, err: %v", string(out["Val"].([]byte)), "test", err)
	}

	var out1 []byte
	err = Unmarshal(got, &out1)
	if string(out1) != "test" {
		t.Errorf("got: %v, want %v", string(out1), "test")
	}
}

func TestInt(t *testing.T) {
	in := map[string]int{"Val": 20}
	got := VerifyMarshal(t, in)
	want := "\x12\x00\x00\x00\x12Val\x00\x14\x00\x00\x00\x00\x00\x00\x00\x00"
	if string(got) != want {
		t.Errorf("got: %v, want %v", string(got), want)
	}

	out := make(map[string]interface{})
	err := Unmarshal(got, &out)
	if out["Val"].(int64) != 20 {
		t.Errorf("got: %v, want %v", out["Val"].(int64), 20)
	}

	var out1 int
	err = Unmarshal(got, &out1)
	if out1 != 20 {
		t.Errorf("got: %v, want %v, err: %v", out1, 20, err)
	}
}

// test that we are calling the right encoding method
// if we use the reflection code, this will fail as reflection
// cannot access the non-exported field
type PrivateStruct struct {
	veryPrivate uint64
}

// an array can use non-pointers for custom marshaler
type PrivateStructList struct {
	List []PrivateStruct
}

// the map has to be using pointers, so the custom marshaler is used
type PrivateStructMap struct {
	Map map[string]*PrivateStruct
}

func (ps *PrivateStruct) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	if key != "" {
		EncodePrefix(buf, Object, key)
	}
	lenWriter := NewLenWriter(buf)

	EncodeUint64(buf, "Type", ps.veryPrivate)

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func (ps *PrivateStruct) UnmarshalBson(buf *bytes.Buffer, kind byte) {
	if kind != EOO && kind != Object {
		panic(NewBsonError("unexpected kind: %v", kind))
	}
	Next(buf, 4)

	for kind := NextByte(buf); kind != EOO; kind = NextByte(buf) {
		key := ReadCString(buf)
		switch key {
		case "Type":
			verifyKind("Type", Ulong, kind)
			ps.veryPrivate = DecodeUint64(buf, kind)
		default:
			Skip(buf, kind)
		}
	}
}

func TestCustomMarshaler(t *testing.T) {
	// This should use the custom marshaller & unmarshaller
	s := PrivateStruct{1}
	got, err := Marshal(&s)
	if err != nil {
		t.Errorf("Marshal error: %v\n", err)
	}
	want := "\x13\x00\x00\x00?Type\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00"
	if string(got) != want {
		t.Errorf("got: %q, want: %q", string(got), want)
	}
	var s2 PrivateStruct
	err = Unmarshal(got, &s2)
	if s2 != s {
		t.Errorf("got: %#v, want: %#v, err: %v", s2, s, err)
	}

	// This should use the custom marshaller & unmarshaller
	sl := PrivateStructList{make([]PrivateStruct, 1)}
	sl.List[0] = s
	got, err = Marshal(&sl)
	if err != nil {
		t.Errorf("Marshal error: %v\n", err)
	}
	want = "&\x00\x00\x00\x04List\x00\x1b\x00\x00\x00\x030\x00\x13\x00\x00\x00?Type\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
	if string(got) != want {
		t.Errorf("got: %q, want: %q", string(got), want)
	}
	var sl2 PrivateStructList
	err = Unmarshal(got, &sl2)
	if !reflect.DeepEqual(sl2, sl) {
		t.Errorf("got: %#v, want: %#v, err: %v", sl2, sl, err)
	}

	// This should use the custom marshaller & unmarshaller
	sm := make(map[string]*PrivateStruct)
	sm["first"] = &s
	got, err = Marshal(sm)
	if err != nil {
		t.Errorf("Marshal error: %v\n", err)
	}
	want = "\x1f\x00\x00\x00\x03first\x00\x13\x00\x00\x00?Type\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00"
	if string(got) != want {
		t.Errorf("got: %q, want: %q", string(got), want)
	}
	sm2 := make(map[string]*PrivateStruct)
	err = Unmarshal(got, &sm2)
	if !reflect.DeepEqual(sm2, sm) {
		t.Errorf("got: %#v, want: %#v, err: %v", sm2, sm, err)
	}

	// This should not use the custom unmarshaller
	sm3 := make(map[string]PrivateStruct)
	err = Unmarshal(got, &sm3)
	if reflect.DeepEqual(sm3, sm) {
		t.Errorf("got: %#v, want 0 in prvateStruct, err: %v", sm3, sm, err)
	}

	// This should not use the custom marshaller
	sm4 := make(map[string]PrivateStruct)
	sm4["first"] = s
	got, err = Marshal(sm4)
	if err != nil {
		t.Errorf("Marshal error: %v\n", err)
	}
	want = "\x11\x00\x00\x00\x03first\x00\x05\x00\x00\x00\x00\x00"
	if string(got) != want {
		t.Errorf("got: %q, want: %q", string(got), want)
	}
}

func VerifyMarshal(t *testing.T, Val interface{}) []byte {
	got, err := Marshal(Val)
	if err != nil {
		t.Errorf("Marshal error: %v\n", err)
	}
	return got
}

type HasPrivate struct {
	private string
	Public  string
}

func TestIgnorePrivateFields(t *testing.T) {
	v := HasPrivate{private: "private", Public: "public"}
	marshaled := VerifyMarshal(t, v)
	unmarshaled := new(HasPrivate)
	Unmarshal(marshaled, unmarshaled)
	if unmarshaled.Public != "Public" && unmarshaled.private != "" {
		t.Errorf("private fields were not ignored: %#v", unmarshaled)
	}
}

type LotsMoreFields struct {
	CommonField1 string
	ExtraField1  string
	ExtraField2  HasPrivate
	ExtraField3  []string
	ExtraField4  int
	CommonField2 string
	ExtraField5  uint64
}

type LotsFewerFields struct {
	CommonField1 string
	CommonField2 string
}

func TestSkipUnknownFields(t *testing.T) {
	v := LotsMoreFields{
		CommonField1: "value1",
		ExtraField1:  "value2",
		ExtraField2:  HasPrivate{private: "private", Public: "public"},
		ExtraField3:  []string{"s1", "s2"},
		CommonField2: "value3",
		ExtraField4:  6455,
		ExtraField5:  345,
	}
	marshaled := VerifyMarshal(t, v)
	unmarshaled := new(LotsFewerFields)
	if err := Unmarshal(marshaled, unmarshaled); err != nil {
		t.Errorf("Marshal error: %v", err)
	}
}

var testMap map[string]interface{}
var testBlob []byte

func init() {
	in := make(map[string]interface{})
	in["bytes"] = []byte("bytes")
	in["float64"] = float64(64)
	in["string"] = "string"
	in["bool"] = true
	in["time"] = time.Unix(1136243045, 0)
	in["int64"] = int64(-0x8000000000000000)
	in["int32"] = int32(-0x80000000)
	in["int"] = int(-0x80000000)
	in["uint64"] = uint64(0xFFFFFFFFFFFFFFFF)
	in["uint32"] = uint32(0xFFFFFFFF)
	in["uint"] = uint(0xFFFFFFFF)
	in["slice"] = []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, nil}
	in["nil"] = nil
	testMap = in
	testBlob, _ = Marshal(testMap)
}

func BenchmarkMarshal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := Marshal(testMap)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkUnmarshal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := Unmarshal(testBlob, map[string]interface{}{})
		if err != nil {
			panic(err)
		}
	}
}
