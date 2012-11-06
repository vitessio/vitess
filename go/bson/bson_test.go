// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bson

import (
	"bytes"
	"testing"
	"time"

	"code.google.com/p/vitess/go/bytes2"
)

func TestVariety(t *testing.T) {
	in := map[string]string{"Val": "test"}
	encoded := VerifyMarshal(t, in)
	expected := []byte("\x13\x00\x00\x00\x05Val\x00\x04\x00\x00\x00\x00test\x00")
	compare(t, encoded, expected)

	out := make(map[string]interface{})
	err := Unmarshal(encoded, &out)
	if in["Val"] != string(out["Val"].([]byte)) {
		t.Errorf("unmarshal doesn't match input: %v\n%v\n%v\n", err, in, out)
	}

	var out1 string
	err = Unmarshal(encoded, &out1)
	if out1 != "test" {
		t.Errorf("unmarshal doesn't match input: %v\n%v\n%v\n", err, in, out1)
	}

	var out2 interface{}
	err = Unmarshal(encoded, &out2)
	if string(out2.(map[string]interface{})["Val"].([]byte)) != "test" {
		t.Errorf("unmarshal doesn't match input: %v\n%v\n%v\n", err, in, out2)
	}

	type mystruct struct {
		Val string
	}
	var out3 mystruct
	err = Unmarshal(encoded, &out3)
	if out3.Val != "test" {
		t.Errorf("unmarshal doesn't match input: %v\n%v\n%v\n", err, in, out3)
	}
}

type alltypes struct {
	Bytes   []byte
	Float64 float64
	String  string
	Bool    bool
	Time    time.Time
	Int32   int32
	Int     int
	Int64   int64
	Uint64  uint64
	Strings []string
	Nil     interface{}
}

func (a *alltypes) UnmarshalBson(buf *bytes.Buffer) {
	Next(buf, 4)

	kind := NextByte(buf)
	for kind != EOO {
		key := ReadCString(buf)
		switch key {
		case "Bytes":
			verifyKind("Bytes", Binary, kind)
			a.Bytes = DecodeBytes(buf, kind)
		case "Float64":
			verifyKind("Float64", Number, kind)
			a.Float64 = DecodeFloat64(buf, kind)
		case "String":
			verifyKind("String", Binary, kind)
			a.String = DecodeString(buf, kind)
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
		case "Strings":
			verifyKind("Strings", Array, kind)
			a.Strings = DecodeStringArray(buf, kind)
		case "Nil":
			verifyKind("Nil", Null, kind)
		default:
			panic(NewBsonError("Unrecognized tag %s", key))
		}
		kind = NextByte(buf)
	}
}

func verifyKind(tag string, expecting, actual byte) {
	if expecting != actual {
		panic(NewBsonError("Decode %s: expecting %v, actual %v", tag, expecting, actual))
	}
}

// TestCustom tests custom unmarshalling
func TestCustom(t *testing.T) {
	a := alltypes{
		Bytes:   []byte("bytes"),
		Float64: float64(64),
		String:  "string",
		Bool:    true,
		Time:    time.Unix(1136243045, 0),
		Int32:   int32(-0x80000000),
		Int:     int(-0x80000000),
		Int64:   int64(-0x8000000000000000),
		Uint64:  uint64(0xFFFFFFFFFFFFFFFF),
		Strings: []string{"a", "b"},
		Nil:     nil,
	}
	encoded := VerifyMarshal(t, a)
	var out alltypes
	err := Unmarshal(encoded, &out)
	if err != nil {
		t.Fatalf("unmarshal fail: %v\n", err)
	}
	if string(out.Bytes) != "bytes" {
		t.Errorf("bytes fail: %s", out.Bytes)
	}
	if out.Float64 != 64 {
		t.Error("float fail: %v", out.Float64)
	}
	if out.String != "string" {
		t.Errorf("string fail: %v", out.String)
	}
	if !out.Bool {
		t.Errorf("bool fail: %v", out.Bool)
	}
	if out.Time.Unix() != 1136243045 {
		t.Errorf("time fail: %v", out.Time)
	}
	if out.Int32 != -0x80000000 {
		t.Errorf("int32 fail: %v", out.Int32)
	}
	if out.Int != -0x80000000 {
		t.Errorf("int fail: %v", out.Int)
	}
	if out.Int64 != -0x8000000000000000 {
		t.Errorf("int64 fail: %v", out.Int64)
	}
	if out.Uint64 != 0xFFFFFFFFFFFFFFFF {
		t.Errorf("uint64 fail: %v", out.Uint64)
	}
	if out.Strings[0] != "a" || out.Strings[1] != "b" {
		t.Errorf("strings fail: %v", out.Strings)
	}

	b := alltypes{Bytes: []byte(""), Strings: []string{"a"}}
	encoded = VerifyMarshal(t, b)
	var outb alltypes
	err = Unmarshal(encoded, &outb)
	if err != nil {
		t.Fatalf("unmarshal fail: %v\n", err)
	}
	if outb.Bytes == nil || len(outb.Bytes) != 0 {
		t.Errorf("nil bytes fail: %s", outb.Bytes)
	}
}

func TestTypes(t *testing.T) {
	in := make(map[string]interface{})
	in["bytes"] = []byte("bytes")
	in["float64"] = float64(64)
	in["string"] = "string"
	in["bool"] = true
	in["time"] = time.Unix(1136243045, 0)
	in["int32"] = int32(-0x80000000)
	in["int"] = int(-0x80000000)
	in["int64"] = int64(-0x8000000000000000)
	in["uint"] = uint(0xFFFFFFFF)
	in["uint32"] = uint32(0xFFFFFFFF)
	in["uint64"] = uint64(0xFFFFFFFFFFFFFFFF)
	in["slice"] = []interface{}{1, nil}
	in["nil"] = nil
	encoded := VerifyMarshal(t, in)

	out := make(map[string]interface{})
	err := Unmarshal(encoded, &out)
	if err != nil {
		t.Fatalf("unmarshal fail: %v\n", err)
	}

	if string(in["bytes"].([]byte)) != "bytes" {
		t.Errorf("bytes fail")
	}
	if out["float64"].(float64) != float64(64) {
		t.Errorf("float fail")
	}
	if string(out["string"].([]byte)) != "string" {
		t.Errorf("string fail")
	}
	if out["bool"].(bool) == false {
		t.Errorf("bool fail")
	}
	tm, ok := out["time"].(time.Time)
	if !ok {
		t.Errorf("time type failed")
	}
	if tm.Unix() != 1136243045 {
		t.Error("time failed")
	}
	if v := out["int32"].(int32); v != int32(-0x80000000) {
		t.Errorf("int32 fail: %v", v)
	}
	if v := out["int"].(int64); v != int64(-0x80000000) {
		t.Errorf("int fail: %v", v)
	}
	if v := out["int64"].(int64); v != int64(-0x8000000000000000) {
		t.Errorf("int64 fail: %v", v)
	}
	if v := out["uint"].(uint64); v != uint64(0xFFFFFFFF) {
		t.Errorf("uint fail: %v", v)
	}
	if v := out["uint32"].(uint64); v != uint64(0xFFFFFFFF) {
		t.Errorf("uint32 fail: %v", v)
	}
	if v := out["uint64"].(uint64); v != uint64(0xFFFFFFFFFFFFFFFF) {
		t.Errorf("uint64 fail: %v", v)
	}
	if v := out["slice"].([]interface{})[0].(int64); v != 1 {
		t.Errorf("slice fail: %v", v)
	}
	if v := out["slice"].([]interface{})[1]; v != nil {
		t.Errorf("slice fail: %v", v)
	}
	if nilval, ok := out["nil"]; !ok || nilval != nil {
		t.Errorf("nil fail")
	}
}

func TestBinary(t *testing.T) {
	in := map[string][]byte{"Val": []byte("test")}
	encoded := VerifyMarshal(t, in)
	expected := []byte("\x13\x00\x00\x00\x05Val\x00\x04\x00\x00\x00\x00test\x00")
	compare(t, encoded, expected)

	out := make(map[string]interface{})
	err := Unmarshal(encoded, &out)
	if string(out["Val"].([]byte)) != "test" {
		t.Errorf("unmarshal doesn't match input: %v\n%v\n%v\n", err, in, out)
	}

	var out1 []byte
	err = Unmarshal(encoded, &out1)
	if string(out1) != "test" {
		t.Errorf("unmarshal doesn't match input: %v\n%v\n%v\n", err, in, out1)
	}
}

func TestInt(t *testing.T) {
	in := map[string]int{"Val": 20}
	encoded := VerifyMarshal(t, in)
	expected := []byte("\x12\x00\x00\x00\x12Val\x00\x14\x00\x00\x00\x00\x00\x00\x00\x00")
	compare(t, encoded, expected)

	out := make(map[string]interface{})
	err := Unmarshal(encoded, &out)
	if out["Val"].(int64) != 20 {
		t.Errorf("unmarshal doesn't match input: %v\n%v\n%v\n", err, in, out)
	}

	var out1 int
	err = Unmarshal(encoded, &out1)
	if out1 != 20 {
		t.Errorf("unmarshal doesn't match input: %v\n%v\n%vn", err, in, out1)
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

func (ps *PrivateStruct) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := NewLenWriter(buf)

	EncodePrefix(buf, Long, "Type")
	EncodeUint64(buf, ps.veryPrivate)

	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func TestCustomMarshaler(t *testing.T) {
	s := &PrivateStruct{1}
	_, err := Marshal(s)
	if err != nil {
		t.Errorf("Marshal error 1: %s\n", err)
	}

	sl := &PrivateStructList{make([]PrivateStruct, 1)}
	sl.List[0] = *s
	_, err = Marshal(sl)
	if err != nil {
		t.Errorf("Marshal error 2: %s\n", err)
	}

	sm := &PrivateStructMap{make(map[string]*PrivateStruct)}
	sm.Map["first"] = s
	_, err = Marshal(sm)
	if err != nil {
		t.Errorf("Marshal error 3: %s\n", err)
	}
}

func VerifyMarshal(t *testing.T, Val interface{}) []byte {
	encoded, err := Marshal(Val)
	if err != nil {
		t.Errorf("marshal2 error: %s\n", err)
	}
	return encoded
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

func compare(t *testing.T, encoded []byte, expected []byte) {
	if len(encoded) != len(expected) {
		t.Errorf("encoding mismatch:\n%#v\n%#v\n", string(encoded), string(expected))
	} else {
		for i := range encoded {
			if encoded[i] != expected[i] {
				t.Errorf("encoding mismatch:\n%#v\n%#v\n", string(encoded), string(expected))
				break
			}
		}
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
	in["int32"] = int32(-0x80000000)
	in["int"] = int(-0x80000000)
	in["int64"] = int64(-0x8000000000000000)
	in["uint"] = uint(0xFFFFFFFF)
	in["uint32"] = uint32(0xFFFFFFFF)
	in["uint64"] = uint64(0xFFFFFFFFFFFFFFFF)
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
