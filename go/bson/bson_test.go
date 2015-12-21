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
	VerifyObject(kind)
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
			verifyKind("Uint64", Long, kind)
			a.Uint64 = DecodeUint64(buf, kind)
		case "Uint32":
			verifyKind("Uint32", Long, kind)
			a.Uint32 = DecodeUint32(buf, kind)
		case "Uint":
			verifyKind("Uint", Long, kind)
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
		panic(NewBsonError("Decode %s, kind is %v, want %v", tag, got, want))
	}
}

// TODO(sougou): Revisit usefulness of this test
func TestUnmarshalUtil(t *testing.T) {
	a := alltypes{
		Bytes:   []byte("bytes"),
		Float64: float64(64),
		String:  "string",
		Bool:    true,
		Time:    time.Unix(1136243045, 0).UTC(),
		Int64:   int64(-0x8000000000000000),
		Int32:   int32(-0x80000000),
		Int:     int(-0x80000000),
		Uint64:  uint64(0xFFFFFFFFFFFFFFFF),
		Uint32:  uint32(0xFFFFFFFF),
		Uint:    uint(0xFFFFFFFF),
		Strings: []string{"a", "b"},
		Nil:     nil,
	}
	got := verifyMarshal(t, a)
	var out alltypes
	verifyUnmarshal(t, got, &out)
	// Verify easter egg
	if out.String != "string1" {
		t.Errorf("got %s, want %s", out.String, "string1")
	}
	out.String = "string"
	if !reflect.DeepEqual(a, out) {
		t.Errorf("got\n%+v, want\n%+v", out, a)
	}

	b := alltypes{Bytes: []byte(""), Strings: []string{"a"}}
	got = verifyMarshal(t, b)
	var outb alltypes
	verifyUnmarshal(t, got, &outb)
	if outb.Bytes == nil || len(outb.Bytes) != 0 {
		t.Errorf("got %q, want nil", string(outb.Bytes))
	}
}

func TestTypes(t *testing.T) {
	in := map[string]interface{}{
		"bytes":   []byte("bytes"),
		"float64": float64(64),
		"string":  "string",
		"bool":    true,
		"time":    time.Unix(1136243045, 0).UTC(),
		"int64":   int64(-0x8000000000000000),
		"int32":   int32(-0x80000000),
		"int":     int(-0x80000000),
		"uint64":  uint64(0xFFFFFFFFFFFFFFFF),
		"uint32":  uint32(0xFFFFFFFF),
		"uint":    uint(0xFFFFFFFF),
		"slice":   []interface{}{1, nil},
		"nil":     nil,
	}
	marshalled := verifyMarshal(t, in)
	got := make(map[string]interface{})
	verifyUnmarshal(t, marshalled, &got)

	want := map[string]interface{}{
		"bytes":   []byte("bytes"),
		"float64": float64(64),
		"string":  []byte("string"),
		"bool":    true,
		"time":    time.Unix(1136243045, 0).UTC(),
		"int64":   int64(-0x8000000000000000),
		"int32":   int32(-0x80000000),
		"int":     int64(-0x80000000),
		"uint64":  int64(-1),
		"uint32":  int64(0xFFFFFFFF),
		"uint":    int64(0xFFFFFFFF),
		"slice":   []interface{}{int64(1), nil},
		"nil":     nil,
	}
	// We do the range so the errors are more precise.
	for k, v := range got {
		if !reflect.DeepEqual(v, want[k]) {
			t.Errorf("got \n%+v, want \n%+v", v, want[k])
		}
	}
}

// test that we are calling the right encoding method
// if we use the reflection code, this will fail as reflection
// cannot access the non-exported field
type PrivateStruct struct {
	veryPrivate uint64
}

func (ps *PrivateStruct) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	EncodeOptionalPrefix(buf, Object, key)
	lenWriter := NewLenWriter(buf)

	EncodeUint64(buf, "Type", ps.veryPrivate)

	lenWriter.Close()
}

func (ps *PrivateStruct) UnmarshalBson(buf *bytes.Buffer, kind byte) {
	VerifyObject(kind)
	Next(buf, 4)

	for kind := NextByte(buf); kind != EOO; kind = NextByte(buf) {
		key := ReadCString(buf)
		switch key {
		case "Type":
			verifyKind("Type", Long, kind)
			ps.veryPrivate = DecodeUint64(buf, kind)
		default:
			Skip(buf, kind)
		}
	}
}

// an array can use non-pointers for custom marshaler
type PrivateStructList struct {
	List []PrivateStruct
}

// the map has to be using pointers, so the custom marshaler is used
type PrivateStructMap struct {
	Map map[string]*PrivateStruct
}

type PrivateStructStruct struct {
	Inner *PrivateStruct
}

func TestCustomStruct(t *testing.T) {
	// This should use the custom marshaler & unmarshaler
	s := PrivateStruct{1}
	got := verifyMarshal(t, &s)
	want := "\x13\x00\x00\x00\x12Type\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00"
	if string(got) != want {
		t.Errorf("got %q, want %q", string(got), want)
	}
	var s2 PrivateStruct
	verifyUnmarshal(t, got, &s2)
	if s2 != s {
		t.Errorf("got \n%+v, want \n%+v", s2, s)
	}

	// This should use the custom marshaler & unmarshaler
	sl := PrivateStructList{make([]PrivateStruct, 1)}
	sl.List[0] = s
	got = verifyMarshal(t, &sl)
	want = "&\x00\x00\x00\x04List\x00\x1b\x00\x00\x00\x030\x00\x13\x00\x00\x00\x12Type\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
	if string(got) != want {
		t.Errorf("got %q, want %q", string(got), want)
	}
	var sl2 PrivateStructList
	verifyUnmarshal(t, got, &sl2)
	if !reflect.DeepEqual(sl2, sl) {
		t.Errorf("got \n%+v, want \n%+v", sl2, sl)
	}

	// This should use the custom marshaler & unmarshaler
	smp := make(map[string]*PrivateStruct)
	smp["first"] = &s
	got = verifyMarshal(t, smp)
	want = "\x1f\x00\x00\x00\x03first\x00\x13\x00\x00\x00\x12Type\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00"
	if string(got) != want {
		t.Errorf("got %q, want %q", string(got), want)
	}
	smp2 := make(map[string]*PrivateStruct)
	verifyUnmarshal(t, got, &smp2)
	if !reflect.DeepEqual(smp2, smp) {
		t.Errorf("got \n%+v, want \n%+v", smp2, smp)
	}

	// This should not use the custom unmarshaler
	sm := make(map[string]PrivateStruct)
	sm["first"] = s
	sm2 := make(map[string]PrivateStruct)
	verifyUnmarshal(t, got, &sm2)
	if reflect.DeepEqual(sm2, sm) {
		t.Errorf("got \n%+v, want \n%+v", sm2, sm)
	}

	// This should not use the custom marshaler
	got = verifyMarshal(t, sm)
	want = "\x11\x00\x00\x00\x03first\x00\x05\x00\x00\x00\x00\x00"
	if string(got) != want {
		t.Errorf("got %q, want %q", string(got), want)
	}

	// This should not use the custom marshaler (or crash)
	nilinner := PrivateStructStruct{}
	got = verifyMarshal(t, &nilinner)
	want = "\f\x00\x00\x00\nInner\x00\x00"
	if string(got) != want {
		t.Errorf("got %q, want %q", string(got), want)
	}
}

type HasPrivate struct {
	private string
	Public  string
}

func TestIgnorePrivateFields(t *testing.T) {
	v := HasPrivate{private: "private", Public: "public"}
	marshaled := verifyMarshal(t, v)
	unmarshaled := new(HasPrivate)
	Unmarshal(marshaled, unmarshaled)
	if unmarshaled.Public != "Public" && unmarshaled.private != "" {
		t.Errorf("private fields were not ignored: %+v", unmarshaled)
	}
}

type LotsMoreFields struct {
	CommonField1 string
	ExtraField1  float64
	ExtraField2  string
	ExtraField3  HasPrivate
	ExtraField4  []string
	CommonField2 string
	ExtraField5  []byte
	ExtraField6  bool
	ExtraField7  time.Time
	ExtraField8  *int
	ExtraField9  int32
	ExtraField10 int64
	ExtraField11 uint64
}

type LotsFewerFields struct {
	CommonField1 string
	CommonField2 string
}

func TestSkipUnknownFields(t *testing.T) {
	v := LotsMoreFields{
		CommonField1: "value1",
		ExtraField1:  1.0,
		ExtraField2:  "abcd",
		ExtraField3:  HasPrivate{private: "private", Public: "public"},
		ExtraField4:  []string{"s1", "s2"},
		CommonField2: "value3",
		ExtraField5:  []byte("abcd"),
		ExtraField6:  true,
		ExtraField7:  time.Now(),
	}
	marshaled := verifyMarshal(t, v)
	unmarshaled := LotsFewerFields{}
	verifyUnmarshal(t, marshaled, &unmarshaled)
	want := LotsFewerFields{
		CommonField1: "value1",
		CommonField2: "value3",
	}
	if unmarshaled != want {
		t.Errorf("got \n%+v, want \n%+v", unmarshaled, want)
	}
}

func TestEncodeFieldNil(t *testing.T) {
	buf := bytes2.NewChunkedWriter(DefaultBufferSize)
	EncodeField(buf, "Val", nil)
	got := string(buf.Bytes())
	want := "\nVal\x00"
	if got != want {
		t.Errorf("nil encode: got %q, want %q", got, want)
	}
}

func TestStream(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	err := MarshalToStream(buf, 1)
	if err != nil {
		t.Error(err)
		return
	}
	want := "\x14\x00\x00\x00\x12_Val_\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00"
	got := buf.String()
	if got != want {
		t.Errorf("got \n%q, want %q", got, want)
	}
	readbuf := bytes.NewBuffer(buf.Bytes())
	var out int64
	err = UnmarshalFromStream(readbuf, &out)
	if err != nil {
		t.Error(err)
		return
	}
	if out != 1 {
		t.Errorf("got %d, want 1", out)
	}
	err = MarshalToStream(buf, make(chan int))
	want = "unexpected type chan int"
	got = err.Error()
	if got != want {
		t.Errorf("got \n%q, want %q", got, want)
	}
}

var testMap map[string]interface{}
var testBlob []byte

func init() {
	testMap = map[string]interface{}{
		"bytes":   []byte("bytes"),
		"float64": float64(64),
		"string":  "string",
		"bool":    true,
		"time":    time.Unix(1136243045, 0),
		"int64":   int64(-0x8000000000000000),
		"int32":   int32(-0x80000000),
		"int":     int(-0x80000000),
		"uint64":  uint64(0xFFFFFFFFFFFFFFFF),
		"uint32":  uint32(0xFFFFFFFF),
		"uint":    uint(0xFFFFFFFF),
		"slice":   []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, nil},
		"nil":     nil,
	}
	testBlob, _ = Marshal(testMap)
}

func BenchmarkMarshal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, err := Marshal(testMap)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUnmarshal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		v := make(map[string]interface{})
		err := Unmarshal(testBlob, &v)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeField(b *testing.B) {
	values := []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for i := 0; i < b.N; i++ {
		buf := bytes2.NewChunkedWriter(2048)
		EncodeField(buf, "Val", values)
		buf.Reset()
	}
}

func BenchmarkEncodeInterface(b *testing.B) {
	values := []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for i := 0; i < b.N; i++ {
		buf := bytes2.NewChunkedWriter(2048)
		EncodeInterface(buf, "Val", values)
		buf.Reset()
	}
}

func verifyMarshal(t *testing.T, val interface{}) []byte {
	got, err := Marshal(val)
	if err != nil {
		t.Errorf("Marshal error for %+v: %v\n", val, err)
	}
	return got
}

func verifyUnmarshal(t *testing.T, buf []byte, val interface{}) {
	if err := Unmarshal(buf, val); err != nil {
		t.Errorf("Unmarshal error for %+v: %v\n", val, err)
	}
}
