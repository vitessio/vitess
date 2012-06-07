// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bson

import (
	"testing"
	"time"
)

func assertTrue(tf bool, msg string, t *testing.T) {
	if !tf {
		t.Error(msg)
	}
}

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
	if out["int32"].(int32) != int32(-0x80000000) {
		t.Errorf("int32 fail")
	}
	if out["int"].(int64) != int64(-0x80000000) {
		t.Errorf("int fail")
	}
	if out["int64"].(int64) != int64(-0x8000000000000000) {
		t.Errorf("int64 fail")
	}
	if out["uint"].(uint64) != uint64(0xFFFFFFFF) {
		t.Errorf("uint fail")
	}
	if out["uint32"].(uint64) != uint64(0xFFFFFFFF) {
		t.Errorf("uint32 fail")
	}
	if out["uint64"].(uint64) != uint64(0xFFFFFFFFFFFFFFFF) {
		t.Errorf("uint64 fail")
	}
	if out["slice"].([]interface{})[0].(int64) != 1 {
		t.Errorf("slice fail")
	}
	if out["slice"].([]interface{})[1] != nil {
		t.Errorf("slice fail")
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

func VerifyMarshal(t *testing.T, Val interface{}) []byte {
	encoded, err := Marshal(Val)
	if err != nil {
		t.Errorf("marshal2 error: %s\n", err)
	}
	return encoded
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
		_, err := Marshal(testBlob)
		if err != nil {
			panic(err)
		}
	}
}
