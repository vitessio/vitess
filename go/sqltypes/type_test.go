// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqltypes

import (
	"bytes"
	"testing"
	"time"
)

func TestNull(t *testing.T) {
	n := Value{}
	if !n.IsNull() {
		t.Errorf("value is not null")
	}
	if n.String() != "" {
		t.Errorf("Expecting '', got %s", n.String())
	}
	b := bytes.NewBuffer(nil)
	n.EncodeSql(b)
	if b.String() != "null" {
		t.Errorf("Expecting null, got %s", b.String())
	}
	n.EncodeAscii(b)
	if b.String() != "nullnull" {
		t.Errorf("Expecting nullnull, got %s", b.String())
	}
	js, err := n.MarshalJSON()
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	if string(js) != "null" {
		t.Errorf("Expecting null, received %s", js)
	}
}

func TestNumeric(t *testing.T) {
	n := Value{Numeric([]byte("1234"))}
	b := bytes.NewBuffer(nil)
	n.EncodeSql(b)
	if b.String() != "1234" {
		t.Errorf("Expecting 1234, got %s", b.String())
	}
	n.EncodeAscii(b)
	if b.String() != "12341234" {
		t.Errorf("Expecting 12341234, got %s", b.String())
	}
	js, err := n.MarshalJSON()
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	if string(js) != "1234" {
		t.Errorf("Expecting 1234, received %s", js)
	}
}

const (
	INVALIDNEG = "-9223372036854775809"
	MINNEG     = "-9223372036854775808"
	MAXPOS     = "18446744073709551615"
	INVALIDPOS = "18446744073709551616"
	NEGFLOAT   = "1.234"
	POSFLOAT   = "-1.234"
)

func TestBuildNumeric(t *testing.T) {
	var n Value
	var err error
	n, err = BuildNumeric(MINNEG)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	if n.String() != MINNEG {
		t.Errorf("Expecting %v, received %s", MINNEG, n.Raw())
	}
	n, err = BuildNumeric(MAXPOS)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	if n.String() != MAXPOS {
		t.Errorf("Expecting %v, received %s", MAXPOS, n.Raw())
	}
	n, err = BuildNumeric("0xA")
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	if n.String() != "10" {
		t.Errorf("Expecting %v, received %s", 10, n.Raw())
	}
	n, err = BuildNumeric("012")
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	if string(n.Raw()) != "10" {
		t.Errorf("Expecting %v, received %s", 10, n.Raw())
	}
	if n, err = BuildNumeric(INVALIDNEG); err == nil {
		t.Errorf("Expecting error")
	}
	if n, err = BuildNumeric(INVALIDPOS); err == nil {
		t.Errorf("Expecting error")
	}
	if n, err = BuildNumeric(NEGFLOAT); err == nil {
		t.Errorf("Expecting error")
	}
	if n, err = BuildNumeric(POSFLOAT); err == nil {
		t.Errorf("Expecting error")
	}
}

const (
	HARDSQL     = "\x00'\"\b\n\r\t\x1A\\"
	HARDESCAPED = "'\\0\\'\\\"\\b\\n\\r\\t\\Z\\\\'"
	HARDASCII   = "'ACciCAoNCRpc'"
)

func TestString(t *testing.T) {
	s := Value{String([]byte(HARDSQL))}
	b := bytes.NewBuffer(nil)
	s.EncodeSql(b)
	if b.String() != HARDESCAPED {
		t.Errorf("Expecting %s, received %s", HARDESCAPED, b.String())
	}
	b = bytes.NewBuffer(nil)
	s.EncodeAscii(b)
	if b.String() != HARDASCII {
		t.Errorf("Expecting %s, received %#v", HARDASCII, b.String())
	}
	s = Value{String([]byte("abcd"))}
	js, err := s.MarshalJSON()
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	if string(js) != "\"YWJjZA==\"" {
		t.Errorf("Expecting \"YWJjZA==\", received %s", js)
	}
}

func TestBuildValue(t *testing.T) {
	v, err := BuildValue(nil)
	if err != nil {
		t.Errorf("%v", err)
	}
	if !v.IsNull() {
		t.Errorf("Expecting null")
	}
	n64, err := v.ParseUint64()
	if err == nil || err.Error() != "value is null" {
		t.Errorf("%v", err)
	}
	v, err = BuildValue(int(-1))
	if err != nil {
		t.Errorf("%v", err)
	}
	if !v.IsNumeric() || v.String() != "-1" {
		t.Errorf("Expecting -1, received %T: %s", v.Inner, v.String())
	}
	v, err = BuildValue(int32(-1))
	if err != nil {
		t.Errorf("%v", err)
	}
	if !v.IsNumeric() || v.String() != "-1" {
		t.Errorf("Expecting -1, received %T: %s", v.Inner, v.String())
	}
	v, err = BuildValue(int64(-1))
	if err != nil {
		t.Errorf("%v", err)
	}
	if !v.IsNumeric() || v.String() != "-1" {
		t.Errorf("Expecting -1, received %T: %s", v.Inner, v.String())
	}
	n64, err = v.ParseUint64()
	if err == nil {
		t.Errorf("-1 shouldn't convert into uint64")
	}
	v, err = BuildValue(uint(1))
	if err != nil {
		t.Errorf("%v", err)
	}
	if !v.IsNumeric() || v.String() != "1" {
		t.Errorf("Expecting 1, received %T: %s", v.Inner, v.String())
	}
	v, err = BuildValue(uint32(1))
	if err != nil {
		t.Errorf("%v", err)
	}
	if !v.IsNumeric() || v.String() != "1" {
		t.Errorf("Expecting 1, received %T: %s", v.Inner, v.String())
	}
	v, err = BuildValue(uint64(1))
	if err != nil {
		t.Errorf("%v", err)
	}
	n64, err = v.ParseUint64()
	if err != nil {
		t.Errorf("%v", err)
	}
	if n64 != 1 {
		t.Errorf("Expecting 1, got %v", n64)
	}
	if !v.IsNumeric() || v.String() != "1" {
		t.Errorf("Expecting 1, received %T: %s", v.Inner, v.String())
	}
	v, err = BuildValue(1.23)
	if err != nil {
		t.Errorf("%v", err)
	}
	if !v.IsFractional() || v.String() != "1.23" {
		t.Errorf("Expecting 1.23, received %T: %s", v.Inner, v.String())
	}
	n64, err = v.ParseUint64()
	if err == nil {
		t.Errorf("1.23 shouldn't convert into uint64")
	}
	v, err = BuildValue("abcd")
	if err != nil {
		t.Errorf("%v", err)
	}
	if !v.IsString() || v.String() != "abcd" {
		t.Errorf("Expecting abcd, received %T: %s", v.Inner, v.String())
	}
	v, err = BuildValue([]byte("abcd"))
	if err != nil {
		t.Errorf("%v", err)
	}
	if !v.IsString() || v.String() != "abcd" {
		t.Errorf("Expecting abcd, received %T: %s", v.Inner, v.String())
	}
	n64, err = v.ParseUint64()
	if err == nil || err.Error() != "value is not Numeric" {
		t.Errorf("%v", err)
	}
	v, err = BuildValue(time.Date(2012, time.February, 24, 23, 19, 43, 10, time.UTC))
	if err != nil {
		t.Errorf("%v", err)
	}
	if !v.IsString() || v.String() != "'2012-02-24 23:19:43'" {
		t.Errorf("Expecting '2012-02-24 23:19:43', received %T: %s", v.Inner, v.String())
	}
	v, err = BuildValue(Numeric([]byte("123")))
	if err != nil {
		t.Errorf("%v", err)
	}
	if !v.IsNumeric() || v.String() != "123" {
		t.Errorf("Expecting 123, received %T: %s", v.Inner, v.String())
	}
	v, err = BuildValue(Fractional([]byte("12.3")))
	if err != nil {
		t.Errorf("%v", err)
	}
	if !v.IsFractional() || v.String() != "12.3" {
		t.Errorf("Expecting 12.3, received %T: %s", v.Inner, v.String())
	}
	v, err = BuildValue(String([]byte("abc")))
	if err != nil {
		t.Errorf("%v", err)
	}
	if !v.IsString() || v.String() != "abc" {
		t.Errorf("Expecting abc, received %T: %s", v.Inner, v.String())
	}
	v, err = BuildValue(float32(1.23))
	if err == nil {
		t.Errorf("Did not receive error")
	}
	v1 := Value{String([]byte("ab"))}
	v, err = BuildValue(v1)
	if err != nil {
		t.Errorf("%v", err)
	}
	if !v.IsString() || v.String() != "ab" {
		t.Errorf("Expecting ab, received %T: %s", v.Inner, v.String())
	}
	v, err = BuildValue(float32(1.23))
	if err == nil {
		t.Errorf("Did not receive error")
	}
}

// Ensure DONTESCAPE is not escaped
func TestEncode(t *testing.T) {
	if SqlEncodeMap[DONTESCAPE] != DONTESCAPE {
		t.Errorf("Encode fail: %v", SqlEncodeMap[DONTESCAPE])
	}
	if SqlDecodeMap[DONTESCAPE] != DONTESCAPE {
		t.Errorf("Decode fail: %v", SqlDecodeMap[DONTESCAPE])
	}
}
