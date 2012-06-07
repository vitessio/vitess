// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package bytes2 gives you alternate implementations of functionality
// similar to go's bytes package

package bytes2

import (
	"testing"
)

func TestWrite(t *testing.T) {
	cw := NewChunkedWriter(5)
	cw.Write([]byte("1234"))
	if string(cw.Bytes()) != "1234" {
		t.Errorf("Expecting 1234, received %s", cw.Bytes())
	}
	cw.WriteString("56")
	if string(cw.Bytes()) != "123456" {
		t.Errorf("Expecting 123456, received %s", cw.Bytes())
	}
	if cw.Len() != 6 {
		t.Errorf("Expecting 6, received %d", cw.Len())
	}
}

func TestTruncate(t *testing.T) {
	cw := NewChunkedWriter(3)
	cw.WriteString("123456789")
	cw.Truncate(8)
	if string(cw.Bytes()) != "12345678" {
		t.Errorf("Expecting 12345678, received %s", cw.Bytes())
	}
	cw.Truncate(5)
	if string(cw.Bytes()) != "12345" {
		t.Errorf("Expecting 12345, received %s", cw.Bytes())
	}
	cw.Truncate(2)
	if string(cw.Bytes()) != "12" {
		t.Errorf("Expecting 12345, received %s", cw.Bytes())
	}
	cw.Reset()
	if cw.Len() != 0 {
		t.Errorf("Expecting 0, received %d", cw.Len())
	}
}

func TestReserve(t *testing.T) {
	cw := NewChunkedWriter(4)
	b := cw.Reserve(2)
	b[0] = '1'
	b[1] = '2'
	cw.WriteByte('3')
	b = cw.Reserve(2)
	b[0] = '4'
	b[1] = '5'
	if string(cw.Bytes()) != "12345" {
		t.Errorf("Expecting 12345, received %s", cw.Bytes())
	}
}

func TestWriteTo(t *testing.T) {
	cw1 := NewChunkedWriter(4)
	cw1.WriteString("123456789")
	cw2 := NewChunkedWriter(5)
	cw1.WriteTo(cw2)
	if string(cw2.Bytes()) != "123456789" {
		t.Errorf("Expecting 123456789, received %s", cw2.Bytes())
	}
}
