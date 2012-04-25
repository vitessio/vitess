/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

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
