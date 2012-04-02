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

package bson

import (
	"bytes"
	"strconv"
)

func DecodeString(buf *bytes.Buffer, kind byte) string {
	switch kind {
	case String:
		l := int(Pack.Uint32(buf.Next(4)))
		s := buf.Next(l - 1)
		NextByte(buf)
		return string(s)
	case Binary:
		l := int(Pack.Uint32(buf.Next(4)))
		NextByte(buf)
		return string(buf.Next(l))
	case Null:
		return ""
	}
	panic(NewBsonError("Unexpected data type %v for string", kind))
}

func DecodeInt(buf *bytes.Buffer, kind byte) int {
	switch kind {
	case Int:
		return int(Pack.Uint32(buf.Next(4)))
	case Long, Ulong:
		return int(Pack.Uint64(buf.Next(8)))
	case Null:
		return 0
	}
	panic(NewBsonError("Unexpected data type %v for int", kind))
}

func DecodeInt64(buf *bytes.Buffer, kind byte) int64 {
	switch kind {
	case Int:
		return int64(int32(Pack.Uint32(buf.Next(4))))
	case Long, Ulong:
		return int64(Pack.Uint64(buf.Next(8)))
	case Null:
		return 0
	}
	panic(NewBsonError("Unexpected data type %v for int", kind))
}

func DecodeUint64(buf *bytes.Buffer, kind byte) uint64 {
	switch kind {
	case Int:
		return uint64(Pack.Uint32(buf.Next(4)))
	case Long, Ulong:
		return Pack.Uint64(buf.Next(8))
	case Null:
		return 0
	}
	panic(NewBsonError("Unexpected data type %v for int", kind))
}

func ExpectIndex(buf *bytes.Buffer, index int) {
	key := ReadCString(buf)
	received, err := strconv.Atoi(key)
	if err != nil {
		panic(NewBsonError("%s", err))
	}
	if received != index {
		panic(NewBsonError("non-sequential index in array. Expected: %d, Received: %d", index, received))
	}
}

func ReadCString(buf *bytes.Buffer) string {
	b, err := buf.ReadBytes(0)
	if err != nil {
		panic(NewBsonError("%s", err))
	}
	return string(b[:len(b)-1])
}

func Next(buf *bytes.Buffer, n int) []byte {
	b := buf.Next(n)
	if len(b) != n {
		panic(NewBsonError("EOF"))
	}
	return b
}

func NextByte(buf *bytes.Buffer) byte {
	return Next(buf, 1)[0]
}
