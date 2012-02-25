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

package hack

import (
	"testing"
)

func TestStringArena(t *testing.T) {
	sarena := NewStringArena(10)
	buf1 := []byte("01234")
	buf2 := []byte("5678")
	buf3 := []byte("ab")
	buf4 := []byte("9")

	s1 := sarena.NewString(buf1)
	checkint(t, len(sarena.buf), 5)
	checkint(t, sarena.SpaceLeft(), 5)
	checkstring(t, s1, "01234")

	s2 := sarena.NewString(buf2)
	checkint(t, len(sarena.buf), 9)
	checkint(t, sarena.SpaceLeft(), 1)
	checkstring(t, s2, "5678")

	// s3 will be allocated outside of sarena
	s3 := sarena.NewString(buf3)
	checkint(t, len(sarena.buf), 9)
	checkint(t, sarena.SpaceLeft(), 1)
	checkstring(t, s3, "ab")

	// s4 should still fit in sarena
	s4 := sarena.NewString(buf4)
	checkint(t, len(sarena.buf), 10)
	checkint(t, sarena.SpaceLeft(), 0)
	checkstring(t, s4, "9")

	sarena.buf[0] = 'A'
	checkstring(t, s1, "A1234")

	sarena.buf[5] = 'B'
	checkstring(t, s2, "B678")

	sarena.buf[9] = 'C'
	// s3 will not change
	checkstring(t, s3, "ab")
	checkstring(t, s4, "C")
	checkstring(t, sarena.str, "A1234B678C")
}

func checkstring(t *testing.T, actual, expected string) {
	if actual != expected {
		t.Errorf("received %s, expecting %s", actual, expected)
	}
}

func checkint(t *testing.T, actual, expected int) {
	if actual != expected {
		t.Errorf("received %d, expecting %d", actual, expected)
	}
}
