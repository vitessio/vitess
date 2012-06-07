// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
